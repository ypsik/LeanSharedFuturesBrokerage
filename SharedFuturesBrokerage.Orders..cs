using Accord.Math.Distances;
using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;
using QuantConnect.Statistics;
using QuantConnect.Util;
using SilverQuant.Lean.Brokerages.Futures.Shared.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using System.Threading.Tasks;

using CxCancelOrderRequest = CryptoExchange.Net.SharedApis.CancelOrderRequest;

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    public abstract partial class SharedFuturesBrokerage
    {
        public virtual decimal MinimumOrderNotionalValue => 0m;

        /// <summary>
        /// Gibt an, ob die Börse Orders in-place ändert (z.B. Bybit) oder Cancel+Replace nutzt (z.B. Hyperliquid).
        /// </summary>
        public virtual bool ExchangeModifiesOrdersInPlace => false;

        #region State Machine Models

        public enum OrderLifeCycleState
        {
            Placing,    // Order ist lokal registriert, BrokerId noch ausstehend (REST-Call läuft)
            Submitted,
            Open,
            PartiallyFilled,
            Filled,
            Canceled,
            Replaced,
            Invalid
        }

        public sealed class OrderState
        {
            public Order Order;
            public decimal OriginalQuantity;
            public decimal FilledQuantity;
            public string BrokerId;
            public string ClientOrderId;
            public OrderLifeCycleState State;
            public DateTime LastUpdateUtc;
            public bool IsUpdatePending;
            public decimal CumulativeFeePaid;

            public decimal Remaining => OriginalQuantity - FilledQuantity;

            public bool IsClosed => State is OrderLifeCycleState.Filled
                                          or OrderLifeCycleState.Canceled
                                          or OrderLifeCycleState.Invalid
                                          or OrderLifeCycleState.Replaced;
        }

        #endregion

        // --- SINGLE SOURCE OF TRUTH ---
        // Primary key: clientOrderId (permanent, never changes).
        // Exchange ID is indexed separately via _orderStateManager for O(1) socket lookups.
        protected readonly OrderStateManager _orderStateManager = new();


        #region Order Management

        protected virtual ExchangeParameters OpenOrdersExchangeParameters => new ExchangeParameters();

        public override List<Order> GetOpenOrders()
        {
            var res = RunSync(() => _orderClient.GetOpenFuturesOrdersAsync(
                new GetOpenOrdersRequest(exchangeParameters: OpenOrdersExchangeParameters)));

            if (!res.Success || res.Data == null) return new List<Order>();

            return res.Data.Select(o =>
            {
                var symbol = Symbol.Create(NormalizeSymbol(o.Symbol), SecurityType.CryptoFuture, Name);
                var sign = o.Side == SharedOrderSide.Sell ? -1m : 1m;
                var qty = (o.OrderQuantity?.QuantityInBaseAsset ?? 0m) * sign;
                var filledQty = (o.QuantityFilled?.QuantityInBaseAsset ?? 0m) * sign;
                var price = o.OrderPrice ?? 0m;

                Order order;

                // FIX: Explizite Trennung der Order-Typen um Portfolio-Korruption beim Startup zu verhindern.
                if (o.OrderType == SharedOrderType.Limit)
                {
                    order = new LimitOrder(symbol, qty, price, DateTime.UtcNow);
                }
                else if (o.OrderType == SharedOrderType.Market)
                {
                    order = new MarketOrder(symbol, qty, DateTime.UtcNow);
                }
                else
                {
                    // Fallback für StopMarket, StopLimit, TrailingStop, etc.
                    // Durch das Mappen auf StopMarketOrder weiß LEAN, dass diese Order
                    // bedingungsgeknüpft ist und führt sie nicht sofort als Market-Order aus.
                    order = new StopMarketOrder(symbol, qty, price, DateTime.UtcNow);
                }

                order.BrokerId.Add(o.OrderId);
                order.Status = MapStatus(o.Status, Math.Abs(filledQty));

                var state = new OrderState
                {
                    Order = order,
                    OriginalQuantity = qty,
                    FilledQuantity = filledQty,
                    BrokerId = o.OrderId,
                    ClientOrderId = o.ClientOrderId ?? string.Empty,
                    State = order.Status == OrderStatus.PartiallyFilled ? OrderLifeCycleState.PartiallyFilled : OrderLifeCycleState.Open,
                    LastUpdateUtc = DateTime.UtcNow
                };

                // TryAdd: nop if clientId already registered (idempotent on reconnect).
                // BrokerId is already set → manager auto-indexes in _statesByExchangeId.
                if (!string.IsNullOrEmpty(state.ClientOrderId))
                    _orderStateManager.TryAdd(state.ClientOrderId, state);

                return order;
            }).ToList();
        }

        public override bool PlaceOrder(Order order)
        {
            decimal executionQuantity = order.Quantity;

            if (MinimumOrderNotionalValue > 0m)
            {
                decimal price = 0m;
                if (order is LimitOrder limitOrder)
                    price = limitOrder.LimitPrice;
                else if (order is StopMarketOrder stopMarketOrder)
                    price = stopMarketOrder.StopPrice;
                else
                    price = _algorithm.Securities[order.Symbol].Price;

                if (price > 0m)
                {
                    decimal currentNotional = Math.Abs(executionQuantity * price);

                    if (currentNotional < MinimumOrderNotionalValue && executionQuantity != 0m)
                    {
                        var props = _spdb.GetSymbolProperties(order.Symbol.ID.Market, order.Symbol, order.Symbol.SecurityType, SettleAsset);
                        decimal baseLotSize = props?.LotSize ?? 0.01m;
                        decimal minUnitsRequired = MinimumOrderNotionalValue / price;
                        decimal adjustedQuantity = Math.Ceiling(minUnitsRequired / baseLotSize) * baseLotSize;

                        if (executionQuantity < 0)
                            adjustedQuantity = -adjustedQuantity;

                        Log.Trace($"{Name}.PlaceOrder: Adjusting execution quantity for {order.Symbol.Value} from {executionQuantity} to {adjustedQuantity} to meet the minimum of ${MinimumOrderNotionalValue}.");
                        executionQuantity = adjustedQuantity;
                    }
                }
            }

            var clientOrderId = GenerateClientId(order.Id);

            var request = new PlaceFuturesOrderRequest(
                GetSharedSymbol(order.Symbol),
                executionQuantity > 0 ? SharedOrderSide.Buy : SharedOrderSide.Sell,
                order.Type == OrderType.Limit ? SharedOrderType.Limit : SharedOrderType.Market,
                new SharedQuantity { QuantityInBaseAsset = Math.Abs(executionQuantity) })
            {
                Price = (order as LimitOrder)?.LimitPrice,
                ClientOrderId = clientOrderId,
                ExchangeParameters = PlaceFuturesOrderExchangeParameters
            };

            // State Machine: Order mit Placing-State registrieren bevor API-Call rausgeht.
            // Socket-Handler kann die Order damit sofort finden falls HL instantan füllt
            // und das WS-Event noch während RunSync() ankommt.
            // BrokerId = clientOrderId (temp) → manager indexes it in _statesByExchangeId as well.
            // No self-reference sentinel needed anymore.
            var placingState = new OrderState
            {
                Order = order,
                OriginalQuantity = executionQuantity,
                FilledQuantity = 0m,
                ClientOrderId = clientOrderId,
                State = OrderLifeCycleState.Placing,
                LastUpdateUtc = DateTime.UtcNow
            };
            _orderStateManager.TryAdd(clientOrderId, placingState);

            var res = RunSync(() => ExecutePlaceOrderAsync(request));
            if (!res.Success)
            {
                // Placing-State wieder austragen
                _orderStateManager.TryRemove(clientOrderId, out _);

                var errorMsg = res.Error?.ToString() ?? "Unknown exchange error";
                Log.Error($"{Name}.PlaceOrder({order.Symbol.Value}): {errorMsg}");
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "PlaceOrder", errorMsg));
                return false;
            }

            order.BrokerId.Add(res.Data.Id);

            // Prüfen ob der State noch im Placing-Zustand ist.
            // Falls nicht, hat 'HandleOrderSocket' bereits übernommen, den Exchange-ID-Swap
            // per MapNewExchangeId durchgeführt und das Submitted-Event gefeuert.
            if (_orderStateManager.TryGetValue(clientOrderId, out var currentState) &&
                currentState.State == OrderLifeCycleState.Placing)
            {
                // 1. State-Properties aktualisieren
                placingState.State = OrderLifeCycleState.Submitted;
                placingState.LastUpdateUtc = DateTime.UtcNow;

                // 2. Exchange-ID im Manager atomar eintragen:
                //    - entfernt temp BrokerId aus _statesByExchangeId
                //    - setzt state.BrokerId = res.Data.Id
                //    - trägt unter res.Data.Id in _statesByExchangeId ein
                //    - ergänzt Order.BrokerId
                //    - _statesByClientId[clientOrderId] bleibt unverändert
                _orderStateManager.MapNewExchangeId(clientOrderId, res.Data.Id);

                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero) { Status = OrderStatus.Submitted });
            }
            // else: Socket hat Placing-State bereits umgebogen + Events gefeuert

            return true;
        }

        public override bool CancelOrder(Order order)
        {
            if (!order.BrokerId.Any()) return false;
            var id = order.BrokerId.Last();

            var res = RunSync(() => ExecuteCancelOrderAsync(new CxCancelOrderRequest(GetSharedSymbol(order.Symbol), id, CancelFuturesOrderExchangeParameters)));
            if (!res.Success)
            {
                var errorMsg = res.Error?.ToString() ?? "Unknown exchange error";
                Log.Error($"{Name}.CancelOrder({order.Symbol.Value}): {errorMsg}");
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "CancelOrder", errorMsg));
                return false;
            }

            // FIX Bug 1: State zuerst aus dem Manager entfernen, dann Event feuern.
            // Damit schlägt der Socket-Handler fehl (TryGetByExchangeId miss) → kein doppeltes Event.
            // Lookup via Exchange-ID → ClientOrderId → TryRemove (bereinigt beide internen Dicts).
            if (_orderStateManager.TryGetByExchangeId(id, out var state))
            {
                _orderStateManager.TryRemove(state.ClientOrderId, out _);
                state.State = OrderLifeCycleState.Canceled;
                state.LastUpdateUtc = DateTime.UtcNow;

                OnOrderEvent(new OrderEvent(state.Order, DateTime.UtcNow, OrderFee.Zero)
                {
                    Status = OrderStatus.Canceled,
                    Message = "Cancel confirmed"
                });
            }

            return true;
        }

        public override bool UpdateOrder(Order order)
        {
            if (!order.BrokerId.Any() || ExecuteUpdateOrderAsync == null)
                return false;

            var ticket = _orderManager.GetOrderTicket(order.Id);
            var lastUpdate = ticket?.UpdateRequests.LastOrDefault();

            decimal price = lastUpdate?.LimitPrice ?? order.Price;
            decimal? quantity = order.Quantity;

            var clientOrderId = GenerateClientId(order.Id);

            if (_orderStateManager.TryGetValue(clientOrderId, out var state))
            {
                if (ExchangeModifiesOrdersInPlace)
                {
                    quantity = null;
                }
                else
                {
                    // 🔥 HYPERLIQUID-FIX: Cancel+Replace-Börsen verlangen die nackte Restmenge!
                    if (lastUpdate?.Quantity.HasValue == true)
                    {
                        var newTotal = lastUpdate.Quantity.Value;
                        var sign = newTotal > 0 ? 1m : -1m;
                        quantity = (Math.Abs(newTotal) - Math.Abs(state.FilledQuantity)) * sign;
                    }
                    else
                    {
                        quantity = state.Remaining;
                    }
                }

                // FIX: LEAN BrokerId-Historie isolieren
                if (!string.IsNullOrEmpty(state.BrokerId))
                {
                    order.BrokerId.Clear();
                    order.BrokerId.Add(state.BrokerId);
                }

                state.IsUpdatePending = true;
            }

            // Minimum notional check
            if (MinimumOrderNotionalValue > 0m && price > 0m && quantity.HasValue)
            {
                decimal currentNotional = Math.Abs(quantity.Value) * price;

                if (currentNotional < MinimumOrderNotionalValue)
                {
                    Log.Trace($"{Name}.UpdateOrder: Rejecting update for {order.Symbol.Value}. " +
                              $"Remaining quantity {quantity} (~{currentNotional:F2}$) " +
                              $"is below minimum ${MinimumOrderNotionalValue}. Returning false.");

                    OnMessage(new BrokerageMessageEvent(
                            BrokerageMessageType.Warning,
                            "UpdateOrderInvalid",
                            $"Order remaining size too small ({currentNotional:F2}$). Update cancelled."));

                    // Wir liefern hier das FALSE zurück, damit LEAN weiß: Update abgelehnt.
                    // Die Order bleibt mit ihrem alten, gültigen State im System bestehen.
                    return false;
                }
            }

            // Call an die Börse mit der exakten Restmenge und isolierten ID
            var res = RunSync(() => ExecuteUpdateOrderAsync(order, price, quantity));

            if (res?.Success != true)
            {
                if (_orderStateManager.TryGetValue(clientOrderId, out var errorState))
                {
                    errorState?.IsUpdatePending = false;
                }

                var errorMsg = res?.Error?.ToString() ?? "Unknown exchange error";

                if (errorMsg.Contains("canceled or filled") || errorMsg.Contains("Cannot modify"))
                {
                    Log.Trace($"{Name}.UpdateOrder: Race condition detected. Order was already filled or canceled on exchange. Suppressing LEAN ghost event.");

                    var terminalBrokerId = order.BrokerId.LastOrDefault();
                    if (terminalBrokerId != null)
                    {
                        _ = Task.Run(() => ReconcileOrderImmediateAsync(terminalBrokerId, order));
                    }

                    return true;
                }

                Log.Error($"{Name}.UpdateOrder({order.Symbol.Value}): {errorMsg}");

                OnMessage(new BrokerageMessageEvent(
                    BrokerageMessageType.Warning,
                    "UpdateOrder",
                    errorMsg));

                return false;
            }

            if (_orderStateManager.TryGetValue(clientOrderId, out var activeState))
            {
                // OriginalQuantity im State ist IMMER das LEAN-Gesamtziel!
                if (lastUpdate?.Quantity.HasValue == true)
                {
                    activeState.OriginalQuantity = lastUpdate.Quantity.Value;
                }
                activeState.LastUpdateUtc = DateTime.UtcNow;
            }

            return true;
        }

        protected virtual ExchangeParameters PlaceFuturesOrderExchangeParameters => new ExchangeParameters();
        protected virtual Task<ExchangeWebResult<SharedId>> ExecutePlaceOrderAsync(PlaceFuturesOrderRequest request)
            => _orderClient.PlaceFuturesOrderAsync(request);

        protected virtual Task<ExchangeWebResult<SharedId>> ExecuteUpdateOrderAsync(Order order, decimal price, decimal? quantity)
            => Task.FromResult<ExchangeWebResult<SharedId>>(null);

        protected virtual ExchangeParameters CancelFuturesOrderExchangeParameters => new ExchangeParameters();
        protected virtual Task<ExchangeWebResult<SharedId>> ExecuteCancelOrderAsync(CxCancelOrderRequest request)
            => _orderClient.CancelFuturesOrderAsync(request);

        /// <summary>
        /// Überschreiben um exchange-spezifische "Order ist bereits terminal"-Fehler zu erkennen.
        /// Wenn true zurückgegeben wird, löst UpdateOrder sofort einen Reconcile für die Order aus.
        /// Beispiel Hyperliquid: errorMsg.Contains("canceled or filled")
        /// </summary>
        protected virtual bool IsTerminalUpdateError(string errorMsg) => false;

        /// <summary>
        /// Holt den echten Order-Status von der Exchange und feuert das korrekte LEAN-Event.
        /// Wird aufgerufen wenn UpdateOrder einen Terminal-Fehler erkennt (Order bereits filled/canceled).
        /// </summary>
        private async Task ReconcileOrderImmediateAsync(string brokerId, Order order)
        {
            try
            {
                var sharedSymbol = GetSharedSymbol(order.Symbol);
                var statusCheck = await _orderClient
                    .GetFuturesOrderAsync(new GetOrderRequest(sharedSymbol, brokerId))
                    .ConfigureAwait(false);

                if (!statusCheck.Success || statusCheck.Data == null)
                {
                    Log.Error($"{Name}.ReconcileOrderImmediateAsync: Failed to fetch status for {brokerId}: {statusCheck.Error}");
                    return;
                }

                var brokerOrder = statusCheck.Data;

                // 🔥 FIX 3: DER RECONCILER-MORD 🔥
                // Wenn die Order auf der Börse noch lebt, darf der Reconciler sie nicht anfassen!
                if (brokerOrder.Status == SharedOrderStatus.Open)
                {
                    Log.Trace($"{Name}.ReconcileOrderImmediateAsync: Order {brokerId} is still OPEN on exchange. Reconciler stands down.");
                    return;
                }

                // Ab hier wissen wir: Die Order ist wirklich tot (Terminal). Jetzt dürfen wir sie aus dem State löschen.
                if (!_orderStateManager.TryGetByExchangeId(brokerId, out var removedState))
                {
                    Log.Trace($"{Name}.ReconcileOrderImmediateAsync: State for {brokerId} already removed (socket beat us).");
                    return;
                }

                _orderStateManager.TryRemove(removedState.ClientOrderId, out _);

                if (brokerOrder.Status == SharedOrderStatus.Filled)
                {
                    var finalFillAbsQty = brokerOrder.QuantityFilled?.QuantityInBaseAsset
                                          ?? Math.Abs(removedState.OriginalQuantity);
                    var sign = removedState.OriginalQuantity > 0 ? 1m : -1m;
                    var finalSignedFillQty = finalFillAbsQty * sign;
                    var remainingToFill = finalSignedFillQty - removedState.FilledQuantity;

                    if (Math.Abs(remainingToFill) > 0)
                    {
                        Log.Trace($"{Name}.ReconcileOrderImmediateAsync: Order {brokerId} confirmed FILLED. Emitting fill event.");
                        OnOrderEvent(new OrderEvent(removedState.Order, DateTime.UtcNow, OrderFee.Zero)
                        {
                            Status = OrderStatus.Filled,
                            FillPrice = brokerOrder.AveragePrice ?? 0,
                            FillQuantity = remainingToFill,
                            OrderFee = new OrderFee(new CashAmount(brokerOrder.Fee ?? 0, brokerOrder.FeeAsset ?? SettleAsset)),
                            Message = "Immediate Reconcile – Fill"
                        });
                    }
                }
                else
                {
                    Log.Trace($"{Name}.ReconcileOrderImmediateAsync: Order {brokerId} confirmed CANCELED. Emitting cancel event.");
                    OnOrderEvent(new OrderEvent(removedState.Order, DateTime.UtcNow, OrderFee.Zero)
                    {
                        Status = OrderStatus.Canceled,
                        Message = "Immediate Reconcile – Cancel"
                    });
                }
            }
            catch (Exception ex)
            {
                Log.Error($"{Name}.ReconcileOrderImmediateAsync Error for {brokerId}: {ex.Message}");
            }
        }
        
        #endregion

        #region Socket / Reconcile

        private void HandleUserTradeSocket(DataEvent<SharedUserTrade[]> update)
        {
            foreach (var trade in update.Data)
            {
                // =======================================================
                // 🔥 RAW DIAGNOSTIC LOGGING 🔥
                // =======================================================
                Log.Trace($"{Name}.HandleOrderSocket RAW PAYLOAD: " +
                          $"UpdateTimeTicks='{trade.Timestamp.Ticks}', " +
                          $"OrderId='{trade.OrderId}', " +
                          $"ClientOrderId='{trade.ClientOrderId}', " +
                          $"Symbol='{trade.Symbol}', " +
                          $"Qty='{trade.Quantity}', " +
                          $"Side='{trade.Side}', " +
                          $"Fee='{trade.Fee}', " +
                          $"Price='{trade.Price}'");

                if (string.IsNullOrEmpty(trade.OrderId)) continue;

                OrderState? state = null;

                // =======================================================
                // 1. VERSUCH: O(1) Lookup via Exchange-ID (Hidden Index)
                // =======================================================
                if (!_orderStateManager.TryGetByExchangeId(trade.OrderId, out state))
                {
                    // =======================================================
                    // 2. VERSUCH: Fallback via ClientOrderId (Master-Dict)
                    // Deckt ab:
                    //   Fall A – Order noch im Placing-State (BrokerId = clientId, daher kein Hit oben)
                    //   Fall B – Cancel+Replace: MapNewExchangeId noch nicht gelaufen,
                    //            aber _statesByClientId[clientOrderId] zeigt immer auf den aktuellen State
                    // =======================================================
                    if (!string.IsNullOrEmpty(trade.ClientOrderId))
                    {
                        if (_orderStateManager.TryGetValue(trade.ClientOrderId, out state))
                        {
                            _orderStateManager.MapNewExchangeId(trade.ClientOrderId, trade.OrderId);

                            // Alias-Cleanup falls neue clientOrderId (z.B. Bitget Edit)
                            if (trade.ClientOrderId != state.ClientOrderId)
                            {
                                _orderStateManager.RemoveAlias(state.ClientOrderId);
                                state.ClientOrderId = trade.ClientOrderId;
                            }
                        }
                    }
                }

                if (state == null)
                {
                    if (state == null)
                    {
                        state = _orderStateManager.GetAllStates().FirstOrDefault(s =>
                            s.Order.Symbol.Value == trade.Symbol &&
                            (s.Order.Direction == (trade.Side == SharedOrderSide.Buy ? OrderDirection.Buy : OrderDirection.Sell)) &&
                            (
                                // Fall A: Reguläre neue Order im Transit (BrokerId ist leer)
                                // JEDER erste Teil-Fill (kleiner oder gleich der Gesamtmenge) wird akzeptiert!
                                (
                                    (s.State == OrderLifeCycleState.Placing || s.State == OrderLifeCycleState.Submitted) &&
                                    string.IsNullOrEmpty(s.BrokerId) &&
                                    Math.Abs(trade.Quantity) <= Math.Abs(s.Remaining)
                                )
                                ||
                                // Fall B: Schwebendes Update (IsUpdatePending ist aktiv)
                                // JEDER Fill (egal wie groß) wird geschluckt, da Kontext eindeutig!
                                (
                                    s.IsUpdatePending &&
                                    (s.State == OrderLifeCycleState.Open || s.State == OrderLifeCycleState.PartiallyFilled || s.State == OrderLifeCycleState.Submitted)
                                )
                            ));

                        if (state != null)
                        {
                            Log.Trace($"{Name}: Heuristic match successful! Linking unknown Trade {trade.OrderId} (Qty: {trade.Quantity}) to ClientOrder {state.ClientOrderId}");

                            // Der Trade-Socket mappt die neue ID sofort! 
                            // Folge-Teil-Fills laufen ab jetzt instantan über den O(1) Exchange-ID Index.
                            _orderStateManager.MapNewExchangeId(state.ClientOrderId, trade.OrderId);

                            if (state.IsUpdatePending)
                            {
                                var brokerId = state.Order.BrokerId;
                                brokerId.Add(trade.Id);
                                OnOrderIdChangedEvent(new BrokerageOrderIdChangedEvent
                                {
                                    OrderId = state.Order.Id,
                                    BrokerId = brokerId
                                });
                            }
                        }
                        else
                        {
                            Log.Trace($"{Name}.HandleUserTradeSocket: Ignoring trade {trade.OrderId}. Neither OrderId nor ClientOrderId {trade.ClientOrderId} found.");
                            continue;
                        }
                    }
                }

                // =======================================================
                // TRADE VERARBEITEN (Da 'state' eine Referenz ist, updaten wir das richtige Objekt!)
                // =======================================================
                var sign = trade.Side == SharedOrderSide.Buy ? 1m : -1m;
                var signedFill = trade.Quantity * sign;
                var fee = trade.Fee ?? 0m;

                state.FilledQuantity += signedFill;
                state.CumulativeFeePaid += fee;
                state.LastUpdateUtc = DateTime.UtcNow;

                var leanStatus = Math.Abs(state.FilledQuantity) >= Math.Abs(state.OriginalQuantity)
                    ? OrderStatus.Filled
                    : OrderStatus.PartiallyFilled;

                state.State = leanStatus == OrderStatus.Filled ? OrderLifeCycleState.Filled : OrderLifeCycleState.PartiallyFilled;

                // Wenn der Trade die Order schließt, räumen wir ab.
                // TryRemove bereinigt beide internen Dicts (_statesByClientId + _statesByExchangeId).
                if (state.IsClosed)
                {
                    _orderStateManager.TryRemove(state.ClientOrderId, out _);
                }

                OnOrderEvent(new OrderEvent(state.Order, DateTime.UtcNow, new OrderFee(new CashAmount(fee, trade.FeeAsset ?? SettleAsset)))
                {
                    Status = leanStatus,
                    FillPrice = trade.Price,
                    FillQuantity = signedFill,
                    Message = "User trade socket"
                });
            }
        }

        private void HandleOrderSocket(DataEvent<SharedFuturesOrder[]> update)
        {
            // =======================================================
            // 🔥 UNZERSTÖRBARER BATCH-FIX: Unabhängig von ClientOrderId
            // =======================================================
            var newOrderUpdates = update.Data.Where(o =>
                o.Status == SharedOrderStatus.Open ||
                o.Status == SharedOrderStatus.Filled).ToList();

            var cancelUpdates = update.Data.Where(o => o.Status == SharedOrderStatus.Canceled).ToList();

            var cancelsToDrop = new HashSet<SharedFuturesOrder>();

            if (newOrderUpdates.Any() && cancelUpdates.Any())
            {
                foreach (var newPayload in newOrderUpdates)
                {
                    // Match rein über Symbol und exakte Exchange-Zeitstempel
                    var match = cancelUpdates.FirstOrDefault(c =>
                        c.Symbol == newPayload.Symbol &&
                        c.UpdateTime == newPayload.UpdateTime);

                    if (match != null)
                    {
                        // Fall A: NewPayload hat KEINE ClientOrderId (Der HL-Standardfehler)
                        // -> Wir holen sie uns von der alten ID aus dem State-Manager
                        if (string.IsNullOrEmpty(newPayload.ClientOrderId))
                        {
                            if (_orderStateManager.TryGetByExchangeId(match.OrderId, out var state))
                            {
                                Log.Trace($"{Name}: Multi-Update Match (Naked)! Injecting ClientOrderId {state.ClientOrderId} into new {newPayload.Status} Order {newPayload.OrderId}");
                                newPayload.ClientOrderId = state.ClientOrderId;
                                cancelsToDrop.Add(match); // Altes Cancel vernichten
                            }
                        }
                        // Fall B: NewPayload HAT bereits eine ClientOrderId
                        // -> Perfekt, aber wir müssen das alte Cancel TROTZDEM vernichten, 
                        // damit es in der Schleife keinen Schaden anrichtet!
                        else
                        {
                            Log.Trace($"{Name}: Multi-Update Match (Identified)! Dropping redundant Cancel for old ID {match.OrderId}");
                            cancelsToDrop.Add(match); // Altes Cancel trotzdem vernichten!
                        }
                    }
                }
            }

            var cleanPayload = update.Data.Where(o => !cancelsToDrop.Contains(o));

            foreach (var o in cleanPayload)
            {
                // =======================================================
                // 🔥 RAW DIAGNOSTIC LOGGING 🔥
                // =======================================================
                Log.Trace($"{Name}.HandleOrderSocket RAW PAYLOAD: " +
                          $"UpdateTimeTicks='{o.UpdateTime?.Ticks}', " +
                          $"OrderId='{o.OrderId}', " +
                          $"ClientOrderId='{o.ClientOrderId}', " +
                          $"Symbol='{o.Symbol}', " +
                          $"Status='{o.Status}', " +
                          $"Qty='{o.OrderQuantity?.QuantityInBaseAsset}', " +
                          $"Price='{o.OrderPrice}'");


                if (string.IsNullOrEmpty(o.OrderId)) continue;

                // -------------------------------------------------------
                // PLACING STATE: Instantaner Fill während PlaceOrder()
                // Order liegt in _statesByClientId[clientOrderId], BrokerId = clientOrderId (temp).
                // -------------------------------------------------------
                if (!string.IsNullOrEmpty(o.ClientOrderId) &&
                    _orderStateManager.TryGetValue(o.ClientOrderId, out var placingCandidate) &&
                    placingCandidate.State == OrderLifeCycleState.Placing &&
                    !_orderStateManager.TryGetByExchangeId(o.OrderId, out _))
                {
                    // 1. State-Properties aktualisieren
                    placingCandidate.State = OrderLifeCycleState.Submitted;
                    placingCandidate.LastUpdateUtc = DateTime.UtcNow;

                    // 2. Exchange-ID atomar im Manager eintragen:
                    //    - entfernt temp clientOrderId aus _statesByExchangeId
                    //    - setzt state.BrokerId = o.OrderId
                    //    - trägt unter o.OrderId in _statesByExchangeId ein
                    //    - ergänzt Order.BrokerId
                    //    - _statesByClientId[o.ClientOrderId] bleibt unverändert
                    _orderStateManager.MapNewExchangeId(o.ClientOrderId, o.OrderId);

                    Log.Trace($"{Name}.HandleOrderSocket: Placing→Submitted for {o.OrderId} via socket. Fill (if any) follows via trade socket.");

                    OnOrderEvent(new OrderEvent(placingCandidate.Order, DateTime.UtcNow, OrderFee.Zero) { Status = OrderStatus.Submitted });
                    continue;
                }

                // -------------------------------------------------------
                // MODIFY / REPLACEMENT DETECTION (Cancel + Replace)
                // -------------------------------------------------------
                if (!string.IsNullOrEmpty(o.ClientOrderId) &&
                    _orderStateManager.TryGetValue(o.ClientOrderId, out var existingState) &&
                    existingState.BrokerId != o.OrderId)
                {
                    // 🔥 THE ANTI-ZOMBIE GUARD 🔥
                    // Verhindert Rückwärts-Swaps, falls das Cancel-Event der ALTEN Order
                    // nach dem New-Event der NEUEN Order eintrifft.
                    if (existingState.Order.BrokerId.Contains(o.OrderId))
                    {
                        Log.Trace($"{Name}.HandleOrderSocket: Ignoring backwards swap to old ID {o.OrderId}.");
                    }
                    else
                    {
                        var oldBrokerId = existingState.BrokerId;

                        // 1. State-Properties aktualisieren
                        existingState.LastUpdateUtc = DateTime.UtcNow;

                        // 2. Exchange-ID atomar tauschen:
                        //    - entfernt alte BrokerId aus _statesByExchangeId
                        //    - setzt state.BrokerId = o.OrderId
                        //    - trägt unter o.OrderId in _statesByExchangeId ein
                        //    - ergänzt Order.BrokerId
                        //    - _statesByClientId[o.ClientOrderId] bleibt unverändert
                        _orderStateManager.MapNewExchangeId(o.ClientOrderId, o.OrderId);

                        // Bitget-Style: neue clientOrderId war temporärer Alias → alten Key entfernen und State updaten
                        if (o.ClientOrderId != existingState.ClientOrderId)
                        {
                            _orderStateManager.RemoveAlias(existingState.ClientOrderId);
                            existingState.ClientOrderId = o.ClientOrderId;
                        }
                        existingState.IsUpdatePending = false;
                        var prevState = existingState.State;
                        existingState.State = existingState.FilledQuantity != 0m
                            ? (Math.Abs(existingState.FilledQuantity) >= Math.Abs(existingState.OriginalQuantity)
                                ? OrderLifeCycleState.Filled
                                : OrderLifeCycleState.PartiallyFilled)
                            : OrderLifeCycleState.Open;

                        if(existingState.State == OrderLifeCycleState.Open && prevState == OrderLifeCycleState.Submitted)
                        {
                            OnOrderEvent(new OrderEvent(existingState.Order, DateTime.UtcNow, OrderFee.Zero)
                            {
                                Status = OrderStatus.UpdateSubmitted,
                                Message = "Order modified"
                            });
                        }

                        var brokerid = existingState.Order.BrokerId;
                        brokerid.Add(o.OrderId);

                        OnOrderIdChangedEvent(new BrokerageOrderIdChangedEvent
                        {
                            OrderId = existingState.Order.Id,
                            BrokerId = brokerid
                        });

                        Log.Trace($"{Name}.HandleOrderSocket: Modify mapped via Socket | Old: {oldBrokerId} → New: {o.OrderId}");
                    }
                }

                // -------------------------------------------------------
                // 🔥 SENSEMANN-CHECK: Fills aus dem Order-Stream vernichten 🔥
                // -------------------------------------------------------
                // Dies MUSS vor dem State-Lookup passieren, damit es auch greift, 
                // wenn der Trade-Socket die Order bereits gelöscht hat!
                if (o.Status == SharedOrderStatus.Filled)
                {
                    Log.Trace($"{Name}.HandleOrderSocket: Hard-ignoring {o.Status} for {o.OrderId} in Order-Stream. Trade-Stream owns this.");

                    // Wir setzen nur das Pending-Flag zurück, falls die Order noch modifiziert wurde.
                    if (_orderStateManager.TryGetByExchangeId(o.OrderId, out var pendingState))
                    {
                        pendingState.IsUpdatePending = false;
                    }
                    continue;
                }

                // -------------------------------------------------------
                // NORMAL STATUS UPDATE (Nur für Canceled / Open etc.)
                // -------------------------------------------------------
                if (_orderStateManager.TryGetByExchangeId(o.OrderId, out var state))
                {
                    // Ignoriere Status-Events von alten, ersetzten Tickets
                    if (o.OrderId != state.BrokerId)
                    {
                        Log.Trace($"{Name}.HandleOrderSocket: Ignoring status '{o.Status}' for old replaced ticket {o.OrderId}. Current active ticket is {state.BrokerId}.");
                        continue;
                    }

                    state.LastUpdateUtc = DateTime.UtcNow;
                    var absFilled = o.QuantityFilled?.QuantityInBaseAsset ?? 0m;
                    var leanStatus = MapStatus(o.Status, absFilled);

                    if (leanStatus is OrderStatus.Canceled or OrderStatus.Invalid)
                    {
                        if (state.IsUpdatePending)
                        {
                            Log.Trace($"{Name}.HandleOrderSocket: Suppressing Cancel event for {state.BrokerId} because an Update is pending.");
                            continue;
                        }

                        state.State = OrderLifeCycleState.Canceled;
                        // TryRemove bereinigt beide internen Dicts.
                        _orderStateManager.TryRemove(state.ClientOrderId, out _);

                        OnOrderEvent(new OrderEvent(state.Order, DateTime.UtcNow, OrderFee.Zero)
                        {
                            Status = leanStatus,
                            Message = "Order socket update"
                        });
                    }
                    else if (leanStatus == OrderStatus.Submitted) // SharedOrderStatus.Open ohne Fill
                    {
                        state.State = OrderLifeCycleState.Open;
                    }
                }
            }
        }

        private async Task ReconcileLoop(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(_reconciliationInterval, ct).ConfigureAwait(false);

                    var openRes = await _orderClient.GetOpenFuturesOrdersAsync(
                        new GetOpenOrdersRequest(exchangeParameters: OpenOrdersExchangeParameters)
                    ).ConfigureAwait(false);

                    if (!openRes.Success || openRes.Data == null)
                    {
                        Log.Error($"{Name}.ReconcileLoop: Failed to fetch open orders: {openRes.Error}");
                        continue;
                    }

                    var openExchangeOrders = openRes.Data
                        .GroupBy(x => x.OrderId)
                        .ToDictionary(g => g.Key, g => g.First());

                    foreach (var state in _orderStateManager.GetAllStates().ToArray())
                    {
                        // Skip orders still in Placing phase – they have no real exchange ID yet.
                        // The REST call hasn't returned; no point querying the exchange for a temp ID.
                        if (state.State == OrderLifeCycleState.Placing) continue;

                        var brokerId = state.BrokerId;

                        var updateStillPending = state.IsUpdatePending &&
                            (DateTime.UtcNow - state.LastUpdateUtc).TotalSeconds < 10;

                        if (updateStillPending ||
                            openExchangeOrders.ContainsKey(brokerId) ||
                            (DateTime.UtcNow - state.LastUpdateUtc).TotalSeconds < 5)
                            continue;

                        var sharedSymbol = GetSharedSymbol(state.Order.Symbol);
                        var statusCheck = await _orderClient
                            .GetFuturesOrderAsync(new GetOrderRequest(sharedSymbol, brokerId))
                                .ConfigureAwait(false);

                        if (!statusCheck.Success || statusCheck.Data == null)
                            {
                            Log.Error($"{Name}.ReconcileLoop: Failed to verify order {brokerId}. Error: {statusCheck.Error}");
                            continue;
                        }

                        var brokerOrder = statusCheck.Data;

                        // SAFE REMOVE via ClientOrderId (bereinigt beide internen Dicts).
                        if (!_orderStateManager.TryRemove(state.ClientOrderId, out var removedState))
                            continue;

                        // CASE 1: FILLED
                        if (brokerOrder.Status == SharedOrderStatus.Filled)
                        {
                            var finalFillAbsQty = brokerOrder.QuantityFilled?.QuantityInBaseAsset
                                                  ?? Math.Abs(removedState.OriginalQuantity);

                            var finalSignedFillQty = finalFillAbsQty * (removedState.OriginalQuantity > 0 ? 1m : -1m);
                            var remainingToFill = finalSignedFillQty - removedState.FilledQuantity;
                            if (Math.Abs(remainingToFill) > 0)
                            {
                                // FIX 3: Doppelbuchungen der Gebühren verhindern!
                                var totalExchangeFee = brokerOrder.Fee ?? 0m;
                                var remainingFee = Math.Max(0m, totalExchangeFee - removedState.CumulativeFeePaid);

                                OnOrderEvent(new OrderEvent(removedState.Order, DateTime.UtcNow, OrderFee.Zero)
                                {
                                    Status = OrderStatus.Filled,
                                    FillPrice = brokerOrder.AveragePrice ?? 0,
                                    FillQuantity = remainingToFill,
                                    OrderFee = new OrderFee(new CashAmount(remainingFee, brokerOrder.FeeAsset ?? SettleAsset)),
                                    Message = "Reconciled Fill"
                                });
                            }
                        }
                        
                        // CASE 2: STILL OPEN
                        else if (brokerOrder.Status == SharedOrderStatus.Open)
                        {
                            // Re-register: TryAdd indexes by both ClientOrderId and BrokerId (exchange ID).
                            removedState.LastUpdateUtc = DateTime.UtcNow;
                            _orderStateManager.TryAdd(removedState.ClientOrderId, removedState);
                            Log.Trace($"{Name}.ReconcileLoop: Order {brokerId} still open on exchange, re-registered.");
                        }
                        // CASE 3: CANCELED / UNKNOWN
                        else
                        {
                            OnOrderEvent(new OrderEvent(removedState.Order, DateTime.UtcNow, OrderFee.Zero)
                            {
                                Status = OrderStatus.Canceled,
                                Message = $"Order {brokerOrder.OrderId} reconciled cancel"
                            });
                        }
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    Log.Error($"{Name}.ReconcileLoop Error: {ex.Message}");
                }
            }
        }

        #endregion

        #region Order Helpers

        protected virtual string GenerateClientId(int orderId)
        {
            return $"0x{((ulong)(StartTime.Ticks + orderId)).ToString("x16").PadLeft(32, '0')}";
        }

        protected virtual string NativeTicker(Symbol symbol) => symbol.Value;

        protected virtual string NormalizeSymbol(string rawSymbol) => rawSymbol;

        private SharedSymbol GetSharedSymbol(Symbol s)
        {
            CurrencyPairUtil.DecomposeCurrencyPair(s, out var baseAsset, out var quoteAsset);
            return new SharedSymbol(TradingMode.PerpetualLinear, baseAsset, quoteAsset);
        }

        private OrderStatus MapStatus(SharedOrderStatus status, decimal filled)
        {
            if (status == SharedOrderStatus.Open)
                return filled > 0 ? OrderStatus.PartiallyFilled : OrderStatus.Submitted;

            return status switch
            {
                SharedOrderStatus.Filled => OrderStatus.Filled,
                SharedOrderStatus.Canceled => OrderStatus.Canceled,
                _ => OrderStatus.None
            };
        }

        #endregion
    }
}