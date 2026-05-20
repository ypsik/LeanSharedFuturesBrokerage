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
        private readonly OrderStateManager _orderStateManager = new();


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
                BrokerId = clientOrderId, // temp key bis echte Exchange-ID bekannt
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
            decimal quantity = order.Quantity;

            var clientOrderId = GenerateClientId(order.Id);

            // Direct lookup via clientOrderId – no two-step _clientToBroker detour needed.
            if (_orderStateManager.TryGetValue(clientOrderId, out var state))
            {
                // 1. PRIORITY: explicit LEAN update request
                if (lastUpdate?.Quantity.HasValue == true)
                {
                    quantity = lastUpdate.Quantity.Value;
                }
                else
                {
                    // 2. FALLBACK: Alter Intent (LEAN semantics)
                    // Wenn kein neues Quantity-Ziel gesendet wurde, bleibt das ursprüngliche Ziel erhalten!
                    quantity = state.OriginalQuantity;
                }

                // FIX: Setze das Flag, bevor der API-Call rausgeht
                state.IsUpdatePending = true;
            }

            // Minimum notional check
            if (MinimumOrderNotionalValue > 0m && price > 0m)
            {
                decimal notional = Math.Abs(quantity) * price;

                if (notional < MinimumOrderNotionalValue)
                {
                    var props = _spdb.GetSymbolProperties(
                        order.Symbol.ID.Market,
                        order.Symbol,
                        order.Symbol.SecurityType,
                        SettleAsset);

                    decimal lotSize = props?.LotSize ?? 0.01m;
                    decimal adjusted = Math.Ceiling((MinimumOrderNotionalValue / price) / lotSize) * lotSize;

                    quantity = quantity < 0 ? -adjusted : adjusted;

                    Log.Trace($"{Name}.UpdateOrder: Adjusting quantity for {order.Symbol.Value} to {Math.Abs(quantity)} to meet minimum ${MinimumOrderNotionalValue}.");
                }
            }

            var res = RunSync(() => ExecuteUpdateOrderAsync(order, clientOrderId, price, quantity));

            if (res?.Success != true)
            {
                // Flag zurücksetzen, wenn der Call hart fehlschlägt
                if (_orderStateManager.TryGetValue(clientOrderId, out var errorState))
                {
                    errorState.IsUpdatePending = false;
                }

                var errorMsg = res?.Error?.ToString() ?? "Unknown exchange error";
                Log.Error($"{Name}.UpdateOrder({order.Symbol.Value}): {errorMsg}");

                OnMessage(new BrokerageMessageEvent(
                    BrokerageMessageType.Warning,
                    "UpdateOrder",
                    errorMsg));

                // TERMINAL ERROR DETECTION:
                // Wenn die Exchange explizit sagt die Order ist bereits closed (z.B. HL: "Cannot modify canceled or filled order"),
                // sofort den echten Status holen und das korrekte Event feuern – nicht auf den nächsten ReconcileLoop warten.
                if (IsTerminalUpdateError(errorMsg))
                {
                    var terminalBrokerId = order.BrokerId.LastOrDefault();
                    if (terminalBrokerId != null)
                    {
                        Log.Trace($"{Name}.UpdateOrder: Terminal error detected for {order.Symbol.Value} ({terminalBrokerId}). Triggering immediate status check.");
                        _ = Task.Run(() => ReconcileOrderImmediateAsync(terminalBrokerId, order));
                    }
                }

                return false;
            }

            // =========================================================
            // 🔥 CRITICAL STATE SYNC FIX (LEAN CONSISTENCY)
            // =========================================================
            if (_orderStateManager.TryGetValue(clientOrderId, out var activeState))
            {
                // Update reflects new INTENT, not execution
                activeState.OriginalQuantity = quantity;
                activeState.LastUpdateUtc = DateTime.UtcNow;

                // safety: if order is already partially filled,
                // ensure remaining stays consistent
                if (Math.Abs(activeState.FilledQuantity) > 0)
                {
                    // nothing to do here except keep consistency
                    // Remaining is derived anyway
                }
            }

            return true;
        }

        protected virtual ExchangeParameters PlaceFuturesOrderExchangeParameters => new ExchangeParameters();
        protected virtual Task<ExchangeWebResult<SharedId>> ExecutePlaceOrderAsync(PlaceFuturesOrderRequest request)
            => _orderClient.PlaceFuturesOrderAsync(request);

        protected virtual Task<ExchangeWebResult<SharedId>> ExecuteUpdateOrderAsync(Order order, string clientOrderId, decimal price, decimal quantity)
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

                // Lookup via Exchange-ID, then remove via ClientOrderId (safe remove).
                // Socket könnte in der Zwischenzeit abgeräumt haben.
                if (!_orderStateManager.TryGetByExchangeId(brokerId, out var removedState))
                {
                    Log.Trace($"{Name}.ReconcileOrderImmediateAsync: State for {brokerId} already removed (socket beat us).");
                    return;
                }

                _orderStateManager.TryRemove(removedState.ClientOrderId, out _);

                var brokerOrder = statusCheck.Data;

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
                if (string.IsNullOrEmpty(trade.OrderId)) continue;

                OrderState state = null;

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
                        }
                    }
                }

                // Wenn sie nach beiden Versuchen immer noch null ist,
                // gehört der Trade definitiv nicht zu unserer Sitzung.
                if (state == null)
                {
                    Log.Trace($"{Name}.HandleUserTradeSocket: Ignoring trade {trade.OrderId}. Neither OrderId nor ClientOrderId {trade.ClientOrderId} found.");
                    continue;
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
            foreach (var o in update.Data)
            {
                // =======================================================
                // 🔥 RAW DIAGNOSTIC LOGGING 🔥
                // Wir loggen exakt, was reinkommt, um das Rätselraten zu beenden.
                // =======================================================
                Log.Trace($"{Name}.HandleOrderSocket RAW PAYLOAD: " +
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
                        // 1. State-Properties aktualisieren
                        existingState.LastUpdateUtc = DateTime.UtcNow;
                        existingState.IsUpdatePending = false;

                        // 2. Exchange-ID atomar tauschen:
                        //    - entfernt alte BrokerId aus _statesByExchangeId
                        //    - setzt state.BrokerId = o.OrderId
                        //    - trägt unter o.OrderId in _statesByExchangeId ein
                        //    - ergänzt Order.BrokerId
                        //    - _statesByClientId[o.ClientOrderId] bleibt unverändert
                        _orderStateManager.MapNewExchangeId(o.ClientOrderId, o.OrderId);

                        OnOrderIdChangedEvent(new BrokerageOrderIdChangedEvent
                        {
                            OrderId = existingState.Order.Id,
                            BrokerId = existingState.Order.BrokerId
                        });

                        Log.Trace($"{Name}.HandleOrderSocket: Modify mapped via Socket | Old: {existingState.BrokerId} → New: {o.OrderId}");
                    }
                }

                // -------------------------------------------------------
                // NORMAL STATUS UPDATE
                // -------------------------------------------------------
                if (_orderStateManager.TryGetByExchangeId(o.OrderId, out var state))
                {
                    state.LastUpdateUtc = DateTime.UtcNow;
                    var absFilled = o.QuantityFilled?.QuantityInBaseAsset ?? 0m;
                    var leanStatus = MapStatus(o.Status, absFilled);

                    // 🔥 DEIN FIX: Wir entmachten den Order-Socket für Fills! 🔥
                    // Wenn die Börse über den Order-Stream "Filled" oder "PartiallyFilled" meldet,
                    // ignorieren wir das hier komplett. Wir WARTEN auf den Trade-Socket,
                    // denn nur der hat die genauen Ausführungspreise und Gebühren!
                    if (leanStatus == OrderStatus.Filled || leanStatus == OrderStatus.PartiallyFilled)
                    {
                        Log.Trace($"{Name}.HandleOrderSocket: Ignoring {leanStatus} for {o.OrderId} in Order-Stream. Deferring to Trade-Stream.");

                        // Wir setzen nur das Pending-Flag zurück, falls die Order gerade modifiziert wurde.
                        state.IsUpdatePending = false;
                        continue;
                    }

                    // Stornos und Ablehnungen behandeln wir weiterhin hier,
                    // da diese keine Trade-Events generieren.
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

                        // -----------------------------
                        // CASE 1: FILLED
                        // -----------------------------
                        if (brokerOrder.Status == SharedOrderStatus.Filled)
                        {
                            var finalFillAbsQty = brokerOrder.QuantityFilled?.QuantityInBaseAsset
                                                  ?? Math.Abs(removedState.OriginalQuantity);

                            var finalSignedFillQty = finalFillAbsQty * (removedState.OriginalQuantity > 0 ? 1m : -1m);
                            var remainingToFill = finalSignedFillQty - removedState.FilledQuantity;

                            if (Math.Abs(remainingToFill) > 0)
                            {
                                // -----------------------------
                                // FIX 3: Doppelbuchungen der Gebühren verhindern!
                                // -----------------------------
                                var totalExchangeFee = brokerOrder.Fee ?? 0m;
                                var remainingFee = Math.Max(0m, totalExchangeFee - removedState.CumulativeFeePaid);

                                OnOrderEvent(new OrderEvent(removedState.Order, DateTime.UtcNow, OrderFee.Zero)
                                {
                                    Status = OrderStatus.Filled,
                                    FillPrice = brokerOrder.AveragePrice ?? 0,
                                    FillQuantity = remainingToFill,
                                    // Wir stellen LEAN nur noch das Rest-Delta in Rechnung
                                    OrderFee = new OrderFee(new CashAmount(remainingFee, brokerOrder.FeeAsset ?? SettleAsset)),
                                    Message = "Reconciled Fill"
                                });
                            }
                        }
                        // -----------------------------
                        // CASE 2: STILL OPEN
                        // -----------------------------
                        else if (brokerOrder.Status == SharedOrderStatus.Open)
                        {
                            // Re-register: TryAdd indexes by both ClientOrderId and BrokerId (exchange ID).
                            removedState.LastUpdateUtc = DateTime.UtcNow;
                            _orderStateManager.TryAdd(removedState.ClientOrderId, removedState);
                            Log.Trace($"{Name}.ReconcileLoop: Order {brokerId} still open on exchange, re-registered.");
                        }
                        // -----------------------------
                        // CASE 3: CANCELED / UNKNOWN
                        // -----------------------------
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
            return $"0x{((ulong)(_startTime.Ticks + orderId)).ToString("x16").PadLeft(32, '0')}";
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