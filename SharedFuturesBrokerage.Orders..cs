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

            public decimal Remaining => OriginalQuantity - FilledQuantity;

            public bool IsClosed => State is OrderLifeCycleState.Filled
                                          or OrderLifeCycleState.Canceled
                                          or OrderLifeCycleState.Invalid
                                          or OrderLifeCycleState.Replaced;
        }

        #endregion

        // --- SINGLE SOURCE OF TRUTH CACHES ---
        private readonly ConcurrentDictionary<string, OrderState> _statesByBrokerId = new();
        private readonly ConcurrentDictionary<string, string> _clientToBroker = new();

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

                // 1. Vorzeichen zentral definieren
                var sign = o.Side == SharedOrderSide.Sell ? -1m : 1m;

                // 2. Beide Quantities konsequent signieren
                var qty = (o.OrderQuantity?.QuantityInBaseAsset ?? 0m) * sign;
                var filledQty = (o.QuantityFilled?.QuantityInBaseAsset ?? 0m) * sign;

                Order order = o.OrderType == SharedOrderType.Limit
                    ? new LimitOrder(symbol, qty, o.OrderPrice ?? 0m, DateTime.UtcNow)
                    : new MarketOrder(symbol, qty, DateTime.UtcNow);

                order.BrokerId.Add(o.OrderId);

                // 3. MapStatus braucht den absoluten Wert, sonst schlägt "filled > 0" bei Shorts fehl
                order.Status = MapStatus(o.Status, Math.Abs(filledQty));

                var state = new OrderState
                {
                    Order = order,
                    OriginalQuantity = qty,
                    FilledQuantity = filledQty, // Jetzt korrekt signiert gespeichert
                    BrokerId = o.OrderId,
                    ClientOrderId = o.ClientOrderId ?? string.Empty,
                    State = order.Status == OrderStatus.PartiallyFilled ? OrderLifeCycleState.PartiallyFilled : OrderLifeCycleState.Open,
                    LastUpdateUtc = DateTime.UtcNow
                };

                _statesByBrokerId[o.OrderId] = state;
                if(!string.IsNullOrEmpty(state.ClientOrderId))
                    _clientToBroker[state.ClientOrderId] = o.OrderId;

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

            var request = new PlaceFuturesOrderRequest(
                GetSharedSymbol(order.Symbol),
                executionQuantity > 0 ? SharedOrderSide.Buy : SharedOrderSide.Sell,
                order.Type == OrderType.Limit ? SharedOrderType.Limit : SharedOrderType.Market,
                new SharedQuantity { QuantityInBaseAsset = Math.Abs(executionQuantity) })
            {
                Price = (order as LimitOrder)?.LimitPrice,
                ClientOrderId = GenerateClientId(order.Id)
            };

            var res = RunSync(() => ExecutePlaceOrderAsync(request));
            if (!res.Success)
            {
                var errorMsg = res.Error?.ToString() ?? "Unknown exchange error";
                Log.Error($"{Name}.PlaceOrder({order.Symbol.Value}): {errorMsg}");
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "PlaceOrder", errorMsg));
                return false;
            }

            order.BrokerId.Add(res.Data.Id);

            var state = new OrderState
            {
                Order = order,
                OriginalQuantity = executionQuantity,
                FilledQuantity = 0m,
                BrokerId = res.Data.Id,
                ClientOrderId = request.ClientOrderId,
                State = OrderLifeCycleState.Submitted,
                LastUpdateUtc = DateTime.UtcNow
            };

            _statesByBrokerId[res.Data.Id] = state;
            _clientToBroker[state.ClientOrderId] = res.Data.Id;

            OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero) { Status = OrderStatus.Submitted });
            return true;
        }

        public override bool CancelOrder(Order order)
        {
            if (!order.BrokerId.Any()) return false;
            var id = order.BrokerId.Last();

            var res = RunSync(() => ExecuteCancelOrderAsync(new CxCancelOrderRequest(GetSharedSymbol(order.Symbol), id)));
            if (!res.Success)
            {
                var errorMsg = res.Error?.ToString() ?? "Unknown exchange error";
                Log.Error($"{Name}.CancelOrder({order.Symbol.Value}): {errorMsg}");
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "CancelOrder", errorMsg));
                return false;
            }

            if (_statesByBrokerId.TryGetValue(id, out var state))
            {
                state.State = OrderLifeCycleState.Canceled;
                state.LastUpdateUtc = DateTime.UtcNow;

                _statesByBrokerId.TryRemove(id, out _);
                _clientToBroker.TryRemove(state.ClientOrderId, out _);
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

            if (_clientToBroker.TryGetValue(clientOrderId, out var brokerId) &&
                _statesByBrokerId.TryGetValue(brokerId, out var state))
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
                var errorMsg = res?.Error?.ToString() ?? "Unknown exchange error";
                Log.Error($"{Name}.UpdateOrder({order.Symbol.Value}): {errorMsg}");

                OnMessage(new BrokerageMessageEvent(
                    BrokerageMessageType.Warning,
                    "UpdateOrder",
                    errorMsg));

                return false;
            }

            // =========================================================
            // 🔥 CRITICAL STATE SYNC FIX (LEAN CONSISTENCY)
            // =========================================================
            if (_clientToBroker.TryGetValue(clientOrderId, out var activeBrokerId) &&
                _statesByBrokerId.TryGetValue(activeBrokerId, out var activeState))
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

        protected virtual Task<ExchangeWebResult<SharedId>> ExecutePlaceOrderAsync(PlaceFuturesOrderRequest request)
            => _orderClient.PlaceFuturesOrderAsync(request);

        protected virtual Task<ExchangeWebResult<SharedId>> ExecuteUpdateOrderAsync(Order order, string clientOrderId, decimal price, decimal quantity)
            => Task.FromResult<ExchangeWebResult<SharedId>>(null);

        protected virtual Task<ExchangeWebResult<SharedId>> ExecuteCancelOrderAsync(CxCancelOrderRequest request)
            => _orderClient.CancelFuturesOrderAsync(request);

        #endregion

        #region Socket / Reconcile

        private void HandleUserTradeSocket(DataEvent<SharedUserTrade[]> update)
        {
            foreach (var trade in update.Data)
            {
                if (string.IsNullOrEmpty(trade.OrderId) || !_statesByBrokerId.TryGetValue(trade.OrderId, out var state))
                    continue;

                var sign = trade.Side == SharedOrderSide.Buy ? 1m : -1m;
                var signedFill = trade.Quantity * sign;

                state.FilledQuantity += signedFill;
                state.LastUpdateUtc = DateTime.UtcNow;

                var leanStatus = Math.Abs(state.FilledQuantity) >= Math.Abs(state.OriginalQuantity)
                    ? OrderStatus.Filled
                    : OrderStatus.PartiallyFilled;

                state.State = leanStatus == OrderStatus.Filled ? OrderLifeCycleState.Filled : OrderLifeCycleState.PartiallyFilled;

                if (state.IsClosed)
                {
                    _statesByBrokerId.TryRemove(state.BrokerId, out _);
                    _clientToBroker.TryRemove(state.ClientOrderId, out _);
                }

                OnOrderEvent(new OrderEvent(state.Order, DateTime.UtcNow, new OrderFee(new CashAmount(trade.Fee ?? 0m, trade.FeeAsset ?? SettleAsset)))
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
                if (string.IsNullOrEmpty(o.OrderId)) continue;

                // Modify Replacement Detection via ClientOrderId
                if (!string.IsNullOrEmpty(o.ClientOrderId) &&
                    _clientToBroker.TryGetValue(o.ClientOrderId, out var oldBrokerId) &&
                    oldBrokerId != o.OrderId)
                {
                    if (_statesByBrokerId.TryGetValue(oldBrokerId, out var existingState))
                    {
                        // Normal path: State vorhanden, BrokerId rebinden
                        _statesByBrokerId.TryRemove(oldBrokerId, out _);

                        existingState.BrokerId = o.OrderId;
                        existingState.LastUpdateUtc = DateTime.UtcNow;

                        _statesByBrokerId[o.OrderId] = existingState;
                        _clientToBroker[o.ClientOrderId] = o.OrderId;

                        existingState.Order.BrokerId.Add(o.OrderId);
                        OnOrderIdChangedEvent(new BrokerageOrderIdChangedEvent
                        {
                            OrderId = existingState.Order.Id,
                            BrokerId = existingState.Order.BrokerId
                        });

                        Log.Trace($"{Name}.HandleOrderSocket: Modify detected for {existingState.Order.Symbol.Value} | OldOID: {oldBrokerId} → NewOID: {o.OrderId}");
                    }
                    else
                    {
                        // State bereits abgeräumt (Reconcile/Socket-Race), aber ID-Mapping
                        // aktualisieren damit _clientToBroker nicht auf veraltete BrokerId zeigt
                        _clientToBroker[o.ClientOrderId] = o.OrderId;

                        Log.Trace($"{Name}.HandleOrderSocket: Modify detected for ClientOrderId {o.ClientOrderId} but old state for BrokerId {oldBrokerId} not found (already reconciled?). ID mapping updated to {o.OrderId}.");
                    }
                }

                // Normal Status Update
                if (_statesByBrokerId.TryGetValue(o.OrderId, out var state))
                {
                    // FIX: FilledQuantity mit Exchange synchronisieren (inkl. korrektem Vorzeichen)
                    var sign = state.OriginalQuantity > 0 ? 1m : -1m;
                    var absFilled = o.QuantityFilled?.QuantityInBaseAsset ?? 0m;

                    state.FilledQuantity = absFilled * sign;
                    state.LastUpdateUtc = DateTime.UtcNow;

                    var leanStatus = MapStatus(o.Status, absFilled);

                    if (leanStatus is OrderStatus.Canceled or OrderStatus.Invalid)
                    {
                        state.State = OrderLifeCycleState.Canceled;

                        _statesByBrokerId.TryRemove(state.BrokerId, out _);
                        _clientToBroker.TryRemove(state.ClientOrderId, out _);

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


                    foreach (var kv in _statesByBrokerId.ToArray())
                    {
                        var brokerId = kv.Key;
                        var state = kv.Value;

                        // -----------------------------
                        // PERFORMANCE- & ZEIT-CHECK (VOR DEM API CALL)
                        // -----------------------------
                        // Wenn die Order im Batch-Call als offen gemeldet wurde oder taufrisch ist, überspringen.
                        if (openExchangeOrders.ContainsKey(brokerId) || (DateTime.UtcNow - state.LastUpdateUtc).TotalSeconds < 5)
                            continue;

                        var sharedSymbol = GetSharedSymbol(state.Order.Symbol);
                        var statusCheck = await _orderClient
                            .GetFuturesOrderAsync(new GetOrderRequest(sharedSymbol, brokerId))
                            .ConfigureAwait(false);

                        // -----------------------------
                        // API FAIL = NO STATE CHANGE (CRITICAL FIX)
                        // -----------------------------
                        if (!statusCheck.Success || statusCheck.Data == null)
                        {
                            Log.Error($"{Name}.ReconcileLoop: Failed to verify order {brokerId}. Error: {statusCheck.Error}");
                            continue;
                        }

                        var brokerOrder = statusCheck.Data;

                        // -----------------------------
                        // SAFE REMOVE (RACE CONDITION PREVENTION)
                        // -----------------------------
                        // Wenn der Socket die Order in der Zwischenzeit abgeräumt hat, hier abbrechen.
                        if (!_statesByBrokerId.TryRemove(brokerId, out var removedState))
                            continue;

                        _clientToBroker.TryRemove(removedState.ClientOrderId, out _);

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
                                OnOrderEvent(new OrderEvent(removedState.Order, DateTime.UtcNow, OrderFee.Zero)
                                {
                                    Status = OrderStatus.Filled,
                                    FillPrice = brokerOrder.AveragePrice ?? 0,
                                    FillQuantity = remainingToFill,
                                    OrderFee = new OrderFee(new CashAmount(brokerOrder.Fee ?? 0, brokerOrder.FeeAsset ?? SettleAsset)),
                                    Message = "Reconciled Fill"
                                });
                            }
                        }
                        // -----------------------------
                        // CASE 2: CANCELED / UNKNOWN
                        // -----------------------------
                        else
                        {
                            OnOrderEvent(new OrderEvent(removedState.Order, DateTime.UtcNow, OrderFee.Zero)
                            {
                                Status = OrderStatus.Canceled,
                                Message = "Reconciled Cancel"
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