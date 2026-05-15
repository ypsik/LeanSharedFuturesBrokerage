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
        /// <summary>
        /// Definiert den minimalen Mindestbestellwert (Notional Value) in der Kontowährung.
        /// Standardmäßig auf 0m gesetzt (deaktiviert). Kann in abgeleiteten Brokerages überschrieben werden.
        /// </summary>
        public virtual decimal MinimumOrderNotionalValue => 0m;


        // --- CACHES ---
        protected readonly ConcurrentDictionary<string, Order> _orderCache = new();
        private readonly ConcurrentDictionary<string, decimal> _filledQtyCache = new();

        #region Order Management

        public override List<Order> GetOpenOrders()
        {
            var res = RunSync(() => _orderClient.GetOpenFuturesOrdersAsync(new GetOpenOrdersRequest()));
            if (!res.Success || res.Data == null) return new List<Order>();

            return res.Data.Select(o =>
            {
                var symbol = Symbol.Create(NormalizeSymbol(o.Symbol), SecurityType.CryptoFuture, Name);
                var qty = (o.OrderQuantity?.QuantityInBaseAsset ?? 0m) * (o.Side == SharedOrderSide.Sell ? -1 : 1);

                Order order = o.OrderType == SharedOrderType.Limit
                    ? new LimitOrder(symbol, qty, o.OrderPrice ?? 0m, DateTime.UtcNow)
                    : new MarketOrder(symbol, qty, DateTime.UtcNow);

                order.BrokerId.Add(o.OrderId);
                order.Status = MapStatus(o.Status, o.QuantityFilled?.QuantityInBaseAsset ?? 0m);

                _orderCache[o.OrderId] = order;
                _filledQtyCache[o.OrderId] = o.QuantityFilled?.QuantityInBaseAsset ?? 0m;

                return order;
            }).ToList();
        }

        public override bool PlaceOrder(Order order)
        {
            // Initialize a local variable for execution quantity since order.Quantity is read-only
            decimal executionQuantity = order.Quantity;

            // Fix 2: If a minimum order value is set, validate and adjust the quantity
            if (MinimumOrderNotionalValue > 0m)
            {
                decimal price = 0m;
                if (order is LimitOrder limitOrder)
                {
                    price = limitOrder.LimitPrice;
                }
                else if (order is StopMarketOrder stopMarketOrder)
                {
                    price = stopMarketOrder.StopPrice;
                }
                else
                {
                    // Fallback to the current market price of the security within LEAN
                    price = _algorithm.Securities[order.Symbol].Price;
                }

                if (price > 0m)
                {
                    decimal currentNotional = Math.Abs(executionQuantity * price);

                    // If the current value is too small, scale up the quantity
                    if (currentNotional < MinimumOrderNotionalValue && executionQuantity != 0m)
                    {
                        // Retrieve the true, native step size (LotSize) from the Symbol Properties Database
                        var props = _spdb.GetSymbolProperties(order.Symbol.ID.Market, order.Symbol, order.Symbol.SecurityType, "USDC");
                        decimal baseLotSize = props?.LotSize ?? 0.01m;

                        // Calculate the exact units required to meet the minimum value
                        decimal minUnitsRequired = MinimumOrderNotionalValue / price;

                        // Round UP to the next valid multiple of the true LotSize
                        decimal adjustedQuantity = Math.Ceiling(minUnitsRequired / baseLotSize) * baseLotSize;

                        // Maintain the mathematical sign (Short / Long)
                        if (executionQuantity < 0)
                        {
                            adjustedQuantity = -adjustedQuantity;
                        }

                        Log.Trace($"{Name}.PlaceOrder: Adjusting execution quantity for {order.Symbol.Value} from {executionQuantity} to {adjustedQuantity} to meet the minimum of ${MinimumOrderNotionalValue}.");

                        // Update the local execution quantity variable
                        executionQuantity = adjustedQuantity;
                    }
                }
            }

            // Use the local executionQuantity for the exchange request instead of order.Quantity
            var request = new PlaceFuturesOrderRequest(
                GetSharedSymbol(order.Symbol),
                executionQuantity > 0 ? SharedOrderSide.Buy : SharedOrderSide.Sell,
                order.Type == OrderType.Limit ? SharedOrderType.Limit : SharedOrderType.Market,
                new SharedQuantity { QuantityInBaseAsset = Math.Abs(executionQuantity) })
            {
                Price = (order as LimitOrder)?.LimitPrice
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
            _orderCache[res.Data.Id] = order;
            _filledQtyCache[res.Data.Id] = 0m;

            OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero) { Status = OrderStatus.Submitted });
            return true;
        }

        public override bool CancelOrder(Order order)
        {
            if (!order.BrokerId.Any()) return false;
            var id = order.BrokerId.First();

            var res = RunSync(() => ExecuteCancelOrderAsync(new CxCancelOrderRequest(GetSharedSymbol(order.Symbol), id)));
            if (!res.Success)
            {
                var errorMsg = res.Error?.ToString() ?? "Unknown exchange error";
                Log.Error($"{Name}.CancelOrder({order.Symbol.Value}): {errorMsg}");
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "CancelOrder", errorMsg));
                return false;
            }

            return true;
        }
        public override bool UpdateOrder(Order order)
        {
            if (!order.BrokerId.Any() || ExecuteUpdateOrderAsync == null) return false;

            var lastUpdate = _orderManager.GetOrderTicket(order.Id).UpdateRequests.Last();

            decimal price = lastUpdate.LimitPrice ?? order.Price;

            decimal quantity = lastUpdate.Quantity.HasValue
                ? lastUpdate.Quantity.Value                                                          
                : _orderCache.TryGetValue(order.BrokerId.Last(), out var cached)
                    ? cached.Quantity                                                                
                    : order.Quantity;                                                               

            var res = RunSync(() => ExecuteUpdateOrderAsync(order, price, quantity));
            if (!res.Success)
            {
                var errorMsg = res.Error?.ToString() ?? "Unknown exchange error";
                Log.Error($"{Name}.UpdateOrder({order.Symbol.Value}): {errorMsg}");
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "UpdateOrder", errorMsg));
                return false;
            }

            return true;
        }

        protected virtual Task<ExchangeWebResult<SharedId>> ExecuteUpdateOrderAsync(Order order, decimal price, decimal quantity)
            => Task.FromResult<ExchangeWebResult<SharedId>>(null);
        // --- Virtual hooks for exchange-specific overrides (e.g. HL vaultAddress) ---

        protected virtual Task<ExchangeWebResult<SharedId>> ExecutePlaceOrderAsync(PlaceFuturesOrderRequest request)
            => _orderClient.PlaceFuturesOrderAsync(request);

        protected virtual Task<ExchangeWebResult<SharedId>> ExecuteCancelOrderAsync(CxCancelOrderRequest request)
            => _orderClient.CancelFuturesOrderAsync(request);

        #endregion

        #region Socket / Reconcile

        private void HandleUserTradeSocket(DataEvent<SharedUserTrade[]> update)
        {
            foreach (var trade in update.Data)
            {
                if (string.IsNullOrEmpty(trade.OrderId)) continue;
                if (!_orderCache.TryGetValue(trade.OrderId, out var order)) continue;

                var tradeQty = trade.Quantity;
                var prevTotal = _filledQtyCache.TryGetValue(trade.OrderId, out var pf) ? pf : 0m;
                var totalFilled = prevTotal + tradeQty;

                _filledQtyCache[trade.OrderId] = totalFilled;

                // Status basierend auf Füllgrad bestimmen (Nur Fills hier)
                var status = totalFilled >= Math.Abs(order.Quantity) ? OrderStatus.Filled : OrderStatus.PartiallyFilled;
                var sign = trade.Side == SharedOrderSide.Buy ? 1 : -1;

                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, new OrderFee(new CashAmount(trade.Fee ?? 0m, trade.FeeAsset ?? "USDC")))
                {
                    Status = status,
                    FillPrice = trade.Price,
                    FillQuantity = tradeQty * sign,
                    OrderFee = new OrderFee(new CashAmount(trade.Fee ?? 0m, trade.FeeAsset ?? "USDC")),
                    Message = "User trade socket"
                });

                if (status == OrderStatus.Filled)
                {
                    _orderCache.TryRemove(trade.OrderId, out _);
                    _filledQtyCache.TryRemove(trade.OrderId, out _);
                }
            }
        }

        private void HandleOrderSocket(DataEvent<SharedFuturesOrder[]> update)
        {
            foreach (var o in update.Data)
            {
                if (string.IsNullOrEmpty(o.OrderId)) continue;
                if (!_orderCache.TryGetValue(o.OrderId, out var order)) continue;

                var totalFilled = o.QuantityFilled?.QuantityInBaseAsset ?? 0m;
                var status = MapStatus(o.Status, totalFilled);

                if (status is OrderStatus.Canceled or OrderStatus.Invalid)
                {
                    OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
                    {
                        Status = status,
                        Message = "order socket"
                    });

                    if (status is OrderStatus.Canceled or OrderStatus.Invalid)
                    {
                        _orderCache.TryRemove(o.OrderId, out _);
                        _filledQtyCache.TryRemove(o.OrderId, out _);
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

                    var open = await _orderClient.GetOpenFuturesOrdersAsync(new GetOpenOrdersRequest()).ConfigureAwait(false);
                    if (!open.Success || open.Data == null)
                    {
                        Log.Error($"{Name}.ReconcileLoop: Failed to fetch open orders: {open.Error}");
                        continue;
                    }

                    var map = open.Data.ToDictionary(x => x.OrderId);
                    foreach (var kv in _orderCache.ToArray())
                    {
                        if (map.ContainsKey(kv.Key)) continue;

                        var symbol = kv.Value.Symbol;
                        var symbolProperties = _spdb.GetSymbolProperties(symbol.ID.Market, symbol, symbol.SecurityType, "USD");

                        string baseAsset = symbol.ID.Symbol;

                        var tradingMode = symbol.SecurityType == SecurityType.CryptoFuture || symbol.SecurityType == SecurityType.Future
                            ? TradingMode.PerpetualLinear
                            : TradingMode.Spot;

                        var sharedSymbol = GetSharedSymbol(symbol);

                        var statusCheck = await _orderClient.GetFuturesOrderAsync(new GetOrderRequest(sharedSymbol, kv.Key)).ConfigureAwait(false);

                        if (statusCheck.Success && statusCheck.Data != null)
                        {
                            var brokerOrder = statusCheck.Data;

                            if (brokerOrder.Status == SharedOrderStatus.Filled)
                            {
                                OnOrderEvent(new OrderEvent(kv.Value, DateTime.UtcNow, OrderFee.Zero)
                                {
                                    Status = OrderStatus.Filled,
                                    FillPrice = brokerOrder.AveragePrice ?? 0,
                                    FillQuantity = (brokerOrder.QuantityFilled?.QuantityInBaseAsset ?? 0) * (kv.Value.Quantity > 0 ? 1 : -1),
                                    OrderFee = new OrderFee(new CashAmount(brokerOrder.Fee ?? 0, brokerOrder.FeeAsset ?? "USDC")),
                                    Message = "Reconciled Fill"
                                });
                            }
                            else
                            {
                                OnOrderEvent(new OrderEvent(kv.Value, DateTime.UtcNow, OrderFee.Zero)
                                {
                                    Status = OrderStatus.Canceled,
                                    Message = "Reconciled Cancel"
                                });
                            }
                        }
                        else
                        {
                            OnOrderEvent(new OrderEvent(kv.Value, DateTime.UtcNow, OrderFee.Zero)
                            {
                                Status = OrderStatus.Canceled,
                                Message = "Reconcile: Order not found on exchange"
                            });
                        }

                        _orderCache.TryRemove(kv.Key, out _);
                        _filledQtyCache.TryRemove(kv.Key, out _);
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

        protected virtual string NativeTicker(Symbol symbol)
        {
            return symbol.Value;
        }

        protected virtual string NormalizeSymbol(string rawSymbol) => rawSymbol;

        protected virtual SharedSymbol GetSharedSymbol(Symbol s)
        {
            CurrencyPairUtil.DecomposeCurrencyPair(s, out var _, out var quoteAsset);
            return new SharedSymbol(TradingMode.PerpetualLinear, s.Value, quoteAsset); ;
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