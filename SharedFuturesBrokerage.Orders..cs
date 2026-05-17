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

        // --- CACHES ---
        protected readonly ConcurrentDictionary<string, Order> _orderCache = new();
        private readonly ConcurrentDictionary<string, decimal> _filledQtyCache = new();

        // NEU: clientOrderId (= LEAN order.Id) → aktueller brokerId
        // Ermöglicht Modify-Tracking wenn Hyperliquid neue OID bei Cancel+Replace erstellt
        private readonly ConcurrentDictionary<string, string> _clientOrderIdToBrokerId = new();

        // NEU: BrokerIds die gerade in einem Modify stecken → Cancel-Event unterdrücken
        private readonly ConcurrentDictionary<string, byte> _pendingModifies = new();

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
                var qty = (o.OrderQuantity?.QuantityInBaseAsset ?? 0m) * (o.Side == SharedOrderSide.Sell ? -1 : 1);

                Order order = o.OrderType == SharedOrderType.Limit
                    ? new LimitOrder(symbol, qty, o.OrderPrice ?? 0m, DateTime.UtcNow)
                    : new MarketOrder(symbol, qty, DateTime.UtcNow);

                order.BrokerId.Add(o.OrderId);
                order.Status = MapStatus(o.Status, o.QuantityFilled?.QuantityInBaseAsset ?? 0m);

                _orderCache[o.OrderId] = order;
                _filledQtyCache[o.OrderId] = o.QuantityFilled?.QuantityInBaseAsset ?? 0m;

                // NEU: clientOrderId Index beim Laden bestehender Orders aufbauen
                if (!string.IsNullOrEmpty(o.ClientOrderId))
                    _clientOrderIdToBrokerId[o.ClientOrderId] = o.OrderId;

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
                        var props = _spdb.GetSymbolProperties(order.Symbol.ID.Market, order.Symbol, order.Symbol.SecurityType, "USDC");
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
            _orderCache[res.Data.Id] = order;
            _filledQtyCache[res.Data.Id] = 0m;

            // NEU: Reverse-Index clientOrderId → aktuelle brokerId
            _clientOrderIdToBrokerId[GenerateClientId(order.Id)] = res.Data.Id;

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

            return true;
        }

        public override bool UpdateOrder(Order order)
        {
            if (!order.BrokerId.Any() || ExecuteUpdateOrderAsync == null) return false;

            var ticket = _orderManager.GetOrderTicket(order.Id);
            var lastUpdate = ticket?.UpdateRequests.LastOrDefault();

            decimal price = lastUpdate?.LimitPrice ?? order.Price;

            decimal quantity;

            if (lastUpdate?.Quantity.HasValue == true)
            {
                quantity = lastUpdate.Quantity.Value;
            }
            else if (_clientOrderIdToBrokerId.TryGetValue(GenerateClientId(order.Id), out var brokerId) &&
                     _orderCache.TryGetValue(brokerId, out var cached))
            {
                var filled = _filledQtyCache.TryGetValue(brokerId, out var f) ? f : 0m;
                quantity = Math.Abs(cached.Quantity) - filled;
                if (cached.Quantity < 0) quantity = -quantity;
            }
            else
            {
                quantity = order.Quantity;
            }

            // Minimum notional check
            if (MinimumOrderNotionalValue > 0m && price > 0m)
            {
                decimal notional = Math.Abs(quantity) * price;
                if (notional < MinimumOrderNotionalValue)
                {
                    var props = _spdb.GetSymbolProperties(order.Symbol.ID.Market, order.Symbol, order.Symbol.SecurityType, "USDC");
                    decimal lotSize = props?.LotSize ?? 0.01m;
                    decimal adjusted = Math.Ceiling((MinimumOrderNotionalValue / price) / lotSize) * lotSize;
                    quantity = quantity < 0 ? -adjusted : adjusted;
                    Log.Trace($"{Name}.UpdateOrder: Adjusting quantity for {order.Symbol.Value} to {Math.Abs(quantity)} to meet minimum ${MinimumOrderNotionalValue}.");
                }
            }

            // NEU: Aktuelle brokerId als "pending modify" markieren
            // → HandleOrderSocket unterdrückt Cancel-Event für diese OID (nur bei Cancel+Replace Börsen)
            var oldBrokerId = order.BrokerId.LastOrDefault();
            if (oldBrokerId == null)
                return false;

            if (!ExchangeModifiesOrdersInPlace)
            {
                _pendingModifies[oldBrokerId] = 0;
            }

            var res = RunSync(() => ExecuteUpdateOrderAsync(order, GenerateClientId(order.Id), price, quantity));

            if (res?.Success != true)
            {
                // NEU: Bei Fehler pending entfernen da kein Modify stattfindet
                if (!ExchangeModifiesOrdersInPlace)
                {
                    _pendingModifies.TryRemove(oldBrokerId, out _);
                }
                var errorMsg = res?.Error?.ToString() ?? "Unknown exchange error";
                Log.Error($"{Name}.UpdateOrder({order.Symbol.Value}): {errorMsg}");
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "UpdateOrder", errorMsg));
                return false;
            }

            // NEU: Bei Erfolg bleibt _pendingModifies[oldBrokerId] aktiv bis
            // HandleOrderSocket den Cancel-Event empfängt und es entfernt.
            // Die neue OID wird in HandleOrderSocket via clientOrderId nachgetragen.

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
                if (string.IsNullOrEmpty(trade.OrderId)) continue;
                if (!_orderCache.TryGetValue(trade.OrderId, out var order)) continue;

                var tradeQty = trade.Quantity;
                var prevTotal = _filledQtyCache.TryGetValue(trade.OrderId, out var pf) ? pf : 0m;
                var totalFilled = prevTotal + tradeQty;

                _filledQtyCache[trade.OrderId] = totalFilled;

                var status = totalFilled >= Math.Abs(order.Quantity) ? OrderStatus.Filled : OrderStatus.PartiallyFilled;
                var sign = trade.Side == SharedOrderSide.Buy ? 1 : -1;

                if (status == OrderStatus.Filled)
                {
                    if (!_orderCache.TryRemove(trade.OrderId, out _)) continue;
                    _filledQtyCache.TryRemove(trade.OrderId, out _);
                    // NEU: clientOrderId Index aufräumen bei Fill
                    _clientOrderIdToBrokerId.TryRemove(GenerateClientId(order.Id), out _);
                }

                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, new OrderFee(new CashAmount(trade.Fee ?? 0m, trade.FeeAsset ?? "USDC")))
                {
                    Status = status,
                    FillPrice = trade.Price,
                    FillQuantity = tradeQty * sign,
                    Message = "User trade socket"
                });
            }
        }

        private void HandleOrderSocket(DataEvent<SharedFuturesOrder[]> update)
        {
            foreach (var o in update.Data)
            {
                if (string.IsNullOrEmpty(o.OrderId)) continue;

                // FALL 1: Bekannte brokerId → normale Verarbeitung
                if (_orderCache.TryGetValue(o.OrderId, out var order))
                {
                    var totalFilled = o.QuantityFilled?.QuantityInBaseAsset ?? 0m;
                    var status = MapStatus(o.Status, totalFilled);

                    if (status is OrderStatus.Canceled or OrderStatus.Invalid)
                    {
                        // NEU: Prüfen ob Modify-Cancel (Cancel+Replace durch Exchange)
                        if (_pendingModifies.TryRemove(o.OrderId, out _))
                        {
                            // Kein Event an LEAN – echter Cancel kommt erst wenn
                            // neuer Open-Event (FALL 2) die OID nicht aktualisiert
                            // Cache-Eintrag bleibt für FALL 2 erhalten
                            continue;
                        }

                        // Echter Cancel → Cache aufräumen und Event feuern
                        if (!_orderCache.TryRemove(o.OrderId, out _)) continue;
                        _filledQtyCache.TryRemove(o.OrderId, out _);

                        // NEU: clientOrderId Index aufräumen
                        if (!string.IsNullOrEmpty(o.ClientOrderId))
                            _clientOrderIdToBrokerId.TryRemove(o.ClientOrderId, out _);

                        OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
                        {
                            Status = status,
                            Message = "order socket"
                        });
                    }
                }
                // NEU: FALL 2: Unbekannte brokerId → Modify-Replacement erkennen via clientOrderId
                // Hyperliquid erstellt bei modify neue OID; clientOrderId bleibt konstant (= LEAN order.Id)
                else if (!string.IsNullOrEmpty(o.ClientOrderId) &&
                         _clientOrderIdToBrokerId.TryGetValue(o.ClientOrderId, out var oldBrokerId) &&
                         _orderCache.TryGetValue(oldBrokerId, out var existingOrder))
                {
                    // Cache von alter auf neue brokerId umhängen
                    _orderCache.TryRemove(oldBrokerId, out _);
                    _filledQtyCache.TryGetValue(oldBrokerId, out var filledSoFar);
                    _filledQtyCache.TryRemove(oldBrokerId, out _);
                    _pendingModifies.TryRemove(oldBrokerId, out _);

                    _orderCache[o.OrderId] = existingOrder;
                    _filledQtyCache[o.OrderId] = filledSoFar;

                    // NEU: Reverse-Index auf neue brokerId aktualisieren
                    _clientOrderIdToBrokerId[o.ClientOrderId] = o.OrderId;

                    // NEU: LEAN über neue brokerId informieren
                    existingOrder.BrokerId.Add(o.OrderId);
                    OnOrderIdChangedEvent(new BrokerageOrderIdChangedEvent
                    {
                        OrderId = existingOrder.Id,
                        BrokerId = existingOrder.BrokerId
                    });

                    Log.Trace($"{Name}.HandleOrderSocket: Modify detected for {existingOrder.Symbol.Value} " +
                              $"| OldOID: {oldBrokerId} → NewOID: {o.OrderId}");
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

                    var open = await _orderClient.GetOpenFuturesOrdersAsync(
                        new GetOpenOrdersRequest(exchangeParameters: OpenOrdersExchangeParameters)
                    ).ConfigureAwait(false);
                    
                    if (!open.Success || open.Data == null)
                    {
                        Log.Error($"{Name}.ReconcileLoop: Failed to fetch open orders: {open.Error}");
                        continue;
                    }

                    var map = open.Data.ToDictionary(x => x.OrderId);
                    foreach (var kv in _orderCache.ToArray())
                    {
                        if (map.ContainsKey(kv.Key)) continue;

                        // NEU: Überspringe Reconciliation, wenn diese Order gerade modifiziert wird.
                        // Verhindert versehentliche Cancels, wenn REST die alte Order schon als Canceled meldet,
                        // der Socket die neue OID aber noch nicht geliefert hat.
                        if (_pendingModifies.ContainsKey(kv.Key)) continue;

                        var symbol = kv.Value.Symbol;
                        var sharedSymbol = GetSharedSymbol(symbol);

                        if (!_orderCache.TryRemove(kv.Key, out _)) continue;
                        _filledQtyCache.TryRemove(kv.Key, out _);
                        // NEU: clientOrderId Index aufräumen bei Reconcile
                        _clientOrderIdToBrokerId.TryRemove(GenerateClientId(kv.Value.Id), out _);

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

        protected virtual SharedSymbol GetSharedSymbol(Symbol s)
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