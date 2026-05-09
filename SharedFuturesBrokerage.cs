using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Packets;
using QuantConnect.Securities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CxCancelOrderRequest = CryptoExchange.Net.SharedApis.CancelOrderRequest;

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    public class SharedFuturesBrokerage : Brokerage, IDataQueueHandler
    {
        private readonly IFuturesOrderRestClient _orderClient;
        private readonly IBalanceRestClient _balanceClient;
        private readonly IKlineSocketClient _klineSocket;
        private readonly IFuturesOrderSocketClient _orderSocket;
        private readonly IFundingRateRestClient _fundingRateClient;
        private readonly IKlineRestClient _klineClient;
        private readonly Func<List<Holding>> _getHoldingsFunc;

        private readonly ConcurrentDictionary<string, Order> _orderCache = new();
        private readonly ConcurrentDictionary<string, decimal> _filledQtyCache = new();

        protected readonly ConcurrentDictionary<Symbol, UpdateSubscription> _subscriptionMap = new();
        protected readonly ConcurrentDictionary<Symbol, byte> _pendingSubscriptions = new();

        // FIX: Speichert die Handler für die Symbole
        private readonly ConcurrentDictionary<Symbol, EventHandler> _dataHandlers = new();

        protected const int MaxQueueSize = 10_000;
        protected readonly BlockingCollection<BaseData> _ticks = new(MaxQueueSize);

        private UpdateSubscription _orderSocketSub;
        private readonly object _connectLock = new();
        private bool _isConnected;

        private CancellationTokenSource _reconcileCts;
        private Task _reconcileTask;

        private readonly TimeSpan _reconciliationInterval = TimeSpan.FromSeconds(30);

        public SharedFuturesBrokerage(string exchangeName) : base(exchangeName) { }

        public SharedFuturesBrokerage(
            string exchangeName,
            IFuturesOrderRestClient orderClient,
            IBalanceRestClient balanceClient,
            IKlineSocketClient klineSocket,
            IFuturesOrderSocketClient orderSocket,
            IFundingRateRestClient fundingRateClient,
            IKlineRestClient klineClient,
            Func<List<Holding>> getHoldingsFunc)
            : base(exchangeName)
        {
            _orderClient = orderClient;
            _balanceClient = balanceClient;
            _klineSocket = klineSocket;
            _orderSocket = orderSocket;
            _fundingRateClient = fundingRateClient;
            _klineClient = klineClient;
            _getHoldingsFunc = getHoldingsFunc;
        }

        #region IDataQueueHandler Implementation

        public virtual IEnumerator<BaseData> Subscribe(SubscriptionDataConfig config, EventHandler handler)
        {
            var symbol = config.Symbol;
            if (symbol.Value.Contains("UNMAPPED")) return null;

            // Handler registrieren
            _dataHandlers[symbol] = handler;

            var shared = GetSharedSymbol(symbol);
            var interval = config.Resolution switch
            {
                Resolution.Minute => SharedKlineInterval.OneMinute,
                Resolution.Hour => SharedKlineInterval.OneHour,
                _ => SharedKlineInterval.OneMinute
            };

            var sub = RunSync(() =>
                _klineSocket.SubscribeToKlineUpdatesAsync(
                    new SubscribeKlineRequest(shared, interval),
                    update =>
                    {
                        var k = update.Data;
                        var bar = new TradeBar
                        {
                            Symbol = symbol,
                            Time = k.OpenTime.ToUniversalTime(),
                            Open = k.OpenPrice,
                            High = k.HighPrice,
                            Low = k.LowPrice,
                            Close = k.ClosePrice,
                            Volume = k.Volume,
                            Period = config.Resolution.ToTimeSpan(),
                            DataType = MarketDataType.TradeBar
                        };

                        EnqueueTick(bar);
                    }));

            if (sub.Success)
                _subscriptionMap[symbol] = sub.Data;
            else
                Log.Error($"{Name}.Subscribe failed: {sub.Error}");

            // FIX: NULL zurückgeben. LEAN nutzt dann GetNextTicks() für die Synchronisation.
            return null;
        }

        public void Unsubscribe(SubscriptionDataConfig config)
        {
            var symbol = config.Symbol;
            _dataHandlers.TryRemove(symbol, out _);
            if (_subscriptionMap.TryRemove(symbol, out var sub))
            {
                RunSync(() => sub.CloseAsync());
            }
        }

        /// <summary>
        /// Zentraler Ausgabepunkt für LEAN. Hier zieht LEAN alle Daten ab.
        /// </summary>
        public IEnumerable<BaseData> GetNextTicks()
        {
            while (_ticks.TryTake(out var tick))
            {
                yield return tick;
            }
        }

        // Hilfsmethode für sicheres Enqueue + Handler Trigger
        protected void EnqueueTick(BaseData tick)
        {
            if (!_ticks.TryAdd(tick))
            {
                _ticks.TryTake(out _);
                _ticks.TryAdd(tick);
            }

            // Handler triggern, damit LEAN weiß, dass Daten zum Abholen bereitliegen
            if (_dataHandlers.TryGetValue(tick.Symbol, out var handler))
            {
                handler?.Invoke(this, EventArgs.Empty);
            }
        }

        public void SetJob(LiveNodePacket job) { }

        #endregion

        #region Connection & Orders (Unchanged but ensured)

        public override bool IsConnected => _isConnected;

        public override void Connect()
        {
            lock (_connectLock)
            {
                if (_isConnected) return;
                if (_balanceClient == null || _orderSocket == null) throw new InvalidOperationException("Clients not configured");

                var auth = RunSync(() => _balanceClient.GetBalancesAsync(new GetBalancesRequest()));
                if (!auth.Success) throw new Exception("Authentication failed");

                var sub = RunSync(() => _orderSocket.SubscribeToFuturesOrderUpdatesAsync(new SubscribeFuturesOrderRequest(), HandleSocket));
                if (!sub.Success) throw new Exception("Order socket failed");

                _orderSocketSub = sub.Data;
                _isConnected = true;
                _reconcileCts = new CancellationTokenSource();
                _reconcileTask = Task.Run(() => ReconcileLoop(_reconcileCts.Token));
            }
        }

        public override void Disconnect()
        {
            _reconcileCts?.Cancel();
            if (_orderSocketSub != null) RunSync(() => _orderSocketSub.CloseAsync());
            foreach (var kv in _subscriptionMap) RunSync(() => kv.Value.CloseAsync());
            _subscriptionMap.Clear();
            _orderCache.Clear();
            _filledQtyCache.Clear();
            _isConnected = false;
        }

        private void HandleSocket(DataEvent<SharedFuturesOrder[]> update)
        {
            foreach (var o in update.Data)
            {
                if (string.IsNullOrEmpty(o.OrderId) || !_orderCache.TryGetValue(o.OrderId, out var order)) continue;

                var totalFilled = o.QuantityFilled?.QuantityInBaseAsset ?? 0m;
                var prev = _filledQtyCache.TryGetValue(o.OrderId, out var pf) ? pf : 0m;
                var delta = totalFilled - prev;
                var status = MapStatus(o.Status, totalFilled);

                if (delta == 0 && status == OrderStatus.Submitted) continue;

                _filledQtyCache[o.OrderId] = totalFilled;
                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
                {
                    Status = status,
                    FillPrice = o.AveragePrice ?? o.OrderPrice ?? 0m,
                    FillQuantity = delta * (order.Quantity > 0 ? 1 : -1),
                    Message = "socket"
                });

                if (status is OrderStatus.Filled or OrderStatus.Canceled or OrderStatus.Invalid)
                {
                    _orderCache.TryRemove(o.OrderId, out _);
                    _filledQtyCache.TryRemove(o.OrderId, out _);
                }
            }
        }

        public override List<Order> GetOpenOrders()
        {
            var res = RunSync(() => _orderClient.GetOpenFuturesOrdersAsync(new GetOpenOrdersRequest()));
            if (!res.Success || res.Data == null) return new List<Order>();
            return res.Data.Select(o => {
                var symbol = Symbol.Create(NormalizeSymbol(o.Symbol), SecurityType.CryptoFuture, Name);
                var qty = (o.OrderQuantity?.QuantityInBaseAsset ?? 0m) * (o.Side == SharedOrderSide.Sell ? -1 : 1);
                Order order = o.OrderType == SharedOrderType.Limit ? new LimitOrder(symbol, qty, o.OrderPrice ?? 0m, DateTime.UtcNow) : new MarketOrder(symbol, qty, DateTime.UtcNow);
                order.BrokerId.Add(o.OrderId);
                order.Status = MapStatus(o.Status, o.QuantityFilled?.QuantityInBaseAsset ?? 0m);
                _orderCache[o.OrderId] = order;
                _filledQtyCache[o.OrderId] = o.QuantityFilled?.QuantityInBaseAsset ?? 0m;
                return order;
            }).ToList();
        }

        public override bool PlaceOrder(Order order)
        {
            var request = new PlaceFuturesOrderRequest(GetSharedSymbol(order.Symbol), order.Quantity > 0 ? SharedOrderSide.Buy : SharedOrderSide.Sell, order.Type == OrderType.Limit ? SharedOrderType.Limit : SharedOrderType.Market, new SharedQuantity { QuantityInBaseAsset = Math.Abs(order.Quantity) }) { Price = (order as LimitOrder)?.LimitPrice };
            var res = RunSync(() => _orderClient.PlaceFuturesOrderAsync(request));
            if (!res.Success) return false;
            order.BrokerId.Add(res.Data.Id);
            _orderCache[res.Data.Id] = order;
            _filledQtyCache[res.Data.Id] = 0m;
            OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero) { Status = OrderStatus.Submitted });
            return true;
        }

        public override bool CancelOrder(Order order)
        {
            if (!order.BrokerId.Any()) return false;
            var res = RunSync(() => _orderClient.CancelFuturesOrderAsync(new CxCancelOrderRequest(GetSharedSymbol(order.Symbol), order.BrokerId.First())));
            return res.Success;
        }

        public override bool UpdateOrder(Order order) => false;

        private async Task ReconcileLoop(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(_reconciliationInterval, ct);
                var open = await _orderClient.GetOpenFuturesOrdersAsync(new GetOpenOrdersRequest()).ConfigureAwait(false);
                if (!open.Success || open.Data == null) continue;
                var map = open.Data.ToDictionary(x => x.OrderId);
                foreach (var kv in _orderCache.ToArray())
                {
                    if (map.ContainsKey(kv.Key)) continue;
                    OnOrderEvent(new OrderEvent(kv.Value, DateTime.UtcNow, OrderFee.Zero) { Status = OrderStatus.Canceled, Message = "reconcile" });
                    _orderCache.TryRemove(kv.Key, out _);
                    _filledQtyCache.TryRemove(kv.Key, out _);
                }
            }
        }

        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() => _balanceClient.GetBalancesAsync(new GetBalancesRequest()));
            return res.Success && res.Data != null ? res.Data.Select(x => new CashAmount(x.Available, x.Asset ?? "USDC")).ToList() : new List<CashAmount>();
        }

        public override List<Holding> GetAccountHoldings() => _getHoldingsFunc?.Invoke() ?? new List<Holding>();

        protected virtual string NormalizeSymbol(string rawSymbol) => rawSymbol;
        protected virtual SharedSymbol GetSharedSymbol(Symbol s) => new SharedSymbol(TradingMode.PerpetualLinear, s.Value, "USDC");
        private OrderStatus MapStatus(SharedOrderStatus status, decimal filled)
        {
            if (status == SharedOrderStatus.Open) return filled > 0 ? OrderStatus.PartiallyFilled : OrderStatus.Submitted;
            return status switch { SharedOrderStatus.Filled => OrderStatus.Filled, SharedOrderStatus.Canceled => OrderStatus.Canceled, _ => OrderStatus.None };
        }

        protected static T RunSync<T>(Func<Task<T>> f) => f().ConfigureAwait(false).GetAwaiter().GetResult();
        protected static void RunSync(Func<Task> f) => f().ConfigureAwait(false).GetAwaiter().GetResult();

        #endregion
    }
}