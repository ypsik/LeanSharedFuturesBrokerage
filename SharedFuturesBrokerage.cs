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
using QuantConnect.Util;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CxCancelOrderRequest = CryptoExchange.Net.SharedApis.CancelOrderRequest;
using LeanHistoryRequest = QuantConnect.Data.HistoryRequest;

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    public abstract class SharedFuturesBrokerage : Brokerage, IDataQueueHandler
    {
        // --- REST & SOCKET CLIENTS ---
        private readonly IFuturesOrderRestClient _orderClient;
        private readonly IBalanceRestClient _balanceClient;
        private readonly IFuturesOrderSocketClient _orderSocket;
        private readonly IKlineRestClient _klineClient;
        private readonly IFundingRateRestClient _fundingRateClient;
        private readonly Func<List<Holding>> _getHoldingsFunc;

        // --- CACHES ---
        private readonly ConcurrentDictionary<string, Order> _orderCache = new();
        private readonly ConcurrentDictionary<string, decimal> _filledQtyCache = new();

        private UpdateSubscription _orderSocketSub;
        private readonly object _connectLock = new();
        private bool _isConnected;

        private CancellationTokenSource _reconcileCts;
        private Task _reconcileTask;
        private readonly TimeSpan _reconciliationInterval = TimeSpan.FromSeconds(30);

        // --- LEAN DATA MANAGEMENT ---
        protected readonly IDataAggregator _aggregator;
        protected readonly EventBasedDataQueueHandlerSubscriptionManager SubscriptionManager;

        protected SharedFuturesBrokerage(
            string exchangeName,
            IFuturesOrderRestClient orderClient,
            IBalanceRestClient balanceClient,
            IFuturesOrderSocketClient orderSocket,
            IFundingRateRestClient fundingRateClient, // 🔥 Neu
            IKlineRestClient klineClient,             
            Func<List<Holding>> getHoldingsFunc)
            : base(exchangeName)
        {
            _orderClient = orderClient;
            _balanceClient = balanceClient;
            _orderSocket = orderSocket;
            _klineClient = klineClient;
            _getHoldingsFunc = getHoldingsFunc;

            _aggregator = Composer.Instance.GetPart<IDataAggregator>();

            SubscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();
            SubscriptionManager.SubscribeImpl += (symbols, tickType) => SubscribeSymbols(symbols, tickType);
            SubscriptionManager.UnsubscribeImpl += (symbols, tickType) => UnsubscribeSymbols(symbols, tickType);
        }

        #region History Implementation

        #region History Implementation

        public override IEnumerable<BaseData> GetHistory(LeanHistoryRequest request)
        {
            if (request.DataType == typeof(MarginInterestRate))
            {
                if (_fundingRateClient == null)
                {
                    Log.Trace($"{Name} GetHistory: FundingRateRestClient not configured — skipping");
                    yield break;
                }

                var res = RunSync(() =>
                    _fundingRateClient.GetFundingRateHistoryAsync(
                        new GetFundingRateHistoryRequest(GetSharedSymbol(request.Symbol))
                        {
                            StartTime = request.StartTimeUtc,
                            EndTime = request.EndTimeUtc
                        }));

                if (!res.Success || res.Data == null)
                    yield break;

                foreach (var rate in res.Data.OrderBy(r => r.Timestamp))
                {
                    yield return new MarginInterestRate
                    {
                        Symbol = request.Symbol,
                        Time = rate.Timestamp,
                        InterestRate = rate.FundingRate
                    };
                }
            }
            else
            {
                if (_klineClient == null)
                {
                    Log.Trace($"{Name} GetHistory: KlineRestClient not configured — skipping");
                    yield break;
                }

                var shared = GetSharedSymbol(request.Symbol);

                var interval = request.Resolution switch
                {
                    Resolution.Minute => (SharedKlineInterval?)SharedKlineInterval.OneMinute,
                    Resolution.Hour => SharedKlineInterval.OneHour,
                    Resolution.Daily => SharedKlineInterval.OneDay,
                    _ => null
                };

                if (interval == null) yield break;

                var barSize = request.Resolution.ToTimeSpan();
                var batchEnd = request.EndTimeUtc;
                var current = request.StartTimeUtc;

                var klineRequest = new GetKlinesRequest(shared, interval.Value)
                {
                    StartTime = current,
                    EndTime = batchEnd
                };

                PageRequest? nextPage = null;

                do
                {
                    var res = RunSync(() =>
                        _klineClient.GetKlinesAsync(klineRequest, nextPage));

                    if (!res.Success || res.Data == null || !res.Data.Any())
                        yield break;

                    foreach (var bar in res.Data.OrderBy(b => b.OpenTime))
                    {
                        yield return new TradeBar
                        {
                            Symbol = request.Symbol,
                            Time = bar.OpenTime,
                            Open = bar.OpenPrice,
                            High = bar.HighPrice,
                            Low = bar.LowPrice,
                            Close = bar.ClosePrice,
                            Volume = bar.Volume,
                            Period = barSize
                        };
                    }

                    nextPage = res.NextPageRequest;
                }
                while (nextPage != null);
            }
        }

        #endregion


        #endregion

        #region IDataQueueHandler Implementation

        public virtual IEnumerator<BaseData> Subscribe(SubscriptionDataConfig config, EventHandler handler)
        {
            if (config.Symbol.Value.Contains("UNMAPPED") || config.Symbol.IsCanonical())
                return null;

            var enumerator = _aggregator.Add(config, handler);
            SubscriptionManager.Subscribe(config);
            return enumerator;
        }

        public virtual void Unsubscribe(SubscriptionDataConfig config)
        {
            SubscriptionManager.Unsubscribe(config);
            _aggregator.Remove(config);
        }

        public void SetJob(LiveNodePacket job) { }

        public IEnumerable<BaseData> GetNextTicks() => Enumerable.Empty<BaseData>();

        #endregion

        #region Abstract Data Methods

        protected abstract bool SubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType);
        protected abstract bool UnsubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType);

        protected void EmitTick(Tick tick)
        {
            _aggregator.Update(tick);
        }

        #endregion

        #region Connection
        public override bool IsConnected => _isConnected;

        public override void Connect()
        {
            lock (_connectLock)
            {
                if (_isConnected) return;

                if (_balanceClient == null || _orderSocket == null)
                    throw new InvalidOperationException("Clients not configured");

                var auth = RunSync(() => _balanceClient.GetBalancesAsync(new GetBalancesRequest()));

                if (!auth.Success)
                    throw new Exception("Authentication failed");

                var sub = RunSync(() =>
                    _orderSocket.SubscribeToFuturesOrderUpdatesAsync(
                        new SubscribeFuturesOrderRequest(),
                        HandleSocket));

                if (!sub.Success)
                    throw new Exception("Order socket failed");

                _orderSocketSub = sub.Data;
                _isConnected = true;

                _reconcileCts = new CancellationTokenSource();
                _reconcileTask = Task.Run(() => ReconcileLoop(_reconcileCts.Token));
            }
        }

        public override void Disconnect()
        {
            _reconcileCts?.Cancel();

            if (_orderSocketSub != null)
                RunSync(() => _orderSocketSub.CloseAsync());

            _orderCache.Clear();
            _filledQtyCache.Clear();

            _isConnected = false;
        }
        #endregion

        #region Orders
        public override List<Order> GetOpenOrders()
        {
            var res = RunSync(() => _orderClient.GetOpenFuturesOrdersAsync(new GetOpenOrdersRequest()));

            if (!res.Success || res.Data == null)
                return new List<Order>();

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
            var request = new PlaceFuturesOrderRequest(
                GetSharedSymbol(order.Symbol),
                order.Quantity > 0 ? SharedOrderSide.Buy : SharedOrderSide.Sell,
                order.Type == OrderType.Limit ? SharedOrderType.Limit : SharedOrderType.Market,
                new SharedQuantity { QuantityInBaseAsset = Math.Abs(order.Quantity) })
            {
                Price = (order as LimitOrder)?.LimitPrice
            };

            var res = RunSync(() => _orderClient.PlaceFuturesOrderAsync(request));
            if (!res.Success) return false;

            order.BrokerId.Add(res.Data.Id);
            _orderCache[res.Data.Id] = order;
            _filledQtyCache[res.Data.Id] = 0m;


            OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
            {
                Status = OrderStatus.Submitted
            });

            return true;
        }

        public override bool CancelOrder(Order order)
        {
            if (!order.BrokerId.Any()) return false;
            var id = order.BrokerId.First();

            var res = RunSync(() =>
                _orderClient.CancelFuturesOrderAsync(
                    new CxCancelOrderRequest(GetSharedSymbol(order.Symbol), id)));

            return res.Success;
        }

        public override bool UpdateOrder(Order order) => false;

        private void HandleSocket(DataEvent<SharedFuturesOrder[]> update)
        {
            foreach (var o in update.Data)
            {
                if (string.IsNullOrEmpty(o.OrderId)) continue;

                if (!_orderCache.TryGetValue(o.OrderId, out var order))
                {
                    Log.Trace($"{Name} unknown order {o.OrderId}");
                    continue;
                }

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

        private async Task ReconcileLoop(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(_reconciliationInterval, ct);

                var open = await _orderClient
                    .GetOpenFuturesOrdersAsync(new GetOpenOrdersRequest())
                    .ConfigureAwait(false);

                if (!open.Success || open.Data == null) continue;

                var map = open.Data.ToDictionary(x => x.OrderId);

                foreach (var kv in _orderCache.ToArray())
                {
                    if (map.ContainsKey(kv.Key)) continue;

                    OnOrderEvent(new OrderEvent(kv.Value, DateTime.UtcNow, OrderFee.Zero)
                    {
                        Status = OrderStatus.Canceled,
                        Message = "reconcile"
                    });

                    _orderCache.TryRemove(kv.Key, out _);
                    _filledQtyCache.TryRemove(kv.Key, out _);
                }
            }
        }
        #endregion

        #region Cash / Holdings
        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() => _balanceClient.GetBalancesAsync(new GetBalancesRequest()));

            return res.Success && res.Data != null
                ? res.Data.Select(x => new CashAmount(x.Available, x.Asset ?? "USDC")).ToList()
                : new List<CashAmount>();
        }

        public override List<Holding> GetAccountHoldings()
            => _getHoldingsFunc?.Invoke() ?? new List<Holding>();
        #endregion

        #region Helpers
        protected virtual string NormalizeSymbol(string rawSymbol) => rawSymbol;

        protected virtual SharedSymbol GetSharedSymbol(Symbol s)
            => new SharedSymbol(TradingMode.PerpetualLinear, s.Value, "USDC");

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

        protected static T RunSync<T>(Func<Task<T>> f)
            => f().ConfigureAwait(false).GetAwaiter().GetResult();

        protected static void RunSync(Func<Task> f)
            => f().ConfigureAwait(false).GetAwaiter().GetResult();
        #endregion
    }
}