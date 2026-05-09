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
// Explicit alias resolves ambiguity between QuantConnect.Data.HistoryRequest
// and QuantConnect.Packets.HistoryRequest.
using LeanHistoryRequest = QuantConnect.Data.HistoryRequest;

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    /// <summary>
    /// Exchange-agnostic base brokerage for perpetual futures via CryptoExchange.Net SharedApis.
    ///
    /// Provides:
    ///  - Order execution  (PlaceOrder / CancelOrder / reconcile loop)
    ///  - Live price ticks (IDataQueueHandler — ticker socket → _ticks)
    ///  - Funding & Kline history (GetHistory for warmup via RestClients)
    ///
    /// Derived classes must override:
    ///  - NormalizeSymbol()      exchange symbol → LEAN ticker
    ///  - GetSharedSymbol()      LEAN symbol      → exchange SharedSymbol
    ///  - Subscribe()            override for exchange-specific live data types
    /// </summary>
    public class SharedFuturesBrokerage : Brokerage, IDataQueueHandler
    {
        private readonly IFuturesOrderRestClient _orderClient;
        private readonly IBalanceRestClient _balanceClient;
        protected readonly ITickerSocketClient _tickerSocket;
        private readonly IFuturesOrderSocketClient _orderSocket;
        private readonly IFundingRateRestClient _fundingRateClient;
        private readonly IKlineRestClient _klineClient;
        private readonly Func<List<Holding>> _getHoldingsFunc;

        private readonly ConcurrentDictionary<string, Order> _orderCache = new();
        private readonly ConcurrentDictionary<string, decimal> _filledQtyCache = new();

        protected readonly ConcurrentDictionary<Symbol, UpdateSubscription> _subscriptionMap = new();
        protected readonly ConcurrentDictionary<Symbol, byte> _pendingSubscriptions = new();

        // Tick + MarginInterestRate queue — drained by LEAN via GetNextTicks().
        protected readonly ConcurrentQueue<BaseData> _ticks = new();

        protected const int MaxQueueSize = 10_000;

        private UpdateSubscription _orderSocketSub;
        private readonly object _connectLock = new();
        private bool _isConnected;

        private CancellationTokenSource _reconcileCts;
        private Task _reconcileTask;

        private readonly TimeSpan _reconciliationInterval = TimeSpan.FromSeconds(30);

        public SharedFuturesBrokerage(
            string exchangeName)
            : base(exchangeName)
        {
        }

        public SharedFuturesBrokerage(
            string exchangeName,
            IFuturesOrderRestClient orderClient,
            IBalanceRestClient balanceClient,
            ITickerSocketClient tickerSocket,
            IFuturesOrderSocketClient orderSocket,
            IFundingRateRestClient fundingRateClient,
            IKlineRestClient klineClient,
            Func<List<Holding>> getHoldingsFunc)
            : base(exchangeName)
        {
            _orderClient = orderClient;
            _balanceClient = balanceClient;
            _tickerSocket = tickerSocket;
            _orderSocket = orderSocket;
            _fundingRateClient = fundingRateClient;
            _klineClient = klineClient;
            _getHoldingsFunc = getHoldingsFunc;
        }

        #region Sync

        protected static T RunSync<T>(Func<Task<T>> f) =>
            f().ConfigureAwait(false).GetAwaiter().GetResult();

        protected static void RunSync(Func<Task> f) =>
            f().ConfigureAwait(false).GetAwaiter().GetResult();

        #endregion

        #region Connection

        public override bool IsConnected => _isConnected;

        public override void Connect()
        {
            lock (_connectLock)
            {
                if (_isConnected) return;

                var auth = RunSync(() =>
                    _balanceClient.GetBalancesAsync(new GetBalancesRequest()));

                if (!auth.Success)
                    throw new Exception(auth.Error?.Message);

                var sub = RunSync(() =>
                    _orderSocket.SubscribeToFuturesOrderUpdatesAsync(
                        new SubscribeFuturesOrderRequest(),
                        HandleSocket));

                if (!sub.Success)
                    throw new Exception(sub.Error?.Message);

                _orderSocketSub = sub.Data;
                _isConnected = true;

                _reconcileCts = new CancellationTokenSource();
                _reconcileTask = Task.Factory.StartNew(
                    () => ReconcileLoop(_reconcileCts.Token),
                    _reconcileCts.Token,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default).Unwrap();
            }
        }

        public override void Disconnect()
        {
            _reconcileCts?.Cancel();
            try { _reconcileTask?.Wait(5000); } catch { }

            RunSync(() => _orderSocketSub?.CloseAsync());
            _orderSocketSub = null;

            foreach (var kv in _subscriptionMap)
                RunSync(() => kv.Value.CloseAsync());

            _subscriptionMap.Clear();
            _orderCache.Clear();
            _filledQtyCache.Clear();
            _isConnected = false;
        }

        #endregion

        #region Socket — Order Updates

        private void HandleSocket(DataEvent<SharedFuturesOrder[]> update)
        {
            foreach (var o in update.Data)
            {
                if (string.IsNullOrEmpty(o.OrderId))
                    continue;

                if (!_orderCache.TryGetValue(o.OrderId, out var order))
                {
                    Log.Trace($"{Name} HandleSocket: unknown order {o.OrderId} — skipping");
                    continue;
                }

                var totalFilled = o.QuantityFilled?.QuantityInBaseAsset ?? 0m;
                var prev = _filledQtyCache.TryGetValue(o.OrderId, out var pf) ? pf : 0m;
                var delta = totalFilled - prev;
                var status = MapStatus(o.Status, totalFilled);

                if (delta == 0 && status == OrderStatus.Submitted)
                    continue;

                _filledQtyCache[o.OrderId] = totalFilled;

                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
                {
                    Status = status,
                    FillPrice = o.AveragePrice ?? o.OrderPrice ?? 0m,
                    FillQuantity = delta * (order.Quantity > 0 ? 1 : -1),
                    Message = "socket"
                });

                if (status is OrderStatus.Filled
                           or OrderStatus.Canceled
                           or OrderStatus.Invalid)
                {
                    _orderCache.TryRemove(o.OrderId, out _);
                    _filledQtyCache.TryRemove(o.OrderId, out _);
                }
            }
        }

        #endregion

        #region Orders

        public override List<Order> GetOpenOrders()
        {
            var res = RunSync(() =>
                _orderClient.GetOpenFuturesOrdersAsync(new GetOpenOrdersRequest()));

            if (!res.Success || res.Data == null)
                return new List<Order>();

            return res.Data.Select(o =>
            {
                var symbol = Symbol.Create(NormalizeSymbol(o.Symbol), SecurityType.CryptoFuture, Name);
                var qty = (o.OrderQuantity?.QuantityInBaseAsset ?? 0m)
                             * (o.Side == SharedOrderSide.Sell ? -1 : 1);

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

            if (!res.Success) return false;

            OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
            {
                Status = OrderStatus.CancelPending
            });

            return true;
        }

        public override bool UpdateOrder(Order order) => false;

        #endregion

        #region Reconcile

        private async Task ReconcileLoop(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try { await Task.Delay(_reconciliationInterval, ct); }
                catch { break; }

                try
                {
                    var open = await _orderClient
                        .GetOpenFuturesOrdersAsync(new GetOpenOrdersRequest())
                        .ConfigureAwait(false);

                    if (!open.Success || open.Data == null)
                        continue;

                    var map = open.Data
                        .GroupBy(x => x.OrderId)
                        .ToDictionary(g => g.Key, g => g.First());

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
                catch (Exception ex) { Log.Error(ex); }
            }
        }

        #endregion

        #region Cash / Holdings

        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() =>
                _balanceClient.GetBalancesAsync(new GetBalancesRequest()));

            return res.Success && res.Data != null
                ? res.Data.Select(x => new CashAmount(x.Available, x.Asset ?? "USDC")).ToList()
                : new List<CashAmount>();
        }

        public override List<Holding> GetAccountHoldings()
        {
            try { return _getHoldingsFunc?.Invoke() ?? new List<Holding>(); }
            catch { return new List<Holding>(); }
        }

        #endregion

        #region History

        /// <summary>
        /// Handles MarginInterestRate and TradeBar history for warmup.
        /// </summary>
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

        #region IDataQueueHandler

        /// <summary>
        /// Subscribes to live price ticks via ticker socket.
        /// Override in derived class to handle additional data types
        /// (e.g. MarginInterestRate via exchange-specific WebSocket).
        /// </summary>
        public virtual IEnumerator<BaseData> Subscribe(
            SubscriptionDataConfig config,
            EventHandler handler)
        {
            var symbol = config.Symbol;
            var shared = GetSharedSymbol(symbol);

            var sub = RunSync(() =>
                _tickerSocket.SubscribeToTickerUpdatesAsync(
                    new SubscribeTickerRequest(shared),
                    update =>
                    {
                        var price = update.Data.LastPrice;

                        if (price == null || price <= 0m) return;

                        if (_ticks.Count > MaxQueueSize)
                        {
                            _ticks.TryDequeue(out _);
                        }

                        var time = update.DataTimeLocal ?? DateTime.UtcNow;

                        BaseData data = config.Type switch
                        {
                            Type t when t == typeof(TradeBar) => CreateTradeBar(symbol, price.Value, time, config),
                            Type t when t == typeof(QuoteBar) => CreateQuoteBar(symbol, price.Value, time, config),
                            _ => CreateTick(symbol, price.Value, time)
                        };

                        _ticks.Enqueue(data);
                        handler?.Invoke(this, EventArgs.Empty);
                    }));

            if (sub.Success)
                _subscriptionMap[symbol] = sub.Data;

            return Enumerable.Empty<BaseData>().GetEnumerator();
        }

        public void Unsubscribe(SubscriptionDataConfig config)
        {
            if (_subscriptionMap.TryRemove(config.Symbol, out var sub))
                RunSync(() => sub.CloseAsync());
        }

        public IEnumerable<BaseData> GetNextTicks()
        {
            while (_ticks.TryDequeue(out var tick))
                yield return tick;
        }

        public void SetJob(LiveNodePacket job) { }

        #endregion

        #region Factory Methods

        private TradeBar CreateTradeBar(Symbol symbol, decimal price, DateTime time, SubscriptionDataConfig config)
        {
            return new TradeBar
            {
                Symbol = symbol,
                Time = time,
                Open = price,
                High = price,
                Low = price,
                Close = price,
                Volume = 0m, // Wichtig: 24h Volumen hier ignorieren
                Period = config.Resolution.ToTimeSpan()
            };
        }

        private QuoteBar CreateQuoteBar(Symbol symbol, decimal price, DateTime time, SubscriptionDataConfig config)
        {
            return new QuoteBar
            {
                Symbol = symbol,
                Time = time,
                Value = price,
                Ask = new Bar(price, price, price, price),
                Bid = new Bar(price, price, price, price),
                Period = config.Resolution.ToTimeSpan()
            };
        }

        private Tick CreateTick(Symbol symbol, decimal price, DateTime time)
        {
            return new Tick
            {
                Symbol = symbol,
                Time = time,
                Value = price,
                TickType = TickType.Trade,
                Quantity = 0m // Wichtig: 24h Volumen hier ignorieren
            };
        }

        #endregion

        #region Helpers

        protected virtual string NormalizeSymbol(string rawSymbol) => rawSymbol;

        protected virtual SharedSymbol GetSharedSymbol(Symbol s)
            => new(TradingMode.PerpetualLinear, s.Value, "USDC");

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