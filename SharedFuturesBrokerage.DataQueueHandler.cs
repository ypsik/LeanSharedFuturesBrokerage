using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Packets;
using QuantConnect.Util;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using LeanHistoryRequest = QuantConnect.Data.HistoryRequest;

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    public abstract partial class SharedFuturesBrokerage : Brokerage, IDataQueueHandler
    {
        // --- REST & SOCKET CLIENTS ---
        private readonly IFuturesOrderRestClient _orderClient;
        private readonly IBalanceRestClient _balanceClient;
        private readonly IFuturesOrderSocketClient _orderSocket;
        private readonly IKlineRestClient _klineClient;
        private readonly IFundingRateRestClient _fundingRateClient;
        private readonly Func<List<Holding>> _getHoldingsFunc;

        // --- CONNECTION FIELDS ---
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
            IFundingRateRestClient fundingRateClient,
            IKlineRestClient klineClient,
            Func<List<Holding>> getHoldingsFunc)
            : base(exchangeName)
        {
            _orderClient = orderClient;
            _balanceClient = balanceClient;
            _orderSocket = orderSocket;
            _fundingRateClient = fundingRateClient;
            _klineClient = klineClient;
            _getHoldingsFunc = getHoldingsFunc;

            _aggregator = Composer.Instance.GetPart<IDataAggregator>();

            SubscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();
            SubscriptionManager.SubscribeImpl += (symbols, tickType) => SubscribeSymbols(symbols, tickType);
            SubscriptionManager.UnsubscribeImpl += (symbols, tickType) => UnsubscribeSymbols(symbols, tickType);
        }

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

                if (!res.Success || res.Data == null) yield break;

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
                var klineRequest = new GetKlinesRequest(shared, interval.Value)
                {
                    StartTime = request.StartTimeUtc,
                    EndTime = request.EndTimeUtc
                };

                PageRequest? nextPage = null;
                do
                {
                    var res = RunSync(() => _klineClient.GetKlinesAsync(klineRequest, nextPage));
                    if (!res.Success || res.Data == null || !res.Data.Any()) yield break;

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

        #region Abstract Data Methods & Connection
        protected abstract bool SubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType);
        protected abstract bool UnsubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType);

        protected void EmitTick(Tick tick) => _aggregator.Update(tick);

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
            _orderCache.Clear();
            _filledQtyCache.Clear();
            _isConnected = false;
        }

        protected static T RunSync<T>(Func<Task<T>> f) => f().ConfigureAwait(false).GetAwaiter().GetResult();
        protected static void RunSync(Func<Task> f) => f().ConfigureAwait(false).GetAwaiter().GetResult();
        #endregion
    }
}