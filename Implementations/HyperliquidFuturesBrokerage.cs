using System;
using System.Collections.Generic;
using System.Linq;
using CryptoExchange.Net.SharedApis;
using HyperLiquid.Net.Clients;
using QuantConnect;
using QuantConnect.Data;
using LeanHistoryRequest = QuantConnect.Data.HistoryRequest;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using SilverQuant.Lean.Brokerages.Futures.Shared;

namespace SilverQuant.Lean.Brokerages.Futures.Hyperliquid
{
    /// <summary>
    /// Hyperliquid-specific overrides on top of SharedFuturesBrokerage.
    ///
    /// Adds:
    ///   - Symbol normalization    ("BTC" ↔ "BTCUSDC")
    ///   - FundingRateRestClient   → funding rate history for warmup via REST
    ///   - Subscribe()             → MarginInterestRate via SubscribeToUserFundingUpdatesAsync
    ///                               (single subscription for all coins, dispatches per symbol)
    ///                               price ticks delegate to base.Subscribe()
    ///   - GetHistory()            → TradeBar (klines) + base for MarginInterestRate
    /// </summary>
    public class HyperliquidFuturesBrokerage : SharedFuturesBrokerage
    {
        private readonly HyperLiquidRestClient _restClient;
        private readonly HyperLiquidSocketClient _socketClient;

        // Funding subscription is global (all coins in one channel).
        // Guard ensures we only subscribe once regardless of how many symbols
        // request MarginInterestRate.
        private CryptoExchange.Net.Objects.Sockets.UpdateSubscription _fundingSubscription;
        private readonly object _fundingSubLock = new();

        public HyperliquidFuturesBrokerage(
            HyperLiquidRestClient restClient,
            HyperLiquidSocketClient socketClient,
            Func<List<Holding>> getHoldingsFunc)
            : base(
                "HyperLiquid",
                restClient.FuturesApi.SharedClient,    // IFuturesOrderRestClient
                restClient.FuturesApi.SharedClient,    // IBalanceRestClient
                socketClient.FuturesApi.SharedClient,  // ITickerSocketClient
                socketClient.FuturesApi.SharedClient,  // IFuturesOrderSocketClient
                getHoldingsFunc)
        {
            _restClient = restClient;
            _socketClient = socketClient;
        }

        // ── Funding rate REST client (warmup history) ────────────────────

        protected override IFundingRateRestClient FundingRateRestClient
            => _restClient.FuturesApi.SharedClient;

        // ── Symbol mapping ───────────────────────────────────────────────

        protected override string NormalizeSymbol(string rawSymbol)
            => rawSymbol.EndsWith("USDC", StringComparison.OrdinalIgnoreCase)
                ? rawSymbol.ToUpperInvariant()
                : rawSymbol.ToUpperInvariant() + "USDC";

        protected override SharedSymbol GetSharedSymbol(Symbol s)
        {
            var ticker = s.Value.ToUpperInvariant();
            var coin = ticker.EndsWith("USDC") ? ticker[..^4] : ticker;
            return new SharedSymbol(TradingMode.PerpetualLinear, coin, "USDC");
        }

        // ── Subscribe ────────────────────────────────────────────────────

        public override IEnumerator<BaseData> Subscribe(
            SubscriptionDataConfig config,
            EventHandler handler)
        {
            if (config.Type == typeof(MarginInterestRate))
            {
                // SubscribeToUserFundingUpdatesAsync delivers funding payments
                // for ALL coins in a single channel. Subscribe once, dispatch
                // per coin into _ticks so LEAN sees MarginInterestRate per symbol.
                lock (_fundingSubLock)
                {
                    if (_fundingSubscription == null)
                    {
                        var sub = RunSync(() =>
                            _socketClient.FuturesApi.Account
                                .SubscribeToUserFundingUpdatesAsync(
                                    null, // null = use API credentials
                                    update =>
                                    {
                                        foreach (var funding in update.Data)
                                        {
                                            if (_ticks.Count >= MaxQueueSize) break;

                                            var leanTicker = NormalizeSymbol(funding.Symbol);
                                            var sym = Symbol.Create(
                                                leanTicker,
                                                SecurityType.CryptoFuture,
                                                "HyperLiquid");

                                            _ticks.Enqueue(new MarginInterestRate
                                            {
                                                Symbol = sym,
                                                Time = funding.Timestamp ?? DateTime.UtcNow,
                                                InterestRate = funding.FundingRate
                                            });
                                        }

                                        handler?.Invoke(this, EventArgs.Empty);
                                    }));

                        if (sub.Success)
                            _fundingSubscription = sub.Data;
                    }
                }

                return GetNextTicks().GetEnumerator();
            }

            // Price ticks — handled generically by base class.
            return base.Subscribe(config, handler);
        }

        // ── History ──────────────────────────────────────────────────────

        /// <summary>
        /// TradeBar           → Hyperliquid klines via REST
        /// MarginInterestRate → funding rate history via base class
        /// </summary>
        public override IEnumerable<BaseData> GetHistory(LeanHistoryRequest request)
        {
            if (request.DataType == typeof(MarginInterestRate))
                return base.GetHistory(request);

            return GetKlineHistory(request);
        }

        private IEnumerable<BaseData> GetKlineHistory(LeanHistoryRequest request)
        {
            var ticker = request.Symbol.Value.ToUpperInvariant();
            var coin = ticker.EndsWith("USDC") ? ticker[..^4] : ticker;
            var shared = new SharedSymbol(TradingMode.PerpetualLinear, coin, "USDC");

            var interval = request.Resolution switch
            {
                Resolution.Minute => (SharedKlineInterval?)SharedKlineInterval.OneMinute,
                Resolution.Hour => SharedKlineInterval.OneHour,
                Resolution.Daily => SharedKlineInterval.OneDay,
                _ => null
            };

            if (interval == null)
                yield break;

            var res = RunSync(() =>
                _restClient.FuturesApi.SharedClient.GetKlinesAsync(
                    new GetKlinesRequest(shared, interval.Value)
                    {
                        StartTime = request.StartTimeUtc,
                        EndTime = request.EndTimeUtc
                    }));

            if (!res.Success || res.Data == null)
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
                    Period = request.Resolution.ToTimeSpan()
                };
            }
        }
    }
}