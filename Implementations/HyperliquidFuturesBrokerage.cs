using System;
using System.Collections.Generic;
using System.Linq;
using CryptoExchange.Net.SharedApis;
using HyperLiquid.Net.Clients;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Securities;
using SilverQuant.Lean.Brokerages.Futures.Shared;

namespace SilverQuant.Lean.Brokerages.Futures.Hyperliquid
{
    /// <summary>
    /// Hyperliquid-specific overrides on top of SharedFuturesBrokerage.
    ///
    /// Adds:
    ///   - Symbol normalization  ("BTC" ↔ "BTCUSDC")
    ///   - GetHistory()          warmup bars via Hyperliquid REST klines
    ///
    /// Everything else (orders, ticks, reconcile) is handled by the base class.
    /// </summary>
    public class HyperliquidFuturesBrokerage : SharedFuturesBrokerage
    {
        private readonly HyperLiquidRestClient _restClient;

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
        }

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

        // ── History / Warmup ─────────────────────────────────────────────

        /// <summary>
        /// Called by LEAN during SetWarmup() when config contains:
        ///   "history-provider": "BrokerageHistoryProvider"
        /// </summary>
        public override IEnumerable<BaseData> GetHistory(HistoryRequest request)
        {
            var ticker = request.Symbol.Value.ToUpperInvariant();
            var coin = ticker.EndsWith("USDC") ? ticker[..^4] : ticker;
            var shared = new SharedSymbol(TradingMode.PerpetualLinear, coin, "USDC");

            var interval = request.Resolution switch
            {
                Resolution.Minute => (SharedKlineInterval?)SharedKlineInterval.OneMinute,
                Resolution.Hour => SharedKlineInterval.OneHour,
                Resolution.Daily => SharedKlineInterval.OneDay,
                _ => null  // Tick/Second not supported via REST klines
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
