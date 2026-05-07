using CryptoExchange.Net.SharedApis;
using HyperLiquid.Net.Clients;
using QuantConnect;
using QuantConnect.Securities;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Generic;

namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    /// <summary>
    /// Hyperliquid perpetuals brokerage.
    ///
    /// Symbol convention:
    ///   Exchange → LEAN :  "BTC"     → "BTCUSDC"
    ///   LEAN → Exchange :  "BTCUSDC" → "BTC"
    ///
    /// All Hyperliquid perpetuals are USDC-settled (PerpetualLinear).
    /// </summary>
    public class HyperliquidFuturesBrokerage : SharedFuturesBrokerage
    {
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
        }

        // ── Symbol mapping ───────────────────────────────────────────────

        /// <summary>
        /// Hyperliquid returns bare coin names ("BTC", "HYPE").
        /// LEAN expects base+quote ticker ("BTCUSDC", "HYPEUSDC").
        /// </summary>
        protected override string NormalizeSymbol(string rawSymbol)
            => rawSymbol.EndsWith("USDC", StringComparison.OrdinalIgnoreCase)
                ? rawSymbol.ToUpperInvariant()
                : rawSymbol.ToUpperInvariant() + "USDC";

        /// <summary>
        /// LEAN symbol value is "BTCUSDC" — strip "USDC" suffix to get
        /// the exchange symbol "BTC" that Hyperliquid expects.
        /// </summary>
        protected override SharedSymbol GetSharedSymbol(Symbol s)
        {
            var ticker = s.Value.ToUpperInvariant();
            var coin = ticker.EndsWith("USDC")
                ? ticker[..^4]   // "BTCUSDC" → "BTC"
                : ticker;

            return new SharedSymbol(TradingMode.PerpetualLinear, coin, "USDC");
        }
    }
}
