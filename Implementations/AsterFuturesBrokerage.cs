using Aster.Net.Clients;
using CryptoExchange.Net.SharedApis;
using QuantConnect;
using QuantConnect.Securities;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Generic;

namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    /// <summary>
    /// Aster Perpetuals Brokerage.
    ///
    /// Symbol-Konvention:
    ///   Exchange -> LEAN :  "BTCUSDT" -> "BTCUSDT"
    ///   LEAN -> Exchange :  "BTCUSDT" -> "BTCUSDT"
    ///
    /// Alle Aster Perpetuals werden in USDT abgerechnet (PerpetualLinear).
    /// </summary>
    public class AsterFuturesBrokerage : SharedFuturesBrokerage
    {
        public AsterFuturesBrokerage() : base("aster")
        {
        }

        internal AsterFuturesBrokerage(
            AsterRestClient restClient,
            AsterSocketClient socketClient,
            Func<List<Holding>> getHoldingsFunc)
            : base(
                "aster",
                restClient.FuturesApi.SharedClient,    // IFuturesOrderRestClient
                restClient.FuturesApi.SharedClient,    // IBalanceRestClient
                socketClient.FuturesApi.SharedClient,  // ITickerSocketClient
                socketClient.FuturesApi.SharedClient,  // IFuturesOrderSocketClient
                restClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                getHoldingsFunc)
        {
        }

        // ── Symbol-Mapping ───────────────────────────────────────────────

        /// <summary>
        /// Aster gibt normalerweise vollständige Ticker zurück (z. B. "BTCUSDT").
        /// Wir stellen sicher, dass das Suffix "USDT" vorhanden ist und alles in Großbuchstaben steht.
        /// </summary>
        protected override string NormalizeSymbol(string rawSymbol)
        {
            var upperSymbol = rawSymbol.ToUpperInvariant();

            return upperSymbol.EndsWith("USDT")
                ? upperSymbol
                : upperSymbol + "USDT";
        }

        /// <summary>
        /// LEAN übergibt das Symbol als "BTCUSDT".
        /// Aster erwartet ebenfalls den vollständigen Ticker ("BTCUSDT").
        /// Daher schneiden wir das Suffix hier NICHT ab.
        /// </summary>
        protected override SharedSymbol GetSharedSymbol(Symbol s)
        {
            var ticker = s.Value.ToUpperInvariant();

            // Wir übergeben den vollen Ticker an Aster, da die API dies erfordert.
            return new SharedSymbol(TradingMode.PerpetualLinear, ticker, "USDT");
        }
    }
}