using Aster.Net.Clients;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using QuantConnect;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using QuantConnect.Securities;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Concurrent;
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
        private readonly AsterRestClient _restClient;
        private readonly AsterSocketClient _socketClient;

        // Trackt offene Socket Subscriptions für den Unsubscribe-Vorgang
        private readonly ConcurrentDictionary<Symbol, UpdateSubscription> _dataSubscriptions = new();

        // Parameterloser Konstruktor für die BrokerageFactory
        public AsterFuturesBrokerage() : base("aster", null, null, null, null, null)
        {
        }

        internal AsterFuturesBrokerage(
            AsterRestClient restClient,
            AsterSocketClient socketClient,
            Func<List<Holding>> getHoldingsFunc)
            : base(
                "aster",                               // 1. Name
                restClient.FuturesApi.SharedClient,    // 2. IFuturesOrderRestClient
                restClient.FuturesApi.SharedClient,    // 3. IBalanceRestClient
                socketClient.FuturesApi.SharedClient,  // 4. IFuturesOrderSocketClient
                restClient.FuturesApi.SharedClient,    // 5. IKlineRestClient (Neu hinzugefügt)
                getHoldingsFunc)                       // 6. getHoldingsFunc
        {
            _restClient = restClient;
            _socketClient = socketClient;
        }

        #region Symbol-Mapping

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

        #endregion

        #region LEAN Data Manager Overrides (Der neue Preisfluss)

        protected override bool SubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType)
        {
            foreach (var symbol in symbols)
            {
                if (tickType == TickType.Trade || tickType == TickType.Quote)
                {
                    var shared = GetSharedSymbol(symbol);

                    // Abonniere Klines über den Shared Client
                    var sub = RunSync(() =>
                        _socketClient.FuturesApi.SharedClient.SubscribeToKlineUpdatesAsync(
                            new SubscribeKlineRequest(shared, SharedKlineInterval.OneMinute),
                            update =>
                            {
                                var k = update.Data;

                                // WICHTIG: Sende Ticks an den Aggregator der Basisklasse
                                EmitTick(new Tick
                                {
                                    Symbol = symbol,
                                    Time = k.OpenTime.ToUniversalTime(),
                                    TickType = TickType.Trade,
                                    Value = k.ClosePrice,
                                    Quantity = k.Volume
                                });
                            }));

                    if (sub.Success)
                    {
                        _dataSubscriptions[symbol] = sub.Data;
                        Log.Trace($"Aster.Subscribe: Erfolgreich abonniert -> {symbol.Value}");
                    }
                    else
                    {
                        Log.Error($"Aster.Subscribe fehlgeschlagen für {symbol.Value}: {sub.Error}");
                    }
                }
            }
            return true;
        }

        protected override bool UnsubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType)
        {
            foreach (var symbol in symbols)
            {
                if (_dataSubscriptions.TryRemove(symbol, out var sub))
                {
                    RunSync(() => sub.CloseAsync());
                    Log.Trace($"Aster.Unsubscribe: Erfolgreich beendet -> {symbol.Value}");
                }
            }
            return true;
        }

        #endregion
    }
}