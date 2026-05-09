using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using HyperLiquid.Net.Clients;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Securities;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace SilverQuant.Lean.Brokerages.Futures.Hyperliquid
{
    public class HyperliquidFuturesBrokerage : SharedFuturesBrokerage
    {
        private readonly HyperLiquidRestClient _restClient;
        private readonly HyperLiquidSocketClient _socketClient;

        private UpdateSubscription _fundingSubscription;
        private readonly object _fundingLock = new();

        // Trackt offene Socket Subscriptions
        private readonly ConcurrentDictionary<Symbol, UpdateSubscription> _klineSubscriptions = new();

        public HyperliquidFuturesBrokerage() : base("hyperliquid", null, null, null, null, null)
        {
        }

        internal HyperliquidFuturesBrokerage(
            HyperLiquidRestClient restClient,
            HyperLiquidSocketClient socketClient,
            Func<List<Holding>> getHoldingsFunc)
            : base(
                "hyperliquid",                               // 1. Name
                restClient.FuturesApi.SharedClient,    // 2. IFuturesOrderRestClient
                restClient.FuturesApi.SharedClient,    // 3. IBalanceRestClient
                socketClient.FuturesApi.SharedClient,  // 4. IFuturesOrderSocketClient
                restClient.FuturesApi.SharedClient,    // 5. IKlineRestClient (Neu hinzugefügt)
                getHoldingsFunc)                       // 6. getHoldingsFunc
        {
            _restClient = restClient;
            _socketClient = socketClient;
        }

        #region Symbol Mapping

        protected override string NormalizeSymbol(string rawSymbol)
        {
            var upper = rawSymbol.ToUpperInvariant();
            return upper.EndsWith("USDC") ? upper : upper + "USDC";
        }

        protected override SharedSymbol GetSharedSymbol(Symbol s)
        {
            var ticker = s.Value.ToUpperInvariant();
            var baseAsset = ticker.EndsWith("USDC") ? ticker[..^4] : ticker;

            return new SharedSymbol(TradingMode.PerpetualLinear, baseAsset, "USDC");
        }

        #endregion

        #region LEAN Data Manager Overrides

        protected override bool SubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType)
        {
            foreach (var symbol in symbols)
            {
                if (tickType == TickType.Trade || tickType == TickType.Quote)
                {
                    // Beispiel: Wir abonnieren Klines für den Symbol
                    var shared = GetSharedSymbol(symbol);

                    var sub = RunSync(() =>
                        _socketClient.FuturesApi.SharedClient.SubscribeToKlineUpdatesAsync(
                            new SubscribeKlineRequest(shared, SharedKlineInterval.OneMinute), // Resolution anpassen falls nötig
                            update =>
                            {
                                var k = update.Data;

                                // WICHTIG: Ticks (Level 1) statt Bars schicken! 
                                // Der Aggregator baut daraus Bars.
                                EmitTick(new Tick
                                {
                                    Symbol = symbol,
                                    Time = k.OpenTime.ToUniversalTime(),
                                    TickType = TickType.Trade,
                                    Value = k.ClosePrice, // Repräsentativer Preis für den Tick
                                    Quantity = k.Volume
                                });
                            }));

                    if (sub.Success)
                    {
                        _klineSubscriptions[symbol] = sub.Data;
                    }
                    else
                    {
                        Log.Error($"Hyperliquid.Subscribe failed for {symbol}: {sub.Error}");
                    }
                }
            }
            return true;
        }

        protected override bool UnsubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType)
        {
            foreach (var symbol in symbols)
            {
                if (_klineSubscriptions.TryRemove(symbol, out var sub))
                {
                    RunSync(() => sub.CloseAsync());
                }
            }
            return true;
        }

        #endregion

        #region Subscribe (FUNDING ONLY SPECIAL CASE)

        // Funding-Rates können direkt als Objekte bleiben, da sie nicht vom Aggregator zusammengefasst werden.
        public override IEnumerator<BaseData> Subscribe(SubscriptionDataConfig config, EventHandler handler)
        {
            if (config.Type == typeof(MarginInterestRate))
            {
                lock (_fundingLock)
                {
                    if (_fundingSubscription == null)
                    {
                        var sub = RunSync(() =>
                            _socketClient.FuturesApi.Account.SubscribeToUserFundingUpdatesAsync(
                                null,
                                update =>
                                {
                                    foreach (var funding in update.Data)
                                    {
                                        var leanSymbol = Symbol.Create(NormalizeSymbol(funding.Symbol), SecurityType.CryptoFuture, Name);
                                        var rate = new MarginInterestRate
                                        {
                                            Symbol = leanSymbol,
                                            Time = funding.Timestamp ?? DateTime.UtcNow,
                                            InterestRate = funding.FundingRate
                                        };

                                        // Für BaseData, das kein Tick ist (z.B. Funding), 
                                        // hat IDataQueueHandler oft eigene Queues, oder wir lassen es über CustomData laufen.
                                        // Der Einfachheit halber kannst du Custom Data an Event-Handler senden.
                                        handler?.Invoke(this, EventArgs.Empty);
                                    }
                                }));

                        if (sub.Success) _fundingSubscription = sub.Data;
                    }
                }

                // Für Funding gibt es keinen Aggregator-Enumerator
                return Enumerable.Empty<BaseData>().GetEnumerator();
            }

            // Für alles andere (Trades, Quotes) ruft es die Basisklasse (und damit den Aggregator) auf
            return base.Subscribe(config, handler);
        }

        #endregion
    }
}