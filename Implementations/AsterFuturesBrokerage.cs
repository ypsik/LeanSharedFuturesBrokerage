using Aster.Net.Clients;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
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

namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    public class AsterFuturesBrokerage : SharedFuturesBrokerage
    {
        private AsterRestClient _restClient;
        private AsterSocketClient _socketClient;

        // Speichert die aktiven Subscriptions pro Symbol und TickType
        private readonly ConcurrentDictionary<string, UpdateSubscription> _subscriptions = new();

        // 1. LEAN DataQueueHandler Konstruktor (Bybit-Style)
        public AsterFuturesBrokerage() : base("aster")
        {
        }

        // 2. Trading-Instanz Konstruktor (für die Factory)
        internal AsterFuturesBrokerage(
            AsterRestClient restClient,
            AsterSocketClient socketClient,
            IDataAggregator aggregator,
            Func<List<Holding>> getHoldingsFunc = null)
            : base("aster")
        {
            _restClient = restClient;
            _socketClient = socketClient;

            InitializeBase(
                restClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                null,
                null,
                restClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                aggregator,
                getHoldingsFunc);
        }

        protected override void InitializeFromJob(QuantConnect.Packets.LiveNodePacket job, IDataAggregator aggregator)
        {
            var apiKey = job.BrokerageData.GetValueOrDefault("aster-api-key", "");
            var apiSecret = job.BrokerageData.GetValueOrDefault("aster-api-secret", "");

            _restClient = new AsterRestClient(); // Hier ggf. Credentials setzen
            _socketClient = new AsterSocketClient();

            InitializeBase(
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                null,
                null,
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                aggregator
            );
        }

        // ── Symbol-Mapping ───────────────────────────────────────────────

        protected override string NormalizeSymbol(string rawSymbol)
        {
            var upper = rawSymbol.ToUpperInvariant();
            return upper.EndsWith("USDT") ? upper : upper + "USDT";
        }

        protected override SharedSymbol GetSharedSymbol(Symbol s, string quoteAsset = "USDT")
        {
            return new SharedSymbol(TradingMode.PerpetualLinear, s.Value.ToUpperInvariant(), quoteAsset);
        }

        // ── Real-Time Data Implementation ────────────────────────────────

        protected override bool SubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType)
        {
            foreach (var symbol in symbols)
            {
                var sharedSymbol = GetSharedSymbol(symbol);
                var subKey = $"{symbol.Value}_{tickType}";

                if (_subscriptions.ContainsKey(subKey)) continue;

                if (tickType == TickType.Trade)
                {
                    // Trades kommen als Array (SharedTrade[])
                    var sub = RunSync(() => _socketClient.FuturesApi.SharedClient.SubscribeToTradeUpdatesAsync(
                        new SubscribeTradeRequest(sharedSymbol),
                        update =>
                        {
                            foreach (var item in update.Data)
                            {
                                EmitTick(new Tick
                                {
                                    Symbol = symbol,
                                    Time = item.Timestamp.ToUniversalTime(),
                                    TickType = TickType.Trade,
                                    Value = item.Price,
                                    Quantity = item.Quantity
                                });
                            }
                        }));

                    if (sub.Success) _subscriptions[subKey] = sub.Data;
                    else Log.Error($"Aster.Subscribe Trade Error: {sub.Error}");
                }
                else if (tickType == TickType.Quote)
                {
                    // Quotes (BookTicker) kommen als einzelnes Objekt (SharedBookTicker)
                    var sub = RunSync(() => _socketClient.FuturesApi.SharedClient.SubscribeToBookTickerUpdatesAsync(
                        new SubscribeBookTickerRequest(sharedSymbol),
                        update =>
                        {
                            var q = update.Data; // Kein foreach nötig
                            EmitTick(new Tick
                            {
                                Symbol = symbol,
                                Time = DateTime.UtcNow,
                                TickType = TickType.Quote,
                                BidPrice = q.BestBidPrice,
                                BidSize = q.BestBidQuantity,
                                AskPrice = q.BestAskPrice,
                                AskSize = q.BestAskQuantity
                            });
                        }));

                    if (sub.Success) _subscriptions[subKey] = sub.Data;
                    else Log.Error($"Aster.Subscribe Quote Error: {sub.Error}");
                }
            }
            return true;
        }

        protected override bool UnsubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType)
        {
            foreach (var symbol in symbols)
            {
                var subKey = $"{symbol.Value}_{tickType}";
                if (_subscriptions.TryRemove(subKey, out var sub))
                {
                    RunSync(() => sub.CloseAsync());
                }
            }
            return true;
        }

        protected override bool SubscribeFunding(Symbol symbol)
        {
            var brokerageSymbol = symbol.Value;
            return true;
        }

    }
}