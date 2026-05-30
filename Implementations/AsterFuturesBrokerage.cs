using Aster.Net.Clients;
using CryptoExchange.Net.Interfaces.Clients;
using CryptoExchange.Net.Objects;
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
        internal AsterFuturesBrokerage(IAlgorithm algorithm,
            AsterRestClient restClient,
            AsterSocketClient socketClient,
            IDataAggregator aggregator,
            Func<List<Holding>> getHoldingsFunc = null)
            : base(algorithm, "aster")
        {
            _restClient = restClient;
            _socketClient = socketClient;

            InitializeBase(
                restClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
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
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                null,
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                aggregator
            );
        }


        protected override async Task<CallResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
               string nativeTicker, Symbol symbol, Func<DateTime, decimal?, bool> onFundingRate)
        {
            return null;
        }

        // ── Symbol-Mapping ───────────────────────────────────────────────

        protected override string NormalizeSymbol(string rawSymbol)
        {
            var upper = rawSymbol.ToUpperInvariant();
            return upper.EndsWith(SettleAsset) ? upper : upper + SettleAsset;
        }

        
        protected override bool SubscribeFunding(Symbol symbol)
        {
            var brokerageSymbol = symbol.Value;
            return true;
        }

        protected override bool UnsubscribeFunding(Symbol symbol)
        {
            if (_subscriptions.TryRemove($"{symbol.Value}_FUNDING", out var sub))
            {
                RunSync(() => sub.CloseAsync());
            }
            return true;
        }


    }
}