using CryptoExchange.Net.Authentication;
using Kraken.Net;
using Kraken.Net.Clients;
using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Util;
using SilverQuant.Lean.Brokerages.Futures.Implementations;
using SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageModels;
using System;
using System.Collections.Generic;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageFactories
{
    public class KrakenFuturesBrokerageFactory : BrokerageFactory
    {
        public KrakenFuturesBrokerageFactory() : base(typeof(KrakenFuturesBrokerage))
        {
            var mhdb = MarketHoursDatabase.FromDataFolder();
            var alwaysOpen = SecurityExchangeHours.AlwaysOpen(TimeZones.Utc);

            mhdb.SetEntry(Market.Kraken, null, SecurityType.CryptoFuture, alwaysOpen, TimeZones.Utc);

        }

        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
            { "kraken-futures-api-key",    Config.Get("kraken-futures-api-key")    },
            { "kraken-futures-api-secret", Config.Get("kraken-futures-api-secret") },
        };

        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider)
        {
            return new BrokerageModels.KrakenBrokerageModel(AccountType.Margin);
        }

        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            var errors = new List<string>();

            var key = Read<string>(job.BrokerageData, "kraken-futures-api-key", errors);
            var secret = Read<string>(job.BrokerageData, "kraken-futures-api-secret", errors);

            if (errors.Any())
                throw new ArgumentException(string.Join(Environment.NewLine, errors));

            // Futures-only credentials: spot argument is null.
            var credentials = new KrakenCredentials(null, new HMACCredential(key, secret));

            var restClient = new KrakenRestClient(options =>
            {
                options.ApiCredentials = credentials;
                options.OutputOriginalData = true;
            });

            var socketClient = new KrakenSocketClient(options =>
            {
                options.ApiCredentials = credentials;
                options.DelayAfterConnect = TimeSpan.FromMilliseconds(500);
                options.SocketIndividualSubscriptionCombineTarget = 50;
            });

            var aggregator = Composer.Instance.GetPart<IDataAggregator>();

            Func<List<Holding>> getHoldingsFunc = () =>
                algorithm.Securities.Values
                    .Where(x => x.Holdings.Quantity != 0)
                    .Select(x => new Holding(x))
                    .ToList();

            algorithm.Settings.DatabasesRefreshPeriod = TimeSpan.FromDays(36500);

            var brokerage = new KrakenFuturesBrokerage(algorithm, restClient, socketClient, aggregator, getHoldingsFunc);

            Composer.Instance.AddPart<IDataQueueHandler>(brokerage);

            return brokerage;
        }

        public override void Dispose() { }
    }
}