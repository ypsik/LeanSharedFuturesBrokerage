using Bitfinex.Net;
using Bitfinex.Net.Clients;
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
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageFactories
{
    public class BitfinexFuturesBrokerageFactory : BrokerageFactory
    {
        public BitfinexFuturesBrokerageFactory() : base(typeof(BitfinexFuturesBrokerage))
        {
            Market.Add("bitfinex", 905);

            var mhdb = MarketHoursDatabase.FromDataFolder();
            var alwaysOpen = SecurityExchangeHours.AlwaysOpen(TimeZones.Utc);

            mhdb.SetEntry("bitfinex", null, SecurityType.CryptoFuture, alwaysOpen, TimeZones.Utc);
        }

        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
            { "bitfinex-api-key", Config.Get("bitfinex-api-key") },
            { "bitfinex-api-secret",  Config.Get("bitfinex-api-secret")  },
        };

        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider)
        {
            return new BybitBrokerageModel(AccountType.Margin);
        }

        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            var errors = new List<string>();

            var address = Read<string>(job.BrokerageData, "bitfinex-api-key", errors);
            var secret = Read<string>(job.BrokerageData, "bitfinex-api-secret", errors);

            if (errors.Any())
                throw new ArgumentException(string.Join(Environment.NewLine, errors));

            errors = new List<string>();

            var credentials = new BitfinexCredentials(address, secret);

            var restClient = new BitfinexRestClient(options =>
            {
                options.ApiCredentials = credentials;
                options.OutputOriginalData = true;
            });

            var socketClient = new BitfinexSocketClient(options =>
            {
                options.ApiCredentials = credentials;
                options.DelayAfterConnect = TimeSpan.FromMilliseconds(500);
                options.SocketIndividualSubscriptionCombineTarget = 50;
            });

            // --- Aggregator & Holdings Setup ---
            var aggregator = Composer.Instance.GetPart<IDataAggregator>();

            Func<List<Holding>> getHoldingsFunc = () =>
                algorithm.Securities.Values
                    .Where(x => x.Holdings.Quantity != 0)
                    .Select(x => new Holding(x))
                    .ToList();

            algorithm.Settings.DatabasesRefreshPeriod = TimeSpan.FromDays(36500);
            var brokerage = new BitfinexFuturesBrokerage(algorithm, restClient, socketClient, aggregator, getHoldingsFunc);

            // Register with MEF Composer so Lean reuses this instance when
            // resolving IDataQueueHandler instead of trying to construct a new one
            Composer.Instance.AddPart<IDataQueueHandler>(brokerage);

            return brokerage;
        }

        public override void Dispose() { }
    }
}
