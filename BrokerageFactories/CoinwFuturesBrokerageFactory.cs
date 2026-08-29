using CoinW.Net;
using CoinW.Net.Clients;
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
using System.Linq;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageFactories
{
    public class CoinwFuturesBrokerageFactory : BrokerageFactory
    {
        public CoinwFuturesBrokerageFactory() : base(typeof(CoinwFuturesBrokerage))
        {
            Market.Add("coinw", 907);

            var mhdb = MarketHoursDatabase.FromDataFolder();
            var alwaysOpen = SecurityExchangeHours.AlwaysOpen(TimeZones.Utc);

            mhdb.SetEntry("coinw", null, SecurityType.CryptoFuture, alwaysOpen, TimeZones.Utc);
        }

        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
            { "coinw-api-key",     Config.Get("coinw-api-key")     },
            { "coinw-api-secret",  Config.Get("coinw-api-secret")  },
        };

        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider)
        {
            return new CoinwBrokerageModel();
        }

        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            var errors = new List<string>();

            var key = Read<string>(job.BrokerageData, "coinw-api-key", errors);
            var secret = Read<string>(job.BrokerageData, "coinw-api-secret", errors);

            if (errors.Any())
                throw new ArgumentException(string.Join(Environment.NewLine, errors));

            var credentials = new CoinWCredentials(key, secret);

            var restClient = new CoinWRestClient(options =>
            {
                options.ApiCredentials = credentials;
                options.OutputOriginalData = true;
            });

            var socketClient = new CoinWSocketClient(options =>
            {
                options.ApiCredentials = credentials;
                options.DelayAfterConnect = TimeSpan.FromMilliseconds(500);
            });

            var aggregator = Composer.Instance.GetPart<IDataAggregator>();

            Func<List<Holding>> getHoldingsFunc = () =>
                algorithm.Securities.Values
                    .Where(x => x.Holdings.Quantity != 0)
                    .Select(x => new Holding(x))
                    .ToList();

            algorithm.Settings.DatabasesRefreshPeriod = TimeSpan.FromDays(36500);

            var brokerage = new CoinwFuturesBrokerage(
                algorithm,
                restClient,
                socketClient,
                aggregator,
                getHoldingsFunc: getHoldingsFunc);

            Composer.Instance.AddPart<IDataQueueHandler>(brokerage);

            return brokerage;
        }

        public override void Dispose() { }
    }
}
