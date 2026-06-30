using OKX.Net;
using OKX.Net.Clients;
using OKX.Net.Enums;
using OKX.Net.Objects;
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
    public class OkxFuturesBrokerageFactory : BrokerageFactory
    {
        public OkxFuturesBrokerageFactory() : base(typeof(OkxFuturesBrokerage))
        {
            Market.Add("okx", 905);

            var mhdb = MarketHoursDatabase.FromDataFolder();
            var alwaysOpen = SecurityExchangeHours.AlwaysOpen(TimeZones.Utc);

            mhdb.SetEntry("okx", null, SecurityType.CryptoFuture, alwaysOpen, TimeZones.Utc);
        }

        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
            { "okx-api-key",        Config.Get("okx-api-key")        },
            { "okx-api-secret",     Config.Get("okx-api-secret")     },
            { "okx-api-passphrase", Config.Get("okx-api-passphrase") },
            { "okx-environment",    Config.Get("okx-environment", "europe") },
        };

        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider)
        {
            return new OkxBrokerageModel();
        }

        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            var errors = new List<string>();

            var key = Read<string>(job.BrokerageData, "okx-api-key", errors);
            var secret = Read<string>(job.BrokerageData, "okx-api-secret", errors);
            var passphrase = Read<string>(job.BrokerageData, "okx-api-passphrase", errors);
            var environmentStr = Read<string>(job.BrokerageData, "okx-environment", errors);

            if (errors.Any())
                throw new ArgumentException(string.Join(Environment.NewLine, errors));

            var environment = OkxFuturesBrokerage.ResolveEnvironment(environmentStr);

            var credentials = new OKXCredentials(key, secret, passphrase);

            var restClient = new OKXRestClient(options =>
            {
                options.Environment = environment;
                options.ApiCredentials = credentials;
                options.SharedApiEuropeUseXPerps = environment == OKXEnvironment.Europe;
                options.OutputOriginalData = true;
            });

            var socketClient = new OKXSocketClient(options =>
            {
                options.Environment = environment;
                options.ApiCredentials = credentials;
                options.SharedApiEuropeUseXPerps = environment == OKXEnvironment.Europe;
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

            var brokerage = new OkxFuturesBrokerage(
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