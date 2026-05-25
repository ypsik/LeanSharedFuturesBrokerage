using BingX.Net;
using BingX.Net.Clients;
using HyperLiquid.Net;
using HyperLiquid.Net.Clients;
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
    public class BingxFuturesBrokerageFactory : BrokerageFactory
    {
        public BingxFuturesBrokerageFactory() : base(typeof(BingxFuturesBrokerage))
        {
            Market.Add("hyperliquid", 904);

            var mhdb = MarketHoursDatabase.FromDataFolder();
            var alwaysOpen = SecurityExchangeHours.AlwaysOpen(TimeZones.Utc);

            mhdb.SetEntry("bingx", null, SecurityType.CryptoFuture, alwaysOpen, TimeZones.Utc);
        }

        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
            { "bingx-api-key", Config.Get("bingx-api-key") },
            { "bingx-api-secret",  Config.Get("bingx-api-secret")  },
        };

        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider)
        {
            return new HyperliquidBrokerageModel(AccountType.Margin);
        }

        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            var errors = new List<string>();

            var address = Read<string>(job.BrokerageData, "bingx-api-key", errors);
            var secret = Read<string>(job.BrokerageData, "bingx-api-secret", errors);

            if (errors.Any())
                throw new ArgumentException(string.Join(Environment.NewLine, errors));

            errors = new List<string>();

            var credentials = new BingXCredentials(address, secret);

            var restClient = new BingXRestClient(options =>
            {
                options.ApiCredentials = credentials;
                options.OutputOriginalData = true;
            });

            var socketClient = new BingXSocketClient(options =>
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
            var brokerage = new BingxFuturesBrokerage(algorithm, restClient, socketClient, aggregator, getHoldingsFunc);

            // Register with MEF Composer so Lean reuses this instance when
            // resolving IDataQueueHandler instead of trying to construct a new one
            Composer.Instance.AddPart<IDataQueueHandler>(brokerage);

            return brokerage;
        }

        public override void Dispose() { }
    }
}
