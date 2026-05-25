using Bitget.Net;
using Bitget.Net.Clients;
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
    public class BitgetFuturesBrokerageFactory : BrokerageFactory
    {
        public BitgetFuturesBrokerageFactory() : base(typeof(BitgetFuturesBrokerage))
        {
            Market.Add("bitget", 903);

            var mhdb = MarketHoursDatabase.FromDataFolder();
            var alwaysOpen = SecurityExchangeHours.AlwaysOpen(TimeZones.Utc);

            mhdb.SetEntry("bitget", null, SecurityType.CryptoFuture, alwaysOpen, TimeZones.Utc);

        }

        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
            { "bitget-api-key", Config.Get("bitget-api-key") },
            { "bitget-api-secret",  Config.Get("bitget-api-secret")  },
            { "bitget-api-pass",  Config.Get("bitget-api-pass")  },
        };

        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider)
        {
            return new BitgetBrokerageModel(AccountType.Margin);
        }

        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            var errors = new List<string>();

            var address = Read<string>(job.BrokerageData, "bitget-api-key", errors);
            var secret = Read<string>(job.BrokerageData, "bitget-api-secret", errors);
            var pass = Read<string>(job.BrokerageData, "bitget-api-pass", errors);

            if (errors.Any())
                throw new ArgumentException(string.Join(Environment.NewLine, errors));

            errors = new List<string>();

            var credentials = new BitgetCredentials(address, secret, pass);

            var restClient = new BitgetRestClient(options =>
            {
                options.ApiCredentials = credentials;
                options.OutputOriginalData = true;
            });

            var socketClient = new BitgetSocketClient(options =>
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
            var brokerage = new BitgetFuturesBrokerage(algorithm, restClient, socketClient, aggregator, getHoldingsFunc);

            // Register with MEF Composer so Lean reuses this instance when
            // resolving IDataQueueHandler instead of trying to construct a new one
            Composer.Instance.AddPart<IDataQueueHandler>(brokerage);

            return brokerage;
        }

        public override void Dispose() { }
    }
}
