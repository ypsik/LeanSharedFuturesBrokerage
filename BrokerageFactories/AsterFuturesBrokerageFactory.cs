using Aster.Net;
using Aster.Net.Clients;
using Aster.Net.Objects;
using CryptoExchange.Net.Interfaces.Clients;
using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Util;
using QuantConnect.Data; // 🔥 Neu für IDataAggregator
using SilverQuant.Lean.Brokerages.Futures.Implementations;
using SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageModels;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq; // 🔥 Neu für .Any() und .Where()
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageFactories
{
    public class AsterFuturesBrokerageFactory : BrokerageFactory
    {
        public AsterFuturesBrokerageFactory() : base(typeof(AsterFuturesBrokerage))
        {
            Market.Add("aster", 902);

            var mhdb = MarketHoursDatabase.FromDataFolder();
            var alwaysOpen = SecurityExchangeHours.AlwaysOpen(TimeZones.Utc);

            mhdb.SetEntry("aster", null, SecurityType.CryptoFuture, alwaysOpen, TimeZones.Utc);
        }

        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
            { "aster-public-address", Config.Get("aster-public-address") },
            { "aster-address", Config.Get("aster-address") },
            { "aster-secret", Config.Get("aster-secret") }
        };

        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider)
        {
            return new AsterBrokerageModel(AccountType.Margin);
        }

        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            var errors = new List<string>();

            var publicAddress = Read<string>(job.BrokerageData, "aster-public-address", errors);
            var address = Read<string>(job.BrokerageData, "aster-address", errors);
            var secret = Read<string>(job.BrokerageData, "aster-secret", errors);

            if (errors.Any())
            {
                throw new ArgumentException(string.Join(Environment.NewLine, errors));
            }

            var asterCredentials = new AsterCredentials(new AsterV3Credential(publicAddress, address, secret));

            var asterRestClient = new AsterRestClient(options => {
                options.ApiCredentials = asterCredentials;
                options.BuilderFeePercentage = 0;
            });

            var asterSocketClient = new AsterSocketClient(options => {
                options.ApiCredentials = asterCredentials;
            });

            // --- Aggregator & Holdings Setup ---
            var aggregator = Composer.Instance.GetPart<IDataAggregator>();

            Func<List<Holding>> getHoldingsFunc = () => {
                return algorithm.Securities.Values
                    .Where(x => x.Holdings.Quantity != 0)
                    .Select(x => new Holding(x))
                    .ToList();
            };

            // --- Fix: Konstruktor mit Aggregator aufrufen ---
            var brokerage = new AsterFuturesBrokerage(asterRestClient, asterSocketClient, aggregator, getHoldingsFunc);

            // Register with MEF Composer so Lean reuses this instance when
            // resolving IDataQueueHandler instead of trying to construct a new one
            Composer.Instance.AddPart<IDataQueueHandler>(brokerage);

            return brokerage;
        }

        public override void Dispose()
        {
        }
    }
}