using HyperLiquid.Net;
using HyperLiquid.Net.Clients;
using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Util;
using QuantConnect.Data; // Wichtig für IDataAggregator
using SilverQuant.Lean.Brokerages.Futures.Implementations;
using SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageModels;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using Aster.Net.Objects;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageFactories
{
    public class HyperliquidFuturesBrokerageFactory : BrokerageFactory
    {
        public HyperliquidFuturesBrokerageFactory() : base(typeof(HyperliquidFuturesBrokerage))
        {
            Market.Add("hyperliquid", 901);

            var mhdb = MarketHoursDatabase.FromDataFolder();
            var alwaysOpen = SecurityExchangeHours.AlwaysOpen(TimeZones.Utc);

            mhdb.SetEntry("hyperliquid", null, SecurityType.CryptoFuture, alwaysOpen, TimeZones.Utc);
        }

        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
            { "hyperliquid-address", Config.Get("hyperliquid-address") },
            { "hyperliquid-secret",  Config.Get("hyperliquid-secret")  },
            { "hyperliquid-vault-address", Config.Get("hyperliquid-vault-address") },            
        };

        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider)
        {
            return new HyperliquidBrokerageModel(AccountType.Margin);
        }

        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            var errors = new List<string>();

            var address = Read<string>(job.BrokerageData, "hyperliquid-address", errors);
            var secret = Read<string>(job.BrokerageData, "hyperliquid-secret", errors);

            if (errors.Any())
                throw new ArgumentException(string.Join(Environment.NewLine, errors));

            errors = new List<string>();
            var vaultAddress = Read<string>(job.BrokerageData, "hyperliquid-vault-address", errors);

            var credentials = new HyperLiquidCredentials(String.IsNullOrEmpty(vaultAddress) ? address : vaultAddress, secret);

            var restClient = new HyperLiquidRestClient(options =>
            {
                options.ApiCredentials = credentials;
                options.BuilderFeePercentage = 0;
                options.OutputOriginalData = true;
            });

            var socketClient = new HyperLiquidSocketClient(options =>
            {
                options.ApiCredentials = credentials;
//                options.MaxConcurrentResubscriptionsPerSocket = 5;
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
            var brokerage = new HyperliquidFuturesBrokerage(algorithm, restClient, socketClient, vaultAddress, aggregator, getHoldingsFunc); 

            // Register with MEF Composer so Lean reuses this instance when
            // resolving IDataQueueHandler instead of trying to construct a new one
            Composer.Instance.AddPart<IDataQueueHandler>(brokerage);

            return brokerage;
        }

        public override void Dispose() { }
    }
}