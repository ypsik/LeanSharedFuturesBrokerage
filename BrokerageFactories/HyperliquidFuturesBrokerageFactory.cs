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
        // Oberes Ende der von Hyperliquid erlaubten Spanne (0.001% - 0.1%),
        // wird verwendet wenn eine Builder-Adresse gesetzt ist, aber kein Fee-Wert in der Config steht.
        private const decimal DefaultBuilderFeePercentageWhenAddressSet = 0.1m;

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
            { "hyperliquid-builder-address", Config.Get("hyperliquid-builder-address") },
            { "hyperliquid-builder-fee", Config.Get("hyperliquid-builder-fee") },
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

            // --- Builder Code Config (Address + Fee%) ---
            // Kein hartes Read<> nutzen, da beide Felder optional sind und nicht in 'errors' münden sollen.
            var builderAddressRaw = job.BrokerageData.TryGetValue("hyperliquid-builder-address", out var bAddr) ? bAddr : null;
            var builderFeeRaw = job.BrokerageData.TryGetValue("hyperliquid-builder-fee", out var bFee) ? bFee : null;

            string builderAddress;
            decimal? builderFeePercentage;

            if (string.IsNullOrWhiteSpace(builderAddressRaw))
            {
                // Keine Builder-Adresse gesetzt -> kein Builder Code, Fee ist 0
                builderAddress = null;
                builderFeePercentage = null;
            }
            else
            {
                builderAddress = builderAddressRaw;

                if (string.IsNullOrWhiteSpace(builderFeeRaw) || !decimal.TryParse(builderFeeRaw, out var parsedFee))
                {
                    // Adresse gesetzt, aber keine (gültige) Fee in der Config -> oberes Ende der Spanne
                    builderFeePercentage = DefaultBuilderFeePercentageWhenAddressSet;
                }
                else
                {
                    builderFeePercentage = parsedFee;
                }
            }

            var credentials = new HyperLiquidCredentials(String.IsNullOrEmpty(vaultAddress) ? address : vaultAddress, secret);

            var restClient = new HyperLiquidRestClient(options =>
            {
                options.ApiCredentials = credentials;
                options.BuilderFeePercentage = builderFeePercentage;
                options.BuilderAddress = builderAddress;
                options.OutputOriginalData = true;
            });

            var socketClient = new HyperLiquidSocketClient(options =>
            {
                options.ApiCredentials = credentials;
                options.DelayAfterConnect = TimeSpan.FromMilliseconds(500);
                options.SocketIndividualSubscriptionCombineTarget = 50;
                options.BuilderFeePercentage = builderFeePercentage;
                options.BuilderAddress = builderAddress;
                options.OutputOriginalData = true;
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