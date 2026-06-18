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
        // Oberes Ende der erlaubten Spanne (0.001% - 0.1%),
        // wird verwendet wenn eine Builder-Adresse gesetzt ist, aber kein Fee-Wert in der Config steht.
        private const decimal DefaultBuilderFeePercentageWhenAddressSet = 0.1m;

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
            { "aster-secret", Config.Get("aster-secret") },
            { "aster-hedge-mode", Config.Get("aster-hedge-mode", "false") },
            { "aster-builder-address", Config.Get("aster-builder-address") },
            { "aster-builder-fee", Config.Get("aster-builder-fee") },
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

            var isHedgeMode = job.BrokerageData.TryGetValue("aster-hedge-mode", out var hm)
                && bool.TryParse(hm, out var parsed) && parsed;

            if (errors.Any())
            {
                throw new ArgumentException(string.Join(Environment.NewLine, errors));
            }

            // --- Builder Code Config (Address + Fee%) ---
            // Optionale Felder, daher kein Read<> (das würde bei Fehlen in 'errors' münden).
            var builderAddressRaw = job.BrokerageData.TryGetValue("aster-builder-address", out var bAddr) ? bAddr : null;
            var builderFeeRaw = job.BrokerageData.TryGetValue("aster-builder-fee", out var bFee) ? bFee : null;

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

            var asterCredentials = new AsterCredentials(new AsterV3Credential(publicAddress, address, secret));

            var asterRestClient = new AsterRestClient(options => {
                options.ApiCredentials = asterCredentials;
                options.BuilderFeePercentage = builderFeePercentage;
                options.BuilderAddress = builderAddress;
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
            var brokerage = new AsterFuturesBrokerage(algorithm, asterRestClient, asterSocketClient, aggregator, isHedgeMode, getHoldingsFunc);

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