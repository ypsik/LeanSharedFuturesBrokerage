using Lighter.Net;
using Lighter.Net.Clients;
using Lighter.Net.Enums;
using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Util;
using QuantConnect.Data;
using SilverQuant.Lean.Brokerages.Futures.Implementations;
using SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageModels;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageFactories
{
    public class LighterFuturesBrokerageFactory : BrokerageFactory
    {
        public LighterFuturesBrokerageFactory() : base(typeof(LighterFuturesBrokerage))
        {
            Market.Add("lighter", 906);

            var mhdb = MarketHoursDatabase.FromDataFolder();
            var alwaysOpen = SecurityExchangeHours.AlwaysOpen(TimeZones.Utc);

            mhdb.SetEntry("lighter", null, SecurityType.CryptoFuture, alwaysOpen, TimeZones.Utc);
        }

        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
            { "lighter-public-address", Config.Get("lighter-public-address") },
            { "lighter-account-index", Config.Get("lighter-account-index") },
            { "lighter-api-key-index", Config.Get("lighter-api-key-index") },
            { "lighter-api-secret", Config.Get("lighter-api-secret") },
            // Optional. JKorf's Library-eigene Integrator Fee (KEIN Lighter-Protokoll-Feature,
            // rein Lighter.Net-seitig). Gate ist lighter-integrator-account-index (siehe unten) -
            // ohne Empfaenger keine Fee, analog zu HL's Builder-Adresse.
            { "lighter-integrator-fee", Config.Get("lighter-integrator-fee") },
            // Optional, ist das GATE: kein Wert -> keine Integrator Fee (analog zu HL's
            // hyperliquid-builder-address). Gesetzt, aber keine lighter-integrator-fee angegeben
            // -> Default 0.01 (1bps, Lib-eigener Wert) greift.
            { "lighter-integrator-account-index", Config.Get("lighter-integrator-account-index") },
            // Optional. standard | plus | premium. Wenn gesetzt, wird der Account-Tier beim Connect
            // per changeAccountTier umgestellt (Lighter erlaubt max. 1 Aufruf pro 24h). Leer -> Tier
            // bleibt unangetastet.
            { "lighter-account-tier", Config.Get("lighter-account-tier") },
        };

        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider)
        {
            return new LighterBrokerageModel(AccountType.Margin);
        }

        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            var errors = new List<string>();

            var publicAddress = Read<string>(job.BrokerageData, "lighter-public-address", errors);
            var accountIndexRaw = Read<string>(job.BrokerageData, "lighter-account-index", errors);
            var secret = Read<string>(job.BrokerageData, "lighter-api-secret", errors);

            if (errors.Any())
                throw new ArgumentException(string.Join(Environment.NewLine, errors));

            if (!long.TryParse(accountIndexRaw, out var accountIndex))
                errors.Add($"Invalid lighter-account-index: '{accountIndexRaw}'");

            // Optional - Standard-API-Key-Slot ist 0, nur bei mehreren API-Keys pro Account relevant.
            var apiKeyIndexRaw = job.BrokerageData.TryGetValue("lighter-api-key-index", out var apiKeyIdx) ? apiKeyIdx : null;
            var apiKeyIndex = 0;

            if (!string.IsNullOrWhiteSpace(apiKeyIndexRaw) && !int.TryParse(apiKeyIndexRaw, out apiKeyIndex))
                errors.Add($"Invalid lighter-api-key-index: '{apiKeyIndexRaw}'");

            // Gate genau wie HL's builderAddress: kein Empfaenger gesetzt -> keine Integrator Fee,
            // Punkt. Nur wenn ein Empfaenger konfiguriert ist, greift ein Fee-Default (analog zu HL's
            // DefaultBuilderFeePercentageWhenAddressSet). Lib-Default in Lighter.Net selbst ist 0.01m.
            const decimal DefaultIntegratorFeePercentageWhenAccountIndexSet = 0.01m;

            var integratorAccountIndexRaw = job.BrokerageData.TryGetValue("lighter-integrator-account-index", out var iAcc) ? iAcc : null;
            var integratorFeeRaw = job.BrokerageData.TryGetValue("lighter-integrator-fee", out var iFee) ? iFee : null;

            long? integratorAccountIndex;
            decimal integratorFeePercentage;

            if (string.IsNullOrWhiteSpace(integratorAccountIndexRaw))
            {
                integratorAccountIndex = null;
                integratorFeePercentage = 0m;
            }
            else
            {
                if (!long.TryParse(integratorAccountIndexRaw, out var parsedAccountIndex))
                {
                    errors.Add($"Invalid lighter-integrator-account-index: '{integratorAccountIndexRaw}'");
                    integratorAccountIndex = null;
                }
                else
                {
                    integratorAccountIndex = parsedAccountIndex;
                }

                if (string.IsNullOrWhiteSpace(integratorFeeRaw) || !decimal.TryParse(integratorFeeRaw, out var parsedFee))
                {
                    integratorFeePercentage = DefaultIntegratorFeePercentageWhenAccountIndexSet;
                }
                else
                {
                    integratorFeePercentage = parsedFee;
                }
            }

            // Optional - nur wenn gesetzt wird der Account-Tier beim Connect umgestellt.
            AccountTier? accountTier = null;
            var accountTierRaw = job.BrokerageData.TryGetValue("lighter-account-tier", out var tierRaw) ? tierRaw : null;
            if (!string.IsNullOrWhiteSpace(accountTierRaw))
            {
                // IsDefined verhindert, dass Zahlenstrings (z.B. "5") als undefinierter Enum-Wert durchrutschen.
                if (Enum.TryParse<AccountTier>(accountTierRaw.Trim(), true, out var parsedTier)
                    && Enum.IsDefined(typeof(AccountTier), parsedTier))
                {
                    accountTier = parsedTier;
                }
                else
                {
                    errors.Add($"Invalid lighter-account-tier: '{accountTierRaw}' (expected standard, plus or premium)");
                }
            }

            if (errors.Any())
                throw new ArgumentException(string.Join(Environment.NewLine, errors));

            var credentials = new LighterCredentials(EthKey.FromPublicKey(publicAddress), accountIndex, apiKeyIndex, secret);

            var restClient = new LighterRestClient(options =>
            {
                options.ApiCredentials = credentials;
                options.OutputOriginalData = true;
                options.IntegratorFeePercentage = integratorFeePercentage;
                if (integratorAccountIndex.HasValue)
                    options.IntegratorAccountIndex = integratorAccountIndex;
            });

            var socketClient = new LighterSocketClient(options =>
            {
                options.ApiCredentials = credentials;
                options.OutputOriginalData = true;
                options.IntegratorFeePercentage = integratorFeePercentage;
                if (integratorAccountIndex.HasValue)
                    options.IntegratorAccountIndex = integratorAccountIndex;
            });

            var aggregator = Composer.Instance.GetPart<IDataAggregator>();

            Func<List<Holding>> getHoldingsFunc = () =>
                algorithm.Securities.Values
                    .Where(x => x.Holdings.Quantity != 0)
                    .Select(x => new Holding(x))
                    .ToList();

            algorithm.Settings.DatabasesRefreshPeriod = TimeSpan.FromDays(36500);
            var brokerage = new LighterFuturesBrokerage(algorithm, restClient, socketClient, aggregator, getHoldingsFunc, accountTier);

            Composer.Instance.AddPart<IDataQueueHandler>(brokerage);

            return brokerage;
        }

        public override void Dispose() { }
    }
}