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
using RestSharp;
using SilverQuant.Lean.Brokerages.Futures.Hyperliquid;
using SilverQuant.Lean.Brokerages.Futures.Implementations;
using SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageModels;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageFactories
{

    public class AsterFuturesBrokerageFactory : BrokerageFactory
    {
        // LÖSUNG 1: Der Konstruktor muss den Typ an die Basisklasse weitergeben
        public AsterFuturesBrokerageFactory() : base(typeof(AsterFuturesBrokerage))
        {
            Market.Add("aster", 902);

            var mhdb = MarketHoursDatabase.FromDataFolder();
            var alwaysOpen = SecurityExchangeHours.AlwaysOpen(TimeZones.Utc);

            mhdb.SetEntry("aster", null, SecurityType.CryptoFuture, alwaysOpen, TimeZones.Utc);

            var spdb = SymbolPropertiesDatabase.FromDataFolder();
            var symbolProperties = new SymbolProperties(
                description: "Aster Perpetual",
                quoteCurrency: "USDT",          // WICHTIG: Damit trennt Lean "BTCUSDT" in "BTC" und "USDT"
                contractMultiplier: 1m,         // Bei Crypto-Futures meist 1
                minimumPriceVariation: 0.0001m, // Fallback Tick-Size (wird später ggf. durch echte Daten überschrieben)
                lotSize: 0.0001m,               // Fallback Min-Order-Size
                marketTicker: string.Empty
            );

            // Das "*" dient als Wildcard. Gilt für ALLE CryptoFutures auf Aster.
            spdb.SetEntry("aster", "*", SecurityType.CryptoFuture, symbolProperties);

        }

        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
            { "aster-public-address", Config.Get("aster-public-address") },
            { "aster-address", Config.Get("aster-address") },
            { "aster-secret", Config.Get("aster-secret") }
        };

        // LÖSUNG 2: Lean verlangt diese Methode zwingend. 
        // Hier geben wir direkt unser neues AsterBrokerageModel zurück!
        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider)
        {
            return new AsterBrokerageModel(AccountType.Margin);
        }

        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            var errors = new List<string>();

            // 2. Nutze die integrierte Read<T> Methode von Lean für sauberes Error-Handling
            //V3 api needs the public ETH address as well.
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

            // Delegate für die Holdings
            Func<List<Holding>> getHoldingsFunc = () => {
                return algorithm.Securities.Values
                    .Where(x => x.Holdings.Quantity != 0)
                    .Select(x => new Holding(x))
                    .ToList();
            };

            // Unsere Brokerage-Instanz erstellen und zurückgeben
            var brokerage = new AsterFuturesBrokerage(asterRestClient, asterSocketClient, getHoldingsFunc);

            // Register with MEF Composer so Lean reuses this instance when
            // resolving IDataQueueHandler instead of trying to construct a new one
            Composer.Instance.AddPart<IDataQueueHandler>(brokerage);

            return brokerage;
        }

        public override void Dispose()
        {
            // Hier könntest du später offene Sockets schließen, 
            // für den Moment reicht ein leeres Dispose.
        }
    }
}
