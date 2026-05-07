using HyperLiquid.Net;
using HyperLiquid.Net.Clients;
using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageModels;
using System;
using System.Collections.Generic;
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageFactories
{
    public class HyperLiquidBrokerageFactory : BrokerageFactory
    {
        // LÖSUNG 1: Der Konstruktor muss den Typ an die Basisklasse weitergeben
        public HyperLiquidBrokerageFactory() : base(typeof(SharedFuturesBrokerage))
        {
        }

        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
            { "hyperliquid-address", Config.Get("hyperliquid-address") },
            { "hyperliquid-secret", Config.Get("hyperliquid-secret") }
        };

        // LÖSUNG 2: Lean verlangt diese Methode zwingend. 
        // Hier geben wir direkt unser neues HyperLiquidBrokerageModel zurück!
        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider)
        {
            return new HyperLiquidBrokerageModel(AccountType.Margin);
        }

        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            var errors = new List<string>();

            // 2. Nutze die integrierte Read<T> Methode von Lean für sauberes Error-Handling
            var address = Read<string>(job.BrokerageData, "hyperliquid-address", errors);
            var secret = Read<string>(job.BrokerageData, "hyperliquid-secret", errors);

            if (errors.Any())
            {
                throw new ArgumentException(string.Join(Environment.NewLine, errors));
            }

            var hlRestClient = new HyperLiquidRestClient(options => {
                options.ApiCredentials = new HyperLiquidCredentials(address, secret);
            });

            var hlSocketClient = new HyperLiquidSocketClient();

            // Delegate für die Holdings
            Func<List<Holding>> getHoldingsFunc = () => {
                return algorithm.Securities.Values
                    .Where(x => x.Holdings.Quantity != 0)
                    .Select(x => new Holding(x))
                    .ToList();
            };

            // Unsere Brokerage-Instanz erstellen und zurückgeben
            return new SharedFuturesBrokerage(
                "HyperLiquid",
                hlRestClient.FuturesApi.SharedClient,
                hlRestClient.FuturesApi.SharedClient,
                hlSocketClient.FuturesApi.SharedClient,
                hlSocketClient.FuturesApi.SharedClient,
                getHoldingsFunc
            );
        }

        public override void Dispose()
        {
            // Hier könntest du später offene Sockets schließen, 
            // für den Moment reicht ein leeres Dispose.
        }
    }
}
