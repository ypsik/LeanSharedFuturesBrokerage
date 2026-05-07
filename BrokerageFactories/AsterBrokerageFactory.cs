using Aster.Net;
using Aster.Net.Clients;
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
    public class AsterBrokerageFactory : BrokerageFactory
    {
        // LÖSUNG 1: Der Konstruktor muss den Typ an die Basisklasse weitergeben
        public AsterBrokerageFactory() : base(typeof(SharedFuturesBrokerage))
        {
        }

        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
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
            var address = Read<string>(job.BrokerageData, "aster-address", errors);
            var secret = Read<string>(job.BrokerageData, "aster-secret", errors);

            if (errors.Any())
            {
                throw new ArgumentException(string.Join(Environment.NewLine, errors));
            }

            var asterRestClient = new AsterRestClient(options => {
                options.ApiCredentials = new AsterCredentials(address, secret);
            });

            var asterSocketClient = new AsterSocketClient();

            // Delegate für die Holdings
            Func<List<Holding>> getHoldingsFunc = () => {
                return algorithm.Securities.Values
                    .Where(x => x.Holdings.Quantity != 0)
                    .Select(x => new Holding(x))
                    .ToList();
            };

            // Unsere Brokerage-Instanz erstellen und zurückgeben
            return new SharedFuturesBrokerage(
                "Aster",
                asterRestClient.FuturesApi.SharedClient,
                asterRestClient.FuturesApi.SharedClient,
                asterSocketClient.FuturesApi.SharedClient,
                asterSocketClient.FuturesApi.SharedClient,
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
