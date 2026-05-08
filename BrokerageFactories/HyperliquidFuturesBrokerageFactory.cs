using HyperLiquid.Net;
using HyperLiquid.Net.Clients;
using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Util;
using SilverQuant.Lean.Brokerages.Futures.Hyperliquid;
using SilverQuant.Lean.Brokerages.Futures.Implementations;
using SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageModels;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;

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
            { "hyperliquid-secret",  Config.Get("hyperliquid-secret")  }
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

            var credentials = new HyperLiquidCredentials(address, secret);

            var restClient = new HyperLiquidRestClient(options =>
            {
                options.ApiCredentials = credentials;
                options.BuilderFeePercentage = 0;
            });

            var socketClient = new HyperLiquidSocketClient(options =>
            {
                options.ApiCredentials = credentials;
            });

            Func<List<Holding>> getHoldingsFunc = () =>
                algorithm.Securities.Values
                    .Where(x => x.Holdings.Quantity != 0)
                    .Select(x => new Holding(x))
                    .ToList();

            // --- Populate SPDB with all live HL assets ---
            var spdb = SymbolPropertiesDatabase.FromDataFolder();

            var metaResult = restClient.FuturesApi.ExchangeData
                .GetExchangeInfoAsync()
                .GetAwaiter().GetResult();

            if (!metaResult.Success)
                throw new Exception($"Failed to load Hyperliquid assets: {metaResult.Error}");

            foreach (var symbol in metaResult.Data.Where(s => !s.IsDelisted))
            {
                var ticker = symbol.Name + "USDC"; // Main DEX = always USDC
                var lotSize = Math.Pow(10, -symbol.QuantityDecimals);  // QuantityDecimals nutzen!

                var symbolProperties = new SymbolProperties(
                    description: $"Hyperliquid {symbol.Name} Perpetual",
                    quoteCurrency: "USDC",
                    contractMultiplier: 1m,
                    minimumPriceVariation: 0.0001m,
                    lotSize: (decimal)lotSize,
                    marketTicker: symbol.Name  // HL erwartet nur "BTC", nicht "BTCUSDC"
                );

                spdb.SetEntry("hyperliquid", ticker, SecurityType.CryptoFuture, symbolProperties);
            }
            var brokerage = new HyperliquidFuturesBrokerage(restClient, socketClient, getHoldingsFunc);

            // Register with MEF Composer so Lean reuses this instance when
            // resolving IDataQueueHandler instead of trying to construct a new one
            Composer.Instance.AddPart<IDataQueueHandler>(brokerage);

            return brokerage;
        }

        public override void Dispose() { }
    }
}