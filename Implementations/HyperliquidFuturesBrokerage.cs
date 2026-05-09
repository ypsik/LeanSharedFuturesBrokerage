using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using HyperLiquid.Net.Clients;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace SilverQuant.Lean.Brokerages.Futures.Hyperliquid
{
    public class HyperliquidFuturesBrokerage : SharedFuturesBrokerage
    {
        private readonly HyperLiquidRestClient _restClient;
        private readonly HyperLiquidSocketClient _socketClient;

        private UpdateSubscription _fundingSubscription;
        private readonly object _fundingLock = new();

        public HyperliquidFuturesBrokerage()
            : base("hyperliquid")
        {
        }

        internal HyperliquidFuturesBrokerage(
            HyperLiquidRestClient restClient,
            HyperLiquidSocketClient socketClient,
            Func<List<Holding>> getHoldingsFunc)
            : base(
                "hyperliquid",
                restClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                getHoldingsFunc)
        {
            _restClient = restClient;
            _socketClient = socketClient;
        }

        #region Symbol Mapping

        protected override string NormalizeSymbol(string rawSymbol)
        {
            var upper = rawSymbol.ToUpperInvariant();
            return upper.EndsWith("USDC") ? upper : upper + "USDC";
        }

        protected override SharedSymbol GetSharedSymbol(Symbol s)
        {
            var ticker = s.Value.ToUpperInvariant();
            var baseAsset = ticker.EndsWith("USDC") ? ticker[..^4] : ticker;

            return new SharedSymbol(
                TradingMode.PerpetualLinear,
                baseAsset,
                "USDC");
        }

        #endregion

        #region Subscribe (FUNDING ONLY SPECIAL CASE)

        public override IEnumerator<BaseData> Subscribe(
            SubscriptionDataConfig config,
            EventHandler handler)
        {
            if (config.Type == typeof(MarginInterestRate))
            {
                lock (_fundingLock)
                {
                    if (_fundingSubscription == null)
                    {
                        var sub = RunSync(() =>
                            _socketClient.FuturesApi.Account
                                .SubscribeToUserFundingUpdatesAsync(
                                    null,
                                    update =>
                                    {
                                        foreach (var funding in update.Data)
                                        {
                                            var symbol = NormalizeSymbol(funding.Symbol);

                                            var leanSymbol = Symbol.Create(
                                                symbol,
                                                SecurityType.CryptoFuture,
                                                Name);

                                            var bar = new MarginInterestRate
                                            {
                                                Symbol = leanSymbol,
                                                Time = funding.Timestamp ?? DateTime.UtcNow,
                                                InterestRate = funding.FundingRate
                                            };

                                            // FIX: Add() statt Enqueue() für BlockingCollection
                                            if (_ticks.Count < MaxQueueSize)
                                            {
                                                _ticks.Add(bar);
                                            }
                                        }

                                        handler?.Invoke(this, EventArgs.Empty);
                                    }));

                        if (sub.Success)
                            _fundingSubscription = sub.Data;
                    }
                }

                // FIX: Nicht Empty zurückgeben! LEAN muss den Enumerator der Queue lesen.
                return base.Subscribe(config, handler);
            }

            return base.Subscribe(config, handler);
        }

        #endregion
    }
}