using System;
using System.Collections.Generic;
using System.Linq;
using CryptoExchange.Net.SharedApis;
using HyperLiquid.Net.Clients;
using QuantConnect;
using QuantConnect.Data;
using LeanHistoryRequest = QuantConnect.Data.HistoryRequest;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using SilverQuant.Lean.Brokerages.Futures.Shared;

namespace SilverQuant.Lean.Brokerages.Futures.Hyperliquid
{
    /// <summary>
    /// Hyperliquid-specific overrides on top of SharedFuturesBrokerage.
    /// </summary>
    public class HyperliquidFuturesBrokerage : SharedFuturesBrokerage
    {
        private readonly HyperLiquidRestClient _restClient;
        private readonly HyperLiquidSocketClient _socketClient;

        private CryptoExchange.Net.Objects.Sockets.UpdateSubscription _fundingSubscription;
        private readonly object _fundingSubLock = new();

        public HyperliquidFuturesBrokerage() : base("Hyperliquid")
        {
        }

        internal HyperliquidFuturesBrokerage(
            HyperLiquidRestClient restClient,
            HyperLiquidSocketClient socketClient,
            Func<List<Holding>> getHoldingsFunc)
            : base(
                "Hyperliquid",
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

        protected override string NormalizeSymbol(string rawSymbol)
            => rawSymbol.EndsWith("USDC", StringComparison.OrdinalIgnoreCase)
                ? rawSymbol.ToUpperInvariant()
                : rawSymbol.ToUpperInvariant() + "USDC";

        protected override SharedSymbol GetSharedSymbol(Symbol s)
        {
            var ticker = s.Value.ToUpperInvariant();
            var coin = ticker.EndsWith("USDC") ? ticker[..^4] : ticker;
            return new SharedSymbol(TradingMode.PerpetualLinear, coin, "USDC");
        }

        public override IEnumerator<BaseData> Subscribe(SubscriptionDataConfig config, EventHandler handler)
        {
            if (config.Type == typeof(MarginInterestRate))
            {
                lock (_fundingSubLock)
                {
                    if (_fundingSubscription == null)
                    {
                        var sub = RunSync(() =>
                            _socketClient.FuturesApi.Account.SubscribeToUserFundingUpdatesAsync(
                                null,
                                update =>
                                {
                                    foreach (var funding in update.Data)
                                    {
                                        if (_ticks.Count >= MaxQueueSize) break;

                                        var leanTicker = NormalizeSymbol(funding.Symbol);
                                        var sym = Symbol.Create(leanTicker, SecurityType.CryptoFuture, "HyperLiquid");

                                        _ticks.Enqueue(new MarginInterestRate
                                        {
                                            Symbol = sym,
                                            Time = funding.Timestamp ?? DateTime.UtcNow,
                                            InterestRate = funding.FundingRate
                                        });
                                    }
                                    handler?.Invoke(this, EventArgs.Empty);
                                }));

                        if (sub.Success) _fundingSubscription = sub.Data;
                    }
                }
                return GetNextTicks().GetEnumerator();
            }

            return base.Subscribe(config, handler);
        }
    }
}