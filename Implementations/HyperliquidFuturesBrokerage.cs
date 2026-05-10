using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using HyperLiquid.Net.Clients;
using HyperLiquid.Net.Enums;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Securities;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

using CxCancelOrderRequest = CryptoExchange.Net.SharedApis.CancelOrderRequest;


namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    public class HyperliquidFuturesBrokerage : SharedFuturesBrokerage
    {
        private HyperLiquidRestClient _restClient;
        private HyperLiquidSocketClient _socketClient;

        private string _vaultAdress;

        private UpdateSubscription _fundingSubscription;
        private readonly object _fundingLock = new();
        private readonly ConcurrentDictionary<string, UpdateSubscription> _subscriptions = new();

        // 1. LEAN DataQueueHandler Konstruktor
        public HyperliquidFuturesBrokerage() : base("hyperliquid")
        {
        }

        // 2. Trading-Instanz Konstruktor (Optionaler Parameter fix)
        internal HyperliquidFuturesBrokerage(
            HyperLiquidRestClient restClient,
            HyperLiquidSocketClient socketClient,
            string vaultAddress,
            IDataAggregator aggregator,
            Func<List<Holding>> getHoldingsFunc = null) // 🔥 Fix: Optional gemacht
            : base("hyperliquid")
        {
            _vaultAdress = vaultAddress;
            _restClient = restClient;
            _socketClient = socketClient;

            InitializeBase(
                restClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                aggregator,
                getHoldingsFunc);
        }

        protected override void InitializeFromJob(QuantConnect.Packets.LiveNodePacket job, IDataAggregator aggregator)
        {
            _restClient = new HyperLiquidRestClient();
            _socketClient = new HyperLiquidSocketClient();

            InitializeBase(
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                aggregator
            );
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
            return new SharedSymbol(TradingMode.PerpetualLinear, baseAsset, "USDC");
        }
        #endregion

        #region LEAN Data Manager

        protected override bool SubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType)
        {
            foreach (var symbol in symbols)
            {
                var shared = GetSharedSymbol(symbol);
                var subKey = $"{symbol.Value}_{tickType}";
                if (_subscriptions.ContainsKey(subKey)) continue;

                if (tickType == TickType.Trade)
                {
                    // Trades kommen oft als Array (SharedTrade[])
                    var sub = RunSync(() => _socketClient.FuturesApi.SharedClient.SubscribeToTradeUpdatesAsync(
                        new SubscribeTradeRequest(shared),
                        update =>
                        {
                            foreach (var item in update.Data)
                            {
                                EmitTick(new Tick
                                {
                                    Symbol = symbol,
                                    Time = item.Timestamp.ToUniversalTime(),
                                    TickType = TickType.Trade,
                                    Value = item.Price,
                                    Quantity = item.Quantity
                                });
                            }
                        }));

                    if (sub.Success) _subscriptions[subKey] = sub.Data;
                }
                else if (tickType == TickType.Quote)
                {
                    // 🔥 FIX: BookTicker (Quotes) ist ein einzelnes Objekt, kein Array!
                    var sub = RunSync(() => _socketClient.FuturesApi.SharedClient.SubscribeToBookTickerUpdatesAsync(
                        new SubscribeBookTickerRequest(shared),
                        update =>
                        {
                            var q = update.Data; // Hier kein foreach nötig
                            EmitTick(new Tick
                            {
                                Symbol = symbol,
                                Time = DateTime.UtcNow,
                                TickType = TickType.Quote,
                                BidPrice = q.BestBidPrice,
                                BidSize = q.BestBidQuantity,
                                AskPrice = q.BestAskPrice,
                                AskSize = q.BestAskQuantity
                            });
                        }));

                    if (sub.Success) _subscriptions[subKey] = sub.Data;
                }
            }
            return true;
        }

        protected override bool UnsubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType)
        {
            foreach (var symbol in symbols)
            {
                if (_subscriptions.TryRemove($"{symbol.Value}_{tickType}", out var sub))
                {
                    RunSync(() => sub.CloseAsync());
                }
            }
            return true;
        }
        #endregion

        #region Funding Special Case
        public override IEnumerator<BaseData> Subscribe(SubscriptionDataConfig config, EventHandler handler)
        {
            if (config.Type == typeof(MarginInterestRate))
            {
                lock (_fundingLock)
                {
                    if (_fundingSubscription == null && _socketClient != null)
                    {
                        var sub = RunSync(() =>
                            _socketClient.FuturesApi.Account.SubscribeToUserFundingUpdatesAsync(
                                null,
                                update =>
                                {
                                    foreach (var funding in update.Data)
                                    {
                                        var leanSymbol = Symbol.Create(NormalizeSymbol(funding.Symbol), SecurityType.CryptoFuture, Name);
                                        var rate = new MarginInterestRate
                                        {
                                            Symbol = leanSymbol,
                                            Time = funding.Timestamp ?? DateTime.UtcNow,
                                            InterestRate = funding.FundingRate
                                        };
                                        handler?.Invoke(rate, EventArgs.Empty);
                                    }
                                }));
                        if (sub.Success) _fundingSubscription = sub.Data;
                    }
                }
                return Enumerable.Empty<BaseData>().GetEnumerator();
            }
            return base.Subscribe(config, handler);
        }
        #endregion

        protected override async Task<ExchangeWebResult<SharedId>> ExecutePlaceOrderAsync(PlaceFuturesOrderRequest request)
        {
            var res = await _restClient.FuturesApi.Trading.PlaceOrderAsync(
                symbol: request.Symbol.BaseAsset,
                side: request.Side == SharedOrderSide.Buy ? OrderSide.Buy : OrderSide.Sell,
                orderType: request.OrderType == SharedOrderType.Limit ? OrderType.Limit : OrderType.Market,
                quantity: request.Quantity?.QuantityInBaseAsset ?? 0m,
                price: request.Price ?? 0m,
                vaultAddress: _vaultAdress);

            if (!res.Success)
                return new ExchangeWebResult<SharedId>(Name, res.Error);

            return new ExchangeWebResult<SharedId>(Name, null);
        }

        protected override async Task<ExchangeWebResult<SharedId>> ExecuteCancelOrderAsync(CxCancelOrderRequest request)
        {
            var res = await _restClient.FuturesApi.Trading.CancelOrderAsync(
                symbol: request.Symbol.BaseAsset,
                orderId: long.Parse(request.OrderId),
                vaultAddress: _vaultAdress);

            if (!res.Success)
                return new ExchangeWebResult<SharedId>(Name, res.Error);

            return new ExchangeWebResult<SharedId>(Name, null);
        }
    }
}