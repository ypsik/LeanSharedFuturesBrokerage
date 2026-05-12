using CryptoExchange.Net.Authentication;
using CryptoExchange.Net.Interfaces.Clients;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.Requests;
using CryptoExchange.Net.SharedApis;
using Fasterflect;
using HyperLiquid.Net;
using HyperLiquid.Net.Clients;
using HyperLiquid.Net.Enums;
using HyperLiquid.Net.Objects.Models;
using QuantConnect;
using QuantConnect.Api;
using QuantConnect.Brokerages;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
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
        private readonly ConcurrentDictionary<Symbol, int> _lastFundingHour = new();
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
                socketClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                aggregator,
                getHoldingsFunc);

        }

        protected override void InitializeFromJob(QuantConnect.Packets.LiveNodePacket job, IDataAggregator aggregator)
        {
            // 1. Instanzen schützen: Nur erstellen, wenn sie null sind
            if (_restClient == null)
            {
                // Falls wir im Live-Modus sind, brauchen wir die Keys aus dem Job
                job.BrokerageData.TryGetValue("hyperliquid-address", out var key);
                job.BrokerageData.TryGetValue("hyperliquid-secret", out var secret);

                _restClient = new HyperLiquidRestClient(options => {
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret))
                        options.ApiCredentials = new HyperLiquidCredentials(key, secret);
                });
            }

            if (_socketClient == null)
            {
                _socketClient = new HyperLiquidSocketClient();
            }

            // 2. User-Details schützen: Nur überschreiben, wenn der Job explizit etwas Neues liefert
            if (String.IsNullOrEmpty(_vaultAdress) && job.BrokerageData.TryGetValue("hyperliquid-vault-address", out var vault) && !string.IsNullOrEmpty(vault))
            {
                _vaultAdress = vault;
            }

            // 3. Basisklasse synchronisieren
            // Wir nutzen die bestehenden (oder gerade erstellten) Instanzen
            InitializeBase(
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                aggregator,
                _getHoldingsFunc
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

        #region Balance
        public override List<CashAmount> GetCashBalance()
        {
            var result = new List<CashAmount>();
            var accountInfo = RunSync(() => _restClient.FuturesApi.Account.GetAccountInfoAsync());
            if (accountInfo)
            {
                result.Add(new CashAmount(accountInfo.Data.MarginSummary?.AccountValue??accountInfo.Data.CrossMarginSummary?.AccountValue??0m, "USDC"));
            }
            return result;                            
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
        /*
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
        */
        protected override bool SubscribeFunding(Symbol symbol)
        {
            _lastFundingHour[symbol] = DateTime.UtcNow.Hour;

            var ticker = symbol.Value.ToUpperInvariant();
            var hyperliquidCoin = ticker.EndsWith("USDC") ? ticker[..^4] : ticker;
            var sub = RunSync(() =>
                _socketClient.FuturesApi.ExchangeData.SubscribeToSymbolUpdatesAsync(hyperliquidCoin, data =>
                {
                    var ticker = data.Data;

                    var currentFunding = ticker.FundingRate;
                    var now = DateTime.UtcNow;
                    var currentHour = now.Hour;

                    // Präzisions-Trigger: Wir vergleichen die aktuelle Stunde mit der letzten gespeicherten.
                    // Sobald die Systemzeit umspringt, wird das nächste eintreffende Paket sofort verarbeitet.
                    if (_lastFundingHour.TryGetValue(symbol, out var lastHour) && currentHour == lastHour)
                    {
                        return;
                    }

                    // Sofortiger Lock für die restliche Stunde
                    _lastFundingHour[symbol] = currentHour;
                    var rounded = new DateTime(now.Year, now.Month, now.Day, currentHour, 0, 0, DateTimeKind.Utc);
                    Log.Trace($"Hyperliquid Funding Update: {ticker.Symbol} -> Rate: {currentFunding}");

                    var funding = new MarginInterestRate
                    {
                        Symbol = symbol,
                        Time = rounded,
                        InterestRate = currentFunding ?? 0
                    };

                    _aggregator.Update(funding);
                })
            );
            if (sub.Success)
            {
                Log.Trace($"{Name} Symbol updates: Subscribed.");

                var subscription = sub.Data;

                subscription.ConnectionLost += () =>
                {
                    _isConnectedOrder = false;
                    Log.Error($"{Name} Symbol updates: Connection lost!");
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "Disconnect", "Symbol updates stream lost."));
                };

                subscription.ConnectionRestored += (duration) =>
                {
                    _isConnectedOrder = true;
                    Log.Trace($"{Name} Symbol updates: Connection restored after {duration}.");
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Information, "Reconnect", $"Symbol updates stream restored. Syncing..."));
                };
            }
            else
            {
                Log.Error($"{Name} SubscribeFunding failed for {symbol}: {sub.Error?.Message}");
                return false;
            }

            return true;
        }

        protected override async Task<ExchangeWebResult<SharedId>> ExecutePlaceOrderAsync(PlaceFuturesOrderRequest request)
        {
            var res = await _restClient.FuturesApi.Trading.PlaceOrderAsync(
                symbol: request.Symbol.BaseAsset,
                side: request.Side == SharedOrderSide.Buy ? OrderSide.Buy : OrderSide.Sell,
                orderType: request.OrderType == SharedOrderType.Limit ? HyperLiquid.Net.Enums.OrderType.Limit : HyperLiquid.Net.Enums.OrderType.Market,
                quantity: request.Quantity?.QuantityInBaseAsset ?? 0m,
                price: request.Price ?? 0m,
                vaultAddress: _vaultAdress);

            if (!res.Success)
                return new ExchangeWebResult<SharedId>(Name, res.Error);

            return new ExchangeWebResult<SharedId>(
                    Name,
                    TradingMode.PerpetualLinear,
                    res.As(new SharedId(res.Data.OrderId.ToString()))
                );
        }

        protected override async Task<ExchangeWebResult<SharedId>> ExecuteCancelOrderAsync(CxCancelOrderRequest request)
        {
            var res = await _restClient.FuturesApi.Trading.CancelOrderAsync(
                symbol: request.Symbol.BaseAsset,
                orderId: long.Parse(request.OrderId),
                vaultAddress: _vaultAdress);

            if (!res.Success)
                return new ExchangeWebResult<SharedId>(Name, res.Error);

            return new ExchangeWebResult<SharedId>(
                    Name,
                    TradingMode.PerpetualLinear,
                    res.As(new SharedId(request.OrderId.ToString()))
                );
        }

        protected override async Task<ExchangeWebResult<SharedId>> ExecuteUpdateOrderAsync(Order order)
        {
            var res = await _restClient.FuturesApi.Trading.EditOrderAsync(
                          symbol: order.Symbol.Value,
                          orderId: order.Id,
                          clientOrderId: null,
                          side: order.Quantity > 0 ? OrderSide.Buy : OrderSide.Sell,
                          orderType: order.Type == QuantConnect.Orders.OrderType.Limit ? HyperLiquid.Net.Enums.OrderType.Limit : HyperLiquid.Net.Enums.OrderType.Market,
                          quantity: Math.Abs(order.Quantity),
                          price: order.Price,
                          vaultAddress: _vaultAdress);

            if (!res.Success)
                return new ExchangeWebResult<SharedId>(Name, res.Error);

            return new ExchangeWebResult<SharedId>(
                    Name,
                    TradingMode.PerpetualLinear,
                    res.As(new SharedId(order.Id.ToString()))
                );
        }
    }
}