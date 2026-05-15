using Accord.IO;
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
using QLNet;
using QuantConnect;
using QuantConnect.Algorithm.Framework.Selection;
using QuantConnect.Api;
using QuantConnect.Brokerages;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Indicators;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantConnect.Util;
using RestSharp;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Timers;

using CxCancelOrderRequest = CryptoExchange.Net.SharedApis.CancelOrderRequest;


namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    public class HyperliquidFuturesBrokerage : SharedFuturesBrokerage
    {
        private HyperLiquidRestClient _restClient;
        private HyperLiquidSocketClient _socketClient;

        private string _vaultAdress;

        private readonly object _fundingUpdateLock = new();
        private bool _fundingUpdateConnected = false;
        private UpdateSubscription _fundingUpdateSubscription;
        private readonly object _fundingLock = new();
        private readonly ConcurrentDictionary<Symbol, int> _lastFundingHour = new();
        private readonly ConcurrentDictionary<string, UpdateSubscription> _subscriptions = new();

        // 1. LEAN DataQueueHandler Konstruktor
        public HyperliquidFuturesBrokerage() : base("hyperliquid")
        {
        }

        // 2. Trading-Instanz Konstruktor (Optionaler Parameter fix)
        internal HyperliquidFuturesBrokerage(
            IAlgorithm algorithm,
            HyperLiquidRestClient restClient,
            HyperLiquidSocketClient socketClient,
            string vaultAddress,
            IDataAggregator aggregator,
            Func<List<Holding>> getHoldingsFunc = null) // 🔥 Fix: Optional gemacht
            : base(algorithm, "hyperliquid")
        {
            _vaultAdress = vaultAddress;
            _restClient = restClient;
            _socketClient = socketClient;

            PopulateSPDB();

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

        public override bool IsConnected => base.IsConnected && _fundingUpdateConnected;

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
 
        private void PopulateSPDB()
        {
            // --- Populate SPDB with all live HL assets ---
            var result = _restClient.FuturesApi.ExchangeData
                 .GetExchangeInfoAsync()
                 .GetAwaiter().GetResult();

            if (!result.Success)
                throw new Exception($"Failed to load Hyperliquid assets: {result.Error}");

            // WICHTIG: Die Summe aus szDecimals und pxDecimals ist bei HL Perps 5!
            const int HL_SUM_DECIMALS = 5;

            foreach (var symbol in result.Data.Where(s => !s.IsDelisted))
            {
                var ticker = symbol.Name + "USDC";

                var lotSize = (decimal)Math.Pow(10, -symbol.QuantityDecimals);

                var priceDecimals = HL_SUM_DECIMALS - symbol.QuantityDecimals;

                decimal tickSize;

                if (priceDecimals >= 0)
                    tickSize = (decimal)Math.Pow(10, -priceDecimals);
                else
                    tickSize = 1m;

                var symbolProperties = new SymbolProperties(
                    description: $"Hyperliquid {symbol.Name} Perpetual",
                    quoteCurrency: "USDC",
                    contractMultiplier: 1m,
                    minimumPriceVariation: tickSize,
                    lotSize: lotSize,
                    marketTicker: symbol.Name
                );

                _spdb.SetEntry("hyperliquid", ticker, SecurityType.CryptoFuture, symbolProperties);
            }
        }

        #region Symbol Mapping
        protected override string NormalizeSymbol(string rawSymbol)
        {
            var upper = rawSymbol.ToUpperInvariant();
            return upper.EndsWith("USDC") ? upper : upper + "USDC";
        }

        protected override SharedSymbol GetSharedSymbol(Symbol s, string quoteAsset = "USDC")
        {
            var ticker = s.Value.ToUpperInvariant();
            var baseAsset = ticker.EndsWith(quoteAsset) ? ticker[..^4] : ticker;
            return new SharedSymbol(TradingMode.PerpetualLinear, baseAsset, quoteAsset);
        }
        protected override string NativeTicker(Symbol symbol)
        {
            CurrencyPairUtil.DecomposeCurrencyPair(symbol, out var baseAsset, out _);
            return baseAsset;
        }

        #endregion

        #region Balance
        public override List<CashAmount> GetCashBalance()
        {
            var result = new List<CashAmount>();
            var accountInfo = RunSync(() => _restClient.FuturesApi.Account.GetAccountInfoAsync());
            if (accountInfo.Success)
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
        
        #region Connect
        public override void Connect()
        {
            lock (_fundingUpdateLock)
            {
                if (_fundingUpdateSubscription == null && _socketClient != null)
                {
                    var sub = RunSync(() =>

                    _socketClient.FuturesApi.Account.SubscribeToUserFundingUpdatesAsync(null, 
                        update =>
                        {
                            OnBalanceUpdated();
                        }));

                    if (sub.Success)
                    {
                        _fundingUpdateSubscription = sub.Data;
                        _fundingUpdateConnected = true;

                        Log.Trace($"{Name} Funding updates: Subscribed.");

                        var subscription = sub.Data;
                        subscription.ConnectionLost += () =>
                        {
                            _fundingUpdateConnected = false;
                            Log.Error($"{Name} Funding updates: Connection lost!");
                            OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "Disconnect", "Funding updates stream lost."));
                        };

                        subscription.ConnectionRestored += (duration) =>
                        {
                            _fundingUpdateConnected = true;
                            Log.Trace($"{Name} Funding updates: Connection restored after {duration}.");
                            OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Information, "Reconnect", $"Funding updates stream restored. Syncing..."));
                        };
                    }                    
                }

                base.Connect();
            }
        }
        public override void Disconnect()
        {
            RunSync(() => _fundingUpdateSubscription?.CloseAsync()?? Task.CompletedTask);
            base.Disconnect();
        }
        #endregion
        
        protected override bool SubscribeFunding(Symbol symbol)
        {
            var hyperliquidCoin = NativeTicker(symbol);
            var subKey = $"{symbol.Value}_FUNDING";

            lock (_fundingLock)
            {
                if (_subscriptions.ContainsKey(subKey))
                {
                    return true;
                }

                var sub = RunSync(() =>
                    _socketClient.FuturesApi.ExchangeData.SubscribeToSymbolUpdatesAsync(hyperliquidCoin, data =>
                    {
                        var tickerData = data.Data;
                        var now = DateTime.UtcNow;
                        var currentHour = now.Hour;

                        bool isFirstTick = false;
                        bool isHourRollover = false;

                        _lastFundingHour.AddOrUpdate(
                            symbol,
                            addValueFactory: (key) =>
                            {
                                isFirstTick = true;
                                return currentHour;
                            },
                            updateValueFactory: (key, oldHour) =>
                            {
                                if (oldHour != currentHour)
                                {
                                    isHourRollover = true;
                                    return currentHour;
                                }
                                return oldHour;
                            });

                        if (!isFirstTick && !isHourRollover)
                        {
                            return;
                        }

                        // --- 1. FUNDING UPDATE (Priorität: Direkt an LEAN senden) ---
                        if (isHourRollover)
                        {
                            var currentFunding = tickerData.FundingRate ?? 0;
                            var roundedTime = new DateTime(now.Year, now.Month, now.Day, currentHour, 0, 0, DateTimeKind.Utc);

                            _aggregator.Update(new MarginInterestRate
                            {
                                Symbol = symbol,
                                Time = roundedTime,
                                InterestRate = currentFunding
                            });

                            Log.Trace($"Hyperliquid Funding Update: {tickerData.Symbol} -> Rate: {currentFunding}");
                        }

                        // --- 2. SPDB FIX (Läuft direkt im Anschluss) ---
                        var oraclePrice = tickerData.OraclePrice ?? tickerData.MarkPrice;
                        if (oraclePrice > 0)
                        {
                            var exponent = (int)Math.Floor(Math.Log10((double)oraclePrice));
                            var decimalPlaces = Math.Max(0, Math.Min(6, 5 - (exponent + 1)));
                            decimal tickSize = (decimal)Math.Pow(10, -decimalPlaces);

                            var props = _spdb.GetSymbolProperties(symbol.ID.Market, symbol, symbol.SecurityType, "USDC");

                            if (props == null || props.MinimumPriceVariation != tickSize)
                            {
                                var newProps = new SymbolProperties(
                                    props?.Description ?? $"Hyperliquid {symbol.Value} Perpetual",
                                    props?.QuoteCurrency ?? "USDC",
                                    props?.ContractMultiplier ?? 1m,
                                    tickSize,
                                    props?.LotSize ?? (decimal)Math.Pow(10, -6),
                                    props?.MarketTicker ?? symbol.Value
                                );
                                _spdb.SetEntry(symbol.ID.Market, symbol.Value, symbol.SecurityType, newProps);
                                if (_algorithm.Securities.ContainsKey(symbol))
                                {
                                    var method = typeof(Security).GetMethod("UpdateSymbolProperties",
                                        BindingFlags.NonPublic | BindingFlags.Instance);

                                    if (method != null)
                                        method.Invoke(_algorithm.Securities[symbol], new object[] { newProps });
                                    else
                                        Log.Error($"{Name}: UpdateSymbolProperties method not found on Security — LEAN API may have changed");
                                }

                                Log.Trace($"{Name}: SPDB Fix for {symbol.Value} - TickSize: {tickSize} (Price: {oraclePrice})");
                            }
                        }
                    })
                );

                if (sub.Success)
                {
                    _subscriptions.TryAdd(subKey, sub.Data);
                    Log.Trace($"{Name} Symbol updates: Subscribed for {symbol.Value}.");

                    var subscription = sub.Data;
                    subscription.ConnectionLost += () =>
                    {
                        Log.Error($"{Name} Symbol {hyperliquidCoin} updates: Connection lost!");
                        OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "Disconnect", $"Symbol {hyperliquidCoin} updates stream lost."));
                    };

                    subscription.ConnectionRestored += (duration) =>
                    {
                        Log.Trace($"{Name} Symbol {hyperliquidCoin} updates: Connection restored after {duration}.");
                        OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Information, "Reconnect", $"Symbol {hyperliquidCoin} updates stream restored. Syncing..."));
                    };

                    return true;
                }

                Log.Error($"{Name} SubscribeFunding failed for {symbol}: {sub.Error?.Message}");
                return false;
            }
        }
        protected override bool UnsubscribeFunding(Symbol symbol)
        {
            var subKey = $"{symbol.Value}_FUNDING";
            if (_subscriptions.TryRemove(subKey, out var sub))
            {
                Log.Trace($"{Name}.UnsubscribeFunding: Found and closing subscription for {subKey}");
                RunSync(() => sub.CloseAsync());
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
            {
                Log.Error($"HL-Update-Error: {res.Error} | " +
                          $"Price: {request.Price ?? 0m} | " +
                          $"OriginalData : {res.OriginalData}");

                return new ExchangeWebResult<SharedId>(Name, res.Error);
            }

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
            {
                Log.Error($"HL-Update-Error: {res.Error} | " +
                          $"OriginalData : {res.OriginalData}");

                return new ExchangeWebResult<SharedId>(Name, res.Error);
            }

            return new ExchangeWebResult<SharedId>(
                    Name,
                    TradingMode.PerpetualLinear,
                    res.As(new SharedId(request.OrderId.ToString()))
                );
        }

        protected override async Task<ExchangeWebResult<SharedId>> ExecuteUpdateOrderAsync(Order order, decimal price, decimal quantity)
        {
            var ticker = NativeTicker(order.Symbol);
            OrderSide side = quantity > 0 ? OrderSide.Buy : OrderSide.Sell;

            var res = await _restClient.FuturesApi.Trading.EditOrderAsync(
                          symbol: ticker,
                          orderId: long.Parse(order.BrokerId.Last()),
                          clientOrderId: null,
                          side: side,
                          orderType: order.Type == QuantConnect.Orders.OrderType.Limit
                              ? HyperLiquid.Net.Enums.OrderType.Limit
                              : HyperLiquid.Net.Enums.OrderType.Market,
                          quantity: Math.Abs(quantity),
                          price: price,
                          vaultAddress: _vaultAdress);

            if (!res.Success)
            {
                Log.Error($"Hyperliquid update error: {res.Error} | Ticker: {ticker} | Price: {price} | OriginalData: {res.OriginalData}");
                return new ExchangeWebResult<SharedId>(Name, res.Error);
            }

            return new ExchangeWebResult<SharedId>(
                    Name,
                    TradingMode.PerpetualLinear,
                    res.As(new SharedId(order.Id.ToString()))
                );
        }
    }
}