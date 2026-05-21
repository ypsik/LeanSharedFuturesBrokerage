using Accord.IO;
using CryptoExchange.Net.Authentication;
using CryptoExchange.Net.Interfaces.Clients;
using CryptoExchange.Net.Objects;
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
        private HyperLiquidSocketClient _socketClientExData; // dedicated for ExchangeData (SubscribeFunding)

        private string _vaultAdress;

        private readonly object _fundingUpdateLock = new();
        private bool _fundingUpdateConnected = false;
        private UpdateSubscription _fundingUpdateSubscription;

        public override decimal MinimumOrderNotionalValue => 10m;
        protected override int MaxHistoryLookbackMinutes => 5000;

        protected override string SettleAsset => "USDC";



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
            _socketClientExData = new HyperLiquidSocketClient();

            PopulateSPDB();

            InitializeBase(
                restClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                aggregator,
                getHoldingsFunc);

        }

        public override bool IsConnected => base.IsConnected && _fundingUpdateConnected;
        protected override int FundingRolloverHours => 1;

        protected override bool IsTerminalUpdateError(string errorMsg)
                => errorMsg.Contains("canceled or filled", StringComparison.OrdinalIgnoreCase);

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

            if (_socketClientExData == null)
            {
                _socketClientExData = new HyperLiquidSocketClient();
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
                var ticker = symbol.Name + SettleAsset;

                var lotSize = (decimal)Math.Pow(10, -symbol.QuantityDecimals);

                var priceDecimals = HL_SUM_DECIMALS - symbol.QuantityDecimals;

                decimal tickSize;

                if (priceDecimals >= 0)
                    tickSize = (decimal)Math.Pow(10, -priceDecimals);
                else
                    tickSize = 1m;

                var symbolProperties = new SymbolProperties(
                    description: $"Hyperliquid {symbol.Name} Perpetual",
                    quoteCurrency: SettleAsset,
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
            return upper.EndsWith(SettleAsset) ? upper : upper + SettleAsset;
        }

        protected override string NativeTicker(Symbol symbol)
        {
            CurrencyPairUtil.DecomposeCurrencyPair(symbol, out var baseAsset, out _);
            return baseAsset;
        }

        #endregion

        #region Connect
        public override void Connect()
        {
            lock (_fundingUpdateLock)
            {
                if (_fundingUpdateSubscription == null && _socketClient != null)
                {
                    _subRateGate.WaitToProceed();
                    DateTime connectTime = StartTime;
                    var sub = RunSync(() =>
                            _socketClient.FuturesApi.Account.SubscribeToUserFundingUpdatesAsync(null,
                            update =>
                            {
                                if (update?.Data == null) return;

                                foreach (var fundingsRecord in update.Data.Where(f => f != null && (f.Timestamp ?? DateTime.MinValue) > connectTime))
                                {
                                    if (_algorithm?.Portfolio?.CashBook != null)
                                    {
                                        _algorithm.Portfolio.CashBook[SettleAsset].AddAmount(fundingsRecord.Usdc);
                                        OnMessage(new FundingBrokerageMessageEvent(SettleAsset, fundingsRecord.Usdc));
                                    }

                                }
                                DateTime timeStamp = update.Data.Max(f => f?.Timestamp ?? DateTime.MinValue);
                                if (timeStamp > connectTime)
                                {
                                    connectTime = timeStamp;
                                }
                            }));

                    SetupSubscriptionEvents(
                                    sub.Success,
                                    sub.Data,
                                    (state) => { _fundingUpdateConnected = state; },
                                    "Funding updates",
                                    "Funding updates subscription failed",
                                    sub.Error?.ToString()
                                );

                    if (sub.Success)
                    {
                        _fundingUpdateSubscription = sub.Data;
                    }
                }

                base.Connect();
            }
        }

        public override void Disconnect()
        {
            RunSync(() => _fundingUpdateSubscription?.CloseAsync() ?? Task.CompletedTask);
            _socketClientExData?.Dispose();
            base.Disconnect();
        }
        #endregion

        protected override async Task<CallResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
            string nativeTicker, Symbol symbol, Func<DateTime, decimal?, bool> onFundingRate)
        {
            return await _socketClientExData.FuturesApi.ExchangeData.SubscribeToSymbolUpdatesAsync(
                nativeTicker, data =>
                {
                    var now = data.DataTime ?? data.ReceiveTime;
                    var tickerData = data.Data;

                    if (!onFundingRate(now, tickerData.FundingRate ?? 0)) return;

                    var oraclePrice = tickerData.OraclePrice ?? tickerData.MarkPrice;
                    if (oraclePrice > 0)
                        UpdateSpdb(symbol, oraclePrice);
                });
        }

        private void UpdateSpdb(Symbol symbol, decimal oraclePrice)
        {
            var exponent = (int)Math.Floor(Math.Log10((double)oraclePrice));
            var decimalPlaces = Math.Max(0, Math.Min(6, 5 - (exponent + 1)));
            decimal tickSize = (decimal)Math.Pow(10, -decimalPlaces);

            var props = _spdb.GetSymbolProperties(symbol.ID.Market, symbol, symbol.SecurityType, SettleAsset);

            if (props != null && props.MinimumPriceVariation == tickSize) return;

            var newProps = new SymbolProperties(
                props?.Description ?? $"Hyperliquid {symbol.Value} Perpetual",
                props?.QuoteCurrency ?? SettleAsset,
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
        /*
        protected override ExchangeParameters PlaceFuturesOrderExchangeParameters
        {
            get
            {
                var parameters = base.PlaceFuturesOrderExchangeParameters;
                if (!String.IsNullOrEmpty(_vaultAdress))
                    parameters.AddValue(new ExchangeParameter("Hyperliquid", "vaultAddress", _vaultAdress));
                return parameters;
            }
        }*/
        

        protected override async Task<ExchangeWebResult<SharedId>> ExecutePlaceOrderAsync(PlaceFuturesOrderRequest request)
        {
            var res = await _restClient.FuturesApi.Trading.PlaceOrderAsync(
                symbol: request.Symbol.BaseAsset,
                side: request.Side == SharedOrderSide.Buy ? OrderSide.Buy : OrderSide.Sell,
                orderType: request.OrderType == SharedOrderType.Limit ? HyperLiquid.Net.Enums.OrderType.Limit : HyperLiquid.Net.Enums.OrderType.Market,
                quantity: request.Quantity?.QuantityInBaseAsset ?? 0m,
                price: request.Price ?? 0m,
                clientOrderId: request.ClientOrderId, // NEU: Zwingend erforderlich für das spätere Socket-Tracking
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
        
        protected override async Task<ExchangeWebResult<SharedId>> ExecuteUpdateOrderAsync(Order order, string clientOrderId, decimal price, decimal quantity)
        {
            var ticker = NativeTicker(order.Symbol);
            OrderSide side = quantity > 0 ? OrderSide.Buy : OrderSide.Sell;

            var res = await _restClient.FuturesApi.Trading.EditOrderAsync(
                          symbol: ticker,
                          orderId: String.IsNullOrEmpty(clientOrderId) ?  long.Parse(order.BrokerId.Last()) : null,
                          clientOrderId: clientOrderId,
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

            // Nutzt die LEAN OrderId als temporären Platzhalter.
            // Die Basisklasse ordnet sie temporär zu, bis der Socket über die ClientOrderId die neue BrokerId meldet.
            return new ExchangeWebResult<SharedId>(
                    Name,
                    TradingMode.PerpetualLinear,
                    res.As(new SharedId(order.Id.ToString()))
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

    }
}