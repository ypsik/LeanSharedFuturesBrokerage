using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using OKX.Net;
using OKX.Net.Clients;
using OKX.Net.Enums;
using OKX.Net.Objects;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantConnect.Util;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    public class OkxFuturesBrokerage : SharedFuturesBrokerage
    {
        private OKXRestClient _restClient;
        private OKXSocketClient _socketClient;
        private OKXSocketClient _socketClientExData;

        // OKX instrument type used for symbol discovery.
        // SWAP    = true perpetuals (Global/US accounts).
        // FUTURES = X-Perps (EU/EEA MiFID-regulated, 5-year expiry with funding rate).
        private readonly InstrumentType _instrumentType;

        // Rule type filter for PopulateSPDB.
        // SymbolRuleType.Perp on EU accounts (X-Perp FUTURES, JSON "xperp").
        // null on Global accounts (SWAP — no ruleType filter needed).
        private readonly SymbolRuleType? _ruleTypeFilter;

        public override bool ExchangeSupportsUserTradeStream => false;

        internal OkxFuturesBrokerage(
            IAlgorithm algorithm,
            OKXRestClient restClient,
            OKXSocketClient socketClient,
            IDataAggregator aggregator,
            InstrumentType instrumentType = InstrumentType.Futures,
            SymbolRuleType? ruleTypeFilter = SymbolRuleType.Perp,
            Func<List<Holding>>? getHoldingsFunc = null)
            : base(algorithm, "okx")
        {
            _restClient = restClient;
            _socketClient = socketClient;
            _instrumentType = instrumentType;
            _ruleTypeFilter = ruleTypeFilter;

            PopulateSPDB();

            // Dedicated unauthenticated socket client for public funding rate subscriptions.
            _socketClientExData = new OKXSocketClient(options =>
            {
                options.Environment = restClient.ClientOptions.Environment;
            });

            InitializeBase(
                _restClient.UnifiedApi.SharedClient,
                _restClient.UnifiedApi.SharedClient,
                _socketClient.UnifiedApi.SharedClient,
                _socketClient.UnifiedApi.SharedClient,
                _socketClient.UnifiedApi.SharedClient,
                _socketClient.UnifiedApi.SharedClient,
                _restClient.UnifiedApi.SharedClient,
                _restClient.UnifiedApi.SharedClient,
                aggregator,
                getHoldingsFunc);
        }

        protected override void InitializeFromJob(QuantConnect.Packets.LiveNodePacket job, IDataAggregator aggregator)
        {
            job.BrokerageData.TryGetValue("okx-api-key", out var key);
            job.BrokerageData.TryGetValue("okx-api-secret", out var secret);
            job.BrokerageData.TryGetValue("okx-api-passphrase", out var passphrase);
            job.BrokerageData.TryGetValue("okx-environment", out var environmentStr);

            var environment = ResolveEnvironment(environmentStr);

            if (_restClient == null)
            {
                _restClient = new OKXRestClient(options =>
                {
                    options.Environment = environment;
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret) && !string.IsNullOrEmpty(passphrase))
                        options.ApiCredentials = new OKXCredentials(key, secret, passphrase);
                });
            }

            if (_socketClient == null)
            {
                _socketClient = new OKXSocketClient(options =>
                {
                    options.Environment = environment;
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret) && !string.IsNullOrEmpty(passphrase))
                        options.ApiCredentials = new OKXCredentials(key, secret, passphrase);
                });
            }

            if (_socketClientExData == null)
            {
                _socketClientExData = new OKXSocketClient(options =>
                {
                    options.Environment = environment;
                });
            }

            InitializeBase(
                _restClient.UnifiedApi.SharedClient,
                _restClient.UnifiedApi.SharedClient,
                _socketClient.UnifiedApi.SharedClient,
                _socketClient.UnifiedApi.SharedClient,
                _socketClient.UnifiedApi.SharedClient,
                _socketClient.UnifiedApi.SharedClient,
                _restClient.UnifiedApi.SharedClient,
                _restClient.UnifiedApi.SharedClient,
                aggregator,
                _getHoldingsFunc
            );
        }

        internal static OKXEnvironment ResolveEnvironment(string? environmentStr)
        {
            if (string.IsNullOrEmpty(environmentStr))
                return OKXEnvironment.Live;

            return environmentStr.Trim().ToLowerInvariant() switch
            {
                "europe" or "eea" => OKXEnvironment.Europe,
                "demo" => OKXEnvironment.Demo,
                _ => OKXEnvironment.Live
            };
        }

        #region Connect / Disconnect

        // OKX AmendOrder keeps the same order ID — identical in-place semantics to Kraken.
        public override bool ExchangeModifiesOrdersInPlace => true;

        public override void Connect()
        {
            base.Connect();
        }

        public override void Disconnect()
        {
            _socketClientExData?.Dispose();
            base.Disconnect();
        }

        #endregion

        private void PopulateSPDB()
        {
            var result = RunSync(() =>
                _restClient.UnifiedApi.ExchangeData.GetSymbolsAsync(_instrumentType));

            if (!result.Success)
                throw new Exception($"Failed to load OKX symbols: {result.Error}");

            foreach (var symbol in result.Data)
            {
                // Workaround: JKorf OKX.Net does not map "xperp" in SymbolRuleType enum (only "perp" exists).
                // OKX returns ruleType="xperp" for X-Perp FUTURES, which deserializes as null.
                // Until the upstream fix lands, filter: EU mode = accept only null RuleType on FUTURES
                // (normal FUTURES have RuleType=Normal, X-Perps have RuleType=null due to missing mapping).
                // Global SWAP mode: _ruleTypeFilter is null → no filter, accept all.
                // TODO: replace null-check with SymbolRuleType.XPerp once JKorf adds [Map("xperp")] XPerp.
                if (_ruleTypeFilter.HasValue && symbol.RuleType != null)
                    continue;

                // Only live/tradeable instruments.
                if (symbol.State != InstrumentState.Live)
                    continue;

                // For FUTURES/SWAP, baseCcy and quoteCcy are empty — parse from instFamily (e.g. "BTC-USD")
                // or fall back to splitting instId ("BTC-USD-310404" → base=BTC, quote=USD).
                string baseAsset, quoteAsset;
                if (!string.IsNullOrEmpty(symbol.InstrumentFamily))
                {
                    var familyParts = symbol.InstrumentFamily.Split('-');
                    baseAsset = familyParts.Length > 0 ? familyParts[0] : string.Empty;
                    quoteAsset = familyParts.Length > 1 ? familyParts[1] : SettleAsset;
                }
                else
                {
                    var idParts = symbol.Symbol.Split('-');
                    baseAsset = idParts.Length > 0 ? idParts[0] : string.Empty;
                    quoteAsset = idParts.Length > 1 ? idParts[1] : SettleAsset;
                }

                if (string.IsNullOrEmpty(baseAsset))
                {
                    Log.Error($"OkxFuturesBrokerage: cannot determine base asset for {symbol.Symbol}, skipping");
                    continue;
                }

                var ticker = baseAsset + quoteAsset;

                var tickSize = symbol.TickSize ?? 0.01m;
                var lotSize = symbol.LotSize ?? 1m;
                if (lotSize <= 0m) lotSize = 1m;

                // settleCcy is the margin/settlement asset for FUTURES/SWAP (e.g. "USD", "USDT").
                var quoteCurrency = string.IsNullOrEmpty(symbol.SettlementAsset) ? SettleAsset : symbol.SettlementAsset;
                var contractMultiplier = symbol.ContractValue ?? 1m;

                var symbolProperties = new SymbolProperties(
                    description: $"OKX {baseAsset} {_instrumentType}",
                    quoteCurrency: quoteCurrency,
                    contractMultiplier: contractMultiplier,
                    minimumPriceVariation: tickSize,
                    lotSize: lotSize,
                    marketTicker: symbol.Symbol    // full native instId, e.g. "ETH-USD-310404"
                );

                _spdb.SetEntry("okx", ticker, SecurityType.CryptoFuture, symbolProperties);

                // Register as Crypto so EnsureCurrencyDataFeed can resolve the base
                // currency conversion rate (same pattern as Kraken/Hyperliquid fix).
                if (!_spdb.ContainsKey("okx", ticker, SecurityType.Crypto))
                {
                    _spdb.SetEntry("okx", ticker, SecurityType.Crypto, symbolProperties);
                }
            }
        }

        // USD for coin-margined X-Perps (ETHUSD UM, BTCUSD UM …).
        protected override string SettleAsset => "USD";

        #region Symbol Mapping

        // OKX native: "BTC-USD-310404" or "BTC-USD-SWAP"
        // LEAN ticker:  "BTCUSD"
        protected override string NormalizeSymbol(string rawSymbol)
        {
            // Strip the third segment (expiry date or "SWAP").
            // "BTC-USD-310404" → "BTCUSD"
            // "BTC-USDT-SWAP"  → "BTCUSDT"
            var parts = rawSymbol.Split('-');
            if (parts.Length >= 2)
                return parts[0] + parts[1];

            return rawSymbol;
        }

        protected override string NativeTicker(Symbol symbol)
        {
            CurrencyPairUtil.DecomposeCurrencyPair(symbol, out var baseAsset, out var quoteAsset);

            if (_instrumentType == InstrumentType.Swap)
                return $"{baseAsset}-{quoteAsset}-SWAP";

            // For FUTURES (X-Perps) the expiry suffix is date-based ("310404").
            // The MarketTicker stored in SPDB during PopulateSPDB is the canonical instId.
            // Ticker key = baseAsset + quoteAsset (e.g. "BTCUSD"), same as PopulateSPDB.
            var leanTicker = baseAsset + quoteAsset;
            var entry = _spdb.GetSymbolProperties("okx", leanTicker, SecurityType.CryptoFuture, quoteAsset);
            if (entry != null && !string.IsNullOrEmpty(entry.MarketTicker))
                return entry.MarketTicker;

            throw new InvalidOperationException(
                $"OKX native ticker not found in SPDB for {symbol.Value}");
        }

        #endregion

        // OKX has a dedicated funding-rate WebSocket channel (public, unauthenticated).
        // Pushed every minute. OKXFundingRate.NextFundingTime is DateTime (non-nullable).
        protected override async Task<CallResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
            string nativeTicker, Symbol symbol, Func<DateTime, decimal?, DateTime?, bool> onFundingRate)
        {
            return await _socketClientExData.UnifiedApi.ExchangeData.SubscribeToFundingRateUpdatesAsync(
                nativeTicker,
                data =>
                {
                    var rate = data.Data;
                    onFundingRate(rate.FundingTime, rate.FundingRate, rate.NextFundingTime);
                });
        }

        // Account equity minus unrealized PnL so LEAN does not double-count open positions.
        // OKXAccountBalance.TotalEquity already includes unrealized PnL; we strip it here
        // to match the pattern used in Kraken, Bybit and Hyperliquid connectors.
        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() => _restClient.UnifiedApi.Account.GetAccountBalanceAsync());

            if (!res.Success || res.Data == null)
            {
                Log.Error($"OkxFuturesBrokerage.GetCashBalance failed: {res.Error}");
                return new List<CashAmount> { new CashAmount(0m, SettleAsset) };
            }

            var totalEquity = res.Data.TotalEquity;
            var unrealizedPnl = res.Data.UnrealizedPnl ?? 0m;
            var balance = totalEquity - unrealizedPnl;

            return new List<CashAmount>
            {
                new CashAmount(balance, SettleAsset)
            };
        }

        // OKX AmendOrder is in-place: same order ID is kept after the amendment.
        protected override async Task<ExchangeWebResult<SharedId>> ExecuteUpdateOrderAsync(
            Order order, decimal price, decimal? quantity)
        {
            var nativeTicker = NativeTicker(order.Symbol);
            var orderIdStr = order.BrokerId.Last();

            if (!long.TryParse(orderIdStr, out var orderId))
            {
                Log.Error($"OKX AmendOrder: cannot parse orderId '{orderIdStr}' as long");
                return new ExchangeWebResult<SharedId>(Name, new InvalidOperationError($"Invalid order ID format: '{orderIdStr}'"));
            }

            var res = await _restClient.UnifiedApi.Trading.AmendOrderAsync(
                symbol: nativeTicker,
                orderId: orderId,
                newPrice: price,
                newQuantity: quantity.HasValue ? Math.Abs(quantity.Value) : (decimal?)null);

            if (!res.Success)
            {
                Log.Error($"OKX AmendOrder error: {res.Error} | OrderId: {orderId} | Price: {price} | OriginalData: {res.OriginalData}");
                return new ExchangeWebResult<SharedId>(Name, res.Error);
            }

            return new ExchangeWebResult<SharedId>(
                Name,
                TradingMode.PerpetualLinear,
                res.As(new SharedId(res.Data.OrderId.ToString()))
            );
        }
    }
}