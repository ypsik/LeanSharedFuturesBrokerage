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
        protected override int? FundingRolloverHours => null;

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

            RunSync(() => _restClient.UnifiedApi.SharedClient.GetFuturesSymbolsAsync(new GetSymbolsRequest()));

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
                    options.SharedApiEuropeUseXPerps = environment == OKXEnvironment.Europe;
                });
                RunSync(() => _restClient.UnifiedApi.SharedClient.GetFuturesSymbolsAsync(new GetSymbolsRequest()));

            }

            if (_socketClient == null)
            {
                _socketClient = new OKXSocketClient(options =>
                {
                    options.Environment = environment;
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret) && !string.IsNullOrEmpty(passphrase))
                        options.ApiCredentials = new OKXCredentials(key, secret, passphrase);
                    options.SharedApiEuropeUseXPerps = environment == OKXEnvironment.Europe;
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

        // OKX fills channel (ws "fills") requires VIP5+ account.
        // Fills are available via the "orders" channel which includes fillSz, fillPx, fee etc.
        public override bool ExchangeSupportsUserTradeStream => false;

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
                // EU accounts: filter to X-Perp contracts only (JSON "xperp" → SymbolRuleType.Perp).
                // Global accounts: _ruleTypeFilter is null → accept all SWAPs.
                if (_ruleTypeFilter.HasValue && symbol.RuleType != _ruleTypeFilter.Value)
                    continue;

                // Only live/tradeable instruments.
                if (symbol.State != InstrumentState.Live)
                    continue;

                // For FUTURES/SWAP, baseCcy and quoteCcy are empty.
                // Use uly (underlying) which is always "BASE-QUOTE" (e.g. "BTC-USD").
                // Fall back to instFamily or instId split if uly is missing.
                string baseAsset, quoteAsset;
                var underlying = symbol.Underlying; // "BTC-USD"
                if (!string.IsNullOrEmpty(underlying))
                {
                    var parts = underlying.Split('-');
                    baseAsset = parts.Length > 0 ? parts[0] : string.Empty;
                    quoteAsset = parts.Length > 1 ? parts[1] : SettleAsset;
                }
                else if (!string.IsNullOrEmpty(symbol.InstrumentFamily))
                {
                    // instFamily = "BTC-USD_UM_XPERP" → take up to first '-' then up to '_'
                    var parts = symbol.InstrumentFamily.Split('-');
                    baseAsset = parts.Length > 0 ? parts[0] : string.Empty;
                    quoteAsset = parts.Length > 1 ? parts[1].Split('_')[0] : SettleAsset;
                }
                else
                {
                    var parts = symbol.Symbol.Split('-');
                    baseAsset = parts.Length > 0 ? parts[0] : string.Empty;
                    quoteAsset = parts.Length > 1 ? parts[1].Split('_')[0] : SettleAsset;
                }

                if (string.IsNullOrEmpty(baseAsset))
                {
                    Log.Error($"OkxFuturesBrokerage: cannot determine base asset for {symbol.Symbol}, skipping");
                    continue;
                }

                // Dirty fix: LEAN's CryptoFuture.IsCryptoCoinFuture() incorrectly classifies any
                // future not quoted in USDT/BUSD/USDC as coin-margined/inverse, which breaks
                // HoldingsValue/UnrealizedProfit/collateral calculation for linear, USD-quoted
                // X-Perp futures (same root cause as Kraken). Only applies to FUTURES (EU X-Perp);
                // SWAP is untouched (always USDT-settled, not affected by this bug).
                // We report USD-quoted X-Perps to LEAN as "USDC"-quoted (append "C" to ticker +
                // quoteCurrency) so they're correctly recognized as linear. GetSharedSymbol()
                // below maps "USDC" back to "USD" (FUTURES only). NativeTicker() for FUTURES
                // uses the SPDB MarketTicker lookup, so it is unaffected by this ticker-key change.
                if (_instrumentType == InstrumentType.Futures && quoteAsset == "USD")
                    quoteAsset += "C";

                var ticker = baseAsset + quoteAsset;

                var tickSize = symbol.TickSize ?? 0.01m;
                var lotSize = symbol.LotSize ?? 1m;
                if (lotSize <= 0m) lotSize = 1m;

                // settleCcy is the margin/settlement asset for FUTURES/SWAP (e.g. "USD", "USDT").
                // Apply the same USD -> USDC dirty fix here (FUTURES only) so quoteCurrency matches the ticker.
                var quoteCurrency = string.IsNullOrEmpty(symbol.SettlementAsset) ? SettleAsset : symbol.SettlementAsset;
                if (_instrumentType == InstrumentType.Futures && quoteCurrency == "USD")
                    quoteCurrency += "C";

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
        protected override string SettleAsset => "USDC";

        #region Symbol Mapping

        // Converts OKX native instId to LEAN ticker.
        // SWAP:    "BTC-USDT-SWAP"          → "BTCUSDT"
        // FUTURES: "BTC-USD_UM_XPERP-310404" → not used (ticker built from uly in PopulateSPDB)
        protected override string NormalizeSymbol(string rawSymbol)
        {
            // Take first two dash-separated segments, strip anything after '_' in segment 2.
            // "BTC-USDT-SWAP"          → "BTC" + "USDT" → "BTCUSDT"
            // "BTC-USD_UM_XPERP-310404"→ "BTC" + "USD"  → "BTCUSD"
            var parts = rawSymbol.Split('-');
            if (parts.Length >= 2)
                return parts[0] + parts[1].Split('_')[0];

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

            if (entry == null && quoteAsset == "USD")
            {
                // GetSharedSymbol() strips the dirty-fix "C" suffix (USDC -> USD), which can
                // cause the symbol to be reconstructed elsewhere as "USD" instead of "USDC".
                // Fall back to the actual SPDB key used during PopulateSPDB.
                leanTicker = baseAsset + quoteAsset + "C";
                entry = _spdb.GetSymbolProperties("okx", leanTicker, SecurityType.CryptoFuture, quoteAsset + "C");
            }

            if (entry != null && !string.IsNullOrEmpty(entry.MarketTicker))
                return entry.MarketTicker;

            throw new InvalidOperationException(
                $"OKX native ticker not found in SPDB for {symbol.Value}");
        }

        protected override SharedSymbol GetSharedSymbol(Symbol s)
        {
            CurrencyPairUtil.DecomposeCurrencyPair(s, out var baseAsset, out var quoteAsset);
            // Only FUTURES (X-Perp) quoteAsset was artificially remapped USD -> USDC in PopulateSPDB.
            // SWAP quoteAsset is untouched, so genuine USDC-quoted SWAP pairs must stay as USDC.
            if (_instrumentType == InstrumentType.Futures)
                quoteAsset = quoteAsset.Replace("USDC", "USD");

            return new SharedSymbol(TradingMode.PerpetualLinear, baseAsset, quoteAsset);
        }

        #endregion

        // OKX has a dedicated funding-rate WebSocket channel (public, unauthenticated).
        // Pushed every minute. OKXFundingRate.NextFundingTime is DateTime (non-nullable).
        protected override async Task<WebSocketResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
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
        protected override async Task<HttpResult<SharedId>> ExecuteUpdateOrderAsync(
            Order order, decimal price, decimal? quantity)
        {
            var nativeTicker = NativeTicker(order.Symbol);
            var orderIdStr = order.BrokerId.Last();

            if (!long.TryParse(orderIdStr, out var orderId))
            {
                Log.Error($"OKX AmendOrder: cannot parse orderId '{orderIdStr}' as long");
                return new HttpResult<SharedId>(Name, null, new InvalidOperationError($"Invalid order ID format: '{orderIdStr}'"));
            }

            var res = await _restClient.UnifiedApi.Trading.AmendOrderAsync(
                symbol: nativeTicker,
                orderId: orderId,
                newPrice: price,
                newQuantity: quantity.HasValue ? Math.Abs(quantity.Value) : (decimal?)null);

            if (!res.Success)
            {
                Log.Error($"OKX AmendOrder error: {res.Error} | OrderId: {orderId} | Price: {price} | OriginalData: {res.OriginalData}");
                return new HttpResult<SharedId>(Name, null, res.Error);
            }

            return new HttpResult<SharedId>(
                Name,
                new SharedId(res.Data.OrderId.ToString()),
                null
            );
        }
    }
}