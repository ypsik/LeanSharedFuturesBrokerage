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
    /// <summary>
    /// Erweitert SymbolProperties um ContractValue (OKX ctVal), getrennt von ContractMultiplier.
    /// ContractMultiplier bleibt fest 1m, da LEAN dieses Feld intern für eigene PnL-/Notional-
    /// Berechnungen nutzt (siehe SecurityHolding.UnrealizedProfit, CryptoFutureHolding.GetQuantityValue)
    /// und dort davon ausgeht, dass Quantity × ContractMultiplier den korrekten Notional-Wert ergibt.
    /// Da wir Quantity über die gesamte Order-/Holdings-Kette hinweg bereits durchgängig auf Base-Asset-
    /// Einheiten normalisieren (nicht Contracts), würde ein ContractMultiplier=ctVal hier zu falschem,
    /// eingefrorenem UnrealizedProfit führen (Quantity wird effektiv doppelt herunterskaliert).
    /// ContractValue (ctVal) wird stattdessen separat gespeichert, ausschließlich für unsere eigene
    /// Contract↔Base-Umrechnung beim OKX-API-Call (ToExchangeQuantity/FromExchangeQuantity).
    /// </summary>
    public class OkxSymbolProperties : SymbolProperties
    {
        public decimal ContractValue { get; }

        public OkxSymbolProperties(string description, string quoteCurrency, decimal minimumPriceVariation,
            decimal lotSize, string marketTicker, decimal contractValue)
            : base(description, quoteCurrency, 1m, minimumPriceVariation, lotSize, marketTicker)
        {
            ContractValue = contractValue;
        }
    }

    public class OkxFuturesBrokerage : SharedFuturesBrokerage
    {
        private OKXRestClient _restClient;
        private OKXSocketClient _socketClient;
        private OKXSocketClient _socketClientExData;

        private readonly object _fundingUpdateLock = new();
        private bool _fundingUpdateConnected = false;
        private UpdateSubscription _fundingUpdateSubscription;

        // OKX instrument type used for symbol discovery.
        // SWAP    = true perpetuals (Global/US accounts).
        // FUTURES = X-Perps (EU/EEA MiFID-regulated, 5-year expiry with funding rate).
        private readonly InstrumentType _instrumentType;

        // Rule type filter for PopulateSPDB.
        // SymbolRuleType.Perp on EU accounts (X-Perp FUTURES, JSON "xperp").
        // null on Global accounts (SWAP — no ruleType filter needed).
        private readonly SymbolRuleType? _ruleTypeFilter;
        protected override int? FundingRolloverHours => null;

        protected override SharedMarginMode? SharedMarginMode => CryptoExchange.Net.SharedApis.SharedMarginMode.Cross;


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

        // TEMP DIAGNOSTIC (Absprache 2026-07-08): Subscribed auf den balance_and_position-Channel
        // ausschließlich zu Logging-Zwecken. Ziel: empirisch pruefen, ob OKXBalanceUpdate.CashBalance
        // bei eventType="funding_fee" tatsaechlich ein Delta ist (Doku nennt es "cashBal" = absoluter
        // Stand, das koennte aber falsch dokumentiert sein - siehe Diskussion). Es findet HIER NOCH
        // KEINE CashBook-Buchung statt, kein FundingBrokerageMessageEvent, kein IsConnected-Gating auf
        // _fundingUpdateConnected. Sobald klar ist ob Delta oder Cash-Stand, wird die echte Funding-
        // Verbuchung nachgezogen (entweder direkt aus dem Delta, oder als Trigger fuer einen REST-Call
        // gegen GetFundingBillDetailsAsync, der garantiert ein Delta/balChg-Feld hat).
        public override void Connect()
        {
            lock (_fundingUpdateLock)
            {
                if (_fundingUpdateSubscription == null && _socketClient != null)
                {
                    var sub = RunSync(() =>
                        _socketClient.UnifiedApi.Account.SubscribeToBalanceAndPositionUpdatesAsync(
                            update =>
                            {
                                var data = update?.Data;
                                if (data == null)
                                    return;

                                // Nur funding_fee Events interessieren fuer diesen Diagnose-Schritt.
                                if (!string.Equals(data.EventType, "funding_fee", StringComparison.OrdinalIgnoreCase))
                                    return;

                                Log.Trace($"OKX funding_fee | Time={data.Time:O}");

                                foreach (var bal in data.BalanceData)
                                {
                                    Log.Trace($"  BalData: Asset={bal.Asset} CashBalance={bal.CashBalance} UpdateTime={bal.UpdateTime:O}");
                                }
                            }));

                    SetupSubscriptionEvents(
                        sub?.Success ?? false,
                        sub?.Data,
                        (state) => { _fundingUpdateConnected = state; },
                        "Balance/Position updates",
                        "Balance/Position updates subscription failed",
                        sub?.Error?.ToString());

                    if (sub?.Success ?? false)
                    {
                        _fundingUpdateSubscription = sub.Data;
                    }
                }
            }
            base.Connect();
        }
        public override void Disconnect()
        {
            RunSync(() => _fundingUpdateSubscription?.CloseAsync() ?? Task.CompletedTask);
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

                var ticker = NormalizeSymbol(symbol.Symbol);
                if (quoteAsset == "USD")
                    quoteAsset += "C";

                var tickSize = symbol.TickSize ?? 0.01m;
                var lotSize = symbol.LotSize ?? 1m;
                if (lotSize <= 0m) lotSize = 1m;

                // settleCcy is the margin/settlement asset for FUTURES/SWAP (e.g. "USD", "USDT").
                var contractMultiplier = symbol.ContractValue ?? 1m;

                // FIX: OKX liefert lotSz/minSz in Contracts, nicht in Base-Asset-Einheiten (z.B. HYPE
                // lotSz=1 bedeutet 1 Contract = 0.1 HYPE bei ctVal=0.1). LEANs interne Order-Validierung
                // vergleicht order.Quantity (Base-Asset) direkt gegen SymbolProperties.LotSize, daher muss
                // LotSize hier in Base-Asset-Einheiten stehen (lotSz * ctVal), konsistent mit allen anderen
                // Exchanges (dort ist ContractMultiplier=1, also unverändert). ToExchangeQuantity() rechnet
                // das für den eigentlichen Order-Placement-Call wieder zurück in Contracts.
                var baseLotSize = lotSize * contractMultiplier;

                // FIX: ContractMultiplier bleibt fest 1m (siehe OkxSymbolProperties-Doku oben) — LEAN
                // nutzt dieses Feld selbst für PnL/Notional-Berechnungen und erwartet dort Quantity
                // bereits in der Einheit, die zusammen mit ContractMultiplier den korrekten Notional
                // ergibt. Da wir Quantity durchgängig auf Base-Asset normalisieren, muss der Multiplier
                // dafür 1 sein. ctVal (contractMultiplier) wird stattdessen separat als ContractValue
                // gespeichert, ausschließlich für unsere eigene Contract-Umrechnung.
                var symbolProperties = new OkxSymbolProperties(
                    description: $"OKX {baseAsset} {_instrumentType}",
                    quoteCurrency: quoteAsset,
                    minimumPriceVariation: tickSize,
                    lotSize: baseLotSize,
                    marketTicker: symbol.Symbol,   // full native instId, e.g. "ETH-USD-310404"
                    contractValue: contractMultiplier
                );

                _spdb.SetEntry(Name, ticker, SecurityType.CryptoFuture, symbolProperties);
                _spdb.SetEntry(Name, ticker, SecurityType.Crypto, symbolProperties);
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
            var parts = rawSymbol.Split('-');
            if (parts.Length < 2)
                return rawSymbol;

            var baseAsset = parts[0];
            var quoteAsset = parts[1].Split('_')[0];

            // Same USD -> USDC dirty fix as PopulateSPDB (FUTURES/X-Perp only), so this method
            // produces the exact same ticker key as PopulateSPDB for the same instrument.
            if (quoteAsset == "USD")
                quoteAsset += "C";

            return baseAsset + quoteAsset;
        }

        protected override string NativeTicker(Symbol symbol)
        {
            CurrencyPairUtil.DecomposeCurrencyPair(symbol, out var baseAsset, out var quoteAsset);

            if (_instrumentType == InstrumentType.Swap)
                return $"{baseAsset}-{quoteAsset}-SWAP";

            // FUTURES (X-Perp): look up the canonical instId (with date suffix) from SPDB.
            // Use symbol.Value directly (raw LEAN ticker), not the decompose-reconstructed
            // baseAsset+quoteAsset, since GetSharedSymbol() strips the dirty-fix "C" suffix
            // and callers may reconstruct/pass the symbol without it (e.g. "XAUUSD" instead
            // of "XAUUSDC" as actually stored in PopulateSPDB).
            if (symbol.Value == null)
                throw new InvalidOperationException($"Cannot get native ticker for symbol {symbol} with null Value");
            var rawTicker = symbol.Value;

            Symbol dbSymbol =
                _instrumentType == InstrumentType.Futures && rawTicker.EndsWith("USD")
                    ? Symbol.Create(rawTicker + "C", SecurityType.CryptoFuture, Name)
                    : symbol;

            var entry = _spdb.GetSymbolProperties(Name, dbSymbol, SecurityType.CryptoFuture, _instrumentType == InstrumentType.Futures && quoteAsset == "USD" ? quoteAsset + "C" : quoteAsset);

            if (entry != null && !string.IsNullOrEmpty(entry.MarketTicker))
                return entry.MarketTicker;

            throw new InvalidOperationException(
                $"OKX native ticker not found in SPDB for {symbol.Value} baseAsset {baseAsset} quoteAsset {quoteAsset}");
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

        protected override TradingMode? OpenOrdersTradingMode =>
            _instrumentType == InstrumentType.Futures ? TradingMode.DeliveryLinear : TradingMode.PerpetualLinear;

        #region Contract Quantity Conversion

        /// <summary>
        /// OKX Futures/Swap-Endpunkte erwarten und liefern Quantities ausschließlich in Contracts
        /// (sz-Parameter), nicht in Base-Asset-Einheiten. Ein Contract entspricht dem in der SPDB
        /// hinterlegten ContractMultiplier (= OKX ctVal, z.B. 0.01 BTC pro Contract).
        /// Lookup via _spdb, befüllt in PopulateSPDB() aus symbol.ContractValue.
        /// </summary>
        private decimal GetContractMultiplier(Symbol symbol)
        {
            var props = _spdb.GetSymbolProperties(Name, symbol, SecurityType.CryptoFuture, SettleAsset) as OkxSymbolProperties;
            var multiplier = props?.ContractValue ?? 1m;
            return multiplier > 0m ? multiplier : 1m;
        }

        /// <summary>
        /// Base-Asset-Menge → Contracts. Rundet IMMER AUF (Ceiling) auf den nächstgültigen
        /// LotSize-Step. Grund: bei Abrundung wäre die tatsächlich an OKX gesendete Menge kleiner
        /// als die von LEAN erwartete OriginalQuantity, wodurch die Order nie vollständig gefüllt
        /// werden kann und als PartiallyFilled/Open unbegrenzt in der State-Machine hängen bleibt.
        /// Ceiling stellt sicher, dass die Exchange-Menge >= LEAN-Zielmenge ist.
        /// roundedBaseQuantity gibt die daraus resultierende, exakt darstellbare Base-Menge zurück,
        /// damit die State-Machine mit dem realen (aufgerundeten) Wert arbeitet.
        /// </summary>
        protected override SharedQuantity ToExchangeQuantity(Symbol symbol, decimal absBaseQuantity, out decimal roundedBaseQuantity)
        {
            var ctVal = GetContractMultiplier(symbol);
            var props = _spdb.GetSymbolProperties(Name, symbol, SecurityType.CryptoFuture, SettleAsset);

            // props.LotSize liegt seit dem SPDB-Fix in Base-Asset-Einheiten vor (lotSz * ctVal),
            // für die Contract-Rundung hier brauchen wir aber den nativen OKX-Lot-Step in Contracts
            // zurück: contractLotStep = baseLotSize / ctVal (ergibt wieder den rohen lotSz-Wert).
            var baseLotSize = props?.LotSize ?? ctVal;
            var contractLotStep = baseLotSize / ctVal;
            if (contractLotStep <= 0m) contractLotStep = 1m;

            var rawContracts = absBaseQuantity / ctVal;

            // Ceiling auf den gültigen Contract-Lot-Step.
            var steppedContracts = Math.Ceiling(rawContracts / contractLotStep) * contractLotStep;
            if (steppedContracts <= 0m)
                steppedContracts = contractLotStep; // Minimum: 1 Lot, nie 0 senden

            roundedBaseQuantity = steppedContracts * ctVal;

            return new SharedQuantity { QuantityInContracts = steppedContracts };
        }

        /// <summary>
        /// Contracts → Base-Asset-Menge (reine Multiplikation, keine Rundung nötig,
        /// da wir hier nur von der Exchange gemeldete Ist-Werte zurückrechnen).
        /// </summary>
        protected override decimal FromExchangeQuantity(Symbol symbol, SharedOrderQuantity? quantity)
        {
            if (quantity == null)
                return 0m;

            var contracts = quantity.QuantityInContracts ?? 0m;
            var ctVal = GetContractMultiplier(symbol);
            return contracts * ctVal;
        }

        /// <summary>
        /// OKX befüllt ausschließlich QuantityInContracts (nie QuantityInBaseAsset). Der Default-Hook
        /// der Basisklasse prüft QuantityInBaseAsset.HasValue und würde bei OKX daher IMMER false
        /// liefern — was fälschlich jeden Fill-Wert auf den OriginalQuantity-Fallback umleiten würde
        /// (der ursprüngliche Bug, den wir hier gerade beheben). Deshalb Override auf Contracts-Feld.
        /// </summary>
        protected override bool HasExchangeQuantity(SharedOrderQuantity? quantity)
            => quantity?.QuantityInContracts.HasValue == true;

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
                    var now = data.DataTime ?? data.ReceiveTime;
                    var rate = data.Data;
                    onFundingRate(now, rate.FundingRate, rate.FundingTime);
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

        protected override string GenerateClientId(int _)
        {
            return (_restClient.UnifiedApi.SharedClient as IFuturesOrderRestClient).GenerateClientOrderId();
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

            // AmendOrder erwartet ebenfalls Contracts (newSz), nicht Base-Asset-Menge.
            // Gleiche Umrechnung + Rundung wie beim initialen PlaceOrder, damit die neue
            // Restmenge auf einen gültigen Contract-Lot-Step fällt.
            decimal? newContractQuantity = null;
            if (quantity.HasValue)
            {
                var sharedQty = ToExchangeQuantity(order.Symbol, Math.Abs(quantity.Value), out _);
                newContractQuantity = sharedQty.QuantityInContracts;
            }

            var res = await _restClient.UnifiedApi.Trading.AmendOrderAsync(
                symbol: nativeTicker,
                orderId: orderId,
                newPrice: price,
                newQuantity: newContractQuantity);

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