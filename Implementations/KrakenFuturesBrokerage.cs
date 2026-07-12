using CryptoExchange.Net.Authentication;
using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using Kraken.Net;
using Kraken.Net.Clients;
using Microsoft.Win32;
using QLNet;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantConnect.Securities.Crypto;
using QuantConnect.Util;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading.Tasks;

namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    /// <summary>
    /// Erweitert SymbolProperties um ContractSize (Kraken Futures "contractSize"), getrennt vom
    /// LEAN-internen ContractMultiplier. Gleiches Pattern wie OkxSymbolProperties.ContractValue:
    /// ContractMultiplier bleibt fest 1m (LEAN nutzt es intern für PnL-/Notional-Berechnung und
    /// erwartet Quantity × ContractMultiplier = Notional, unsere Quantity ist aber durchgängig
    /// Base-Asset). ContractSize wird separat gehalten, ausschließlich für die eigene
    /// Contract↔Base-Umrechnung beim Kraken-API-Call (ToExchangeQuantity/FromExchangeQuantity).
    ///
    /// ContractValueTradePrecision bestimmt zusätzlich die zulässige Rundungsgranularität der
    /// Contract-Menge (Kraken-Feld "contractValueTradePrecision", verifiziert gegen die Live-
    /// Instrument-Liste): z.B. 4 bei PF_XBTUSD (0.0001-Schritte, fraktionale Contracts erlaubt),
    /// 0 bei PF_TRXUSD (nur ganze Contracts), -3 bei PF_PEPEUSD (nur 1000er-Schritte). Erfüllt
    /// exakt die gleiche Rolle wie OKX' contractLotStep (dort aus dem expliziten lotSz-Feld
    /// abgeleitet) — Kraken liefert stattdessen die Präzision als Dezimalstellen-Exponent statt
    /// als eigenes Lot-Size-Feld.
    /// </summary>
    public class KrakenSymbolProperties : SymbolProperties
    {
        public decimal ContractSize { get; }
        public int ContractValueTradePrecision { get; }

        public KrakenSymbolProperties(string description, string quoteCurrency, decimal minimumPriceVariation,
            decimal lotSize, string marketTicker, decimal contractSize, int contractValueTradePrecision)
            : base(description, quoteCurrency, 1m, minimumPriceVariation, lotSize, marketTicker)
        {
            ContractSize = contractSize;
            ContractValueTradePrecision = contractValueTradePrecision;
        }
    }

    public class KrakenFuturesBrokerage : SharedFuturesBrokerage
    {
        private KrakenRestClient _restClient;
        private KrakenSocketClient _socketClient;
        private KrakenSocketClient _socketClientExData;

        private readonly object _fundingUpdateLock = new();
        private bool _fundingUpdateConnected = false;
        private UpdateSubscription _fundingUpdateSubscription;
        protected override int? FundingRolloverHours => null;

        // Kraken locked die Rate für die kommende Stunde bereits zu Stundenbeginn fix
        // (kein 8h-TWAP-Settlement wie bei Bybit/Hyperliquid) -> beim Rollover den neuen,
        // bereits gültigen Wert melden statt den alten, jetzt abgelaufenen Wert.
        protected override bool EmitFundingRateImmediately => true;

        internal KrakenFuturesBrokerage(
            IAlgorithm algorithm,
            KrakenRestClient restClient,
            KrakenSocketClient socketClient,
            IDataAggregator aggregator,
            Func<List<Holding>>? getHoldingsFunc = null)
            : base(algorithm, "kraken")
        {
            _restClient = restClient;
            _socketClient = socketClient;
            // OutputOriginalData=true: notwendig damit data.OriginalData (Raw-JSON) im Ticker-Handler
            // verfügbar ist, für Diagnose des RelativeFundingRate-Parsing bei XBTUSDC.
            _socketClientExData = new KrakenSocketClient(options => { options.OutputOriginalData = true; });

            PopulateSPDB();

            InitializeBase(
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                aggregator,
                getHoldingsFunc);
        }

        protected override void InitializeFromJob(QuantConnect.Packets.LiveNodePacket job, IDataAggregator aggregator)
        {
            if (_restClient == null)
            {
                job.BrokerageData.TryGetValue("kraken-futures-api-key", out var key);
                job.BrokerageData.TryGetValue("kraken-futures-api-secret", out var secret);

                _restClient = new KrakenRestClient(options =>
                {
                    // Futures-only: first argument (Spot credentials) is null.
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret))
                        options.ApiCredentials = new KrakenCredentials(
                            null,
                            new HMACCredential(key, secret));
                });
            }

            if (_socketClient == null)
            {
                job.BrokerageData.TryGetValue("kraken-futures-api-key", out var key);
                job.BrokerageData.TryGetValue("kraken-futures-api-secret", out var secret);

                _socketClient = new KrakenSocketClient(options =>
                {
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret))
                        options.ApiCredentials = new KrakenCredentials(
                            null,
                            new HMACCredential(key, secret));
                });
            }

            if (_socketClientExData == null)
                _socketClientExData = new KrakenSocketClient(options => { options.OutputOriginalData = true; });

            InitializeBase(
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
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

        #region Connect

        public override bool IsConnected => base.IsConnected && _fundingUpdateConnected;

        // Kraken edit keeps the same order ID (status: "edited", no cancel+replace).
        public override bool ExchangeModifiesOrdersInPlace => true;

        public override void Connect()
        {
            lock (_fundingUpdateLock)
            {
                if (_fundingUpdateSubscription == null && _socketClient != null)
                {
                    _subRateGate.WaitToProceed();

                    // account_log feed: snapshot on connect, then one new_entry per event.
                    // Funding payments have Info == "funding rate change" and carry RealizedFunding.
                    // We ignore the snapshot (historical entries already settled) and only act
                    // on live update entries.
                    var sub = RunSync(() =>
                        _socketClient.FuturesApi.SubscribeToAccountLogUpdatesAsync(
                            snapshotHandler: _ => { /* ignore historical snapshot */ },
                            updateHandler: update =>
                            {
                                var entry = update.Data.NewEntry;
                                if (entry?.RealizedFunding == null || entry.RealizedFunding == 0m)
                                    return;

                                if (!entry.Info.Equals("funding rate change", StringComparison.OrdinalIgnoreCase))
                                    return;

                                if (_algorithm?.Portfolio?.CashBook != null)
                                {
                                    // RealizedFunding is negative when paid, positive when received.
                                    _algorithm.Portfolio.CashBook[SettleAsset].AddAmount(entry.RealizedFunding.Value);
                                    OnMessage(new FundingBrokerageMessageEvent(
                                        entry.Asset ?? SettleAsset,
                                        entry.RealizedFunding.Value));
                                }
                            }));

                    SetupSubscriptionEvents(
                        sub?.Success ?? false,
                        sub?.Data,
                        (state) => _fundingUpdateConnected = state,
                        "Account log updates",
                        "Account log subscription failed",
                        sub?.Error?.ToString()
                    );

                    if (sub?.Success ?? false)
                        _fundingUpdateSubscription = sub.Data;
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

        private void PopulateSPDB()
        {
            var result = RunSync(() => _restClient.FuturesApi.ExchangeData.GetSymbolsAsync());

            if (!result.Success)
                throw new Exception($"Failed to load Kraken symbols: {result.Error}");

            foreach (var symbol in result.Data.Where(s => s.Tradeable && s.Type == Kraken.Net.Enums.SymbolType.FlexibleFutures))
            {
                var ticker = NormalizeSymbol(symbol.Symbol);

                var tickSize = symbol.TickSize ?? 0.01m;

                // ContractSize ist bei Kraken (wie ctVal bei OKX) die Base-Asset-Menge pro Contract.
                // Aktuell (Stand 2026-07) bei allen PF_-Symbolen konstant 1, verifiziert gegen die
                // Live-Instrument-Liste (futures.kraken.com/derivatives/api/v3/instruments).
                var contractSize = symbol.ContractSize ?? 1m;
                if (contractSize <= 0m) contractSize = 1m;

                // KORREKTUR: Die Contract-Menge ist NICHT zwingend ganzzahlig — Kraken definiert die
                // zulässige Rundungsgranularität über contractValueTradePrecision (analog OKX' lotSz,
                // nur als Dezimalstellen-Exponent statt als eigenes Lot-Size-Feld ausgedrückt).
                // Verifiziert gegen die Live-Instrument-Liste, Werte variieren stark pro Symbol:
                // PF_XBTUSD=4 (0.0001-Schritte, fraktionale Contracts erlaubt), PF_TRXUSD=0 (nur
                // ganze Contracts), PF_PEPEUSD=-3 (nur 1000er-Schritte). Ein pauschales Ceiling auf
                // ganze Zahlen (wie zunächst implementiert) wäre für PF_XBTUSD z.B. grob falsch:
                // eine Zielmenge von 0.05 BTC würde auf 1 volles BTC aufgerundet.
                var contractValueTradePrecision = (int)(symbol.ContractValueTradePrecision ?? 0m);
                var contractLotStep = Pow10(-contractValueTradePrecision);
                if (contractLotStep <= 0m) contractLotStep = 1m;

                // props.LotSize (Base-Asset-Einheiten, für LEANs interne Order-Validierung) muss
                // konsistent mit dem nativen Contract-Lot-Step sein: baseLotSize = contractLotStep * contractSize,
                // exakt das gleiche Pattern wie bei OKX (dort: baseLotSize = lotSz * ctVal).
                var baseLotSize = contractLotStep * contractSize;

                var symbolProperties = new KrakenSymbolProperties(
                    description: $"Kraken {symbol.BaseAsset} Perpetual",
                    // Dirty fix: must match NormalizeSymbol's appended "USDC" suffix, see comment there.
                    quoteCurrency: "USDC",
                    minimumPriceVariation: tickSize,
                    lotSize: baseLotSize,
                    marketTicker: symbol.Symbol,
                    contractSize: contractSize,
                    contractValueTradePrecision: contractValueTradePrecision
                );

                _spdb.SetEntry(Name, ticker, SecurityType.CryptoFuture, symbolProperties);
                _spdb.SetEntry(Name, ticker, SecurityType.Crypto, symbolProperties);
            }
        }



        protected override string SettleAsset => "USDC";

        #region Symbol Mapping

        protected override string NormalizeSymbol(string rawSymbol)
        {
            var ticker = rawSymbol.Replace("PF_", "");
            // Dirty fix: LEAN's CryptoFuture.IsCryptoCoinFuture() incorrectly classifies any
            // future not quoted in
            // /BUSD/USDC as coin-margined/inverse, which breaks
            // HoldingsValue/UnrealizedProfit/collateral calculation for Kraken's linear,
            // USD-quoted futures. We report every Kraken future to LEAN as "USDC"-quoted
            // (by appending "C") so it gets correctly recognized as linear. The USDC/USD
            // spread is negligible for portfolio valuation purposes (~0.01-0.05%).
            // NativeTicker() strips the appended "C" again before actually communicating
            // with Kraken's real API.
            return ticker.EndsWith("USD") ? ticker + "C" : ticker;
        }

        protected override string NativeTicker(Symbol symbol)
        {
            CurrencyPairUtil.DecomposeCurrencyPair(symbol, out var baseAsset, out var quoteAsset);
            // quoteAsset is "USDC" due to the dirty fix above; Kraken's real API only
            // knows "USD" — strip the artificially appended "C" back off here.
            var realQuoteAsset = quoteAsset == "USDC" ? "USD" : quoteAsset;
            return $"PF_{baseAsset}{realQuoteAsset}";
        }

        protected override SharedSymbol GetSharedSymbol(Symbol s)
        {
            CurrencyPairUtil.DecomposeCurrencyPair(s, out var baseAsset, out var quoteAsset);
            return new SharedSymbol(TradingMode.PerpetualLinear, baseAsset, quoteAsset.Replace("USDC", "USD"));
        }

        #endregion

        #region Contract Quantity Conversion

        /// <summary>
        /// Kraken Futures PlaceOrder/EditOrder erwarten Quantity in Contracts, nicht in Base-Asset-
        /// Einheiten ("Quantity for Buy.Limit required in contracts" — gleiches Symptom wie bei OKX).
        /// Ein Contract entspricht dem in der SPDB hinterlegten ContractSize (Kraken-Feld "contractSize"),
        /// aktuell verifiziert konstant 1 über alle Symbole. Lookup via _spdb, befüllt in PopulateSPDB().
        /// </summary>
        private decimal GetContractSize(Symbol symbol)
        {
            var props = _spdb.GetSymbolProperties(Name, symbol, SecurityType.CryptoFuture, SettleAsset) as KrakenSymbolProperties;
            var size = props?.ContractSize ?? 1m;
            return size > 0m ? size : 1m;
        }

        /// <summary>
        /// Rundungsgranularität der Contract-Menge in Contract-Einheiten (KEIN Ganzzahl-Zwang!).
        /// Kraken kodiert das über ContractValueTradePrecision statt über ein explizites lotSz-Feld
        /// wie OKX: 10^-precision, z.B. precision=4 → Schritt 0.0001 (PF_XBTUSD), precision=0 →
        /// Schritt 1 (PF_TRXUSD), precision=-3 → Schritt 1000 (PF_PEPEUSD). Verifiziert gegen die
        /// Live-Instrument-Liste — die Werte variieren stark pro Symbol, ein pauschales Ceiling auf
        /// ganze Zahlen wäre für die meisten Majors (BTC, ETH, ...) grob falsch.
        /// </summary>
        private decimal GetContractLotStep(Symbol symbol)
        {
            var props = _spdb.GetSymbolProperties(Name, symbol, SecurityType.CryptoFuture, SettleAsset) as KrakenSymbolProperties;
            var precision = props?.ContractValueTradePrecision ?? 0;
            var step = Pow10(-precision);
            return step > 0m ? step : 1m;
        }

        private static decimal Pow10(int exponent)
        {
            decimal result = 1m;
            if (exponent >= 0)
            {
                for (var i = 0; i < exponent; i++) result *= 10m;
            }
            else
            {
                for (var i = 0; i < -exponent; i++) result /= 10m;
            }
            return result;
        }

        /// <summary>
        /// Base-Asset-Menge → Contracts. Rundet IMMER AUF (Ceiling) auf den nächstgültigen
        /// ContractLotStep (siehe GetContractLotStep) — exakt das gleiche Pattern wie bei OKX.
        /// Ceiling stellt sicher, dass die an Kraken gesendete Menge >= LEAN-Zielmenge ist (bei
        /// Abrundung würde die Order nie vollständig gefüllt werden können).
        /// roundedBaseQuantity gibt die daraus resultierende, exakt darstellbare Base-Menge zurück.
        /// </summary>
        protected override SharedQuantity ToExchangeQuantity(Symbol symbol, decimal absBaseQuantity, out decimal roundedBaseQuantity)
        {
            var contractSize = GetContractSize(symbol);
            var contractLotStep = GetContractLotStep(symbol);

            var rawContracts = absBaseQuantity / contractSize;
            var steppedContracts = Math.Ceiling(rawContracts / contractLotStep) * contractLotStep;
            if (steppedContracts <= 0m)
                steppedContracts = contractLotStep; // Minimum: 1 Lot, nie 0 senden

            roundedBaseQuantity = steppedContracts * contractSize;

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
            var contractSize = GetContractSize(symbol);
            return contracts * contractSize;
        }

        /// <summary>
        /// Kraken befüllt (wie OKX) ausschließlich QuantityInContracts, nie QuantityInBaseAsset.
        /// Der Default-Hook der Basisklasse würde bei Kraken daher immer false liefern und fälschlich
        /// auf den OriginalQuantity-Fallback umleiten. Deshalb Override auf Contracts-Feld.
        /// </summary>
        protected override bool HasExchangeQuantity(SharedOrderQuantity? quantity)
            => quantity?.QuantityInContracts.HasValue == true;

        #endregion

        // Public ticker feed for funding rate polling, via the unauthenticated extra client.
        protected override async Task<WebSocketResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
            string nativeTicker, Symbol symbol, Func<DateTime, decimal?, DateTime?, bool> onFundingRate)
        {
            return await _socketClientExData.FuturesApi.SubscribeToTickerUpdatesAsync(
                nativeTicker, data =>
                {
                    var tickerData = data.Data;

                    // Diagnose Raw-JSON vs. geparster Wert für XBTUSDC (vermuteter Parsing-Bug bei
                    // hochpreisigen Symbolen, z.B. Exponent-Verlust bei wissenschaftlicher Notation).
                    Log.Trace($"{Name} Funding Diagnostic | Parsed RelativeFundingRate: {tickerData.RelativeFundingRate} | Raw: {data.OriginalData}");

                    onFundingRate(tickerData.Timestamp, tickerData.RelativeFundingRate, tickerData.NextFundingRateTime);
                });
        }

        // Kraken Futures multi-collateral (flex) wallet balance minus unrealized PnL.
        // BalanceValue = USD value of all collateral (haircut-free).
        // ProfitAndLoss = unrealized PnL in USD (JSON field: "pnl").
        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() => _restClient.FuturesApi.Account.GetBalancesAsync());

            var flex = res?.Data?.MultiCollateralMarginAccount;
            var balance = flex?.BalanceValue ?? 0m;

            return
            [
                new(balance, SettleAsset)
            ];
        }

        protected override string GenerateClientId(int _)
        {
            return _restClient.FuturesApi.SharedClient.GenerateClientOrderId();
        }

        // Kraken edit is true in-place: same order ID is kept, status returns "edited".
        // No cancel+replace, no new ID — same pattern as Bybit.
        protected override async Task<HttpResult<SharedId>> ExecuteUpdateOrderAsync(
            Order order, decimal price, decimal? quantity)
        {
            // EditOrder erwartet ebenfalls Contracts, nicht Base-Asset-Menge. Gleiche Umrechnung
            // + Rundung wie beim initialen PlaceOrder (siehe ToExchangeQuantity), damit die neue
            // Restmenge auf einen gültigen Contract-Wert fällt.
            decimal? newContractQuantity = null;
            if (quantity.HasValue)
            {
                var sharedQty = ToExchangeQuantity(order.Symbol, Math.Abs(quantity.Value), out _);
                newContractQuantity = sharedQty.QuantityInContracts;
            }

            var res = await _restClient.FuturesApi.Trading.EditOrderAsync(
                orderId: order.BrokerId.Last(),
                price: price,
                quantity: newContractQuantity);

            if (!res.Success)
            {
                Log.Error($"Update error: {res.Error} | OrderId: {order.BrokerId.Last()} | Price: {price} | OriginalData: {res.OriginalData}");
                return new HttpResult<SharedId>(Name, null, res.Error);
            }

            // The order ID is unchanged after an in-place edit.
            return new HttpResult<SharedId>(
                Name,
                new SharedId(res.Data.OrderId.ToString()),
                null
            );
        }
    }
}