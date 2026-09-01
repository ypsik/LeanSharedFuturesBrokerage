using CoinW.Net;
using CoinW.Net.Clients;
using CoinW.Net.Enums;
using CoinW.Net.Objects;
using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
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
using System.Threading;
using System.Threading.Tasks;

namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    /// <summary>
    /// Erweitert SymbolProperties um ContractSize (CoinW oneLotSize) und NativeLotStep
    /// (CoinW minSize, nativer Lot-Schritt in Contracts - bei allen bisher geprüften Symbolen
    /// (BTC/TRX/HYPE/PAXG/ZEC) konstant 1, aber pro Symbol konfigurierbar laut API).
    /// Analog zu OkxSymbolProperties: LotSize (Basisklasse) bleibt in Base-Asset-Einheiten
    /// (oneLotSize * NativeLotStep) für LEANs eigene Order-Validierung, ContractSize wird
    /// separat für unsere eigene Contract-Umrechnung (ToExchangeQuantity/FromExchangeQuantity)
    /// gehalten. ContractMultiplier bleibt fest 1m aus demselben Grund wie bei OKX (siehe dort).
    /// </summary>
    public class CoinwSymbolProperties(string description, string quoteCurrency, decimal minimumPriceVariation,
        decimal lotSize, string marketTicker, decimal contractSize, decimal nativeLotStep, decimal minimumOrderSize) : SymbolProperties(description, quoteCurrency, 1m, minimumPriceVariation, lotSize, marketTicker, minimumOrderSize)
    {
        public decimal ContractSize { get; } = contractSize;
        public decimal NativeLotStep { get; } = nativeLotStep;
    }

    public class CoinwFuturesBrokerage : SharedFuturesBrokerage
    {
        private CoinWRestClient _restClient;
        private CoinWSocketClient _socketClient;
        private CoinWSocketClient _socketClientExData;

        private bool _fundingUpdateConnected = false;
        // CoinW hat kein echtes One-Way-Mode-Konzept - Long und Short sind laut Doku strukturell
        // immer getrennte Buecher (kein "Net"/"Both" wie bei OKX/BingX).
        // V1-SCOPE: fix Long (Buy oeffnet/erweitert die Long-Position, Sell schliesst/reduziert sie).
        // V2-TODO: Short-Support geplant - braucht dann echtes Bestands-basiertes PositionSide-
        // Routing (aktuelles Holding pro Symbol ansehen: Buy bei bestehendem Short -> Short
        // reduzieren (PositionSide=Short); Buy bei flach/Long -> Long eroeffnen/erweitern
        // (PositionSide=Long); analog fuer Sell). Betrifft SharedPositionSide unten UND
        // ExecuteUpdateOrderAsync's `side`-Variable (beide aktuell hart auf Long).

        protected override int? FundingRolloverHours => null; // settledPeriod variiert pro Symbol (4h/8h), kein fixer globaler Wert - Rollover-Erkennung läuft rein über den Socket-Callback, s.u.

        protected override SharedMarginMode? SharedMarginMode => CryptoExchange.Net.SharedApis.SharedMarginMode.Isolated;

        protected override SharedPositionSide? SharedPositionSide => CryptoExchange.Net.SharedApis.SharedPositionSide.Long;

        // BESTAETIGT gegen offizielle CoinW-Doku (PUT /v1/perpum/order, "Modify an Order"):
        // Response liefert originId (alte Order-ID) UND editId (neue Order-ID) als getrennte
        // Felder - kein echtes In-Place-Amend wie OKX, sondern server-seitiges Cancel+Replace
        // in einem atomaren Call. Wir remappen die BrokerId daher selbst in
        // ExecuteUpdateOrderAsync (analog zum BingX-Fix), statt ExchangeModifiesOrdersInPlace
        // auf true zu setzen.
        public override bool ExchangeModifiesOrdersInPlace => false;

        // Kein separater Cancel-vor-Replace-Schritt noetig: entweder EditOrderAsync gelingt
        // atomar (Happy Path, s. ExecuteUpdateOrderAsync), oder es schlaegt fehl - und laut Doku
        // ist die Order dann bereits storniert (s. IsRejectedUpdateError unten), ein erneutes
        // explizites Cancel vor dem Replace waere in diesem Fall ueberfluessig und wuerde nur
        // unnoetig gegen eine bereits tote Order laufen.
        protected override bool RequiresExplicitCancelBeforeReplace => false;

        /// <summary>
        /// Hyperliquid-Pattern (siehe Basisklassen-Kommentar zu IsRejectedUpdateError): CoinW's
        /// Doku sagt explizit, dass JEDER Fehler waehrend EditOrderAsync die Order storniert
        /// (nicht nur bestimmte Race-Faelle wie bei Hyperliquids Post-Only-Reject). Jeder
        /// Edit-Fehlschlag wird daher unbedingt als "alte Order bereits tot" behandelt und loest
        /// automatisch den generischen ExecuteReplaceWorkaround der Basisklasse aus (frische Order
        /// mit denselben Parametern, kein zusaetzliches Cancel noetig da RequiresExplicitCancel-
        /// BeforeReplace=false), statt selbst nur zu reconcilen und die Order unbearbeitet zu lassen.
        /// </summary>
        protected override bool IsRejectedUpdateError(string errorMsg) => true;

        // CoinW hat keinen dedizierten User-Trade-Socket-Channel (siehe SubscribeToOrderUpdatesAsync,
        // das QuantityFilled/AveragePrice/Fee direkt mitliefert) - Fills laufen wie bei OKX ueber den
        // Order-Stream.
        public override bool ExchangeSupportsUserTradeStream => false;

        internal CoinwFuturesBrokerage(
            IAlgorithm algorithm,
            CoinWRestClient restClient,
            CoinWSocketClient socketClient,
            IDataAggregator aggregator,
            Func<List<Holding>>? getHoldingsFunc = null)
            : base(algorithm, "coinw")
        {
            _restClient = restClient;
            _socketClient = socketClient;

            PopulateSPDB();

            _socketClientExData = new CoinWSocketClient();

            InitializeBase(
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                new CoinwBookTickerAdapter(_socketClient.FuturesApi.SharedClient),
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                null, // kein IUserTradeSocketClient - ExchangeSupportsUserTradeStream=false, Fills laufen ueber den Order-Stream
                null, // kein IFundingRateRestClient - ICoinWRestClientFuturesApiShared implementiert das nicht; _fundingRateClient==null wird in SharedFuturesBrokerage.Data.cs abgefangen (nur GetHistory/MarginInterestRate betroffen, Live-Funding laeuft ueber CreateFundingSubscriptionAsync)
                _restClient.FuturesApi.SharedClient,
                aggregator,
                getHoldingsFunc);
            // Kein orderManagementSocket: CoinW's ICoinWSocketClientFuturesApiShared implementiert
            // IFuturesOrderManagementSocketClient nicht (kein Order-Placement ueber Socket, anders
            // als OKX/Bybit/Hyperliquid/Lighter) - Place/Cancel/Update laufen ausschliesslich ueber REST.
        }

        protected override void InitializeFromJob(QuantConnect.Packets.LiveNodePacket job, IDataAggregator aggregator)
        {
            job.BrokerageData.TryGetValue("coinw-api-key", out var key);
            job.BrokerageData.TryGetValue("coinw-api-secret", out var secret);

            if (_restClient == null)
            {
                _restClient = new CoinWRestClient(options =>
                {
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret))
                        options.ApiCredentials = new CoinWCredentials(key, secret);
                });
                PopulateSPDB();
            }

            if (_socketClient == null)
            {
                _socketClient = new CoinWSocketClient(options =>
                {
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret))
                        options.ApiCredentials = new CoinWCredentials(key, secret);
                    options.DelayAfterConnect = TimeSpan.FromMilliseconds(500);
                });
            }

            if (_socketClientExData == null)
            {
                _socketClientExData = new CoinWSocketClient();
            }

            InitializeBase(
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                new CoinwBookTickerAdapter(_socketClient.FuturesApi.SharedClient),
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                null,
                null, // kein IFundingRateRestClient, s. Kommentar im Konstruktor oben
                _restClient.FuturesApi.SharedClient,
                aggregator,
                _getHoldingsFunc);
        }

        #region Connect / Disconnect

        public override bool IsConnected => base.IsConnected && _fundingUpdateConnected;

        public override void Connect()
        {
            _fundingUpdateConnected = true;
            base.Connect();
        }

        public override void Disconnect()
        {
            _fundingUpdateConnected = false;
            _socketClientExData?.Dispose();
            base.Disconnect();
        }

        #endregion

        // USDT-M only - CoinW listet daneben auch USDC-Kontrakte, fuer die Strategie aktuell
        // nicht relevant (alle Ziel-Coins BTC/TRX/HYPE/PAXG/ZEC sind USDT-quotiert).
        protected override string SettleAsset => "USDT";

        private void PopulateSPDB()
        {
            var result = RunSync(() => _restClient.FuturesApi.ExchangeData.GetSymbolsAsync());

            if (!result.Success)
                throw new Exception($"Failed to load CoinW symbols: {result.Error}");

            foreach (var s in result.Data.Where(x => x.Status == FuturesSymbolStatus.Online
                                                    && x.QuoteAsset.Equals("usdt", StringComparison.OrdinalIgnoreCase)))
            {
                var baseAsset = s.BaseAsset.ToUpperInvariant();
                var quoteAsset = s.QuoteAsset.ToUpperInvariant();
                var ticker = baseAsset + quoteAsset;

                var tickSize = (decimal)Math.Pow(10, -s.PriceDecimals);
                var nativeLotStep = s.MinPositionQuantity > 0m ? s.MinPositionQuantity : 1m; // "minSize" - nativer Lot-Schritt in Contracts, bei BTC/TRX/HYPE/PAXG/ZEC bislang durchgehend 1
                var contractSize = s.LotSize > 0m ? s.LotSize : 1m; // "oneLotSize" - Base-Asset-Menge pro Contract (CoinW-Aequivalent zu OKX ctVal)

                // LotSize (Basisklasse, fuer LEANs eigene Order-Validierung) in Base-Asset-Einheiten,
                // analog zum OKX-Fix: baseLotSize = nativerLotSchritt(Contracts) * contractSize.
                var baseLotSize = nativeLotStep * contractSize;

                var symbolProperties = new CoinwSymbolProperties(
                    description: $"CoinW {baseAsset} Perpetual",
                    quoteCurrency: quoteAsset,
                    minimumPriceVariation: tickSize,
                    lotSize: baseLotSize,
                    marketTicker: s.Name, // native "instrument"-Wert fuer USDT-Kontrakte laut Doku: nur der Base-Asset-Name, z.B. "BTC" (nicht "BTCUSDT")
                    contractSize: contractSize,
                    nativeLotStep: nativeLotStep,
                    minimumOrderSize: baseLotSize
                );

                _spdb.SetEntry(Name, ticker, SecurityType.CryptoFuture, symbolProperties);
                _spdb.SetEntry(Name, ticker, SecurityType.Crypto, symbolProperties);
            }
        }

        #region Symbol Mapping

        protected override string NativeTicker(Symbol symbol)
        {
            var props = _spdb.GetSymbolProperties(Name, symbol, SecurityType.CryptoFuture, SettleAsset) as CoinwSymbolProperties;
            if (props != null && !string.IsNullOrEmpty(props.MarketTicker))
                return props.MarketTicker;

            // Fallback: fuer USDT-Kontrakte reicht laut Doku der reine Base-Asset-Name.
            CurrencyPairUtil.DecomposeCurrencyPair(symbol, out var baseAsset, out _);
            return baseAsset;
        }

        protected override string NormalizeSymbol(string rawSymbol) => rawSymbol.ToUpperInvariant() + "USDT";

        protected override SharedSymbol GetSharedSymbol(Symbol s)
        {
            CurrencyPairUtil.DecomposeCurrencyPair(s, out var baseAsset, out var quoteAsset);
            return new SharedSymbol(TradingMode.PerpetualLinear, baseAsset, quoteAsset, NativeTicker(s));
        }

        #endregion

        #region Contract Quantity Conversion

        /// <summary>
        /// CoinW's ContractSize (oneLotSize) - Base-Asset-Menge pro Contract. Lookup via _spdb,
        /// befuellt in PopulateSPDB() aus s.LotSize (JSON "oneLotSize").
        /// </summary>
        private decimal GetContractSize(Symbol symbol)
        {
            var props = _spdb.GetSymbolProperties(Name, symbol, SecurityType.CryptoFuture, SettleAsset) as CoinwSymbolProperties;
            var size = props?.ContractSize ?? 1m;
            return size > 0m ? size : 1m;
        }

        private decimal GetNativeLotStep(Symbol symbol)
        {
            var props = _spdb.GetSymbolProperties(Name, symbol, SecurityType.CryptoFuture, SettleAsset) as CoinwSymbolProperties;
            var step = props?.NativeLotStep ?? 1m;
            return step > 0m ? step : 1m;
        }

        /// <summary>
        /// Base-Asset-Menge -> Contracts. Ceiling auf den naechstgueltigen nativen Lot-Schritt,
        /// analog zu OKX (nie unter die von LEAN erwartete Zielmenge runden - sonst haengt die
        /// Order als PartiallyFilled/Open unbegrenzt in der State-Machine).
        /// </summary>
        protected override SharedQuantity ToExchangeQuantity(Symbol symbol, decimal absBaseQuantity, out decimal roundedBaseQuantity)
        {
            var contractSize = GetContractSize(symbol);
            var nativeLotStep = GetNativeLotStep(symbol);

            var rawContracts = absBaseQuantity / contractSize;
            var steppedContracts = Math.Ceiling(rawContracts / nativeLotStep) * nativeLotStep;
            if (steppedContracts <= 0m)
                steppedContracts = nativeLotStep;

            roundedBaseQuantity = steppedContracts * contractSize;

            return new SharedQuantity { QuantityInContracts = steppedContracts };
        }

        /// <summary>
        /// Contracts -> Base-Asset-Menge (reine Multiplikation).
        /// </summary>
        protected override decimal FromExchangeQuantity(Symbol symbol, SharedOrderQuantity? quantity)
        {
            if (quantity == null)
                return 0m;

            var contracts = quantity.QuantityInContracts ?? 0m;
            return contracts * GetContractSize(symbol);
        }

        /// <summary>
        /// Wir platzieren durchgaengig mit QuantityInContracts (analog OKX) statt dem bei CoinW
        /// ebenfalls moeglichen QuantityInBaseAsset-Pfad des Shared-Clients, damit dieselbe
        /// Contract-Logik auch fuer ClosePositionAsync und Fill-/Position-Reporting greift, die
        /// laut CoinW.Net-Quellcode ausschliesslich Contracts kennen (kein BaseAsset-Fallback dort).
        /// </summary>
        protected override bool HasExchangeQuantity(SharedOrderQuantity? quantity)
            => quantity?.QuantityInContracts.HasValue == true;

        #endregion

        protected override string GenerateClientId(int _)
            => (_restClient.FuturesApi.SharedClient as IFuturesOrderRestClient)!.GenerateClientOrderId();

        #region In-Place Update -> tatsaechlich Cancel+Replace (siehe Klassenkommentar oben)

        /// <summary>
        /// CoinW's "EditOrderAsync" (PUT /v1/perpum/order) ist laut offizieller Doku ein
        /// atomarer serverseitiger Cancel+Replace: Response liefert originId (alte ID) und
        /// editId (neue ID) getrennt. Zwei Besonderheiten laut Doku, die hier beachtet werden:
        /// (1) Leverage kann NICHT geaendert werden - ein Versuch fuehrt zu Fehler 9081 und die
        ///     Order wird storniert, daher wird hier immer die aktuell am Security konfigurierte
        ///     Leverage unveraendert erneut mitgeschickt.
        /// (2) Bei JEDEM Fehler waehrend des Edit wird die Order storniert (nicht nur der
        ///     Modify-Versuch abgelehnt, wie bei Bitget/BingX) - ein fehlgeschlagenes Edit gilt
        ///     hier also immer als "Order tot, nicht als "nochmal versuchen". Anders als urspruenglich
        ///     angenommen ist das genau der Hyperliquid-Fall: IsRejectedUpdateError() liefert daher
        ///     bewusst unbedingt true, damit JEDER Fehler automatisch in den generischen
        ///     ExecuteReplaceWorkaround-Pfad der Basisklasse springt (frische Order, kein zusaetzliches
        ///     Cancel noetig da die alte laut Doku schon weg ist) statt nur passiv zu reconcilen.
        ///
        /// CoinW hat kein echtes One-Way-Mode-Konzept (Long/Short strukturell immer getrennte
        /// Buecher, kein "Net"/"Both"). V1-SCOPE: fix Long (s. SharedPositionSide oben) - direction/
        /// side ist daher IMMER Long, unabhaengig von order.Direction: eine Buy-Order erweitert die
        /// Long-Position, eine Sell-Order reduziert/schliesst sie, aber in beiden Faellen ist das
        /// betroffene Buch dasselbe (Long). Direction==Buy->Long, Direction==Sell->Short waere hier
        /// falsch gewesen (siehe Diskussion) - eine Sell-Order zum Schliessen einer Long-Position
        /// haette damit faelschlich eine neue Short-Position eroeffnet statt die Long-Position zu
        /// reduzieren. V2-TODO: bei Short-Support muss `side` hier ebenso wie SharedPositionSide auf
        /// Bestands-basiertes Routing umgestellt werden.
        /// </summary>
        protected override async Task<HttpResult<SharedId>> ExecuteUpdateOrderAsync(
            Order order, decimal price, decimal? quantity)
        {
            if (!quantity.HasValue)
            {
                Log.Error("CoinW update error: quantity not provided");
                return new HttpResult<SharedId>(Name, null, ArgumentError.Missing(nameof(quantity)));
            }

            var brokerIdStr = order.BrokerId.LastOrDefault();
            if (string.IsNullOrEmpty(brokerIdStr) || !long.TryParse(brokerIdStr, out var orderId))
            {
                Log.Error($"CoinW update error: missing or invalid brokerId '{brokerIdStr}' for order {order.Id}");
                return new HttpResult<SharedId>(Name, null, new InvalidOperationError("Missing or invalid broker order id"));
            }

            if (!_orderStateManager.TryGetByExchangeId(brokerIdStr, out var state))
            {
                Log.Error($"CoinW update error: old state missing for brokerId {brokerIdStr}");
                return new HttpResult<SharedId>(Name, null, new InvalidOperationError("old state missing"));
            }

            var ticker = NativeTicker(order.Symbol);
            var sharedQty = ToExchangeQuantity(order.Symbol, Math.Abs(quantity.Value), out _);
            var contractQuantity = sharedQty.QuantityInContracts ?? 0m;

            // Immer Long: CoinW handeln wir ausschliesslich long (s. SharedPositionSide), das
            // betroffene Buch aendert sich nie, egal ob die Order gerade oeffnet oder schliesst.
            var side = CoinW.Net.Enums.PositionSide.Long;

            // Leverage laut Doku nicht aenderbar (sonst Error 9081 + Order storniert) - aktuell
            // am Security konfigurierten Wert unveraendert erneut mitschicken.
            var leverage = (int)Math.Max(1m, _algorithm.Securities.TryGetValue(order.Symbol, out var sec) ? sec.Leverage : 1m);

            var res = await _restClient.FuturesApi.Trading.EditOrderAsync(
                orderId: orderId,
                symbol: ticker,
                side: side,
                orderType: FuturesOrderType.Plan, // Chase repriced ausschliesslich Limit-Orders
                quantity: contractQuantity,
                leverage: leverage,
                price: price,
                quantityUnit: QuantityUnit.Contracts,
                marginType: MarginType.IsolatedMargin
            ).ConfigureAwait(false);

            if (!res.Success)
            {
                // Kein manuelles Reconcile mehr hier noetig: IsRejectedUpdateError()=true laesst
                // den Aufrufer (SharedFuturesBrokerage.Orders.cs, UpdateOrder) bei JEDEM Fehler
                // automatisch in ExecuteReplaceWorkaround springen, der eine frische Order mit
                // denselben Parametern platziert (die alte gilt laut CoinW-Doku als storniert,
                // RequiresExplicitCancelBeforeReplace=false ueberspringt daher den sonst dortigen
                // Cancel-Schritt). Einfach den Fehler durchreichen.
                Log.Error($"CoinW EditOrder error: {res.Error} | OrderId: {orderId} | Ticker: {ticker} | Price: {price} " +
                          "- Order gilt laut CoinW-API-Doku als storniert, Basisklasse faengt das via ExecuteReplaceWorkaround ab.");
                return new HttpResult<SharedId>(Name, null, res.Error);
            }

            // editId != originId (siehe Klassenkommentar) - BrokerId selbst remappen, da der
            // generische Aufrufer in SharedFuturesBrokerage.Orders.cs (UpdateOrder) das bei einem
            // erfolgreichen ExecuteUpdateOrderAsync NICHT automatisch tut (das passiert nur im
            // ExecuteReplaceWorkaround-Pfad, den wir hier bewusst nicht nutzen, s.o.).
            var newBrokerId = res.Data.EditId.ToString();
            var mapped = _orderStateManager.MapNewExchangeId(state.ClientOrderId, newBrokerId);

            if (mapped)
            {
                OnOrderIdChangedEvent(new BrokerageOrderIdChangedEvent
                {
                    OrderId = order.Id,
                    BrokerId = order.BrokerId
                });
            }

            Log.Trace($"CoinW EditOrder mapped | Old: {orderId} -> New: {newBrokerId}.");

            return new HttpResult<SharedId>(Name, new SharedId(newBrokerId), null);
        }

        #endregion

        #region Cash Balance

        // Kein einzelnes "Equity"-Feld wie bei OKX/Bitget - GetBalancesAsync liefert stattdessen
        // AvailableUsdt (frei), Holding/alMargin (in Positionen gebundene Margin) und Frozen/alFreeze
        // (in offenen Orders gebundene Margin) getrennt von CrossUnrealizedPnl. Summe der ersten drei
        // entspricht der Kontogesamtsumme OHNE unrealisierten PnL - identisch zum Muster
        // "Equity minus UnrealizedPnl" bei OKX/Bitget, nur dass CoinW von vornherein getrennt liefert
        // statt es aus einem kombinierten TotalEquity-Feld herausrechnen zu muessen. Deckt sich mit
        // der Formel, die JKorf selbst im Shared-Balance-Client verwendet (AvailableUsdt + Holding +
        // Frozen), s. CoinWRestClientFuturesApiShared.GetBalancesAsync.
        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() => _restClient.FuturesApi.Account.GetBalancesAsync());

            if (!res.Success || res.Data == null)
            {
                Log.Error($"CoinwFuturesBrokerage.GetCashBalance failed: {res.Error}");
                return [];
            }

            var balance = res.Data.AvailableUsdt + res.Data.Holding + res.Data.Frozen;

            return
            [
                new CashAmount(balance, SettleAsset)
            ];
        }

        #endregion

        #region Funding

        // CoinW hat einen dedizierten oeffentlichen Funding-Rate-Socket-Channel (unauthenticated,
        // aehnlich OKX/Bitget-Pattern). "nt" (Timestamp-Feld im Socket-Modell) wird als naechster
        // Settlement-Zeitpunkt interpretiert - ACHTUNG, noch nicht live gegen echte Rollover-Events
        // verifiziert (JKorf's XML-Doc-Kommentar zu CoinWFundingRate.Timestamp/"nt" ist knapp und
        // nennt nur "Timestamp", nicht explizit "next" oder "current"; die instruments-Antwort liefert
        // mit settledAt/settledPeriod einen unabhaengigen Alternativweg falls sich das als falsch
        // herausstellt).
        protected override async Task<WebSocketResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
            string nativeTicker, Symbol symbol, Func<DateTime, decimal?, DateTime?, (bool ShouldEmit, bool IsFirstTick)> onFundingRate)
        {
            return await _socketClientExData.FuturesApi.SubscribeToFundingRateUpdatesAsync(
                nativeTicker,
                data =>
                {
                    var rate = data.Data;
                    if (onFundingRate(rate.Timestamp, rate.FundingRate, null).ShouldEmit)
                    {
                        Task.Run(async () =>
                        {
                            await Task.Delay(5000); // 5s warten bis der Funding-Fee-Eintrag im Balance-Update erscheint
                            // CoinW hat (anders als OKX/Bitget) keinen separaten Bills/Ledger-REST-Endpoint fuer
                            // Funding-Fees im Futures-API - ueber SubscribeToBalanceUpdatesAsync (Delta-Push) oder
                            // GetBalancesAsync-Polling nachziehen, sobald live verifiziert ist, wie CoinW Funding-Fees
                            // tatsaechlich in der Balance auftauchen laesst (vermutlich still in AvailableUsdt/Holding
                            // eingerechnet statt als separat auslesbares Ereignis wie OKX's Bills).
                        });
                    }
                }).ConfigureAwait(false);
        }

        #endregion

        /// <summary>
        /// Adapter: CoinW.Net's Shared-Socket-Client implementiert kein IBookTickerSocketClient
        /// (kein dedizierter Best-Bid/Ask-Channel bei CoinW - weder REST-Ticker noch Socket-Ticker
        /// fuehren Bid/Ask, nur last_price/fair_price/24h-Stats). Einzige verfuegbare Quelle fuer
        /// Best Bid/Ask ist IOrderBookSocketClient (volle Orderbuch-Tiefe, kein Level-Parameter).
        /// Nimmt daher bei jedem Orderbuch-Update ungethrottelt Bids[0]/Asks[0] als Ersatz.
        /// Reiner Wrapper um CoinW.Net's eigene, unveraenderte IOrderBookSocketClient-Methode -
        /// kein Eingriff in CoinW.Net selbst.
        /// </summary>
        private sealed class CoinwBookTickerAdapter : IBookTickerSocketClient
        {
            private readonly IOrderBookSocketClient _orderBookClient;

            public CoinwBookTickerAdapter(IOrderBookSocketClient orderBookClient)
            {
                _orderBookClient = orderBookClient;
            }

            public SubscribeBookTickerOptions SubscribeBookTickerOptions { get; } =
                new SubscribeBookTickerOptions("CoinW", false);

            // IBookTickerSocketClient erbt (ueber ISharedClient) diese generischen Client-Member.
            // Da der gewrappte _orderBookClient (CoinW's eigener Shared-Socket-Client) ISharedClient
            // bereits vollstaendig implementiert (IOrderBookSocketClient : ISharedClient), reichen
            // wir hier einfach 1:1 durch statt eigene Logik zu bauen.
            public string Exchange => _orderBookClient.Exchange;
            public TradingMode[] SupportedTradingModes => _orderBookClient.SupportedTradingModes;
            public bool Authenticated => _orderBookClient.Authenticated;
            public SharedClientInfo Discover() => _orderBookClient.Discover();
            public string FormatSymbol(string baseAsset, string quoteAsset, TradingMode tradingMode, DateTime? deliverDate = null)
                => _orderBookClient.FormatSymbol(baseAsset, quoteAsset, tradingMode, deliverDate);
            public void SetDefaultExchangeParameter(string name, object value)
                => _orderBookClient.SetDefaultExchangeParameter(name, value);
            public void ResetDefaultExchangeParameters()
                => _orderBookClient.ResetDefaultExchangeParameters();

            public async Task<WebSocketResult<UpdateSubscription>> SubscribeToBookTickerUpdatesAsync(
                SubscribeBookTickerRequest request, Action<DataEvent<SharedBookTicker>> handler, CancellationToken ct = default)
            {
                var symbol = request.Symbol ?? throw new ArgumentException("Symbol is not set", nameof(request));
                var symbolName = symbol.BaseAsset + symbol.QuoteAsset; // rein informativ, wird von SharedFuturesBrokerage.Data.cs fuer BookTicker-Updates nicht ausgewertet

                return await _orderBookClient.SubscribeToOrderBookUpdatesAsync(
                    new SubscribeOrderBookRequest(symbol, exchangeParameters: request.ExchangeParameters),
                    update =>
                    {
                        var book = update.Data;
                        var bestBid = book.Bids.FirstOrDefault();
                        var bestAsk = book.Asks.FirstOrDefault();
                        if (bestBid == null || bestAsk == null)
                            return; // leeres/einseitiges Update - kein sinnvoller BookTicker ableitbar, wird uebersprungen

                        var isContracts = book.QuantityType == SharedQuantityType.Contracts;
                        var ticker = new SharedBookTicker(
                            symbol,
                            symbolName,
                            bestAsk.Price,
                            new SharedOrderQuantity(contractQuantity: isContracts ? bestAsk.Quantity : null,
                                                     baseAssetQuantity: isContracts ? null : bestAsk.Quantity),
                            bestBid.Price,
                            new SharedOrderQuantity(contractQuantity: isContracts ? bestBid.Quantity : null,
                                                     baseAssetQuantity: isContracts ? null : bestBid.Quantity));

                        handler(update.ToType(ticker));
                    }, ct).ConfigureAwait(false);
            }
        }
    }
}
