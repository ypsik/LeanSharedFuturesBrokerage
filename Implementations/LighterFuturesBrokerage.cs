using CryptoExchange.Net.Interfaces.Clients;
using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Errors;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using Lighter.Net;
using Lighter.Net.Clients;
using Lighter.Net.Enums;
using QuantConnect;
using QuantConnect.Brokerages;
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

using CxCancelOrderRequest = CryptoExchange.Net.SharedApis.CancelOrderRequest;

namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    public class LighterFuturesBrokerage : SharedFuturesBrokerage
    {
        private LighterRestClient _restClient;
        private LighterSocketClient _socketClient;
        private LighterSocketClient _socketClientExData; // dediziert fuer ExchangeData (Funding-Rate/Mark/Index Ticker)

        private readonly object _fundingUpdateLock = new();
        private bool _fundingUpdateConnected = false;
        private UpdateSubscription _fundingUpdateSubscription;

        protected override string SettleAsset => "USDC";

        protected override bool EmitFundingRateImmediately => true;
        public override bool ExchangeModifiesOrdersInPlace => true;

        public override bool IsConnected => base.IsConnected && _fundingUpdateConnected;

        // TODO: unbestaetigt - Lighter Ticker-Update liefert funding_timestamp pro Update.
        // Muss gegen Live-Daten geprueft werden ob das ein festes Stunden-Intervall ist (wie HL)
        // oder variabel. Bis dahin auf HL-Wert (1h) belassen.
        protected override int? FundingRolloverHours => 1;
        public override decimal MinimumOrderNotionalValue => 10m;

        internal LighterFuturesBrokerage(
            IAlgorithm algorithm,
            LighterRestClient restClient,
            LighterSocketClient socketClient,
            IDataAggregator aggregator,
            Func<List<Holding>>? getHoldingsFunc = null)
            : base(algorithm, "lighter")
        {
            _restClient = restClient;
            _socketClient = socketClient;
            _socketClientExData = new LighterSocketClient();

            PopulateSPDB();

            // Slot-Reihenfolge 1:1 aus HyperliquidFuturesBrokerage gespiegelt (orderClient, ?, socketOrder,
            // socketTrade, socketPosition, socketBalance, symbolClient, tickerClient, aggregator, getHoldingsFunc).
            // ANDERS ALS ASTER: Lighter's SocketClientExchangeApiShared implementiert IBalanceSocketClient,
            // daher hier SharedClient statt null im 6. Slot.
            // Slot 7 (symbolClient) erstmal auf null gesetzt (Aleks' Aenderung) - muss beim Testen geklaert
            // werden ob/wofuer die Basisklasse das dort separat braucht, wenn Slot 1 bereits denselben
            // SharedClient (der auch IFuturesSymbolRestClient implementiert) liefert.
            InitializeBase(
                restClient.ExchangeApi.SharedClient,
                restClient.ExchangeApi.SharedClient,
                socketClient.ExchangeApi.SharedClient,
                socketClient.ExchangeApi.SharedClient,
                socketClient.ExchangeApi.SharedClient,
                socketClient.ExchangeApi.SharedClient,
                restClient.ExchangeApi.SharedClient,
                restClient.ExchangeApi.SharedClient,
                aggregator,
                getHoldingsFunc);
        }

        protected override void InitializeFromJob(QuantConnect.Packets.LiveNodePacket job, IDataAggregator aggregator)
        {
            if (_restClient == null)
            {
                var creds = GetCredentialsFromJob(job);
                _restClient = new LighterRestClient(options =>
                {
                    if (creds != null)
                        options.ApiCredentials = creds;
                });
            }

            if (_socketClient == null)
            {
                var creds = GetCredentialsFromJob(job);
                _socketClient = new LighterSocketClient(options =>
                {
                    if (creds != null)
                        options.ApiCredentials = creds;
                });
            }

            if (_socketClientExData == null)
                _socketClientExData = new LighterSocketClient();

            InitializeBase(
                _restClient.ExchangeApi.SharedClient,
                _restClient.ExchangeApi.SharedClient,
                _socketClient.ExchangeApi.SharedClient,
                _socketClient.ExchangeApi.SharedClient,
                _socketClient.ExchangeApi.SharedClient,
                _socketClient.ExchangeApi.SharedClient,
                _restClient.ExchangeApi.SharedClient,
                _restClient.ExchangeApi.SharedClient,
                aggregator
            );
        }

        private static LighterCredentials? GetCredentialsFromJob(QuantConnect.Packets.LiveNodePacket job)
        {
            job.BrokerageData.TryGetValue("lighter-public-address", out var publicAddress);
            job.BrokerageData.TryGetValue("lighter-account-index", out var accountIndexStr);
            job.BrokerageData.TryGetValue("lighter-api-key-index", out var apiKeyIndexStr);
            job.BrokerageData.TryGetValue("lighter-api-secret", out var secret);

            if (string.IsNullOrEmpty(publicAddress) || string.IsNullOrEmpty(secret))
                return null;

            return new LighterCredentials(
                EthKey.FromPublicKey(publicAddress),
                long.Parse(accountIndexStr ?? "0"),
                int.Parse(apiKeyIndexStr ?? "0"),
                secret);
        }

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
                        _socketClient.ExchangeApi.Account.SubscribeToAccountUpdatesAsync(null,
                        update =>
                        {
                            if (update?.Data?.FundingHistories == null || update.Data.FundingHistories.Count == 0)
                                return;

                            foreach (var funding in update.Data.FundingHistories
                                   .SelectMany(kvp => kvp.Value)
                                   .Where(f => f.Timestamp > connectTime))
                            {
                                if (_algorithm?.Portfolio?.CashBook != null)
                                {
                                    _algorithm.Portfolio.CashBook[SettleAsset].AddAmount(funding.Change);
                                    OnMessage(new FundingBrokerageMessageEvent(SettleAsset, funding.Change));
                                }
                            }

                            var maxTimestamp = update.Data.FundingHistories.SelectMany(kvp => kvp.Value).Max(h => h?.Timestamp ?? DateTime.UtcNow);
                            if (maxTimestamp > connectTime)
                                connectTime = maxTimestamp;
                        }));

                    SetupSubscriptionEvents(
                                    sub?.Success??false,
                                    sub?.Data,
                                    (state) => { _fundingUpdateConnected = state; },
                                    "Funding updates",
                                    "Funding updates subscription failed",
                                    sub?.Error?.ToString()
                                );

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
            var result = RunSync(() => _restClient.ExchangeApi.ExchangeData.GetSymbolsAsync(symbolType: SymbolTypeFilter.Perp));

            if (!result.Success)
                throw new Exception($"Failed to load Lighter assets: {result.Error}");

            foreach (var symbol in result.Data.Where(s => s.Status == SymbolStatus.Active))
            {
                var ticker = symbol.Symbol + SettleAsset;

                // Anders als HL (fixe Summe pxDecimals+szDecimals=5): Lighter liefert Preis- und
                // Mengen-Dezimalstellen direkt pro Symbol - kein dynamisches SPDB-Update noetig.
                var lotSize = (decimal)Math.Pow(10, -symbol.SupportedQuantityDecimals);
                var tickSize = (decimal)Math.Pow(10, -symbol.SupportedPriceDecimals);

                var symbolProperties = new SymbolProperties(
                    description: $"Lighter {symbol.Symbol} Perpetual",
                    quoteCurrency: SettleAsset,
                    contractMultiplier: 1m,
                    minimumPriceVariation: tickSize,
                    lotSize: lotSize,
                    marketTicker: symbol.Symbol,
                    minimumOrderSize: symbol.MinBaseQuantity
                );

                _spdb.SetEntry(Name, ticker, SecurityType.Crypto, symbolProperties);
                _spdb.SetEntry(Name, ticker, SecurityType.CryptoFuture, symbolProperties);
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

        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() => _restClient.ExchangeApi.Account.GetAccountsAsync());
            if (!res.Success || res.Data == null)
            {
                Log.Error($"Cash {res.Error?.Message}");
                return [];
            }

            var accountIndex = _restClient.ExchangeApi.ApiCredentials?.Credential.AccountIndex;
            var account = res.Data.Accounts.SingleOrDefault(x => x.AccountIndex == accountIndex);
            if (account == null)
            {
                Log.Error("Cash: account not found in accounts response");
                return [new CashAmount(0m, SettleAsset)];
            }

            return [new CashAmount(account.Assets?.Sum(x => x.MarginBalance) ?? 0m, SettleAsset)];
        }

        // Das hier ist NUR die Funding-RATE (fuer SPDB/Strategie), nicht die tatsaechliche
        // Funding-PAYMENT-Verbuchung. Letztere laeuft ueber SubscribeToAccountUpdatesAsync
        // in Connect() (account_all Channel, FundingHistories-Feld) - Analogon zu HL's
        // SubscribeToUserFundingUpdatesAsync.
        protected override async Task<WebSocketResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
            string nativeTicker, Symbol symbol, Func<DateTime, decimal?, DateTime?, (bool ShouldEmit, bool IsFirstTick)> onFundingRate)
        {
            return await _socketClientExData.ExchangeApi.ExchangeData.SubscribeToFuturesTickerUpdatesAsync(
                nativeTicker, data =>
                {
                    var ticker = data.Data.Ticker;

                    onFundingRate(ticker.FundingTimestamp, ticker.FundingRate, null);
                });
        }

        protected override string GenerateClientId(int _)
        {
            return (_restClient.ExchangeApi.SharedClient as IFuturesOrderRestClient).GenerateClientOrderId();
        }


        // Kein Override von ExecutePlaceOrderAsync / ExecuteCancelOrderAsync noetig:
        // Lighter's Shared-Client implementiert IFuturesOrderRestClient.PlaceFuturesOrderAsync bereits
        // korrekt fuer unser State-Machine-Pattern - er gibt die lokal generierte ClientOrderIndex als
        // SharedId zurueck (kein synchrones OrderId-Ergebnis moeglich, da Lighter On-Chain-TX-basiert ist:
        // PlaceOrder liefert nur tx_hash). Der Swap auf die echte OrderIndex passiert automatisch in
        // HandleOrderSocket via o.ClientOrderId-Match (Lighter setzt ClientOrderId auf jedem Order-Update -
        // bestaetigt in LighterSocketClientExchangeApiShared.cs).
        // CancelFuturesOrderAsync passt ebenfalls 1:1 (nutzt order.BrokerId, das nach dem Socket-Swap
        // die echte OrderIndex enthaelt).

        protected override async Task<HttpResult<SharedId>> ExecuteUpdateOrderAsync(Order order, decimal price, decimal? quantity)
        {
            var ticker = NativeTicker(order.Symbol);
            var brokerId = order.BrokerId.LastOrDefault();
            if (!long.TryParse(brokerId, out var orderIndex))
            {
                Log.Error($"Update error: invalid brokerId '{brokerId}'");
                return new HttpResult<SharedId>(Name, null, new InvalidOperationError("invalid brokerId"));
            }

            // Aster-Style: state.OriginalQuantity (Single Source of Truth im OrderStateManager) statt
            // order.Quantity als Fallback - order.Quantity ist nicht zuverlaessig konsistent mit dem
            // tatsaechlich getrackten Zustand. Lighter's EditOrderAsync braucht quantity als
            // non-nullable decimal (anders als HL, wo ein fehlender Wert schlicht abgelehnt wird).
            if (!_orderStateManager.TryGetByExchangeId(brokerId, out var state))
            {
                Log.Error($"Update error: old state missing for brokerId {brokerId}");
                return new HttpResult<SharedId>(Name, null, new InvalidOperationError("old state missing"));
            }

            var editQuantity = quantity.HasValue ? Math.Abs(quantity.Value) : Math.Abs(state.OriginalQuantity);

            // WICHTIG: Lighter's EditOrderAsync existiert nicht im IFuturesOrderRestClient Shared-Interface
            // (nur PlaceFuturesOrderAsync/CancelFuturesOrderAsync sind gemappt) - deshalb direkter Trading-Call.
            // Anders als HL: kein side/orderType Parameter noetig, nur quantity/price/triggerPrice.
            var res = await _restClient.ExchangeApi.Trading.EditOrderAsync(
                symbol: ticker,
                orderIndex: orderIndex,
                quantity: editQuantity,
                price: price).ConfigureAwait(false);

            if (!res.Success)
            {
                Log.Error($"Lighter update error: {res.Error} | Ticker: {ticker} | Price: {price} | OriginalData: {res.OriginalData}");
                return new HttpResult<SharedId>(Name, null, res.Error);
            }

            // EditOrder modifiziert dieselbe OrderIndex in-place (SignModifyOrder-TX on-chain) -
            // es entsteht KEINE neue Order-ID (anders als Aster's Cancel+Replace). BrokerId bleibt bestehen.
            return new HttpResult<SharedId>(Name, new SharedId(brokerId), null);
        }
    }
}