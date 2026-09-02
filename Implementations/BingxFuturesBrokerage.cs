using BingX.Net.Clients;
using BingX.Net.Enums;
using BingX.Net.Interfaces.Clients;
using BingX.Net.Objects.Models;
using CryptoExchange.Net.Interfaces.Clients;
using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using CryptoExchange.Net.Trackers.UserData;
using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantConnect.Util;
using RestSharp;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static QLNet.Bond;

namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    public class BingxFuturesBrokerage : SharedFuturesBrokerage
    {
        private BingXRestClient _restClient;
        private BingXSocketClient _socketClient;
        private BingXSocketClient _socketClientExData;
        private readonly object _fundingUpdateLock = new();
        private bool _fundingUpdateConnected = false;
        private UpdateSubscription? _fundingUpdateSubscription;
        private CancellationTokenSource? _fundingCts;

        private bool _isHedgeMode = false;

        public override bool ExchangeSupportsUserTradeStream => false;
        public override decimal MinimumOrderNotionalValue => 2.0m;

        internal BingxFuturesBrokerage(
            IAlgorithm algorithm,
            BingXRestClient restClient,
            BingXSocketClient socketClient,
            IDataAggregator aggregator,
            Func<List<Holding>>? getHoldingsFunc = null,
            bool isHedgeMode = true)
            : base(algorithm, "bingx")
        {
            _restClient = restClient;
            _socketClient = socketClient;
            _socketClientExData = new BingXSocketClient();
            _isHedgeMode = isHedgeMode;

            PopulateSPDB();

            InitializeBase(
                restClient.PerpetualFuturesApi.SharedClient,
                restClient.PerpetualFuturesApi.SharedClient,
                socketClient.PerpetualFuturesApi.SharedClient,
                socketClient.PerpetualFuturesApi.SharedClient,
                socketClient.PerpetualFuturesApi.SharedClient,
                null,// user trade stream wird von BingX nicht unterstützt, daher null
                restClient.PerpetualFuturesApi.SharedClient,
                restClient.PerpetualFuturesApi.SharedClient,
                aggregator,
                getHoldingsFunc);
        }

        private void PopulateSPDB()
        {
            var result = RunSync(() => _restClient.PerpetualFuturesApi.ExchangeData
                .GetContractsAsync());

            if (!result.Success)
                throw new Exception($"Failed to load BingX assets: {result.Error}");

            foreach (var contract in result.Data.Where(c => c.Status == 1))
            {
                var ticker = contract.Asset + contract.Currency;

                var tickSize = (decimal)Math.Pow(10, -contract.PricePrecision);

                var lotSize = (decimal)Math.Pow(10, -contract.QuantityPrecision);

                var symbolProperties = new SymbolProperties(
                    description: $"BingX {contract.Asset} Perpetual",
                    quoteCurrency: contract.Currency,
                    contractMultiplier: 1m,
                    minimumPriceVariation: tickSize,
                    lotSize: lotSize,
                    marketTicker: contract.Symbol,
                    minimumOrderSize: contract.MinOrderQuantity
                );

                _spdb.SetEntry(Name, ticker, SecurityType.CryptoFuture, symbolProperties);
                _spdb.SetEntry(Name, ticker, SecurityType.Crypto, symbolProperties);

            }
        }

        protected override void InitializeFromJob(QuantConnect.Packets.LiveNodePacket job, IDataAggregator aggregator)
        {
            if (_restClient == null)
            {
                job.BrokerageData.TryGetValue("bingx-api-key", out var key);
                job.BrokerageData.TryGetValue("bingx-api-secret", out var secret);

                _restClient = new BingXRestClient(options => {
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret))
                        options.ApiCredentials = new BingX.Net.BingXCredentials(key, secret);
                });
            }

            if (_socketClient == null)
            {
                job.BrokerageData.TryGetValue("bingx-api-key", out var key);
                job.BrokerageData.TryGetValue("bingx-api-secret", out var secret);
                _socketClient = new BingXSocketClient(options => {
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret))
                        options.ApiCredentials = new BingX.Net.BingXCredentials(key, secret);
                });
            }

            if (_socketClientExData == null)
            {
                _socketClientExData = new BingXSocketClient();
            }

            // Hedge-Mode aus Job-Config lesen, Fallback bleibt true (bisheriges Verhalten)
            if (job.BrokerageData.TryGetValue("bingx-hedge-mode", out var hedgeModeStr)
                && bool.TryParse(hedgeModeStr, out var hedgeModeParsed))
            {
                _isHedgeMode = hedgeModeParsed;
            }

            InitializeBase(
                _restClient.PerpetualFuturesApi.SharedClient,
                _restClient.PerpetualFuturesApi.SharedClient,
                _socketClient.PerpetualFuturesApi.SharedClient,
                _socketClient.PerpetualFuturesApi.SharedClient,
                _socketClient.PerpetualFuturesApi.SharedClient,
                null, // user trade stream wird von BingX nicht unterstützt, daher null
                _restClient.PerpetualFuturesApi.SharedClient,
                _restClient.PerpetualFuturesApi.SharedClient,
                aggregator,
                _getHoldingsFunc
            );
        }

        protected override string NormalizeSymbol(string rawSymbol)
                => rawSymbol.Replace("-", "");

        protected override string NativeTicker(Symbol symbol)
        {
            CurrencyPairUtil.DecomposeCurrencyPair(symbol, out var baseAsset, out var quoteAsset);
            return $"{baseAsset}-{quoteAsset}";
        }


        #region Connect

        public override bool IsConnected => base.IsConnected && _fundingUpdateConnected;
        public override bool ExchangeModifiesOrdersInPlace => false;
        protected override SharedPositionSide? SharedPositionSide => _isHedgeMode ? CryptoExchange.Net.SharedApis.SharedPositionSide.Long : null;


        protected override ExchangeParameters PlaceFuturesOrderExchangeParameters
        {
            get
            {
                var parameters = base.PlaceFuturesOrderExchangeParameters;
                return parameters;
            }
        }
        protected override ExchangeParameters OrderUpdatesExchangeParameters
        {
            get
            {
                var parameters = base.OrderUpdatesExchangeParameters;
                return parameters;
            }
        }

        protected override ExchangeParameters OpenOrdersExchangeParameters
        {
            get
            {
                var parameters = base.OpenOrdersExchangeParameters;
                return parameters;
            }
        }
        protected override ExchangeParameters AccountHoldingsExchangeParameters
        {
            get
            {
                var parameters = base.AccountHoldingsExchangeParameters;
                return parameters;
            }
        }

        protected override ExchangeParameters GetFundingRateHistoryParameters
        {
            get
            {
                var parameters = base.GetFundingRateHistoryParameters;
                return parameters;
            }
        }

        public override void Connect()
        {
            lock (_fundingUpdateLock)
            {
                // Start the isolated user stream setup
                EstablishUserStreamSubscription();
            }
            base.Connect();
        }

        /// <summary>
        /// Handles the isolated logic for fetching the ListenKey and subscribing to the WebSocket.
        /// </summary>
        private void EstablishUserStreamSubscription()
        {
            if (_fundingUpdateSubscription != null || _socketClient == null)
            {
                return;
            }

            _subRateGate.WaitToProceed();

            var sub = RunSync(() =>
                _socketClient.PerpetualFuturesApi.SubscribeToUserDataUpdatesAsync(
                    onAccountUpdate: update =>
                    {
                        if (update.Data?.Update?.Trigger == "FUNDING_FEE")
                        {
                            foreach (var fundingsRecord in update.Data.Update.Balances)
                            {
                                if (_algorithm?.Portfolio?.CashBook != null)
                                {
                                    _algorithm.Portfolio.CashBook[SettleAsset].AddAmount(fundingsRecord.BalanceChange);
                                    OnMessage(new FundingBrokerageMessageEvent(fundingsRecord.Asset ?? SettleAsset, fundingsRecord.BalanceChange));
                                }
                            }
                        }
                    },
                    onListenKeyExpiredUpdate: expiredEvent =>
                    {
                        Log.Trace("BingX: ListenKey expired! Initiating reconnect...");
                        Task.Run(() => ReconnectUserStream());
                    }));

            SetupSubscriptionEvents(
                sub?.Success ?? false,
                sub?.Data,
                (state) => _fundingUpdateConnected = state,
                "Wallet updates",
                "Wallet updates subscription failed",
                sub?.Error?.ToString()
            );

            if (sub?.Success ?? false)
                _fundingUpdateSubscription = sub.Data;
        }

        /// <summary>
        /// Safely terminates the old subscription and rebuilds ONLY the user stream.
        /// </summary>
        private void ReconnectUserStream()
        {
            lock (_fundingUpdateLock)
            {
                try
                {
                    if (_fundingUpdateSubscription != null && _socketClient != null)
                    {
                        RunSync(() => _socketClient.UnsubscribeAsync(_fundingUpdateSubscription));
                        _fundingUpdateSubscription = null;
                    }
                }
                catch (Exception ex)
                {
                    Log.Error($"BingX: Error during unsubscribe while reconnecting: {ex.Message}");
                }

                EstablishUserStreamSubscription();
            }
        }

        public override void Disconnect()
        {
            // Nur noch Cancel() hier - kein Dispose() und kein "= null" mehr.
            // Der Loop in SubscribeFunding haelt seine eigene lokale Referenz auf die
            // CancellationTokenSource und disposed sie selbst in seinem eigenen finally-Block,
            // sobald er (durch die Cancellation) beendet wird. So gibt es keine Race Condition
            // mehr zwischen Disconnect() und dem noch laufenden Background-Task.
            _fundingCts?.Cancel();
            RunSync(() => _fundingUpdateSubscription?.CloseAsync() ?? Task.CompletedTask);
            _socketClientExData?.Dispose();
            base.Disconnect();
        }
        #endregion

        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() => _restClient.PerpetualFuturesApi.Account.GetBalancesAsync());
            var balance = res?.Data?.FirstOrDefault();
            if (balance == null)
            {
                return [];
            }
            var result = new List<CashAmount>
            {
                new(balance?.Balance ?? 0, balance?.Asset ?? SettleAsset)
            };
            return result;
        }

        protected override bool SubscribeFunding(Symbol symbol)
        {
            var nativeTicker = NativeTicker(symbol);

            // Lokale CTS-Instanz für diesen Aufruf/Loop. Das Feld _fundingCts wird nur für
            // Disconnect() als externer Cancel-Trigger gehalten - der Loop selbst arbeitet
            // ausschließlich mit dieser lokalen Kopie ("cts"/"token"), damit ein paralleles
            // Disconnect() (das das Feld auf null setzen bzw. disposen würde) den Loop nicht
            // mit einer NullReferenceException/ObjectDisposedException abschießen kann.
            var cts = new CancellationTokenSource();
            _fundingCts = cts;
            var token = cts.Token;

            _ = Task.Run(async () =>
            {
                DateTime? nextFundingTime = null;

                Log.Trace($"{Name} Funding poll initialization for {nativeTicker}");
                try
                {
                    while (!token.IsCancellationRequested)
                    {
                        try
                        {
                            // 1. Initialer Abruf der NextFundingTime außerhalb oder falls der State verloren ging
                            if (nextFundingTime == null)
                            {
                                var initResult = await _restClient.PerpetualFuturesApi.ExchangeData
                                    .GetFundingRateAsync(nativeTicker, token);

                                if (!initResult.Success)
                                {
                                    Log.Error($"{Name} SubscribeFunding initial fetch failed for {nativeTicker}: {initResult.Error}");
                                    await Task.Delay(TimeSpan.FromMinutes(1), token);
                                    continue;
                                }

                                nextFundingTime = initResult.Data.NextFundingTime;
                            }

                            // 2. Berechnen und Warten bis zum Settlement
                            var delay = nextFundingTime.Value - DateTime.UtcNow;

                            if (delay > TimeSpan.Zero)
                            {
                                await Task.Delay(delay.Add(TimeSpan.FromTicks(100)), token);
                            }
                            else
                            {
                                // Fallback falls die Zeit in der Vergangenheit liegt / System-Uhr-Abweichungen
                                await Task.Delay(TimeSpan.FromTicks(100), token);
                            }

                            // 3. Nach dem Settlement: Einzigen Call ausführen, um die neue Rate + die NÄCHSTE FundingTime zu holen
                            var rateResult = await _restClient.PerpetualFuturesApi.ExchangeData
                                .GetFundingRateAsync(nativeTicker, token);

                            if (rateResult.Success)
                            {
                                var roundedTime = new DateTime(
                                    nextFundingTime.Value.Year, nextFundingTime.Value.Month, nextFundingTime.Value.Day,
                                    nextFundingTime.Value.Hour, 0, 0, DateTimeKind.Utc);

                                _aggregator?.Update(new MarginInterestRate
                                {
                                    Symbol = symbol,
                                    Time = roundedTime,
                                    InterestRate = rateResult.Data.LastFundingRate
                                });

                                Log.Trace($"{Name} Funding Update: {symbol.Value} -> Rate: {rateResult.Data.LastFundingRate} @ {roundedTime}");

                                // Target für die nächste Iteration direkt auf den neuen Wert der Exchange setzen
                                nextFundingTime = rateResult.Data.NextFundingTime;
                            }
                            else
                            {
                                Log.Error($"{Name} SubscribeFunding rate fetch failed for {nativeTicker}: {rateResult.Error}");
                                // Bei Fehler Target zurücksetzen, um im nächsten Loop neu zu initialisieren
                                nextFundingTime = null;
                                await Task.Delay(TimeSpan.FromMinutes(1), token);
                            }
                        }
                        catch (OperationCanceledException) { break; }
                        catch (Exception ex)
                        {
                            Log.Error($"{Name} SubscribeFunding exception for {nativeTicker}: {ex.Message}");
                            nextFundingTime = null; // Sicherhaltshalber zurücksetzen
                            await Task.Delay(TimeSpan.FromMinutes(1), token);
                        }
                    }
                }
                finally
                {
                    // Dispose passiert hier, in derselben "Funktion"/demselben Loop, der die CTS
                    // auch erzeugt hat - nicht mehr in Disconnect(), das nur noch cancelt.
                    cts.Dispose();
                }
            }, token);

            return true;
        }

        protected override string GenerateClientId(int _)
        {
            return _restClient.PerpetualFuturesApi.SharedClient.GenerateClientOrderId();
        }


        protected override async Task<HttpResult<SharedId>> ExecuteUpdateOrderAsync(
                Order order, decimal price, decimal? quantity)
        {
            if (!quantity.HasValue)
            {
                Log.Error($"Update error: quantity not provided");
                return new HttpResult<SharedId>(Name, null, ArgumentError.Missing("Quantity"));
            }

            var ticker = NativeTicker(order.Symbol);

            var brokerId = order.BrokerId.LastOrDefault();
            if (!long.TryParse(brokerId, out var exchangeOrderId))
            {
                Log.Error($"Update error: invalid brokerId '{brokerId}'");
                return new HttpResult<SharedId>(Name, null, new InvalidOperationError("invalid brokerId"));
            }

            if (!_orderStateManager.TryGetByExchangeId(brokerId, out var state))
            {
                Log.Error($"Update error: old state missing for brokerId {brokerId}");
                return new HttpResult<SharedId>(Name, null, new InvalidOperationError("old state missing"));
            }

            var side = order.Quantity > 0 ? BingX.Net.Enums.OrderSide.Buy : BingX.Net.Enums.OrderSide.Sell;
            var positionSide = _isHedgeMode
                ? (SharedPositionSide == CryptoExchange.Net.SharedApis.SharedPositionSide.Long ? BingX.Net.Enums.PositionSide.Long : BingX.Net.Enums.PositionSide.Short)
                : BingX.Net.Enums.PositionSide.Both;

            string newClientOrderId = GenerateClientId(order.Id);
            _orderStateManager.TryAdd(newClientOrderId, state);

            var res = await _restClient.PerpetualFuturesApi.Trading.CancelReplaceOrderAsync(
                orderId: exchangeOrderId,
                clientOrderId: null,
                mode: CancelReplaceMode.StopOnFailure,
                symbol: ticker,
                side: side,
                type: FuturesOrderType.Limit,
                positionSide: positionSide,
                quantity: Math.Abs(quantity.Value),
                price: price,
                newClientOrderId: newClientOrderId);

            if (!res.Success)
            {
                _orderStateManager.RemoveAlias(newClientOrderId);
                Log.Error($"Update error: {res.Error} | Ticker: {ticker} | Price: {price} | OriginalData: {res.OriginalData}");
                return new HttpResult<SharedId>(Name, null, res.Error);
            }

            // Cancel-replace produces a new exchange order → return new ID (unlike Bitget EditOrder)
            var newExchangeId = res.Data?.NewOrder?.OrderId.ToString();

            // Wir mappen die neue BrokerId hier direkt aus dem REST-Response, der zuverlässig
            // die echte neue Order-Id liefert — unabhängig vom betroffenen Socket-Feld.
            if (!string.IsNullOrEmpty(newExchangeId) && newExchangeId != brokerId)
            {
                if (_orderStateManager.MapNewExchangeId(newClientOrderId, newExchangeId))
                {
                    // Wir waren zuerst - alte ClientOrderId jetzt aufräumen.
                    if (state.ClientOrderId != newClientOrderId)
                        _orderStateManager.RemoveAlias(state.ClientOrderId);

                    state.ClientOrderId = newClientOrderId;
                    state.IsUpdatePending = false;

                    OnOrderIdChangedEvent(new BrokerageOrderIdChangedEvent
                    {
                        OrderId = order.Id,
                        BrokerId = order.BrokerId
                    });

                    Log.Trace($"{Name}.ExecuteUpdateOrderAsync: Manually mapped replace | Old: {brokerId} -> New: {newExchangeId}.");
                }
                else
                {
                    // Socket war schneller und hat bereits alles korrekt selbst gemacht: BrokerId gemappt,
                    // state.ClientOrderId auf newClientOrderId umgebogen, alte ID entfernt. Nichts zu tun -
                    // newClientOrderId NICHT entfernen, das ist jetzt der aktive, korrekte Eintrag.
                    Log.Trace($"{Name}.ExecuteUpdateOrderAsync: Exchange-Id mapping already done via socket (race) | Old: {brokerId} -> New: {newExchangeId}.");
                }
            }
            // else: kein verwertbarer newExchangeId vom REST-Response — Alias absichtlich NICHT
            // hier entfernen, falls der Socket (Race Condition) den Alias zwischenzeitlich bereits
            // selbst aufgelöst hat. RemoveAlias gibt jetzt bool zurück — dort prüfen, ob der Socket
            // erfolgreich war, bevor wir hier eingreifen.

            return new HttpResult<SharedId>(
                Name,
                String.IsNullOrEmpty(newExchangeId) ? null : new SharedId(newExchangeId),
                null
            );
        }
    }
}