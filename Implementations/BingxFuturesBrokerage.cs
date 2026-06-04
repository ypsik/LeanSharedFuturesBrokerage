using BingX.Net.Clients;
using BingX.Net.Interfaces.Clients;
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
        private CancellationTokenSource _fundingCts;
        private CancellationTokenSource? _userStreamCts;

        public override bool ExchangeSupportsUserTradeStream => false;

        internal BingxFuturesBrokerage(
            IAlgorithm algorithm,
            BingXRestClient restClient,
            BingXSocketClient socketClient,
            IDataAggregator aggregator,
            Func<List<Holding>>? getHoldingsFunc = null)
            : base(algorithm, "bingx")
        {
            _restClient = restClient;
            _socketClient = socketClient;
            _socketClientExData = new BingXSocketClient();

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

                var lotSize = contract.MinOrderQuantity;

                var symbolProperties = new SymbolProperties(
                    description: $"BingX {contract.Asset} Perpetual",
                    quoteCurrency: contract.Currency,
                    contractMultiplier: 1m,
                    minimumPriceVariation: tickSize,
                    lotSize: lotSize,
                    marketTicker: contract.Symbol
                );

                _spdb.SetEntry("bingx", ticker, SecurityType.CryptoFuture, symbolProperties);
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
                _socketClient = new BingXSocketClient();
            }

            if (_socketClientExData == null)
            {
                _socketClientExData = new BingXSocketClient();
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
        public override bool ExchangeModifiesOrdersInPlace => true;

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
            _fundingCts = new CancellationTokenSource();
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

            // Isolated token source specifically for the ListenKey lifecycle and keep-alive loop
            _userStreamCts = new CancellationTokenSource();

            // 1. Request ListenKey via REST-API
            var listenKeyResult = RunSync(() => _restClient.PerpetualFuturesApi.Account.StartUserStreamAsync(_userStreamCts.Token));

            if (!listenKeyResult.Success)
            {
                Log.Error($"BingX: Failed to create UserStream: {listenKeyResult.Error}");
                return;
            }

            ListenKey = listenKeyResult.Data;

            // 2. Subscribe to WebSocket with the generated listenKey and handle expiration
            var sub = RunSync(() =>
                _socketClient.PerpetualFuturesApi.SubscribeToUserDataUpdatesAsync(
                    ListenKey,
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
                    onOrderUpdate: null,
                    onConfigurationUpdate: null,
                    onListenKeyExpiredUpdate: expiredEvent =>
                    {
                        Log.Trace("BingX: ListenKey expired! Initiating reconnect...");

                        // Trigger reconnect logic asynchronously outside the lock to prevent deadlocks
                        Task.Run(() => ReconnectUserStream());
                    },
                    ct: _userStreamCts.Token));

            SetupSubscriptionEvents(
                sub.Success,
                sub.Data,
                (state) => _fundingUpdateConnected = state,
                "Wallet updates",
                "Wallet updates subscription failed",
                sub.Error?.ToString()
            );

            if (sub.Success)
            {
                _fundingUpdateSubscription = sub.Data;

                // 3. Start background keep-alive loop using the dedicated token
                StartListenKeyKeepAliveLoop(ListenKey, _userStreamCts.Token);
            }
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
                    // Cancel old token (safely stops the previous keep-alive loop only)
                    _userStreamCts?.Cancel();
                    _userStreamCts?.Dispose();
                    _userStreamCts = null;

                    // Unsubscribe old session if exists
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

                // ONLY rebuild the specific user stream, leaving other Connect() logic completely alone
                EstablishUserStreamSubscription();
            }
        }

        private void StartListenKeyKeepAliveLoop(string listenKey, CancellationToken ct)
        {
            Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(TimeSpan.FromMinutes(45), ct);

                        if (ct.IsCancellationRequested) break;

                        var pingResult = await _restClient.PerpetualFuturesApi.Account.KeepAliveUserStreamAsync(listenKey, ct);
                        if (!pingResult.Success)
                        {
                            Log.Error($"BingX: Keep-alive for ListenKey failed: {pingResult.Error}");
                        }
                    }
                    catch (TaskCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"BingX: Error in ListenKey keep-alive loop: {ex.Message}");
                    }
                }
            }, ct);
        }

        public override void Disconnect()
        {
            _fundingCts?.Cancel();
            _fundingCts?.Dispose();
            _userStreamCts?.Cancel();
            _userStreamCts?.Dispose();
            RunSync(() => _fundingUpdateSubscription?.CloseAsync() ?? Task.CompletedTask);
            _socketClientExData?.Dispose();
            base.Disconnect();
        }
        #endregion


        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() => _restClient.PerpetualFuturesApi.Account.GetBalancesAsync());
            var balance = res?.Data?.FirstOrDefault();
            var result = new List<CashAmount>
            {
                new CashAmount(balance?.Balance ?? 0, SettleAsset)
            };
            return result;
        }

        protected override bool SubscribeFunding(Symbol symbol)
        {
            var nativeTicker = NativeTicker(symbol);

            _ = Task.Run(async () =>
            {
                DateTime? nextFundingTime = null;

                while (!_fundingCts.Token.IsCancellationRequested)
                {
                    Log.Trace($"{Name} Funding poll initialization for {nativeTicker}");
                    try
                    {
                        // 1. Initialer Abruf der NextFundingTime außerhalb oder falls der State verloren ging
                        if (nextFundingTime == null)
                        {
                            var initResult = await _restClient.PerpetualFuturesApi.ExchangeData
                                .GetFundingRateAsync(nativeTicker, _fundingCts.Token);

                            if (!initResult.Success)
                            {
                                Log.Error($"{Name} SubscribeFunding initial fetch failed for {nativeTicker}: {initResult.Error}");
                                await Task.Delay(TimeSpan.FromMinutes(1), _fundingCts.Token);
                                continue;
                            }

                            nextFundingTime = initResult.Data.NextFundingTime;
                        }

                        // 2. Berechnen und Warten bis zum Settlement
                        var delay = nextFundingTime.Value - DateTime.UtcNow;

                        if (delay > TimeSpan.Zero)
                        {
                            await Task.Delay(delay.Add(TimeSpan.FromTicks(100)), _fundingCts.Token);
                        }
                        else
                        {
                            // Fallback falls die Zeit in der Vergangenheit liegt / System-Uhr-Abweichungen
                            await Task.Delay(TimeSpan.FromTicks(100), _fundingCts.Token);
                        }

                        // 3. Nach dem Settlement: Einzigen Call ausführen, um die neue Rate + die NÄCHSTE FundingTime zu holen
                        var rateResult = await _restClient.PerpetualFuturesApi.ExchangeData
                            .GetFundingRateAsync(nativeTicker, _fundingCts.Token);

                        if (rateResult.Success)
                        {
                            var roundedTime = new DateTime(
                                nextFundingTime.Value.Year, nextFundingTime.Value.Month, nextFundingTime.Value.Day,
                                nextFundingTime.Value.Hour, 0, 0, DateTimeKind.Utc);

                            _aggregator.Update(new MarginInterestRate
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
                            await Task.Delay(TimeSpan.FromMinutes(1), _fundingCts.Token);
                        }
                    }
                    catch (OperationCanceledException) { break; }
                    catch (Exception ex)
                    {
                        Log.Error($"{Name} SubscribeFunding exception for {nativeTicker}: {ex.Message}");
                        nextFundingTime = null; // Sicherhaltshalber zurücksetzen
                        await Task.Delay(TimeSpan.FromMinutes(1), _fundingCts.Token);
                    }
                }
            }, _fundingCts.Token);

            return true;
        }
    }
}