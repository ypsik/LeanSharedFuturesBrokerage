using Aster.Net;
using Aster.Net.Clients;
using Aster.Net.Enums;
using Aster.Net.Objects;
using CryptoExchange.Net.Interfaces.Clients;
using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Securities;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    public class AsterFuturesBrokerage : SharedFuturesBrokerage
    {
        private AsterRestClient _restClient;
        private AsterSocketClient _socketClient;
        private AsterSocketClient _socketClientExData;

        public override bool ExchangeSupportsUserTradeStream => false;

        private readonly object _fundingUpdateLock = new();
        private bool _fundingUpdateConnected = false;
        private UpdateSubscription _fundingUpdateSubscription;
        private CancellationTokenSource _fundingCts;
        private CancellationTokenSource? _userStreamCts;

        public override bool IsConnected => base.IsConnected && _fundingUpdateConnected;
        protected override SharedPositionSide? SharedPositionSide => CryptoExchange.Net.SharedApis.SharedPositionSide.Long;


        // 1. LEAN DataQueueHandler Konstruktor (Bybit-Style)
        public AsterFuturesBrokerage() : base("aster")
        {
        }

        // 2. Trading-Instanz Konstruktor (für die Factory)
        internal AsterFuturesBrokerage(IAlgorithm algorithm,
            AsterRestClient restClient,
            AsterSocketClient socketClient,
            IDataAggregator aggregator,
            Func<List<Holding>>? getHoldingsFunc = null)
            : base(algorithm, "aster")
        {
            _restClient = restClient;
            _socketClient = socketClient;
            _socketClientExData = new AsterSocketClient();

            PopulateSPDB();

            InitializeBase(
                restClient.FuturesV3Api.SharedClient,
                restClient.FuturesV3Api.SharedClient,
                socketClient.FuturesV3Api.SharedClient,
                socketClient.FuturesV3Api.SharedClient,
                _socketClient.FuturesV3Api.SharedClient,
                null,
                restClient.FuturesV3Api.SharedClient,
                restClient.FuturesV3Api.SharedClient,
                aggregator,
                getHoldingsFunc);
        }

        private void PopulateSPDB()
        {
            var result = RunSync(() => _restClient.FuturesV3Api.ExchangeData.GetExchangeInfoAsync());

            if (!result.Success)
                throw new Exception($"Failed to load Aster assets: {result.Error}");

            foreach (var symbol in result.Data.Symbols.Where(s => s.Status == Aster.Net.Enums.SymbolStatus.Trading))
            {
                var tickSize = symbol.PriceFilter?.TickSize
                    ?? (decimal)Math.Pow(10, -symbol.PricePrecision);

                var lotSize = symbol.LotSizeFilter?.MinQuantity
                    ?? (decimal)Math.Pow(10, -symbol.QuantityPrecision);

                var symbolProperties = new SymbolProperties(
                    description: $"Aster {symbol.BaseAsset} Perpetual",
                    quoteCurrency: symbol.QuoteAsset,
                    contractMultiplier: 1m,
                    minimumPriceVariation: tickSize,
                    lotSize: lotSize,
                    marketTicker: symbol.Name
                );

                _spdb.SetEntry("aster", symbol.BaseAsset + symbol.QuoteAsset, SecurityType.CryptoFuture, symbolProperties);
            }
        }

        protected override void InitializeFromJob(QuantConnect.Packets.LiveNodePacket job, IDataAggregator aggregator)
        {
            if (_restClient == null)
            {
                var publicAddress = job.BrokerageData.GetValueOrDefault("aster-public-address", "");
                var key = job.BrokerageData.GetValueOrDefault("aster-api-key", "");
                var secret = job.BrokerageData.GetValueOrDefault("aster-api-secret", "");

                _restClient = new AsterRestClient(options => {
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret))
                        options.ApiCredentials = new AsterCredentials(new AsterV3Credential(publicAddress, key, secret));
                });
            }

            if (_socketClient == null)
            {
                var publicAddress = job.BrokerageData.GetValueOrDefault("aster-public-address", "");
                var key = job.BrokerageData.GetValueOrDefault("aster-api-key", "");
                var secret = job.BrokerageData.GetValueOrDefault("aster-api-secret", "");

                _socketClient = new AsterSocketClient(options => {
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret))
                        options.ApiCredentials = new AsterCredentials(new AsterV3Credential(publicAddress, key, secret));
                });
            }

            if (_socketClientExData == null)
            {
                _socketClientExData = new AsterSocketClient();
            }

            InitializeBase(
                _restClient.FuturesV3Api.SharedClient,
                _restClient.FuturesV3Api.SharedClient,
                _socketClient.FuturesV3Api.SharedClient,
                _socketClient.FuturesV3Api.SharedClient,
                _socketClient.FuturesV3Api.SharedClient,
                null,
                _restClient.FuturesV3Api.SharedClient,
                _restClient.FuturesV3Api.SharedClient,
                aggregator
            );
        }

        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() => _restClient.FuturesV3Api.Account.GetAccountInfoAsync());
            var result = new List<CashAmount>
            {
                new((res?.Data?.TotalMarginBalance ?? 0) - (res?.Data?.TotalCrossUnrealizedPnl ?? 0), SettleAsset)
            };
            return result;
        }

        protected override async Task<CallResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
            string nativeTicker, Symbol symbol, Func<DateTime, decimal?, DateTime?, bool> onFundingRate)
        {
            return await _socketClientExData.FuturesV3Api.SubscribeToMarkPriceUpdatesAsync(
                nativeTicker, null, data =>
                {
                    var now = data.DataTime ?? data.ReceiveTime;
                    onFundingRate(now, data.Data.FundingRate, data.Data.NextFundingTime);
                });
        }

        #region Connect

        public override void Connect()
        {
            _fundingCts = new CancellationTokenSource();
            lock (_fundingUpdateLock)
            {
                EstablishUserStreamSubscription();
            }
            base.Connect();
        }

        private void EstablishUserStreamSubscription()
        {
            if (_fundingUpdateSubscription != null || _socketClient == null)
                return;

            _subRateGate.WaitToProceed();

            _userStreamCts = new CancellationTokenSource();

            // 1. ListenKey via REST holen
            var listenKeyResult = RunSync(() => _restClient.FuturesV3Api.Account.StartUserStreamAsync(_userStreamCts.Token));

            if (!listenKeyResult.Success)
            {
                Log.Error($"Aster: Failed to create UserStream: {listenKeyResult.Error}");
                return;
            }

            ListenKey = listenKeyResult.Data;

            // 2. WebSocket mit ListenKey subscriben
            DateTime connectTime = StartTime;
            var sub = RunSync(() =>
                _socketClient.FuturesV3Api.SubscribeToUserDataUpdatesAsync(
                    ListenKey,
                    onAccountUpdate: update =>
                    {
                        if (update?.Data == null) return;
                        if (update.Data.UpdateData.Reason != AccountUpdateReason.FundingFee) return;

                        foreach (var balance in update.Data.UpdateData.Balances
                            .Where(b => b != null && b.BalanceChange != 0))
                        {
                            if (_algorithm?.Portfolio?.CashBook != null)
                            {
                                _algorithm.Portfolio.CashBook[SettleAsset].AddAmount(balance.BalanceChange);
                                OnMessage(new FundingBrokerageMessageEvent(SettleAsset, balance.BalanceChange));
                            }
                        }

                        var eventTime = update.Data.TransactionTime;
                        if (eventTime > connectTime)
                            connectTime = eventTime;
                    },
                    onListenKeyExpired: _ =>
                    {
                        Log.Trace("Aster: ListenKey expired! Initiating reconnect...");
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

                // 3. Keep-alive Loop starten
                StartListenKeyKeepAliveLoop(ListenKey, _userStreamCts.Token);
            }
        }

        private void ReconnectUserStream()
        {
            lock (_fundingUpdateLock)
            {
                try
                {
                    _userStreamCts?.Cancel();
                    _userStreamCts?.Dispose();
                    _userStreamCts = null;

                    if (_fundingUpdateSubscription != null && _socketClient != null)
                    {
                        RunSync(() => _socketClient.UnsubscribeAsync(_fundingUpdateSubscription));
                        _fundingUpdateSubscription = null;
                    }
                }
                catch (Exception ex)
                {
                    Log.Error($"Aster: Error during unsubscribe while reconnecting: {ex.Message}");
                }

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

                        var pingResult = await _restClient.FuturesV3Api.Account.KeepAliveUserStreamAsync(listenKey, ct);
                        if (!pingResult.Success)
                            Log.Error($"Aster: Keep-alive for ListenKey failed: {pingResult.Error}");
                    }
                    catch (TaskCanceledException) { break; }
                    catch (Exception ex)
                    {
                        Log.Error($"Aster: Error in ListenKey keep-alive loop: {ex.Message}");
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
    }
}