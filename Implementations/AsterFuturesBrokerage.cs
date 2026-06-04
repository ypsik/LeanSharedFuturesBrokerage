using Aster.Net.Clients;
using Aster.Net.Enums;
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
        public override bool IsConnected => base.IsConnected && _fundingUpdateConnected;

        // 1. LEAN DataQueueHandler Konstruktor (Bybit-Style)
        public AsterFuturesBrokerage() : base("aster")
        {
        }

        // 2. Trading-Instanz Konstruktor (für die Factory)
        internal AsterFuturesBrokerage(IAlgorithm algorithm,
            AsterRestClient restClient,
            AsterSocketClient socketClient,
            IDataAggregator aggregator,
            Func<List<Holding>> getHoldingsFunc = null)
            : base(algorithm, "aster")
        {
            _restClient = restClient;
            _socketClient = socketClient;
            _socketClientExData = new AsterSocketClient();

            PopulateSPDB();

            InitializeBase(
                restClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                null,
                restClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                aggregator,
                getHoldingsFunc);
        }

        private void PopulateSPDB()
        {
            var result = RunSync(() => _restClient.FuturesApi.ExchangeData.GetExchangeInfoAsync());

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
            var apiKey = job.BrokerageData.GetValueOrDefault("aster-api-key", "");
            var apiSecret = job.BrokerageData.GetValueOrDefault("aster-api-secret", "");

            _restClient = new AsterRestClient(); // Hier ggf. Credentials setzen
            _socketClient = new AsterSocketClient();
            _socketClientExData = new AsterSocketClient();

            InitializeBase(
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                null,
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
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
            string nativeTicker, Symbol symbol, Func<DateTime, decimal?, bool> onFundingRate)
        {
            return await _socketClientExData.FuturesV3Api.SubscribeToMarkPriceUpdatesAsync(
                nativeTicker, null, data =>
                {
                    var now = data.DataTime ?? data.ReceiveTime;

                    onFundingRate(now, data.Data.FundingRate);
                });
        }

        public override void Connect()
        {
            lock (_fundingUpdateLock)
            {
                if (_fundingUpdateSubscription == null && _socketClient != null)
                {
                    _subRateGate.WaitToProceed();
                    DateTime connectTime = StartTime;

                    var listenKey = RunSync(() => _restClient.FuturesApi.Account.StartUserStreamAsync());
                    if (!listenKey.Success)
                        throw new Exception($"Failed to start Aster user stream: {listenKey.Error}");

                    var sub = RunSync(() =>
                        _socketClient.FuturesApi.SubscribeToUserDataUpdatesAsync(
                            listenKey.Data,
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
                        _fundingUpdateSubscription = sub.Data;
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
    }
}