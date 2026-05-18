using Bybit.Net.Clients;
using CryptoExchange.Net.Interfaces.Clients;
using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using CryptoExchange.Net.Trackers.UserData;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    public class BybitFuturesBrokerage : SharedFuturesBrokerage
    {
        private BybitRestClient _restClient;
        private BybitSocketClient _socketClient;
        private BybitSocketClient _socketClientExData;

        private readonly object _fundingUpdateLock = new();
        private bool _fundingUpdateConnected = false;
        private UpdateSubscription _fundingUpdateSubscription;

        internal BybitFuturesBrokerage(
            IAlgorithm algorithm,
            BybitRestClient restClient,
            BybitSocketClient socketClient,
            IDataAggregator aggregator,
            Func<List<Holding>> getHoldingsFunc = null)
            : base(algorithm, "bybit")
        {
            _restClient = restClient;
            _socketClient = socketClient;
            _socketClientExData = new BybitSocketClient();

            InitializeBase(
                restClient.V5Api.SharedClient,
                restClient.V5Api.SharedClient,
                socketClient.V5LinearApi.SharedClient,
                socketClient.V5PrivateApi.SharedClient,
                socketClient.V5LinearApi.SharedClient,
                socketClient.V5PrivateApi.SharedClient,
                socketClient.V5PrivateApi.SharedClient,
                restClient.V5Api.SharedClient,
                restClient.V5Api.SharedClient,
                aggregator,
                getHoldingsFunc);
        }

        protected override void InitializeFromJob(QuantConnect.Packets.LiveNodePacket job, IDataAggregator aggregator)
        {
            if (_restClient == null)
            {
                job.BrokerageData.TryGetValue("bybit-api-key", out var key);
                job.BrokerageData.TryGetValue("bybit-api-secret", out var secret);

                _restClient = new BybitRestClient(options => {
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret))
                        options.ApiCredentials = new Bybit.Net.BybitCredentials(key, secret);
                });
            }

            if (_socketClient == null)
            {
                _socketClient = new BybitSocketClient();
            }

            if (_socketClientExData == null)
            {
                _socketClientExData = new BybitSocketClient();
            }

            InitializeBase(
                _restClient.V5Api.SharedClient,
                _restClient.V5Api.SharedClient,
                _socketClient.V5LinearApi.SharedClient,
                _socketClient.V5PrivateApi.SharedClient,
                _socketClient.V5LinearApi.SharedClient,
                _socketClient.V5PrivateApi.SharedClient,
                _socketClient.V5PrivateApi.SharedClient,
                _restClient.V5Api.SharedClient,
                _restClient.V5Api.SharedClient,
                aggregator,
                _getHoldingsFunc
            );
        }

        #region Connect

        public override bool IsConnected => base.IsConnected && _fundingUpdateConnected;
        public override bool ExchangeModifiesOrdersInPlace => true;

        protected override ExchangeParameters OpenOrdersExchangeParameters
        {
            get
            {
                var parameters = new ExchangeParameters();
                parameters.AddValue(new ExchangeParameter("Bybit", "SettleAsset", SettleAsset));
                return parameters;
            }
        }
        protected override ExchangeParameters AccountHoldingsExchangeParameters
        {
            get
            {
                var parameters = new ExchangeParameters();
                parameters.AddValue(new ExchangeParameter("Bybit", "category", "linear"));
                parameters.AddValue(new ExchangeParameter("Bybit", "settleCoin", SettleAsset));
                parameters.AddValue(new ExchangeParameter("Bybit", "SettleAsset", SettleAsset));
                return parameters;
            }
        }

        protected override ExchangeParameters GetFundingRateHistoryParameters
        {
            get
            {
                var parameters = new ExchangeParameters();
                parameters.AddValue(new ExchangeParameter("Bybit", "category", "linear"));
                return parameters;
            }
        }

        public override void Connect()
        {
            lock (_fundingUpdateLock)
            {
                if (_fundingUpdateSubscription == null && _socketClient != null)
                {
                    _subRateGate.WaitToProceed();
                    var sub = RunSync(() =>
                        _socketClient.V5PrivateApi.SubscribeToWalletUpdatesAsync(update =>
                        {
                            OnBalanceUpdated();
                        }));

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
                    }
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

        protected override async Task<CallResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
           string nativeTicker, Symbol symbol, Func<DateTime, decimal, bool> onFundingRate)
        {
            return await _socketClientExData.V5LinearApi.SubscribeToTickerUpdatesAsync(
                nativeTicker, data =>
                {
                    var now = data.DataTime ?? data.ReceiveTime;
                    var tickerData = data.Data;

                    if(tickerData.FundingRate.HasValue)
                        onFundingRate(now, tickerData.FundingRate.Value);
                });
        }

        public override List<CashAmount> GetCashBalance()
        {
            if (Balance.HasValue)
                return new List<CashAmount> { new CashAmount(Balance.Value, SettleAsset) };

            var res = RunSync(() => _restClient.V5Api.Account.GetBalancesAsync(Bybit.Net.Enums.AccountType.Unified));
            var result = new List<CashAmount>
            {
                new CashAmount(res?.Data?.List?.FirstOrDefault()?.TotalMarginBalance ?? 0, SettleAsset)
            };
            return result;
        }

        protected override async Task<CallResult<UpdateSubscription>> ExecuteBalanceSubscriptionAsync(Action<List<CashAmount>> onUpdate)
        {
            return await _socketClient.V5PrivateApi.SubscribeToWalletUpdatesAsync(update =>
            {
                var wallet = update.Data.FirstOrDefault();
                if (wallet?.TotalMarginBalance.HasValue??false)
                {
                    onUpdate(
                        [
                            new CashAmount(wallet.TotalMarginBalance.Value, SettleAsset)
                        ]);
                }
            });
        }

        protected override async Task<ExchangeWebResult<SharedId>> ExecuteUpdateOrderAsync(Order order, string clientOrderId, decimal price, decimal quantity)
        {
            var ticker = NativeTicker(order.Symbol);

            var res = await _restClient.V5Api.Trading.EditOrderAsync(
                          category: Bybit.Net.Enums.Category.Linear,
                          symbol: ticker,
                          orderId: order.BrokerId.Last(),
                          clientOrderId: clientOrderId,
                          price: price,
                          quantity: Math.Abs(quantity));

            if (!res.Success)
            {
                Log.Error($"Bybit update error: {res.Error} | Ticker: {ticker} | Price: {price} | OriginalData: {res.OriginalData}");
                return new ExchangeWebResult<SharedId>(Name, res.Error);
            }

            // KORREKTUR: Bybit verändert die OrderId bei einem Modify NICHT. 
            // Daher wird hier die echte, bestätigte OrderId durchgereicht.
            return new ExchangeWebResult<SharedId>(
                    Name,
                    TradingMode.PerpetualLinear,
                    res.As(new SharedId(res.Data.OrderId.ToString()))
                );
        }
    }
}