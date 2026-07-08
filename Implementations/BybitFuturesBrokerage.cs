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
            Func<List<Holding>>? getHoldingsFunc = null)
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
                job.BrokerageData.TryGetValue("bybit-api-key", out var key);
                job.BrokerageData.TryGetValue("bybit-api-secret", out var secret);
                _socketClient = new BybitSocketClient(options =>
                {
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret))
                        options.ApiCredentials = new Bybit.Net.BybitCredentials(key, secret);
                });
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
                _restClient.V5Api.SharedClient,
                _restClient.V5Api.SharedClient,
                aggregator,
                _getHoldingsFunc
            );
        }

        #region Connect

        public override bool IsConnected => base.IsConnected && _fundingUpdateConnected;
        public override bool ExchangeModifiesOrdersInPlace => true;

        protected override int? FundingRolloverHours => null;


        protected override ExchangeParameters OpenOrdersExchangeParameters
        {
            get
            {
                var parameters = base.OpenOrdersExchangeParameters;
                parameters.AddValue(new ExchangeParameter("Bybit", "SettleAsset", SettleAsset));
                return parameters;
            }
        }
        protected override ExchangeParameters AccountHoldingsExchangeParameters
        {
            get
            {
                var parameters = base.AccountHoldingsExchangeParameters;
                parameters.AddValue(new ExchangeParameter("Bybit", "category", "linear"));
                parameters.AddValue(new ExchangeParameter("Bybit", "settleCoin", SettleAsset));
                return parameters;
            }
        }

        protected override ExchangeParameters GetFundingRateHistoryParameters
        {
            get
            {
                var parameters = base.GetFundingRateHistoryParameters;
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
                        _socketClient.V5PrivateApi.SubscribeToUserTradeUpdatesAsync(update =>
                        {
                            foreach (var fundingsRecord in update.Data.Where(f => f?.TradeType != null && f.TradeType == Bybit.Net.Enums.TradeType.Funding))
                            {
                                if (_algorithm?.Portfolio?.CashBook != null)
                                {
                                    var fundings = -fundingsRecord.Fee ?? 0m;
                                    _algorithm.Portfolio.CashBook[SettleAsset].AddAmount(fundings);
                                    OnMessage(new FundingBrokerageMessageEvent(fundingsRecord.FeeAsset??SettleAsset, fundings));
                                }
                            }
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

        protected override async Task<WebSocketResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
           string nativeTicker, Symbol symbol, Func<DateTime, decimal?, DateTime?,  bool> onFundingRate)
        {
            return await _socketClientExData.V5LinearApi.SubscribeToTickerUpdatesAsync(
                nativeTicker, data =>
                {
                    var now = data.DataTime ?? data.ReceiveTime;
                    var tickerData = data.Data;

                    // Wir reichen die FundingRate direkt durch, auch wenn sie null ist.
                    onFundingRate(now, tickerData.FundingRate, tickerData.NextFundingTime);
                });
        }

        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() => _restClient.V5Api.Account.GetBalancesAsync(Bybit.Net.Enums.AccountType.Unified));
            var result = new List<CashAmount>
            {
                new((res?.Data?.List?.FirstOrDefault()?.TotalMarginBalance ?? 0) - (res?.Data?.List?.FirstOrDefault()?.TotalPerpUnrealizedPnl ?? 0), SettleAsset)
            };
            return result;
        }

        protected override async Task<HttpResult<SharedId>> ExecuteUpdateOrderAsync(Order order, decimal price, decimal? quantity)
        {
            var ticker = NativeTicker(order.Symbol);

            var res = await _restClient.V5Api.Trading.EditOrderAsync(
                          category: Bybit.Net.Enums.Category.Linear,
                          symbol: ticker,
                          orderId: order.BrokerId.Last(),
                          price: price,
                          quantity: quantity.HasValue ? Math.Abs(quantity.Value) : null);

            if (!res.Success)
            {
                Log.Error($"Update error: {res.Error} | Ticker: {ticker} | Price: {price} | OriginalData: {res.OriginalData}");
                return new HttpResult<SharedId>(Name, null, res.Error);
            }

            // KORREKTUR: Bybit verändert die OrderId bei einem Modify NICHT. 
            // Daher wird hier die echte, bestätigte OrderId durchgereicht.
            return new HttpResult<SharedId>(
                    Name,
                    new SharedId(res.Data.OrderId.ToString()),
                    null
                );
        }
    }
}