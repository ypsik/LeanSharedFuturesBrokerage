
using Bitfinex.Net.Clients;
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
using RestSharp;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    public class BitfinexFuturesBrokerage : SharedFuturesBrokerage
    {
        private BitfinexRestClient _restClient;
        private BitfinexSocketClient _socketClient;
        private BitfinexSocketClient _socketClientExData;

        private readonly object _fundingUpdateLock = new();
        private bool _fundingUpdateConnected = false;
        private UpdateSubscription _fundingUpdateSubscription;

        internal BitfinexFuturesBrokerage(
            IAlgorithm algorithm,
            BitfinexRestClient restClient,
            BitfinexSocketClient socketClient,
            IDataAggregator aggregator,
            Func<List<Holding>>? getHoldingsFunc = null)
            : base(algorithm, "bitfinex")
        {
            _restClient = restClient;
            _socketClient = socketClient;
            _socketClientExData = new BitfinexSocketClient();

            InitializeBase(
//                restClient.SpotApi.SharedClient,
                null,
                restClient.SpotApi.SharedClient,
                socketClient.SpotApi.SharedClient,
                null,
//                socketClient.SpotApi.SharedClient,
                socketClient.SpotApi.SharedClient,
                socketClient.SpotApi.SharedClient,
                null,
//                restClient.SpotApi.SharedClient,
                restClient.SpotApi.SharedClient,
                aggregator,
                getHoldingsFunc);
        }

        protected override void InitializeFromJob(QuantConnect.Packets.LiveNodePacket job, IDataAggregator aggregator)
        {
            if (_restClient == null)
            {
                job.BrokerageData.TryGetValue("bitfinex-api-key", out var key);
                job.BrokerageData.TryGetValue("bitfinex-api-secret", out var secret);

                _restClient = new BitfinexRestClient(options => {
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret))
                        options.ApiCredentials = new Bitfinex.Net.BitfinexCredentials(key, secret);
                });
            }

            if (_socketClient == null)
            {
                _socketClient = new BitfinexSocketClient();
            }

            if (_socketClientExData == null)
            {
                _socketClientExData = new BitfinexSocketClient();
            }

            InitializeBase(
                null,
//                _restClient.SpotApi.SharedClient,
                _restClient.SpotApi.SharedClient,
                _socketClient.SpotApi.SharedClient,
              null,
//                _socketClient.SpotApi.SharedClient,
                _socketClient.SpotApi.SharedClient,
                _socketClient.SpotApi.SharedClient,
                null,
//                _restClient.SpotApi.SharedClient,
                _restClient.SpotApi.SharedClient,
                aggregator,
                _getHoldingsFunc
            );
        }

        #region Connect

        public override bool IsConnected => base.IsConnected && _fundingUpdateConnected;
        public override bool ExchangeModifiesOrdersInPlace => true;

        public override void Connect()
        {
            lock (_fundingUpdateLock)
            {
                if (_fundingUpdateSubscription == null && _socketClient != null)
                {
                    _subRateGate.WaitToProceed();
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
           string nativeTicker, Symbol symbol, Func<DateTime, decimal?, bool> onFundingRate)
        {
            return null;
        }

        public override List<CashAmount> GetCashBalance()
        {
            return new List<CashAmount> { new CashAmount(0m, SettleAsset) };
        }

        protected override async Task<ExchangeWebResult<SharedId>> ExecuteUpdateOrderAsync(Order order, decimal price, decimal? quantity)
        {
            var ticker = NativeTicker(order.Symbol);

            return null;

        }
    }
}