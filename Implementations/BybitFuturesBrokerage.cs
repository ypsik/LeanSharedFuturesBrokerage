using Bybit.Net.Clients;
using CryptoExchange.Net.Interfaces.Clients;
using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using HyperLiquid.Net;
using HyperLiquid.Net.Clients;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Generic;
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    public class BybitFuturesBrokerage : SharedFuturesBrokerage
    {
        BybitRestClient _restClient;
        BybitSocketClient _socketClient;
        BybitSocketClient _socketClientExData;

        private readonly object _fundingUpdateLock = new();
        private bool _fundingUpdateConnected = false;
        private UpdateSubscription _fundingUpdateSubscription;



        internal BybitFuturesBrokerage(
            IAlgorithm algorithm,
            BybitRestClient restClient,
            BybitSocketClient socketClient,
            IDataAggregator aggregator,
            Func<List<Holding>> getHoldingsFunc = null) // 🔥 Fix: Optional gemacht
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
            // 1. Instanzen schützen: Nur erstellen, wenn sie null sind
            if (_restClient == null)
            {
                // Falls wir im Live-Modus sind, brauchen wir die Keys aus dem Job
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

            // 13. Basisklasse synchronisieren
            // Wir nutzen die bestehenden (oder gerade erstellten) Instanzen
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

        public override void Connect()
        {
            lock (_fundingUpdateLock)
            {
                if (_fundingUpdateSubscription == null && _socketClient != null)
                {
                    _subRateGate.WaitToProceed();
                    var sub = RunSync(() =>
                        // Bei Bybit V5 werden Funding-Gebühren direkt im Wallet verbucht.
                        // Das Abonnement des Wallet-Streams informiert dich über jede Balance-Änderung (inkl. Funding).
                        _socketClient.V5PrivateApi.SubscribeToWalletUpdatesAsync(update =>
                        {
                            OnBalanceUpdated();
                        }));

                    SetupSubscriptionEvents(
                                    sub.Success,
                                    sub.Data,
                                    (state) => _fundingUpdateConnected = state,
                                    "Wallet updates",
                                    "Wallet updates subscription failed"
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
            // Nutzt die Bybit.Net CloseAsync Methode für das Subscription-Objekt
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
                    var now = DateTime.UtcNow;
                    var tickerData = data.Data;

                    onFundingRate(now, tickerData.FundingRate ?? 0);

                });
        }


        protected override async Task<ExchangeWebResult<SharedId>> ExecuteUpdateOrderAsync(Order order, decimal price, decimal quantity)
        {
            var ticker = NativeTicker(order.Symbol);

            // Bybit V5 benötigt für EditOrder primär die OrderId oder ClientOrderId. 
            // Side und OrderType sind bei der Änderung einer bestehenden Order nicht erforderlich.
            var res = await _restClient.V5Api.Trading.EditOrderAsync(
                          category: Bybit.Net.Enums.Category.Linear,
                          symbol: ticker,
                          orderId: order.BrokerId.Last(),
                          price: price,
                          quantity: Math.Abs(quantity));

            if (!res.Success)
            {
                Log.Error($"Bybit update error: {res.Error} | Ticker: {ticker} | Price: {price} | OriginalData: {res.OriginalData}");
                return new ExchangeWebResult<SharedId>(Name, res.Error);
            }

            return new ExchangeWebResult<SharedId>(
                    Name,
                    TradingMode.PerpetualLinear,
                    res.As(new SharedId(order.Id.ToString()))
                );
        }
    }
}
