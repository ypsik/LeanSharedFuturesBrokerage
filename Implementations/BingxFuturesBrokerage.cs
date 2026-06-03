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
        private UpdateSubscription _fundingUpdateSubscription;
        private CancellationTokenSource _fundingCts;

        public override bool ExchangeSupportsUserTradeStream => false;

        internal BingxFuturesBrokerage(
            IAlgorithm algorithm,
            BingXRestClient restClient,
            BingXSocketClient socketClient,
            IDataAggregator aggregator,
            Func<List<Holding>>? getHoldingsFunc = null)
            : base(algorithm, "bybit")
        {
            _restClient = restClient;
            _socketClient = socketClient;
            _socketClientExData = new BingXSocketClient();

            InitializeBase(
                restClient.PerpetualFuturesApi.SharedClient,
                restClient.PerpetualFuturesApi.SharedClient,
                socketClient.PerpetualFuturesApi.SharedClient,
                socketClient.PerpetualFuturesApi.SharedClient,
                socketClient.PerpetualFuturesApi.SharedClient,
                null,//socketClient.PerpetualFuturesApi.SharedClient,
                restClient.PerpetualFuturesApi.SharedClient,
                restClient.PerpetualFuturesApi.SharedClient,
                aggregator,
                getHoldingsFunc);
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
                null,//socketClient.PerpetualFuturesApi.SharedClient,
                _restClient.PerpetualFuturesApi.SharedClient,
                _restClient.PerpetualFuturesApi.SharedClient,
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
                var parameters = base.PlaceFuturesOrderExchangeParameters;
                return parameters;
            }
        }
        protected override ExchangeParameters AccountHoldingsExchangeParameters
        {
            get
            {
                var parameters = base.PlaceFuturesOrderExchangeParameters;
                return parameters;
            }
        }

        protected override ExchangeParameters GetFundingRateHistoryParameters
        {
            get
            {
                var parameters = base.PlaceFuturesOrderExchangeParameters;
                return parameters;
            }
        }

        public override void Connect()
        {
            _fundingCts = new CancellationTokenSource();
            lock (_fundingUpdateLock)
            {
                /*
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
                                    OnMessage(new FundingBrokerageMessageEvent(fundingsRecord.FeeAsset ?? SettleAsset, fundings));
                                }
                            }
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
                */
                base.Connect();
            }
        }

        public override void Disconnect()
        {
            _fundingCts?.Cancel();
            _fundingCts?.Dispose();
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
                new CashAmount((balance?.Balance ?? 0) - (balance?.UnrealizedProfit ?? 0), SettleAsset)
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


        protected override async Task<ExchangeWebResult<SharedId>> ExecuteUpdateOrderAsync(Order order, decimal price, decimal? quantity)
        {
            var ticker = NativeTicker(order.Symbol);
            /*
            var res = await _restClient.PerpetualFuturesApi.Trading.(
                          symbol: ticker,
                          orderId: order.BrokerId.Last(),
                          price: price,
                          quantity: Math.Abs(quantity));

            if (!res.Success)
            {
                Log.Error($"Bingx update error: {res.Error} | Ticker: {ticker} | Price: {price} | OriginalData: {res.OriginalData}");
                return new ExchangeWebResult<SharedId>(Name, res.Error);
            }

            // KORREKTUR: Bybit verändert die OrderId bei einem Modify NICHT. 
            // Daher wird hier die echte, bestätigte OrderId durchgereicht.
            return new ExchangeWebResult<SharedId>(
                    Name,
                    TradingMode.PerpetualLinear,
                    res.As(new SharedId(res.Data.OrderId.ToString()))
                );
            */
            throw new NotImplementedException();

        }
    }
}