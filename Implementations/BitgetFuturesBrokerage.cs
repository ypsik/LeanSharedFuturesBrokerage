using Bitget.Net;
using Bitget.Net.Clients;
using Bitget.Net.Enums;
using Bybit.Net.Clients;
using CryptoExchange.Net.Interfaces.Clients;
using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using CryptoExchange.Net.Trackers.UserData;
using NSec.Cryptography;
using QuantConnect;
using QuantConnect.Algorithm.Framework.Selection;
using QuantConnect.Brokerages;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;
using RestSharp;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using SilverQuant.Lean.Brokerages.Futures.Shared.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    public class BitgetFuturesBrokerage : SharedFuturesBrokerage
    {
        private BitgetRestClient _restClient;
        private BitgetSocketClient _socketClient;
        private BitgetSocketClient _socketClientExData;
        private bool _fundingUpdateConnected = false;

        internal BitgetFuturesBrokerage(
            IAlgorithm algorithm,
            BitgetRestClient restClient,
            BitgetSocketClient socketClient,
            IDataAggregator aggregator,
            Func<List<Holding>>? getHoldingsFunc = null)
            : base(algorithm, "bitget")
        {
            _restClient = restClient;
            _socketClient = socketClient;
            _socketClientExData = new BitgetSocketClient();

            PopulateSPDB();

            InitializeBase(
                restClient.FuturesApiV2.SharedClient,
                restClient.FuturesApiV2.SharedClient,
                socketClient.FuturesApiV2.SharedClient,
                socketClient.FuturesApiV2.SharedClient,
                socketClient.FuturesApiV2.SharedClient,
                socketClient.FuturesApiV2.SharedClient,
                socketClient.FuturesApiV2.SharedClient,
                restClient.FuturesApiV2.SharedClient,
                restClient.FuturesApiV2.SharedClient,
                aggregator,
                getHoldingsFunc);
        }

        protected override void InitializeFromJob(QuantConnect.Packets.LiveNodePacket job, IDataAggregator aggregator)
        {
            if (_restClient == null)
            {
                job.BrokerageData.TryGetValue("bitget-api-key", out var key);
                job.BrokerageData.TryGetValue("bitget-api-secret", out var secret);
                job.BrokerageData.TryGetValue("bitget-api-pass", out var pass);

                _restClient = new BitgetRestClient(options => {
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret))
                        options.ApiCredentials = new BitgetCredentials(key: key, secret: secret, pass: pass);
                });
            }

            if (_socketClient == null)
            {

                job.BrokerageData.TryGetValue("bitget-api-key", out var key);
                job.BrokerageData.TryGetValue("bitget-api-secret", out var secret);
                job.BrokerageData.TryGetValue("bitget-api-pass", out var pass);

                var socketClient = new BitgetSocketClient(options =>
                {
                    options.ApiCredentials = new BitgetCredentials(key: key, secret: secret, pass: pass);
                    options.DelayAfterConnect = TimeSpan.FromMilliseconds(500);
                    options.SocketIndividualSubscriptionCombineTarget = 50;
                });

                _socketClient = new BitgetSocketClient();
            }

            if (_socketClientExData == null)
            {
                _socketClientExData = new BitgetSocketClient();
            }

            InitializeBase(
                _restClient.FuturesApiV2.SharedClient,
                _restClient.FuturesApiV2.SharedClient,
                _socketClient.FuturesApiV2.SharedClient,
                _socketClient.FuturesApiV2.SharedClient,
                _socketClient.FuturesApiV2.SharedClient,
                _socketClient.FuturesApiV2.SharedClient,
                _socketClient.FuturesApiV2.SharedClient,
                _restClient.FuturesApiV2.SharedClient,
                _restClient.FuturesApiV2.SharedClient,
                aggregator,
                _getHoldingsFunc
            );
        }

        private void PopulateSPDB()
        {
            var result = RunSync(() => _restClient.FuturesApiV2.ExchangeData
                 .GetContractsAsync(Bitget.Net.Enums.BitgetProductTypeV2.UsdtFutures));

            if (!result.Success)
                throw new Exception($"Failed to load Bitget assets: {result.Error}");

            foreach (var contract in result.Data.Where(c => c.Status == Bitget.Net.Enums.V2.FuturesSymbolStatus.Normal))
            {
                var ticker = contract.Symbol;

                // Mindestschrittweite des Preises aus den echten PriceDecimals berechnen
                var tickSize = (decimal)Math.Pow(10, -contract.PriceDecimals);

                // LotSize ist laut deiner Klasse MinOrderQuantity
                var lotSize = contract.MinOrderQuantity;

                var symbolProperties = new SymbolProperties(
                    description: $"Bitget {contract.BaseAsset} Perpetual",
                    quoteCurrency: SettleAsset,
                    contractMultiplier: 1m, // Bitget USDT-Futures rechnen standardmäßig in Base-Asset Units (1)
                    minimumPriceVariation: tickSize,
                    lotSize: lotSize,
                    marketTicker: contract.Symbol
                );

                _spdb.SetEntry("bitget", ticker, SecurityType.CryptoFuture, symbolProperties);
            }
        }

        public override decimal MinimumOrderNotionalValue => 5m;

        protected override ExchangeParameters PlaceFuturesOrderExchangeParameters
        {
            get
            {
                var parameters = base.PlaceFuturesOrderExchangeParameters;
                parameters.AddValue(new ExchangeParameter("Bitget", "ProductType", "UsdtFutures"));
                parameters.AddValue(new ExchangeParameter("Bitget", "MarginAsset", SettleAsset));
                return parameters;
            }
        }
        protected override ExchangeParameters CancelFuturesOrderExchangeParameters
        {
            get
            {
                var parameters = base.CancelFuturesOrderExchangeParameters;
                parameters.AddValue(new ExchangeParameter("Bitget", "ProductType", "UsdtFutures"));
                return parameters;
            }
        }

        protected override ExchangeParameters OpenOrdersExchangeParameters
        {
            get
            {
                var parameters = base.OpenOrdersExchangeParameters;
                parameters.AddValue(new ExchangeParameter("Bitget", "ProductType", "UsdtFutures"));
                return parameters;
            }
        }
        
        protected override ExchangeParameters AccountHoldingsExchangeParameters
        {
            get
            {
                var parameters = base.AccountHoldingsExchangeParameters;
                parameters.AddValue(new ExchangeParameter("Bitget", "ProductType", "UsdtFutures"));
                parameters.AddValue(new ExchangeParameter("Bitget", "MarginAsset", SettleAsset));
                return parameters;
            }
        }
        
        protected override ExchangeParameters OrderUpdatesExchangeParameters
        {
            get
            {
                var parameters = base.OrderUpdatesExchangeParameters;
                parameters.AddValue(new ExchangeParameter("Bitget", "ProductType", "UsdtFutures"));
                return parameters;
            }
        }

        protected override ExchangeParameters UserTradesExchangeParameters
        {
            get
            {
                var parameters = base.UserTradesExchangeParameters;
                parameters.AddValue(new ExchangeParameter("Bitget", "ProductType", "UsdtFutures"));
                return parameters;
            }
        }

        protected override ExchangeParameters GetKlinesHistoryParameters
        {
            get
            {
                var parameters = base.GetKlinesHistoryParameters;
                parameters.AddValue(new ExchangeParameter("Bitget", "ProductType", "UsdtFutures"));
                return parameters;
            }
        }

        protected override ExchangeParameters GetFundingRateHistoryParameters
        {
            get
            {
                var parameters = base.GetFundingRateHistoryParameters;
                parameters.AddValue(new ExchangeParameter("Bitget", "ProductType", "UsdtFutures"));
                return parameters;
            }
        }

        protected override ExchangeParameters TradesExchangeParameters
        {
            get
            {
                var parameters = base.TradesExchangeParameters;
                parameters.AddValue(new ExchangeParameter("Bitget", "ProductType", "UsdtFutures"));
                return parameters;
            }
        }

        protected override ExchangeParameters BookTickerExchangeParameters
        {
            get
            {
                var parameters = base.BookTickerExchangeParameters;
                parameters.AddValue(new ExchangeParameter("Bitget", "ProductType", "UsdtFutures"));
                return parameters;
            }
        }

        #region Connect

        public override bool IsConnected => base.IsConnected && _fundingUpdateConnected;
        public override bool ExchangeModifiesOrdersInPlace => false;

        private CancellationTokenSource _fundingPollCts = new();

        public override void Connect()
        {
            _fundingUpdateConnected = true;
            _fundingPollCts = new CancellationTokenSource();
            Task.Run(() => FundingPollLoopAsync(_fundingPollCts.Token));
            base.Connect();
        }

        private async Task FundingPollLoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                var nextPoll = GetNextFundingPollTime();
                var delay = nextPoll - DateTime.UtcNow;
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, ct).ContinueWith(_ => { }, ct);

                if (ct.IsCancellationRequested) break;

                await PollFundingFeesAsync().ConfigureAwait(false);
            }
        }

        private DateTime GetNextFundingPollTime()
        {
            var now = DateTime.UtcNow;
            var intervalHours = FundingRolloverHours; // 8
            var hoursSinceMidnight = now.TimeOfDay.TotalHours;
            var hoursUntilNext = intervalHours - (hoursSinceMidnight % intervalHours);
            return now.AddHours(hoursUntilNext).AddMinutes(1);
        }

        private async Task PollFundingFeesAsync()
        {
            var result = await _restClient.FuturesApiV2.Account.GetLedgerAsync(
                productType: BitgetProductTypeV2.UsdtFutures,
                businessType: "contract_settle_fee",
                idLessThan: null,
                startTime: DateTime.UtcNow.AddMinutes(-30)
            ).ConfigureAwait(false);

            if (!result.Success || result.Data == null)
            {
                Log.Error($"Funding fee poll failed: {result.Error}");
                return;
            }
            foreach (var entry in result.Data.Entries)
            {
                var amount = entry.Quantity;
                _algorithm?.Portfolio?.CashBook[SettleAsset].AddAmount(amount);
                OnMessage(new FundingBrokerageMessageEvent(SettleAsset, amount));
            }
        }

        public override void Disconnect()
        {
            _fundingUpdateConnected = false;
            _socketClientExData?.Dispose();
            _fundingPollCts.Cancel();
            base.Disconnect();
        }
        #endregion


        protected override async Task<CallResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
           string nativeTicker, Symbol symbol, Func<DateTime, decimal?, bool> onFundingRate)
        {
            return await _socketClientExData.FuturesApiV2.SubscribeToTickerUpdatesAsync(Bitget.Net.Enums.BitgetProductTypeV2.UsdtFutures,
                nativeTicker, data =>
                {
                    var now = data.DataTime ?? data.ReceiveTime;
                    var tickerData = data.Data;

                    // Wir reichen die FundingRate direkt durch, auch wenn sie null ist.
                    onFundingRate(now, tickerData.FirstOrDefault()?.FundingRate);
                });
        }

        public override List<CashAmount> GetCashBalance()
        {
            if (Balance.HasValue)
                return new List<CashAmount> { new CashAmount(Balance.Value, SettleAsset) };

            var res = RunSync(() => _restClient.FuturesApiV2.Account.GetBalancesAsync(Bitget.Net.Enums.BitgetProductTypeV2.UsdtFutures));
            var result = new List<CashAmount>
            {
                new CashAmount(res?.Data?.FirstOrDefault()?.CrossMarginMaxAvailable ?? 0 + res?.Data?.FirstOrDefault()?.CrossMarginUsed ?? 0, SettleAsset)
            };
            return result;
        }

        protected override async Task<CallResult<UpdateSubscription>> ExecuteBalanceSubscriptionAsync(Action<List<CashAmount>> onUpdate)
        {
            return await _socketClient.FuturesApiV2.SubscribeToBalanceUpdatesAsync(Bitget.Net.Enums.BitgetProductTypeV2.UsdtFutures, update =>
            {
                var wallet = update.Data.FirstOrDefault();
                if(wallet != null)
                    onUpdate(
                        [
                            new CashAmount(wallet.UsdtEquity, SettleAsset)
                        ]);
            });
        }

        protected override async Task<ExchangeWebResult<SharedId>> ExecuteUpdateOrderAsync(
            Order order, decimal price, decimal? quantity)
        {
            if (!quantity.HasValue)
            {
                Log.Error($"Update error: quantity not provided");
                return new ExchangeWebResult<SharedId>(Name, ArgumentError.Missing("Quantity"));
            }

            var ticker = NativeTicker(order.Symbol);

            string newClientOrderId = null;
            var brokerId = order.BrokerId.LastOrDefault();
            if (_orderStateManager.TryGetByExchangeId(brokerId, out var state))
            {
                newClientOrderId = _restClient.FuturesApiV2.SharedClient.GenerateClientOrderId();
                _orderStateManager.TryAdd(newClientOrderId, state);
            }
            else
            {
                Log.Error($"Update error: old state missing for brokerId {brokerId}");
                return new ExchangeWebResult<SharedId>(Name, new InvalidOperationError("old state missing"));                
            }

            var res = await _restClient.FuturesApiV2.Trading.EditOrderAsync(
                productType: Bitget.Net.Enums.BitgetProductTypeV2.UsdtFutures,
                symbol: ticker,
                orderId: brokerId,
                clientOrderId: newClientOrderId,
                newPrice: price,
                newQuantity: Math.Abs(quantity.Value));

            if (!res.Success)
            {
                if (!string.IsNullOrEmpty(newClientOrderId))
                    _orderStateManager.RemoveAlias(newClientOrderId);

                Log.Error($"Bitget update error: {res.Error} | Ticker: {ticker} | Price: {price} | OriginalData: {res.OriginalData}");
                return new ExchangeWebResult<SharedId>(Name, res.Error);
            }

            return new ExchangeWebResult<SharedId>(
                Name,
                TradingMode.PerpetualLinear,
                res.As(new SharedId(res.Data.OrderId.ToString()))
            );
        }

    }
}