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

        protected override bool IsRejectedUpdateError(string errorMsg) =>
                errorMsg.Contains("40922") || errorMsg.Contains("Only work order modifications are allowed");

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

                _socketClient = new BitgetSocketClient(options =>
                {
                    options.ApiCredentials = new BitgetCredentials(key: key, secret: secret, pass: pass);
                    options.DelayAfterConnect = TimeSpan.FromMilliseconds(500);
                    options.SocketIndividualSubscriptionCombineTarget = 50;
                });
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

                var tickSize = (decimal)Math.Pow(10, -contract.PriceDecimals);
                var lotSize = contract.QuantityStep;

                var symbolProperties = new SymbolProperties(
                    description: $"Bitget {contract.BaseAsset} Perpetual",
                    quoteCurrency: SettleAsset,
                    contractMultiplier: 1m,
                    minimumPriceVariation: tickSize,
                    lotSize: lotSize,
                    marketTicker: contract.Symbol
                );

                _spdb.SetEntry(Name, ticker, SecurityType.CryptoFuture, symbolProperties);
                _spdb.SetEntry(Name, ticker, SecurityType.Crypto, symbolProperties);
            }
        }

        public override decimal MinimumOrderNotionalValue => 5m;
        protected override int? FundingRolloverHours => null;


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

        protected override string GenerateClientId(int _)
        {
            return _restClient.FuturesApiV2.SharedClient.GenerateClientOrderId();
        }

        #region Connect

        public override bool IsConnected => base.IsConnected && _fundingUpdateConnected;
        public override bool ExchangeModifiesOrdersInPlace => false;

        public override void Connect()
        {
            _fundingUpdateConnected = true;
            base.Connect();
        }

        public override void Disconnect()
        {
            _fundingUpdateConnected = false;
            _socketClientExData?.Dispose();
            base.Disconnect();
        }
        #endregion


        protected override async Task<WebSocketResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
           string nativeTicker, Symbol symbol, Func<DateTime, decimal?, DateTime?, (bool ShouldEmit, bool IsFirstTick)> onFundingRate)
        {
            return await _socketClientExData.FuturesApiV2.SubscribeToTickerUpdatesAsync(
                BitgetProductTypeV2.UsdtFutures, nativeTicker, data =>
                {
                    var ticker = data.Data.FirstOrDefault();
                    var now = ticker?.Timestamp ?? data.DataTime ?? data.ReceiveTime;

                    if (onFundingRate(now, ticker?.FundingRate, ticker?.NextFundingTime).ShouldEmit)
                    {
                        // Rollover detected via Socket → poll ledger for actual funding fees
                        Task.Run(async () =>
                        {
                            await Task.Delay(5000); // wait 5s for ledger entry to appear
                            await PollFundingFeesAsync();
                        });
                    }
                });
        }

        private long _lastLedgerIdProcessed = 0;
        private int _fundingPollRunning = 0;

        private async Task PollFundingFeesAsync()
        {
            // Only one poll at a time — multiple symbols may trigger simultaneously
            if (Interlocked.CompareExchange(ref _fundingPollRunning, 1, 0) != 0)
                return;

            try
            {
                var result = await _restClient.FuturesApiV2.Account.GetLedgerAsync(
                    productType: BitgetProductTypeV2.UsdtFutures,
                    businessType: "contract_settle_fee",
                    idLessThan: null,
                    startTime: DateTime.UtcNow.AddMinutes(-1)
                ).ConfigureAwait(false);

                if (!result.Success || result.Data == null)
                {
                    Log.Error($"Funding fee poll failed: {result.Error}");
                    return;
                }

                foreach (var entry in result.Data.Entries
                    .Where(e => e.Id > _lastLedgerIdProcessed)
                    .OrderBy(e => e.Id))
                {
                    _algorithm?.Portfolio?.CashBook[SettleAsset].AddAmount(entry.Quantity);
                    OnMessage(new FundingBrokerageMessageEvent(SettleAsset, entry.Quantity));
                    Interlocked.Exchange(ref _lastLedgerIdProcessed, entry.Id);
                }
            }
            finally
            {
                Interlocked.Exchange(ref _fundingPollRunning, 0);
            }
        }

        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() => _restClient.FuturesApiV2.Account.GetBalancesAsync(Bitget.Net.Enums.BitgetProductTypeV2.UsdtFutures));
            var data = res?.Data?.FirstOrDefault();
            if (data == null)
            {
                Log.Error($"Failed to retrieve cash balance: {res?.Error}");
                return [];
            }
            
            var cashBalance = (data?.UsdtEquity ?? 0) - (data?.UnrealizedProfitAndLoss ?? 0);
            return
            [
                new CashAmount(cashBalance, SettleAsset)
            ];
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

            string newClientOrderId = GenerateClientId(order.Id);
            var brokerId = order.BrokerId.LastOrDefault();
            if (brokerId != null && _orderStateManager.TryGetByExchangeId(brokerId, out var state))
            {
                _orderStateManager.TryAdd(newClientOrderId, state);
            }
            else
            {
                Log.Error($"Update error: old state missing for brokerId {brokerId}");
                return new HttpResult<SharedId>(Name, null, new InvalidOperationError("old state missing"));
            }

            var res = await _restClient.FuturesApiV2.Trading.EditOrderAsync(
                productType: BitgetProductTypeV2.UsdtFutures,
                symbol: ticker,
                orderId: brokerId,
                newClientOrderId: newClientOrderId,
                newPrice: price,
                newQuantity: Math.Abs(quantity.Value));

            if (!res.Success)
            {
                if (!string.IsNullOrEmpty(newClientOrderId))
                    _orderStateManager.RemoveAlias(newClientOrderId);

                Log.Error($"Bitget update error: {res.Error} | Ticker: {ticker} | Price: {price} | OriginalData: {res.OriginalData}");
                return new HttpResult<SharedId>(Name, null, res.Error);
            }

            return new HttpResult<SharedId>(
                Name,
                new SharedId(brokerId),
                null
            );
        }
    }
}