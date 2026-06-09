using CryptoExchange.Net.Authentication;
using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using Kraken.Net;
using Kraken.Net.Clients;
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
    public class KrakenFuturesBrokerage : SharedFuturesBrokerage
    {
        private KrakenRestClient _restClient;
        private KrakenSocketClient _socketClient;
        private KrakenSocketClient _socketClientExData;

        private readonly object _fundingUpdateLock = new();
        private bool _fundingUpdateConnected = false;
        private UpdateSubscription _fundingUpdateSubscription;

        internal KrakenFuturesBrokerage(
            IAlgorithm algorithm,
            KrakenRestClient restClient,
            KrakenSocketClient socketClient,
            IDataAggregator aggregator,
            Func<List<Holding>>? getHoldingsFunc = null)
            : base(algorithm, "kraken")
        {
            _restClient = restClient;
            _socketClient = socketClient;
            // Dedicated unauthenticated socket client for public ticker/funding subscriptions.
            _socketClientExData = new KrakenSocketClient();

            InitializeBase(
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                aggregator,
                getHoldingsFunc);
        }

        protected override void InitializeFromJob(QuantConnect.Packets.LiveNodePacket job, IDataAggregator aggregator)
        {
            if (_restClient == null)
            {
                job.BrokerageData.TryGetValue("kraken-futures-api-key", out var key);
                job.BrokerageData.TryGetValue("kraken-futures-api-secret", out var secret);

                _restClient = new KrakenRestClient(options =>
                {
                    // Futures-only: first argument (Spot credentials) is null.
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret))
                        options.ApiCredentials = new KrakenCredentials(
                            null,
                            new HMACCredential(key, secret));
                });
            }

            if (_socketClient == null)
            {
                job.BrokerageData.TryGetValue("kraken-futures-api-key", out var key);
                job.BrokerageData.TryGetValue("kraken-futures-api-secret", out var secret);

                _socketClient = new KrakenSocketClient(options =>
                {
                    if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(secret))
                        options.ApiCredentials = new KrakenCredentials(
                            null,
                            new HMACCredential(key, secret));
                });
            }

            if (_socketClientExData == null)
                _socketClientExData = new KrakenSocketClient();

            InitializeBase(
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _socketClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                _restClient.FuturesApi.SharedClient,
                aggregator,
                _getHoldingsFunc
            );
        }

        #region Connect

        public override bool IsConnected => base.IsConnected && _fundingUpdateConnected;

        // Kraken edit keeps the same order ID (status: "edited", no cancel+replace).
        public override bool ExchangeModifiesOrdersInPlace => true;

        public override void Connect()
        {
            lock (_fundingUpdateLock)
            {
                if (_fundingUpdateSubscription == null && _socketClient != null)
                {
                    _subRateGate.WaitToProceed();

                    // account_log feed: snapshot on connect, then one new_entry per event.
                    // Funding payments have Info == "funding rate change" and carry RealizedFunding.
                    // We ignore the snapshot (historical entries already settled) and only act
                    // on live update entries.
                    var sub = RunSync(() =>
                        _socketClient.FuturesApi.SubscribeToAccountLogUpdatesAsync(
                            snapshotHandler: _ => { /* ignore historical snapshot */ },
                            updateHandler: update =>
                            {
                                var entry = update.Data.NewEntry;
                                if (entry?.RealizedFunding == null || entry.RealizedFunding == 0m)
                                    return;

                                if (!entry.Info.Equals("funding rate change", StringComparison.OrdinalIgnoreCase))
                                    return;

                                if (_algorithm?.Portfolio?.CashBook != null)
                                {
                                    // RealizedFunding is negative when paid, positive when received.
                                    _algorithm.Portfolio.CashBook[SettleAsset].AddAmount(entry.RealizedFunding.Value);
                                    OnMessage(new FundingBrokerageMessageEvent(
                                        entry.Asset ?? SettleAsset,
                                        entry.RealizedFunding.Value));
                                }
                            }));

                    SetupSubscriptionEvents(
                        sub.Success,
                        sub.Data,
                        (state) => _fundingUpdateConnected = state,
                        "Account log updates",
                        "Account log subscription failed",
                        sub.Error?.ToString()
                    );

                    if (sub.Success)
                        _fundingUpdateSubscription = sub.Data;
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

        protected override string SettleAsset => "USD";

        #region Symbol Mapping

        // Lean symbol.Value is e.g. "XBTUSD" — Kraken Futures needs "PF_XBTUSD".
        protected override string NativeTicker(Symbol symbol) => "PF_" + symbol.Value;

        // Exchange returns "PF_XBTUSD" — strip the prefix to get the Lean-side symbol "XBTUSD".
        protected override string NormalizeSymbol(string rawSymbol) =>
            rawSymbol.StartsWith("PF_", StringComparison.OrdinalIgnoreCase)
                ? rawSymbol[3..].ToUpperInvariant()
                : rawSymbol.ToUpperInvariant();

        #endregion

        // Public ticker feed for funding rate polling, via the unauthenticated extra client.
        protected override async Task<CallResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
            string nativeTicker, Symbol symbol, Func<DateTime, decimal?, DateTime?, bool> onFundingRate)
        {
            return await _socketClientExData.FuturesApi.SubscribeToTickerUpdatesAsync(
                nativeTicker, data =>
                {
                    var tickerData = data.Data;
                    onFundingRate(data.ReceiveTime, tickerData.FundingRate, tickerData.NextFundingRateTime);
                });
        }

        // Kraken Futures multi-collateral (flex) wallet balance minus unrealized PnL.
        // BalanceValue = USD value of all collateral (haircut-free).
        // ProfitAndLoss = unrealized PnL in USD (JSON field: "pnl").
        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() => _restClient.FuturesApi.Account.GetBalancesAsync());

            var flex = res?.Data?.MultiCollateralMarginAccount;
            var balance = (flex?.BalanceValue ?? 0m) - (flex?.ProfitAndLoss ?? 0m);

            return new List<CashAmount>
            {
                new CashAmount(balance, SettleAsset)
            };
        }

        // Kraken edit is true in-place: same order ID is kept, status returns "edited".
        // No cancel+replace, no new ID — same pattern as Bybit.
        protected override async Task<ExchangeWebResult<SharedId>> ExecuteUpdateOrderAsync(
            Order order, decimal price, decimal? quantity)
        {
            var res = await _restClient.FuturesApi.Trading.EditOrderAsync(
                orderId: order.BrokerId.Last(),
                price: price,
                quantity: quantity.HasValue ? Math.Abs(quantity.Value) : null);

            if (!res.Success)
            {
                Log.Error($"Update error: {res.Error} | OrderId: {order.BrokerId.Last()} | Price: {price} | OriginalData: {res.OriginalData}");
                return new ExchangeWebResult<SharedId>(Name, res.Error);
            }

            // The order ID is unchanged after an in-place edit.
            return new ExchangeWebResult<SharedId>(
                Name,
                TradingMode.PerpetualLinear,
                res.As(new SharedId(res.Data.OrderId.ToString()))
            );
        }
    }
}