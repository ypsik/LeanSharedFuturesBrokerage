using CryptoExchange.Net.Authentication;
using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using Kraken.Net;
using Kraken.Net.Clients;
using Microsoft.Win32;
using QLNet;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantConnect.Securities.Crypto;
using QuantConnect.Util;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
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
        protected override int? FundingRolloverHours => null;

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

            PopulateSPDB();

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

        private void PopulateSPDB()
        {
            var result = RunSync(() => _restClient.FuturesApi.ExchangeData.GetSymbolsAsync());

            if (!result.Success)
                throw new Exception($"Failed to load Kraken symbols: {result.Error}");

            foreach (var symbol in result.Data.Where(s => s.Tradeable && s.Type == Kraken.Net.Enums.SymbolType.FlexibleFutures))
            {
                var ticker = NormalizeSymbol(symbol.Symbol);

                var tickSize = symbol.TickSize ?? 0.01m;

                // ContractSize is the actual minimum lot size (e.g. 1 for BTC, 10 for TRX).
                // ContractValueTradePrecision is decimal precision, not a quantity.
                var lotSize = symbol.ContractSize ?? 1m;
                if (lotSize <= 0m) lotSize = 1m;

                var symbolProperties = new SymbolProperties(
                    description: $"Kraken {symbol.BaseAsset} Perpetual",
                    // Dirty fix: must match NormalizeSymbol's appended "USDT" suffix, see comment there.
                    quoteCurrency: "USDT",
                    contractMultiplier: 1m,
                    minimumPriceVariation: tickSize,
                    lotSize: lotSize,
                    marketTicker: symbol.Symbol
                );

                _spdb.SetEntry("kraken", ticker, SecurityType.CryptoFuture, symbolProperties);

                // Register as Crypto only if LEAN's built-in Kraken spot market does not already have this pair,
                // so that EnsureCurrencyDataFeed can resolve the base currency conversion rate.
                if (!_spdb.ContainsKey("kraken", ticker, SecurityType.Crypto))
                {
                    _spdb.SetEntry("kraken", ticker, SecurityType.Crypto, symbolProperties);
                }
            }
        }



        protected override string SettleAsset => "USD";

        #region Symbol Mapping

        protected override string NormalizeSymbol(string rawSymbol)
        {
            var ticker = rawSymbol.Replace("PF_", "");
            // Dirty fix: LEAN's CryptoFuture.IsCryptoCoinFuture() incorrectly classifies any
            // future not quoted in
            // /BUSD/USDC as coin-margined/inverse, which breaks
            // HoldingsValue/UnrealizedProfit/collateral calculation for Kraken's linear,
            // USD-quoted futures. We report every Kraken future to LEAN as "USDT"-quoted
            // (by appending "T") so it gets correctly recognized as linear. The USDT/USD
            // spread is negligible for portfolio valuation purposes (~0.01-0.05%).
            // NativeTicker() strips the appended "T" again before actually communicating
            // with Kraken's real API.
            return ticker.EndsWith("USD") ? ticker + "T" : ticker;
        }

        protected override string NativeTicker(Symbol symbol)
        {
            CurrencyPairUtil.DecomposeCurrencyPair(symbol, out var baseAsset, out var quoteAsset);
            // quoteAsset is "USDT" due to the dirty fix above; Kraken's real API only
            // knows "USD" — strip the artificially appended "T" back off here.
            var realQuoteAsset = quoteAsset == "USDT" ? "USD" : quoteAsset;
            return $"PF_{baseAsset}{realQuoteAsset}";
        }

        #endregion

        // Public ticker feed for funding rate polling, via the unauthenticated extra client.
        protected override async Task<WebSocketResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
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
            var balance = flex?.BalanceValue ?? 0m;

            return
            [
                new(balance, SettleAsset)
            ];
        }

        // Kraken edit is true in-place: same order ID is kept, status returns "edited".
        // No cancel+replace, no new ID — same pattern as Bybit.
        protected override async Task<HttpResult<SharedId>> ExecuteUpdateOrderAsync(
            Order order, decimal price, decimal? quantity)
        {
            var res = await _restClient.FuturesApi.Trading.EditOrderAsync(
                orderId: order.BrokerId.Last(),
                price: price,
                quantity: quantity.HasValue ? Math.Abs(quantity.Value) : null);

            if (!res.Success)
            {
                Log.Error($"Update error: {res.Error} | OrderId: {order.BrokerId.Last()} | Price: {price} | OriginalData: {res.OriginalData}");
                return new HttpResult<SharedId>(Name, null, res.Error);
            }

            // The order ID is unchanged after an in-place edit.
            return new HttpResult<SharedId>(
                Name,
                new SharedId(res.Data.OrderId.ToString()),
                null
            );
        }
    }
}