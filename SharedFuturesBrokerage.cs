using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CryptoExchange.Net.SharedApis;
using CryptoExchange.Net.Objects.Sockets;
using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Packets;

using CxCancelOrderRequest = CryptoExchange.Net.SharedApis.CancelOrderRequest;

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    /// <summary>
    /// Exchange-agnostic base brokerage for perpetual futures via CryptoExchange.Net SharedApis.
    ///
    /// IDataQueueHandler is intentionally NOT implemented here.
    /// Market data (ticks) must be provided by a separate IDataQueueHandler / IDataProvider
    /// registered in LEAN's live node config. Exchange-specific derived classes may implement
    /// IDataQueueHandler if they need to serve ticks through _tickerSocket directly.
    /// </summary>
    public class SharedFuturesBrokerage : Brokerage
    {
        private readonly IFuturesOrderRestClient _orderClient;
        private readonly IBalanceRestClient _balanceClient;
        protected readonly ITickerSocketClient _tickerSocket;
        private readonly IFuturesOrderSocketClient _orderSocket;
        private readonly Func<List<Holding>> _getHoldingsFunc;

        private readonly ConcurrentDictionary<string, Order> _orderCache = new();
        private readonly ConcurrentDictionary<string, decimal> _filledQtyCache = new();

        // Available for derived classes that implement IDataQueueHandler.
        protected readonly ConcurrentDictionary<Symbol, UpdateSubscription> _subscriptionMap = new();
        protected readonly ConcurrentDictionary<Symbol, byte> _pendingSubscriptions = new();

        private UpdateSubscription _orderSocketSub;
        private readonly object _connectLock = new();

        private bool _isConnected;

        private CancellationTokenSource _reconcileCts;
        private Task _reconcileTask;

        private readonly TimeSpan _reconciliationInterval = TimeSpan.FromSeconds(30);

        public SharedFuturesBrokerage(
            string exchangeName,
            IFuturesOrderRestClient orderClient,
            IBalanceRestClient balanceClient,
            ITickerSocketClient tickerSocket,
            IFuturesOrderSocketClient orderSocket,
            Func<List<Holding>> getHoldingsFunc)
            : base(exchangeName)
        {
            _orderClient = orderClient;
            _balanceClient = balanceClient;
            _tickerSocket = tickerSocket;
            _orderSocket = orderSocket;
            _getHoldingsFunc = getHoldingsFunc;
        }

        #region Sync

        private static T RunSync<T>(Func<Task<T>> f) =>
            f().ConfigureAwait(false).GetAwaiter().GetResult();

        private static void RunSync(Func<Task> f) =>
            f().ConfigureAwait(false).GetAwaiter().GetResult();

        #endregion

        #region Connection

        public override bool IsConnected => _isConnected;

        public override void Connect()
        {
            lock (_connectLock)
            {
                if (_isConnected) return;

                var auth = RunSync(() =>
                    _balanceClient.GetBalancesAsync(new GetBalancesRequest()));

                if (!auth.Success)
                    throw new Exception(auth.Error?.Message);

                var sub = RunSync(() =>
                    _orderSocket.SubscribeToFuturesOrderUpdatesAsync(
                        new SubscribeFuturesOrderRequest(),
                        HandleSocket));

                if (!sub.Success)
                    throw new Exception(sub.Error?.Message);

                _orderSocketSub = sub.Data;
                _isConnected = true;

                _reconcileCts = new CancellationTokenSource();
                _reconcileTask = Task.Factory.StartNew(
                () => ReconcileLoop(_reconcileCts.Token),
                _reconcileCts.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default).Unwrap();
            }
        }

        public override void Disconnect()
        {
            _reconcileCts?.Cancel();

            try { _reconcileTask?.Wait(5000); } catch { }

            RunSync(() => _orderSocketSub?.CloseAsync());
            _orderSocketSub = null;

            _orderCache.Clear();
            _filledQtyCache.Clear();
            _subscriptionMap.Clear();

            _isConnected = false;
        }

        #endregion

        #region Socket

        private void HandleSocket(DataEvent<SharedFuturesOrder[]> update)
        {
            foreach (var o in update.Data)
            {
                if (string.IsNullOrEmpty(o.OrderId))
                    continue;

                if (!_orderCache.TryGetValue(o.OrderId, out var order))
                {
                    // Expected during the narrow window between PlaceFuturesOrderAsync
                    // returning and _orderCache being written. Also fires for orders
                    // placed outside this session. Logged for race-window diagnostics.
                    Log.Trace($"{Name} HandleSocket: unknown order {o.OrderId} — skipping");
                    continue;
                }

                var totalFilled = o.QuantityFilled?.QuantityInBaseAsset ?? 0m;
                var prev = _filledQtyCache.TryGetValue(o.OrderId, out var pf) ? pf : 0m;
                var delta = totalFilled - prev;

                var status = MapStatus(o.Status, totalFilled);

                // Skip no-op: no new fill and still just submitted
                if (delta == 0 && status == OrderStatus.Submitted)
                    continue;

                _filledQtyCache[o.OrderId] = totalFilled;

                OnOrderEvent(new OrderEvent(
                    order,
                    DateTime.UtcNow,
                    OrderFee.Zero)
                {
                    Status = status,
                    FillPrice = o.AveragePrice ?? o.OrderPrice ?? 0m,
                    FillQuantity = delta * (order.Quantity > 0 ? 1 : -1),
                    Message = "socket"
                });

                if (status is OrderStatus.Filled
                           or OrderStatus.Canceled
                           or OrderStatus.Invalid)
                {
                    _orderCache.TryRemove(o.OrderId, out _);
                    _filledQtyCache.TryRemove(o.OrderId, out _);
                }
            }
        }

        #endregion

        #region Orders

        public override List<Order> GetOpenOrders()
        {
            var res = RunSync(() =>
                _orderClient.GetOpenFuturesOrdersAsync(new GetOpenOrdersRequest()));

            if (!res.Success || res.Data == null)
                return new List<Order>();

            return res.Data.Select(o =>
            {
                var symbol = Symbol.Create(NormalizeSymbol(o.Symbol), SecurityType.CryptoFuture, Name);

                var qty = (o.OrderQuantity?.QuantityInBaseAsset ?? 0m)
                          * (o.Side == SharedOrderSide.Sell ? -1 : 1);

                Order order = o.OrderType == SharedOrderType.Limit
                    ? new LimitOrder(symbol, qty, o.OrderPrice ?? 0m, DateTime.UtcNow)
                    : new MarketOrder(symbol, qty, DateTime.UtcNow);

                order.BrokerId.Add(o.OrderId);
                order.Status = MapStatus(o.Status, o.QuantityFilled?.QuantityInBaseAsset ?? 0m);

                _orderCache[o.OrderId] = order;
                _filledQtyCache[o.OrderId] = o.QuantityFilled?.QuantityInBaseAsset ?? 0m;

                return order;
            }).ToList();
        }

        public override bool PlaceOrder(Order order)
        {
            var request = new PlaceFuturesOrderRequest(
                GetSharedSymbol(order.Symbol),
                order.Quantity > 0 ? SharedOrderSide.Buy : SharedOrderSide.Sell,
                order.Type == OrderType.Limit ? SharedOrderType.Limit : SharedOrderType.Market,
                new SharedQuantity { QuantityInBaseAsset = Math.Abs(order.Quantity) })
            {
                Price = (order as LimitOrder)?.LimitPrice
            };

            var res = RunSync(() => _orderClient.PlaceFuturesOrderAsync(request));

            if (!res.Success)
                return false;

            // Cache with real broker ID immediately after REST returns.
            // Narrow race window (socket event arriving between REST return and cache write)
            // can only be fully closed via ClientOrderId pre-registration — implement in
            // derived class if the exchange supports it.
            order.BrokerId.Add(res.Data.Id);
            _orderCache[res.Data.Id] = order;
            _filledQtyCache[res.Data.Id] = 0m;

            OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
            {
                Status = OrderStatus.Submitted
            });

            return true;
        }

        public override bool CancelOrder(Order order)
        {
            if (!order.BrokerId.Any())
                return false;

            var id = order.BrokerId.First();

            var res = RunSync(() =>
                _orderClient.CancelFuturesOrderAsync(
                    new CxCancelOrderRequest(GetSharedSymbol(order.Symbol), id)));

            if (!res.Success)
                return false;

            // Order stays in cache until socket confirms Canceled (HandleSocket removes it)
            // or reconcile loop cleans it up after the interval.
            OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
            {
                Status = OrderStatus.CancelPending
            });

            return true;
        }

        public override bool UpdateOrder(Order order) => false;

        #endregion

        #region Reconcile

        private async Task ReconcileLoop(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try { await Task.Delay(_reconciliationInterval, ct); }
                catch { break; }

                try
                {
                    var open = await _orderClient
                        .GetOpenFuturesOrdersAsync(new GetOpenOrdersRequest())
                        .ConfigureAwait(false);

                    // Skip only on genuine API failure or null data.
                    // An empty list (all orders closed) is valid and triggers cache cleanup.
                    if (!open.Success || open.Data == null)
                        continue;

                    // FIX: GroupBy guards against malformed exchange responses that
                    // return duplicate OrderIds — ToDictionary would throw otherwise.
                    var map = open.Data
                        .GroupBy(x => x.OrderId)
                        .ToDictionary(g => g.Key, g => g.First());

                    foreach (var kv in _orderCache.ToArray())
                    {
                        if (map.ContainsKey(kv.Key))
                            continue;

                        OnOrderEvent(new OrderEvent(kv.Value, DateTime.UtcNow, OrderFee.Zero)
                        {
                            Status = OrderStatus.Canceled,
                            Message = "reconcile"
                        });

                        _orderCache.TryRemove(kv.Key, out _);
                        _filledQtyCache.TryRemove(kv.Key, out _);
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex);
                }
            }
        }

        #endregion

        #region Cash / Holdings

        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() =>
                _balanceClient.GetBalancesAsync(new GetBalancesRequest()));

            return res.Success && res.Data != null
                ? res.Data.Select(x => new CashAmount(x.Available, x.Asset ?? "USDC")).ToList()
                : new List<CashAmount>();
        }

        public override List<Holding> GetAccountHoldings()
        {
            try { return _getHoldingsFunc?.Invoke() ?? new List<Holding>(); }
            catch { return new List<Holding>(); }
        }

        #endregion

        #region Helpers

        /// <summary>
        /// Maps an exchange-native symbol string to the LEAN-compatible format.
        /// Override in derived classes when the exchange returns symbols that differ
        /// from LEAN convention (e.g. "BTC-PERP" → "BTCUSDC").
        /// </summary>
        protected virtual string NormalizeSymbol(string rawSymbol) => rawSymbol;

        protected virtual SharedSymbol GetSharedSymbol(Symbol s)
            => new(TradingMode.PerpetualLinear, s.Value, "USDC");

        private OrderStatus MapStatus(SharedOrderStatus status, decimal filled)
        {
            if (status == SharedOrderStatus.Open)
                return filled > 0
                    ? OrderStatus.PartiallyFilled
                    : OrderStatus.Submitted;

            return status switch
            {
                SharedOrderStatus.Filled => OrderStatus.Filled,
                SharedOrderStatus.Canceled => OrderStatus.Canceled,
                _ => OrderStatus.None
            };
        }

        #endregion
    }
}