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
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Packets;

using CxCancelOrderRequest = CryptoExchange.Net.SharedApis.CancelOrderRequest;

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    public class SharedFuturesBrokerage : Brokerage, IDataQueueHandler
    {
        private readonly IFuturesOrderRestClient _orderClient;
        private readonly IBalanceRestClient _balanceClient;
        private readonly ITickerSocketClient _tickerSocket;
        private readonly IFuturesOrderSocketClient _orderSocket;

        private readonly Func<List<Holding>> _getHoldingsFunc;

        private readonly ConcurrentQueue<BaseData> _ticks = new ConcurrentQueue<BaseData>();
        private readonly ConcurrentDictionary<string, Order> _orderCache = new ConcurrentDictionary<string, Order>();
        private readonly ConcurrentDictionary<string, decimal> _filledQtyCache = new ConcurrentDictionary<string, decimal>();

        private readonly Dictionary<Symbol, UpdateSubscription> _subscriptionMap = new Dictionary<Symbol, UpdateSubscription>();
        private readonly HashSet<Symbol> _pendingSubscriptions = new HashSet<Symbol>();

        private UpdateSubscription _orderSocketSub;

        private readonly object _subscriptionLock = new object();
        private readonly object _connectLock = new object();

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
            Func<List<Holding>> getHoldingsFunc) : base(exchangeName)
        {
            _orderClient = orderClient ?? throw new ArgumentNullException(nameof(orderClient));
            _balanceClient = balanceClient ?? throw new ArgumentNullException(nameof(balanceClient));
            _tickerSocket = tickerSocket ?? throw new ArgumentNullException(nameof(tickerSocket));
            _orderSocket = orderSocket ?? throw new ArgumentNullException(nameof(orderSocket));
            _getHoldingsFunc = getHoldingsFunc ?? throw new ArgumentNullException(nameof(getHoldingsFunc));
        }

        #region Sync Helpers

        private static T RunSync<T>(Func<Task<T>> asyncFunc) =>
            asyncFunc().ConfigureAwait(false).GetAwaiter().GetResult();

        private static void RunSync(Func<Task> asyncFunc) =>
            asyncFunc().ConfigureAwait(false).GetAwaiter().GetResult();

        #endregion

        #region Connection

        public override bool IsConnected => _isConnected;

        public override void Connect()
        {
            lock (_connectLock)
            {
                if (_isConnected) return;

                var auth = RunSync(() => _balanceClient.GetBalancesAsync(new GetBalancesRequest()));
                if (!auth.Success)
                    throw new Exception($"Auth failed: {auth.Error?.Message}");

                var subResult = RunSync(() =>
                    _orderSocket.SubscribeToFuturesOrderUpdatesAsync(
                        new SubscribeFuturesOrderRequest(),
                        update =>
                        {
                            foreach (var item in update.Data)
                            {
                                var orderId = item.OrderId;
                                if (string.IsNullOrEmpty(orderId)) continue;
                                if (!_orderCache.TryGetValue(orderId, out var cachedOrder)) continue;

                                var totalFilled = item.QuantityFilled?.QuantityInBaseAsset ?? 0m; 
                                var status = MapStatus(item.Status, totalFilled);
                                var sideMultiplier = cachedOrder.Quantity > 0 ? 1m : -1m;

                                var prevFilled = _filledQtyCache.TryGetValue(orderId, out var pf) ? pf : 0m;
                                var deltaFill = totalFilled - prevFilled;

                                if (deltaFill <= 0m &&
                                    status != OrderStatus.Filled &&
                                    status != OrderStatus.Canceled &&
                                    status != OrderStatus.Invalid)
                                    continue;

                                _filledQtyCache[orderId] = totalFilled;

                                var price = item.AveragePrice ?? item.OrderPrice ?? 0m;

                                OnOrderEvent(new OrderEvent(
                                    cachedOrder,
                                    update.DataTimeLocal ?? DateTime.UtcNow,
                                    CalculateFee(cachedOrder, price))
                                {
                                    Status = status,
                                    FillPrice = price,
                                    FillQuantity = deltaFill * sideMultiplier,
                                    Message = "Socket Update"
                                });

                                if (status == OrderStatus.Filled ||
                                    status == OrderStatus.Canceled ||
                                    status == OrderStatus.Invalid)
                                {
                                    _orderCache.TryRemove(orderId, out _);
                                    _filledQtyCache.TryRemove(orderId, out _);
                                }
                            }
                        }));

                if (!subResult.Success)
                    throw new Exception($"Order socket failed: {subResult.Error?.Message}");

                _orderSocketSub = subResult.Data;
                _isConnected = true;

                _reconcileCts = new CancellationTokenSource();
                _reconcileTask = Task.Run(() => ReconciliationLoop(_reconcileCts.Token));
            }
        }

        public override void Disconnect()
        {
            _reconcileCts?.Cancel();

            try { _reconcileTask?.Wait(TimeSpan.FromSeconds(5)); }
            catch { }

            lock (_subscriptionLock)
            {
                foreach (var sub in _subscriptionMap.Values)
                    RunSync(() => sub.CloseAsync());

                _subscriptionMap.Clear();
                _pendingSubscriptions.Clear();
            }

            if (_orderSocketSub != null)
            {
                RunSync(() => _orderSocketSub.CloseAsync());
                _orderSocketSub = null;
            }

            _orderCache.Clear();
            _filledQtyCache.Clear();

            _isConnected = false;
        }

        #endregion

        #region Orders

        public override List<Order> GetOpenOrders()
        {
            var result = RunSync(() =>
                _orderClient.GetOpenFuturesOrdersAsync(new GetOpenOrdersRequest()));

            if (!result.Success || result.Data == null)
                return new List<Order>();

            return result.Data.Select(item =>
            {
                var symbol = Symbol.Create(item.Symbol, SecurityType.CryptoFuture, Name);

                var side = item.Side == SharedOrderSide.Sell ? -1m : 1m;

                var quantity =
                    (item.OrderQuantity?.QuantityInBaseAsset ?? 0m) * side;

                var createTime = item.CreateTime ?? DateTime.UtcNow;

                Order order =
                    item.OrderType == SharedOrderType.Limit
                        ? new LimitOrder(symbol, quantity, item.OrderPrice ?? 0m, createTime)
                        : new MarketOrder(symbol, quantity, createTime);

                var brokerId = item.OrderId;

                order.BrokerId.Add(brokerId);
                order.Status = MapStatus(item.Status, item.QuantityFilled?.QuantityInBaseAsset ?? 0m);

                _orderCache[brokerId] = order;
                _filledQtyCache[brokerId] = item.QuantityFilled?.QuantityInBaseAsset ?? 0m;

                return order;

            }).ToList();
        }

        public override bool PlaceOrder(Order order)
        {
            var sharedSymbol = GetSharedSymbol(order.Symbol);

            var request = new PlaceFuturesOrderRequest(
                sharedSymbol,
                order.Quantity > 0 ? SharedOrderSide.Buy : SharedOrderSide.Sell,
                order.Type == OrderType.Limit ? SharedOrderType.Limit : SharedOrderType.Market,
                new SharedQuantity { QuantityInBaseAsset = Math.Abs(order.Quantity) })
            {
                Price = (order as LimitOrder)?.LimitPrice
            };

            var result = RunSync(() => _orderClient.PlaceFuturesOrderAsync(request));

            if (!result.Success)
            {
                Log.Error($"{Name}.PlaceOrder: {result.Error?.Message}");
                return false;
            }

            var brokerId = result.Data.Id;

            _orderCache[brokerId] = order;
            _filledQtyCache.TryAdd(brokerId, 0m);   // FIX

            order.BrokerId.Add(brokerId);

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

            var brokerId = order.BrokerId.First();
            var sharedSymbol = GetSharedSymbol(order.Symbol);

            var result = RunSync(() =>
                _orderClient.CancelFuturesOrderAsync(
                    new CxCancelOrderRequest(sharedSymbol, brokerId)));

            if (!result.Success)
            {
                Log.Error($"{Name}.CancelOrder: {result.Error?.Message}");
                return false;
            }

            // KEIN Cache remove hier
            // Socket oder Reconciliation wird den final state liefern

            OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
            {
                Status = OrderStatus.CancelPending,
                Message = "Cancel requested"
            });

            return true;
        }


        public override bool UpdateOrder(Order order)
        {
            Log.Error($"{Name}.UpdateOrder not supported.");
            return false;
        }

        #endregion

        #region Reconciliation

        private async Task ReconciliationLoop(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try { await Task.Delay(_reconciliationInterval, ct); }
                catch (TaskCanceledException) { break; }

                try { ReconcileOrders(); }
                catch (Exception ex) { Log.Error($"{Name}.Reconcile error: {ex.Message}"); }
            }
        }

        private void ReconcileOrders()
        {
            var result = RunSync(() =>
                _orderClient.GetOpenFuturesOrdersAsync(new GetOpenOrdersRequest()));

            if (!result.Success || result.Data == null)
                return;

            var exchangeOpen =
                result.Data.ToDictionary(o => o.OrderId, o => o);

            foreach (var kvp in _orderCache.ToArray())
            {
                var brokerId = kvp.Key;
                var order = kvp.Value;

                if(exchangeOpen.ContainsKey(brokerId)) 
                    continue;

                var sharedSymbol = GetSharedSymbol(order.Symbol);

                var statusResult = RunSync(() =>
                    _orderClient.GetFuturesOrderAsync(
                        new GetOrderRequest(sharedSymbol, brokerId)));

                if (!statusResult.Success || statusResult.Data == null)
                {
                    EmitOrderEvent(order, OrderStatus.Canceled, 0m, 0m);
                    _orderCache.TryRemove(brokerId, out _);
                    _filledQtyCache.TryRemove(brokerId, out _);
                    continue;
                }

                var totalFilled =
                    statusResult.Data.QuantityFilled?.QuantityInBaseAsset ?? 0m;

                var exchangeStatus = MapStatus(statusResult.Data.Status, totalFilled);

                var prevFilled =
                    _filledQtyCache.TryGetValue(brokerId, out var pf) ? pf : 0m;

                var deltaFill = totalFilled - prevFilled;

                var price =
                    statusResult.Data.AveragePrice ??
                    statusResult.Data.OrderPrice ??
                    0m;

                if (deltaFill > 0m)
                {
                    var side = order.Quantity > 0 ? 1m : -1m;

                    EmitOrderEvent(
                        order,
                        exchangeStatus,
                        price,
                        deltaFill * side);
                }
                else
                {
                    EmitOrderEvent(order, exchangeStatus, 0m, 0m);
                }

                if (exchangeStatus == OrderStatus.Filled ||
                    exchangeStatus == OrderStatus.Canceled ||
                    exchangeStatus == OrderStatus.Invalid)
                {
                    _orderCache.TryRemove(brokerId, out _);
                    _filledQtyCache.TryRemove(brokerId, out _);
                }
            }
        }

        private void EmitOrderEvent(Order order, OrderStatus status, decimal price, decimal qty)
        {
            var fee = price > 0 ? CalculateFee(order, price) : OrderFee.Zero;

            OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, fee)
            {
                Status = status,
                FillPrice = price,
                FillQuantity = qty,
                Message = "Reconciliation"
            });
        }

        #endregion

        #region Account

        public override List<CashAmount> GetCashBalance()
        {
            var result = RunSync(() =>
                _balanceClient.GetBalancesAsync(new GetBalancesRequest()));

            if (!result.Success || result.Data == null)
            {
                Log.Error($"{Name}.GetCashBalance: {result.Error?.Message}");
                return new List<CashAmount>();
            }

            return result.Data
                .Select(b => new CashAmount(
                    b.Available,
                    b.Asset ?? "USDC"))
                .ToList();
        }

        public override List<Holding> GetAccountHoldings()
        {
            try { return _getHoldingsFunc.Invoke(); }
            catch { return new List<Holding>(); }
        }

        #endregion

        #region IDataQueueHandler

        public IEnumerable<BaseData> GetNextTicks()
        {
            while (_ticks.TryDequeue(out var t))
                yield return t;
        }

        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            var symbol = dataConfig.Symbol;

            lock (_subscriptionLock)
            {
                if (_subscriptionMap.ContainsKey(symbol) ||
                    !_pendingSubscriptions.Add(symbol))
                    return null;
            }

            try
            {
                var sharedSymbol = GetSharedSymbol(symbol);

                var subResult = RunSync(() =>
                    _tickerSocket.SubscribeToTickerUpdatesAsync(
                        new SubscribeTickerRequest(sharedSymbol),
                        update =>
                        {
                            _ticks.Enqueue(new Tick
                            {
                                Symbol = symbol,
                                Time = update?.DataTimeLocal ?? DateTime.UtcNow,
                                Value = update?.Data.LastPrice ?? 0m,
                                TickType = TickType.Trade,
                                Quantity = 0m
                            });

                            newDataAvailableHandler?.Invoke(this, EventArgs.Empty);
                        }));

                if (subResult.Success)
                {
                    lock (_subscriptionLock)
                        _subscriptionMap[symbol] = subResult.Data;
                }
                else
                {
                    Log.Error($"{Name}.Subscribe error: {subResult.Error?.Message}");
                }
            }
            finally
            {
                lock (_subscriptionLock)
                    _pendingSubscriptions.Remove(symbol);
            }

            return GetNextTicks().GetEnumerator();
        }

        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            UpdateSubscription sub = null;

            lock (_subscriptionLock)
            {
                if (_subscriptionMap.TryGetValue(dataConfig.Symbol, out sub))
                    _subscriptionMap.Remove(dataConfig.Symbol);
            }

            if (sub != null)
                RunSync(() => sub.CloseAsync());
        }

        public void SetJob(LiveNodePacket job) { }

        #endregion

        #region Helpers

        protected virtual SharedSymbol GetSharedSymbol(Symbol leanSymbol)
        {
            return new SharedSymbol(
                TradingMode.PerpetualLinear,
                leanSymbol.Value,
                "USDC");
        }

        protected virtual OrderFee CalculateFee(Order order, decimal price)
        {
            var rate =
                order.Type == OrderType.Limit
                    ? 0.0002m
                    : 0.0005m;

            return new OrderFee(
                new CashAmount(Math.Abs(order.Quantity) * price * rate, "USDC"));
        }

        private OrderStatus MapStatus(SharedOrderStatus status, decimal totalFilled)
        {
            if (status == SharedOrderStatus.Open)
                return totalFilled > 0
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