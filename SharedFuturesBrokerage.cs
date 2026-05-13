using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Util;
using QuantConnect.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using QuantConnect.Securities;

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    // WICHTIG: abstract partial muss hier stehen!
    public abstract partial class SharedFuturesBrokerage : Brokerage, IDataQueueHandler
    {
        protected IFuturesOrderRestClient _orderClient;
        protected IBalanceRestClient _balanceClient;
        protected IFuturesOrderSocketClient _orderSocket;
        protected IUserTradeSocketClient _tradeSocket;
        protected IBalanceSocketClient _balanceSocket;
        protected IKlineRestClient _klineClient;
        protected IFundingRateRestClient _fundingRateClient;
        protected Func<List<Holding>> _getHoldingsFunc;

        protected SymbolPropertiesDatabase _spdb;

        protected IDataAggregator _aggregator;
        protected EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager;
        protected bool _isInitialized;
        protected LiveNodePacket _job;

        protected UpdateSubscription _orderSocketSub;
        protected readonly object _connectLock = new();
        private bool _isConnectedOrder, _isConnectedBalance;
        protected CancellationTokenSource _reconcileCts;
        protected Task _reconcileTask;
        protected readonly TimeSpan _reconciliationInterval = TimeSpan.FromSeconds(30);

        protected SharedFuturesBrokerage(string exchangeName) : base(exchangeName)
        {
            _spdb = SymbolPropertiesDatabase.FromDataFolder();
        }

        protected void InitializeBase(
            IFuturesOrderRestClient orderClient,
            IBalanceRestClient balanceClient,
            IFuturesOrderSocketClient orderSocket,
            IUserTradeSocketClient tradeSocket,
            IBalanceSocketClient balanceSocket,
            IFundingRateRestClient fundingRateClient,
            IKlineRestClient klineClient,
            IDataAggregator aggregator, // <-- Der kommt jetzt an
            Func<List<Holding>> getHoldingsFunc = null)
        {
            // SICHERHEITSGURT: Wenn wir schon initialisiert sind, der Aggregator aber null war 
            // (z.B. weil die Factory zu früh dran war), dann updaten wir ihn hier durch den SetJob-Aufruf!
            if (_isInitialized)
            {
                if (_aggregator == null && aggregator != null)
                {
                    _aggregator = aggregator;
                    Log.Trace($"{Name}: Aggregator loaded on SetJob.");
                }
                return;
            }

            _orderClient = orderClient;
            _balanceClient = balanceClient;
            _orderSocket = orderSocket;
            _tradeSocket = tradeSocket;
            _balanceSocket = balanceSocket;
            _fundingRateClient = fundingRateClient;
            _klineClient = klineClient;
            _aggregator = aggregator;
            _getHoldingsFunc = getHoldingsFunc;

            _spdb = SymbolPropertiesDatabase.FromDataFolder();

            _subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();
            _subscriptionManager.SubscribeImpl += (symbols, tickType) => SubscribeSymbols(symbols, tickType);
            _subscriptionManager.UnsubscribeImpl += (symbols, tickType) => UnsubscribeSymbols(symbols, tickType);

            _isInitialized = true;
        }

        public override bool IsConnected => _isConnectedOrder && _isConnectedBalance;

        public override void Connect()
        {
            lock (_connectLock)
            {
                if (_balanceClient == null || _orderSocket == null) throw new InvalidOperationException("Clients not configured");

                if (!_isConnectedOrder)
                {
                    var sub = RunSync(() => _orderSocket.SubscribeToFuturesOrderUpdatesAsync(new SubscribeFuturesOrderRequest(), HandleOrderSocket));
                    if (sub.Success)
                    {
                        var subscription = sub.Data;

                        subscription.ConnectionLost += () =>
                        {
                            _isConnectedOrder = false;
                            Log.Error($"{Name}: Connection lost!");
                            OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "Disconnect", "Order stream lost."));
                        };

                        subscription.ConnectionRestored += (duration) =>
                        {
                            _isConnectedOrder = true;
                            Log.Trace($"{Name}: Connection restored after {duration}.");
                            OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Information, "Reconnect", $"Order stream restored. Syncing..."));
                            //Task.Run(async () => await ForceReconcile());
                        };
                    }
                    else
                        throw new Exception("Order socket failed");


                    _orderSocketSub = sub.Data;
                    _isConnectedOrder = true;
                    _reconcileCts = new CancellationTokenSource();
                    _reconcileTask = Task.Run(() => ReconcileLoop(_reconcileCts.Token));
                }
                if(!_isConnectedBalance)
                {
                    RunSync(() => SubscribeToBalanceUpdatesAsync());
                }
            }
        }

        public override void Disconnect()
        {
            _reconcileCts?.Cancel();
            if (_orderSocketSub != null) RunSync(() => _orderSocketSub.CloseAsync());
            if (_balanceUpdatesSocketSub != null) RunSync(() => _balanceUpdatesSocketSub.CloseAsync());            
            _orderCache.Clear();
            _filledQtyCache.Clear();
            _isConnectedOrder = false;
            _isConnectedBalance = false;
        }

        public virtual void SetJob(LiveNodePacket job)
        {
            _job = job;
            var aggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(
                Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager"),
                forceTypeNameOnExisting: false);

            InitializeFromJob(job, aggregator);
            if (!IsConnected) Connect();
        }

        protected abstract void InitializeFromJob(LiveNodePacket job, IDataAggregator aggregator);
        protected static T RunSync<T>(Func<Task<T>> f) => f().ConfigureAwait(false).GetAwaiter().GetResult();
        protected static void RunSync(Func<Task> f) => f().ConfigureAwait(false).GetAwaiter().GetResult();
    }
}