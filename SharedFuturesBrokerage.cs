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

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    // WICHTIG: abstract partial muss hier stehen!
    public abstract partial class SharedFuturesBrokerage : Brokerage, IDataQueueHandler
    {
        protected IFuturesOrderRestClient _orderClient;
        protected IBalanceRestClient _balanceClient;
        protected IFuturesOrderRestClient _futuresOrderClient;
        protected IFuturesOrderSocketClient _orderSocket;
        protected IUserTradeSocketClient _tradeSocket;
        protected IKlineRestClient _klineClient;
        protected IFundingRateRestClient _fundingRateClient;
        protected Func<List<Holding>> _getHoldingsFunc;

        protected IDataAggregator _aggregator;
        protected EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager;
        protected bool _isInitialized;
        protected LiveNodePacket _job;

        protected UpdateSubscription _orderSocketSub;
        protected readonly object _connectLock = new();
        protected bool _isConnected;
        protected CancellationTokenSource _reconcileCts;
        protected Task _reconcileTask;
        protected readonly TimeSpan _reconciliationInterval = TimeSpan.FromSeconds(30);

        protected SharedFuturesBrokerage(string exchangeName) : base(exchangeName)
        {
        }

        protected void InitializeBase(
            IFuturesOrderRestClient orderClient,
            IBalanceRestClient balanceClient,
            IFuturesOrderRestClient futuresOrderClient,
            IFuturesOrderSocketClient orderSocket,
            IUserTradeSocketClient tradeSocket,
            IFundingRateRestClient fundingRateClient,
            IKlineRestClient klineClient,
            IDataAggregator aggregator, // <-- Der kommt jetzt an
            Func<List<Holding?>> getHoldingsFunc = null)
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
            _futuresOrderClient = futuresOrderClient;
            _orderSocket = orderSocket;
            _tradeSocket = tradeSocket;
            _fundingRateClient = fundingRateClient;
            _klineClient = klineClient;
            _aggregator = aggregator;
            _getHoldingsFunc = getHoldingsFunc;

            _subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();
            _subscriptionManager.SubscribeImpl += (symbols, tickType) => SubscribeSymbols(symbols, tickType);
            _subscriptionManager.UnsubscribeImpl += (symbols, tickType) => UnsubscribeSymbols(symbols, tickType);

            _isInitialized = true;
        }

        // --- HIER SIND DIE METHODEN, DIE DEIN COMPILER VERMISST HAT ---

        public override bool IsConnected => _isConnected;

        public override void Connect()
        {
            lock (_connectLock)
            {
                if (_isConnected) return;
                if (_balanceClient == null || _orderSocket == null) throw new InvalidOperationException("Clients not configured");

                InitialPositionAndCashSync();

                var sub = RunSync(() => _orderSocket.SubscribeToFuturesOrderUpdatesAsync(new SubscribeFuturesOrderRequest(), HandleSocket));
                if (sub.Success)
                {
                    var subscription = sub.Data;

                    subscription.ConnectionLost += () =>
                    {
                        _isConnected = false;
                        OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "Disconnect", "Order stream lost."));
                    };

                    subscription.ConnectionRestored += (duration) =>
                    {
                        _isConnected = true;
                        Log.Trace($"{Name}: Connection restored after {duration}.");
                        OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Information, "Reconnect", $"Order stream restored. Syncing..."));
                        //Task.Run(async () => await ForceReconcile());
                    };
                }
                else
                    throw new Exception("Order socket failed");


                _orderSocketSub = sub.Data;
                _isConnected = true;
                _reconcileCts = new CancellationTokenSource();
                _reconcileTask = Task.Run(() => ReconcileLoop(_reconcileCts.Token));
            }
        }

        private void InitialPositionAndCashSync()
        {
            try
            {
                // 1. Cash laden (hast du bereits implementiert)
                var auth = RunSync(() => _balanceClient.GetBalancesAsync(new GetBalancesRequest()));
                if (!auth.Success) throw new Exception("Authentication failed");

                // 2. Positionen laden
                var holdings = GetAccountHoldings();
                foreach (var holding in holdings)
                {
                    if (holding.Quantity != 0)
                    {
                        Log.Trace($"{Name}.InitialSync: Vorhandene Position gefunden: {holding.Symbol} {holding.Quantity}");
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"{Name}.InitialPositionAndCashSync(): {ex.Message}");
            }
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