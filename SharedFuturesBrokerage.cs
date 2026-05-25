using CryptoExchange.Net.Interfaces.Clients;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using QLNet;
using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Util;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;


namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    // WICHTIG: abstract partial muss hier stehen!
    public abstract partial class SharedFuturesBrokerage : Brokerage, IDataQueueHandler
    {
        protected IAlgorithm _algorithm;
        protected SecurityTransactionManager _orderManager;

        protected IBookTickerSocketClient _bookTickerSocket;
        protected ITradeSocketClient _tradeSocket;
        protected IFuturesOrderRestClient _orderClient;
        protected IBalanceRestClient _balanceClient;
        protected IFuturesOrderSocketClient _orderSocket;
        protected IUserTradeSocketClient _userTradeSocket;
        protected IBalanceSocketClient _balanceSocket;
        protected IKlineRestClient _klineClient;
        protected IFundingRateRestClient _fundingRateClient;
        protected Func<List<Holding>>? _getHoldingsFunc;

        protected SymbolPropertiesDatabase _spdb;

        protected IDataAggregator _aggregator;
        protected EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager;
        protected bool _isInitialized;
        protected LiveNodePacket _job;

        protected UpdateSubscription _orderSocketSub, _userTradeSocketSub;
        protected readonly object _connectLock = new();
        protected readonly object _balanceUpdatesConnectLock = new();
        private bool _isConnectedOrder, _isConnectedUserTrade, _isConnectedBalance;
        protected CancellationTokenSource _reconcileCts;
        protected Task? _reconcileTask = null;
        protected readonly TimeSpan _reconciliationInterval = TimeSpan.FromSeconds(30);

        protected static readonly RateGate _subRateGate = new RateGate(3, TimeSpan.FromSeconds(1));

        private static DateTime _startTime = DateTime.UtcNow;
        protected static DateTime StartTime => _startTime;

        protected virtual string SettleAsset => "USDT";

        protected SharedFuturesBrokerage(string exchangeName) : base(exchangeName)
        {
            _spdb = SymbolPropertiesDatabase.FromDataFolder();
        }
        protected SharedFuturesBrokerage(IAlgorithm algorithm, string exchangeName) : base(exchangeName)
        {
            _algorithm = algorithm;
            _orderManager = _algorithm.Transactions;
            _spdb = SymbolPropertiesDatabase.FromDataFolder();
        }

        protected void InitializeBase(
            IFuturesOrderRestClient orderClient,
            IBalanceRestClient balanceClient,
            IBookTickerSocketClient bookTickerSocket,
            IFuturesOrderSocketClient orderSocket,
            ITradeSocketClient tradeSocket,
            IUserTradeSocketClient userTradeSocket,
            IBalanceSocketClient balanceSocket,
            IFundingRateRestClient fundingRateClient,
            IKlineRestClient klineClient,
            IDataAggregator aggregator, // <-- Der kommt jetzt an
            Func<List<Holding>>? getHoldingsFunc = null)
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
            _bookTickerSocket = bookTickerSocket;
            _orderSocket = orderSocket;
            _tradeSocket = tradeSocket;
            _userTradeSocket = userTradeSocket;
            _balanceSocket = balanceSocket;
            _fundingRateClient = fundingRateClient;
            _klineClient = klineClient;
            _aggregator = aggregator;
            _getHoldingsFunc = getHoldingsFunc;

            _spdb = SymbolPropertiesDatabase.FromDataFolder();

            _subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager(
                tickType => tickType.ToString() // "Trade" ≠ "Quote" → separate Channels
            );
            
            _subscriptionManager.SubscribeImpl += (symbols, tickType) => SubscribeSymbols(symbols, tickType);
            _subscriptionManager.UnsubscribeImpl += (symbols, tickType) => UnsubscribeSymbols(symbols, tickType);

            _isInitialized = true;
        }

        public override bool AccountInstantlyUpdated { get; } = true;

        public override bool IsConnected => _isConnectedOrder && _isConnectedUserTrade && _isConnectedBalance;

        protected virtual ExchangeParameters UserTradesExchangeParameters => new ExchangeParameters();
        protected virtual ExchangeParameters OrderUpdatesExchangeParameters => new ExchangeParameters();

        public override void Connect()
        {
            lock (_connectLock)
            {
                if (_balanceClient == null || _orderSocket == null || _userTradeSocket == null) throw new InvalidOperationException("Clients not configured");

                if (_reconcileTask == null)
                {
                    _reconcileCts = new CancellationTokenSource();
                    _reconcileTask = Task.Run(() => ReconcileLoop(_reconcileCts.Token));
                }

                if (!_isConnectedUserTrade)
                {
                    _subRateGate.WaitToProceed();

                    var userTradeRequest = new CryptoExchange.Net.SharedApis.SubscribeUserTradeRequest
                    {
                        ExchangeParameters = UserTradesExchangeParameters // <--- Nutzt die spezifische Property für User Trades
                    };

                    var sub = RunSync(() => _userTradeSocket.SubscribeToUserTradeUpdatesAsync(userTradeRequest, HandleUserTradeSocket));
                    
                    SetupSubscriptionEvents(sub.Success, sub.Data, state => _isConnectedUserTrade = state, "User trade", "User trade socket failed", sub.Error?.ToString());
                    if (sub.Success)
                    {
                        _userTradeSocketSub = sub.Data;
                    }

                    
                }

                if (!_isConnectedOrder)
                {
                    _subRateGate.WaitToProceed();
                    var sub = RunSync(() => _orderSocket.SubscribeToFuturesOrderUpdatesAsync(new SubscribeFuturesOrderRequest
                        {
                            ExchangeParameters = OrderUpdatesExchangeParameters
                        }, 
                        HandleOrderSocket));

                    SetupSubscriptionEvents(sub.Success, sub.Data, state => _isConnectedOrder = state, "Order", "Order socket failed", sub.Error?.ToString());
                    if (sub.Success)
                    {
                        _orderSocketSub = sub.Data;
                    }
                }

                if (!_isConnectedBalance)
                {
                    lock (_balanceUpdatesConnectLock)
                        RunSync(() => SubscribeToBalanceUpdatesAsync());
                }
            }
        }

        protected void SetupSubscriptionEvents(bool isSuccess, dynamic subscriptionData, Action<bool> setConnectedState, string streamName, string errorMessage, string? errorDetails = null)
        {
            if (isSuccess)
            {
                Log.Trace($"{Name} {streamName}: Subscribed.");

                subscriptionData.ConnectionLost += new Action(() =>
                {
                    setConnectedState(false);
                    Log.Error($"{streamName}: Connection lost!");
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "Disconnect", $"{streamName} stream lost."));
                });

                subscriptionData.ConnectionRestored += new Action<TimeSpan>((duration) =>
                {
                    setConnectedState(true);
                    Log.Trace($"{streamName}: Connection restored after {duration}.");
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Information, "Reconnect", $"{streamName} stream restored. Syncing..."));
                    //Task.Run(async () => await ForceReconcile());
                });

                setConnectedState(true);
            }
            else
            {
                Log.Error($"{streamName} {errorMessage} | Details: {errorDetails ?? "No additional error info available."}");
            }
        }

        public override void Disconnect()
        {
            _reconcileCts?.Cancel();
            if (_userTradeSocketSub != null) RunSync(() => _userTradeSocketSub.CloseAsync());
            if (_orderSocketSub != null) RunSync(() => _orderSocketSub.CloseAsync());
            if (_balanceUpdatesSocketSub != null) RunSync(() => _balanceUpdatesSocketSub.CloseAsync());            
            _isConnectedUserTrade = false;
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