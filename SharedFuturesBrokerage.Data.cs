using CryptoExchange.Net.Interfaces.Clients;
using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    public abstract partial class SharedFuturesBrokerage
    {
        private readonly ConcurrentDictionary<string, UpdateSubscription> _subscriptions = new();
        private readonly object _fundingLock = new();
        private readonly ConcurrentDictionary<Symbol, int> _lastFundingHour = new();

        protected int _maxHistoryLookbackDays = 5;

        #region IDataQueueHandler
        public virtual IEnumerator<BaseData> Subscribe(SubscriptionDataConfig config, EventHandler handler)
        {
            Log.Trace($"Subscribe() called: {config.Symbol} | {config.Type.Name}");

            if (!_isInitialized || config.Symbol.Value.Contains("UNMAPPED")) return null;

            var enumerator = _aggregator.Add(config, handler);
            if (config.Type == typeof(MarginInterestRate))
                SubscribeFunding(config.Symbol);
            else
                _subscriptionManager.Subscribe(config);
            return enumerator;
        }

        public virtual void Unsubscribe(SubscriptionDataConfig config)
        {
            if (_isInitialized)
            {
                if (config.Type == typeof(MarginInterestRate))
                    UnsubscribeFunding(config.Symbol);
                else
                    _subscriptionManager.Unsubscribe(config);
                _aggregator.Remove(config);
            }
        }
        #endregion

        #region History Implementation
        public override IEnumerable<BaseData> GetHistory(QuantConnect.Data.HistoryRequest request)
        {
            var minStartTimeUtc = request.EndTimeUtc.AddDays(-_maxHistoryLookbackDays);
            var startTimeUtc = request.StartTimeUtc < minStartTimeUtc ? minStartTimeUtc : request.StartTimeUtc;

            if (request.DataType == typeof(MarginInterestRate))
            {
                if (_fundingRateClient == null) yield break;
                var res = RunSync(() => _fundingRateClient.GetFundingRateHistoryAsync(
                    new GetFundingRateHistoryRequest(GetSharedSymbol(request.Symbol))
                    {
                        StartTime = startTimeUtc,
                        EndTime = request.EndTimeUtc
                    }));

                if (res.Success && res.Data != null)
                    foreach (var rate in res.Data.OrderBy(r => r.Timestamp))
                        yield return new MarginInterestRate { Symbol = request.Symbol, Time = rate.Timestamp, InterestRate = rate.FundingRate };
            }
            else
            {
                if (_klineClient == null) yield break;
                var shared = GetSharedSymbol(request.Symbol);
                var interval = request.Resolution switch
                {
                    Resolution.Minute => (SharedKlineInterval?)SharedKlineInterval.OneMinute,
                    Resolution.Hour => SharedKlineInterval.OneHour,
                    Resolution.Daily => SharedKlineInterval.OneDay,
                    _ => null
                };
                if (interval == null) yield break;

                var klineReq = new GetKlinesRequest(shared, interval.Value) { StartTime = startTimeUtc, EndTime = request.EndTimeUtc };
                PageRequest? nextPage = null;
                do
                {
                    var res = RunSync(() => _klineClient.GetKlinesAsync(klineReq, nextPage));
                    if (!res.Success || res.Data == null) yield break;
                    foreach (var bar in res.Data.OrderBy(b => b.OpenTime))
                        yield return new TradeBar { Symbol = request.Symbol, Time = bar.OpenTime, Open = bar.OpenPrice, High = bar.HighPrice, Low = bar.LowPrice, Close = bar.ClosePrice, Volume = bar.Volume, Period = request.Resolution.ToTimeSpan() };
                    nextPage = res.NextPageRequest;
                } while (nextPage != null);
            }
        }
        #endregion

        #region LEAN Data Manager
        private bool SubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType)
        {
            foreach (var symbol in symbols)
            {
                var shared = GetSharedSymbol(symbol);
                var subKey = $"{NativeTicker(symbol)}_{tickType.ToString()}";
                if (_subscriptions.ContainsKey(subKey)) continue;

                _subRateGate.WaitToProceed();

                if (tickType == TickType.Trade)
                {
                    var sub = RunSync(() => _tradeSocket.SubscribeToTradeUpdatesAsync(
                        new SubscribeTradeRequest(shared),
                        update =>
                        {
                            foreach (var item in update.Data)
                            {
                                EmitTick(new Tick
                                {
                                    Symbol = symbol,
                                    Time = item.Timestamp.ToUniversalTime(),
                                    TickType = TickType.Trade,
                                    Value = item.Price,
                                    Quantity = item.Quantity
                                });
                            }
                        }));

                    SetupSubscriptionEvents(sub.Success, sub.Data, _ => { }, $"{symbol.Value} Trade", $"Trade subscription failed for {symbol.Value}");
                    if (sub.Success)
                    {
                        _subscriptions[subKey] = sub.Data;
                    }
                }
                else if (tickType == TickType.Quote)
                {
                    var sub = RunSync(() => _bookTickerSocket.SubscribeToBookTickerUpdatesAsync(
                        new SubscribeBookTickerRequest(shared),
                        update =>
                        {
                            var q = update.Data;
                            EmitTick(new Tick
                            {
                                Symbol = symbol,
                                Time = DateTime.UtcNow,
                                TickType = TickType.Quote,
                                BidPrice = q.BestBidPrice,
                                BidSize = q.BestBidQuantity,
                                AskPrice = q.BestAskPrice,
                                AskSize = q.BestAskQuantity
                            });
                        }));

                    SetupSubscriptionEvents(sub.Success, sub.Data, _ => { }, $"{symbol.Value} Quote", $"Quote subscription failed for {symbol.Value}");
                    if (sub.Success)
                    {
                        _subscriptions[subKey] = sub.Data;
                    }

                    if (sub.Success) _subscriptions[subKey] = sub.Data;
                }
            }
            return true;
        }

        private bool UnsubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType)
        {
            foreach (var symbol in symbols)
            {
                var subKey = $"{NativeTicker(symbol)}_{tickType.ToString()}";
                if (_subscriptions.TryRemove(subKey, out var sub))
                {
                    RunSync(() => sub.CloseAsync());
                }
            }
            return true;
        }
        #endregion


        protected virtual bool SubscribeFunding(Symbol symbol)
        {
            var nativeTicker = NativeTicker(symbol);
            var subKey = $"{nativeTicker}_FUNDING";

            lock (_fundingLock)
            {
                if (_subscriptions.ContainsKey(subKey)) return true;

                _subRateGate.WaitToProceed();

                // Base builds the funding handler. HL captures now in the socket callback and passes it in.
                // Returns true on first tick or rollover — HL uses this to gate exchange-specific logic.
                Func<DateTime, decimal, bool> onFundingRate = (now, fundingRate) =>
                {
                    bool isFirstTick = false;
                    bool isHourRollover = false;
                    var currentHour = now.Hour;

                    _lastFundingHour.AddOrUpdate(
                        symbol,
                        addValueFactory: _ => { isFirstTick = true; return currentHour; },
                        updateValueFactory: (_, oldHour) =>
                        {
                            if (oldHour != currentHour) { isHourRollover = true; return currentHour; }
                            return oldHour;
                        });

                    if (!isFirstTick && !isHourRollover) return false;

                    if (isHourRollover)
                    {
                        var roundedTime = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Utc);
                        _aggregator.Update(new MarginInterestRate { Symbol = symbol, Time = roundedTime, InterestRate = fundingRate });
                        Log.Trace($"{Name} Funding Update: {symbol.Value} -> Rate: {fundingRate}");
                    }

                    return true;
                };

                var sub = RunSync(() => CreateFundingSubscriptionAsync(nativeTicker, symbol, onFundingRate));

                SetupSubscriptionEvents(sub.Success, sub.Data, _ => { },
                    $"Funding {nativeTicker}",
                    $"SubscribeFunding failed for {symbol}: {sub.Error?.Message}");

                if (sub.Success)
                {
                    _subscriptions.TryAdd(subKey, sub.Data);
                    return true;
                }
                return false;
            }
        }

        protected virtual bool UnsubscribeFunding(Symbol symbol)
        {
            var nativeTicker = NativeTicker(symbol);
            var subKey = $"{nativeTicker}_FUNDING";
            if (_subscriptions.TryRemove(subKey, out var sub))
                RunSync(() => sub.CloseAsync());
            return true;
        }

        protected abstract Task<CallResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
            string nativeTicker, Symbol symbol, Func<DateTime, decimal, bool> onFundingRate);

        protected void EmitTick(Tick tick) => _aggregator?.Update(tick);
    }
}