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
        private readonly ConcurrentDictionary<Symbol, (int Hour, decimal Rate)> _lastFundingState = new();

        protected virtual int FundingRolloverHours => 8;

        protected virtual int MaxHistoryLookbackMinutes => 7200;

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

        protected virtual ExchangeParameters GetFundingRateHistoryParameters => new ExchangeParameters();
        protected virtual ExchangeParameters GetKlinesHistoryParameters => new ExchangeParameters();

        public override IEnumerable<BaseData> GetHistory(QuantConnect.Data.HistoryRequest request)
        {
            Log.Trace($"GetHistory called: Symbol={request.Symbol}, DataType={request.DataType.Name}, Resolution={request.Resolution}, StartUtc={request.StartTimeUtc}, EndUtc={request.EndTimeUtc}");

            var minStartTimeUtc = request.EndTimeUtc.AddMinutes(-MaxHistoryLookbackMinutes);
            var startTimeUtc = request.StartTimeUtc < minStartTimeUtc ? minStartTimeUtc : request.StartTimeUtc;

            if (request.DataType == typeof(MarginInterestRate))
            {
                if (_fundingRateClient == null) yield break;

                var fundingReq = new GetFundingRateHistoryRequest(GetSharedSymbol(request.Symbol))
                {
                    StartTime = startTimeUtc,
                    EndTime = request.EndTimeUtc,
                    ExchangeParameters = GetFundingRateHistoryParameters
                };

                int totalRatesLoaded = 0;
                int pagesLoaded = 0;
                PageRequest? nextPage = null;
                do
                {
                    ExchangeWebResult<SharedFundingRate[]> res = null;
                    int retryCount = 0;
                    const int maxRetries = 5;

                    while (retryCount < maxRetries)
                    {
                        res = RunSync(
                            async () =>
                            {
                                await Task.Delay(150).ConfigureAwait(false);
                                return await _fundingRateClient.GetFundingRateHistoryAsync(fundingReq, nextPage).ConfigureAwait(false);
                            }
                        );

                        if (res.Success && res.Data != null)
                            break;

                        if (res.Error?.Message != null && (res.Error.Message.Contains("Too many visits") || res.Error.Message.Contains("Rate Limit")))
                        {
                            retryCount++;
                            int delay = 1200 * retryCount;
                            Log.Error($"Rate Limit hit for {request.Symbol} (MarginInterestRate). Retry {retryCount}/{maxRetries} after {delay}ms...");
                            System.Threading.Thread.Sleep(delay);
                            continue;
                        }
                        break;
                    }

                    if (res == null || !res.Success || res.Data == null)
                    {
                        if (pagesLoaded > 0)
                        {
                            Log.Trace($"GetHistory (MarginInterestRate) for {request.Symbol} stopped paging at page {pagesLoaded + 1} (No more data or end of timeline). Total loaded: {totalRatesLoaded}");
                            break;
                        }

                        string diag = res == null
                            ? "Result object is completely NULL (RunSync failed)."
                            : $"Success={res.Success}, DataIsNull={res.Data == null}, ErrorMsg='{res.Error?.Message}', Code='{res.Error?.Code}'";
                        Log.Error($"GetHistory Error (MarginInterestRate) for {request.Symbol}: {diag}");
                        yield break;
                    }

                    if (res.Data.Length == 0) break;

                    foreach (var rate in res.Data.OrderBy(r => r.Timestamp))
                    {
                        yield return new MarginInterestRate { Symbol = request.Symbol, Time = rate.Timestamp, InterestRate = rate.FundingRate };
                        totalRatesLoaded++;
                    }

                    pagesLoaded++;
                    nextPage = res.NextPageRequest;
                } while (nextPage != null);
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

                var klineReq = new GetKlinesRequest(shared, interval.Value) { StartTime = startTimeUtc, EndTime = request.EndTimeUtc, ExchangeParameters = GetKlinesHistoryParameters };

                int totalCandlesLoaded = 0;
                int pagesLoaded = 0;
                PageRequest? nextPage = null;
                do
                {
                    ExchangeWebResult<SharedKline[]>? res = null;
                    int retryCount = 0;
                    const int maxRetries = 5;

                    while (retryCount < maxRetries)
                    {
                        res = RunSync(
                            async () =>
                            {
                                await Task.Delay(150).ConfigureAwait(false);
                                return await _klineClient.GetKlinesAsync(klineReq, nextPage).ConfigureAwait(false);
                            }
                        );

                        if (res.Success && res.Data != null)
                            break;

                        if (res.Error?.Message != null && (res.Error.Message.Contains("Too many visits") || res.Error.Message.Contains("Rate Limit")))
                        {
                            retryCount++;
                            int delay = 1200 * retryCount;
                            Log.Error($"Rate Limit hit for {request.Symbol} (Klines). Retry {retryCount}/{maxRetries} after {delay}ms...");
                            System.Threading.Thread.Sleep(delay);
                            continue;
                        }
                        break;
                    }

                    if (res == null || !res.Success || res.Data == null)
                    {
                        if (pagesLoaded > 0)
                        {
                            Log.Trace($"GetHistory (Klines) for {request.Symbol} stopped paging at page {pagesLoaded + 1} (No more data or end of timeline). Total loaded: {totalCandlesLoaded}");
                            break;
                        }

                        string diag = res == null
                            ? "Result object is completely NULL (RunSync failed)."
                            : $"Success={res.Success}, DataIsNull={res.Data == null}, ErrorMsg='{res.Error?.Message}', Code='{res.Error?.Code}'";
                        Log.Error($"GetHistory Error (Klines) for {request.Symbol}: {diag}");
                        yield break;
                    }

                    if (res.Data.Length == 0) break;

                    foreach (var bar in res.Data.OrderBy(b => b.OpenTime))
                    {
                        if (request.DataType == typeof(QuoteBar))
                        {
                            yield return new QuoteBar
                            {
                                Symbol = request.Symbol,
                                Time = bar.OpenTime,
                                Bid = new Bar(bar.OpenPrice, bar.HighPrice, bar.LowPrice, bar.ClosePrice),
                                Ask = new Bar(bar.OpenPrice, bar.HighPrice, bar.LowPrice, bar.ClosePrice),
                                Value = bar.ClosePrice,
                                Period = request.Resolution.ToTimeSpan()
                            };
                        }
                        else
                        {
                            yield return new TradeBar
                            {
                                Symbol = request.Symbol,
                                Time = bar.OpenTime,
                                Open = bar.OpenPrice,
                                High = bar.HighPrice,
                                Low = bar.LowPrice,
                                Close = bar.ClosePrice,
                                Volume = bar.Volume,
                                Period = request.Resolution.ToTimeSpan()
                            };
                        }
                        totalCandlesLoaded++;
                    }

                    pagesLoaded++;
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

                    SetupSubscriptionEvents(sub.Success, sub.Data, _ => { }, $"{symbol.Value} Trade", $"Trade subscription failed for {symbol.Value}", sub.Error?.ToString());
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

                    SetupSubscriptionEvents(sub.Success, sub.Data, _ => { }, $"{symbol.Value} Quote", $"Quote subscription failed for {symbol.Value}", sub.Error?.ToString());
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

                // Signatur auf decimal? geändert
                Func<DateTime, decimal?, bool> onFundingRate = (now, fundingRate) =>
                {
                    bool isFirstTick = false;
                    bool isHourRollover = false;
                    decimal rateToReport = 0m;

                    var currentHour = (now.Hour / FundingRolloverHours) * FundingRolloverHours;

                    // _lastFundingState ist ein ConcurrentDictionary<Symbol, (int Hour, decimal Rate)>
                    _lastFundingState.AddOrUpdate(
                      symbol,
                      addValueFactory: _ =>
                      {
                          isFirstTick = true;
                          return (currentHour, fundingRate ?? 0m);
                      },
                      updateValueFactory: (_, state) =>
                      {
                          if (state.Hour != currentHour)
                          {
                              isHourRollover = true;
                              rateToReport = state.Rate;
                              return (currentHour, fundingRate ?? state.Rate);
                          }

                          // Innerhalb der Stunde: Nur aktualisieren, wenn ein Wert vorhanden ist
                          return (state.Hour, fundingRate ?? state.Rate);
                      });

                    if (!isFirstTick && !isHourRollover) return false;

                    if (isHourRollover)
                    {
                        var roundedTime = new DateTime(now.Year, now.Month, now.Day, currentHour, 0, 0, DateTimeKind.Utc);
                        // Nutzt die ermittelte rateToReport (entweder neu vom Exchange oder aus dem Cache)
                        _aggregator.Update(new MarginInterestRate { Symbol = symbol, Time = roundedTime, InterestRate = rateToReport });
                        Log.Trace($"{Name} Funding Update: {symbol.Value} -> Rate: {rateToReport} (Rollover)");
                    }

                    return true;
                };

                var sub = RunSync(() => CreateFundingSubscriptionAsync(nativeTicker, symbol, onFundingRate));

                SetupSubscriptionEvents(sub.Success, sub.Data, _ => { },
                  $"Funding {nativeTicker}",
                  $"SubscribeFunding failed for {symbol}: {sub.Error?.Message}",
                  sub.Error?.ToString());

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
            string nativeTicker, Symbol symbol, Func<DateTime, decimal?, bool> onFundingRate);

        protected void EmitTick(Tick tick) => _aggregator?.Update(tick);
    }
}