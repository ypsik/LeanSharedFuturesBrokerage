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
        private readonly ConcurrentDictionary<Symbol, (DateTime? NextFundingTime, decimal Rate)> _lastFundingState = new();

        protected virtual int? FundingRolloverHours => 8;

        /// <summary>
        /// true: Rate wird sofort emittiert, sobald sich der vom Socket gelieferte Wert ändert
        /// (z.B. Kraken: Rate steht bereits zu Stundenbeginn fix, kein Grund auf Rollover zu warten).
        /// false (Default): Rate wird erst beim nächsten Rollover-Trigger emittiert, mit dem zuvor
        /// gültigen (jetzt final abgeschlossenen) Wert — passend für Exchanges mit TWAP-Settlement,
        /// bei denen die Rate erst am Ende der Periode final feststeht (z.B. Bybit, Hyperliquid).
        /// </summary>
        protected virtual bool EmitFundingRateImmediately => false;

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
            var sharedSymbol = GetSharedSymbol(request.Symbol);
            Log.Trace($"GetHistory called: Symbol={sharedSymbol.BaseAsset + sharedSymbol.QuoteAsset}, DataType={request.DataType.Name}, Resolution={request.Resolution}, StartUtc={request.StartTimeUtc}, EndUtc={request.EndTimeUtc}");

            var minStartTimeUtc = request.EndTimeUtc.AddMinutes(-MaxHistoryLookbackMinutes);
            var startTimeUtc = request.StartTimeUtc < minStartTimeUtc ? minStartTimeUtc : request.StartTimeUtc;

            if (request.DataType == typeof(MarginInterestRate))
            {
                if (_fundingRateClient == null) yield break;

                var fundingReq = new GetFundingRateHistoryRequest(sharedSymbol)
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
                    HttpResult<SharedFundingRate[]>? res = null;
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
                            Log.Error($"Rate Limit hit for {sharedSymbol.SymbolName} (MarginInterestRate). Retry {retryCount}/{maxRetries} after {delay}ms...");
                            System.Threading.Thread.Sleep(delay);
                            continue;
                        }
                        break;
                    }

                    if (res == null || !res.Success || res.Data == null)
                    {
                        if (pagesLoaded > 0)
                        {
                            Log.Trace($"GetHistory (MarginInterestRate) for {sharedSymbol.SymbolName} stopped paging at page {pagesLoaded + 1} (No more data or end of timeline). Total loaded: {totalRatesLoaded}");
                            break;
                        }

                        string diag = res == null
                            ? "Result object is completely NULL (RunSync failed)."
                            : $"Success={res.Success}, DataIsNull={res.Data == null}, ErrorMsg='{res.Error?.Message}', Code='{res.Error?.Code}'";
                        Log.Error($"GetHistory Error (MarginInterestRate) for {sharedSymbol.SymbolName}: {diag}");
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
                var interval = request.Resolution switch
                {
                    Resolution.Minute => (SharedKlineInterval?)SharedKlineInterval.OneMinute,
                    Resolution.Hour => SharedKlineInterval.OneHour,
                    Resolution.Daily => SharedKlineInterval.OneDay,
                    _ => null
                };
                if (interval == null) yield break;

                var klineReq = new GetKlinesRequest(sharedSymbol, interval.Value) { StartTime = startTimeUtc, EndTime = request.EndTimeUtc, ExchangeParameters = GetKlinesHistoryParameters };

                int totalCandlesLoaded = 0;
                int pagesLoaded = 0;
                PageRequest? nextPage = null;
                do
                {
                    HttpResult<SharedKline[]>? res = null;
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
                            Log.Error($"Rate Limit hit for {sharedSymbol.SymbolName} (Klines). Retry {retryCount}/{maxRetries} after {delay}ms...");
                            System.Threading.Thread.Sleep(delay);
                            continue;
                        }
                        break;
                    }

                    if (res == null || !res.Success || res.Data == null)
                    {
                        if (pagesLoaded > 0)
                        {
                            Log.Trace($"GetHistory (Klines) for {sharedSymbol.SymbolName} stopped paging at page {pagesLoaded + 1} (No more data or end of timeline). Total loaded: {totalCandlesLoaded}");
                            break;
                        }

                        string diag = res == null
                            ? "Result object is completely NULL (RunSync failed)."
                            : $"Success={res.Success}, DataIsNull={res.Data == null}, ErrorMsg='{res.Error?.Message}', Code='{res.Error?.Code}'";
                        Log.Error($"GetHistory Error (Klines) for {sharedSymbol.SymbolName}: {diag}");
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

        protected virtual ExchangeParameters TradesExchangeParameters => new ExchangeParameters();
        protected virtual ExchangeParameters BookTickerExchangeParameters => new ExchangeParameters();

        private bool SubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType)
        {
            foreach (var symbol in symbols)
            {
                var shared = GetSharedSymbol(symbol);

                // FIX: NativeTicker() kann für Symbole werfen, die nicht in unserer SPDB stehen
                // (z.B. LEANs automatische Cross-Currency-Cash-Konvertierungs-Subscriptions wie
                // XAUEUR/USDCEUR, die durch AccountCurrency-Umstellungen entstehen und nichts mit
                // unseren tatsächlich gehandelten Instrumenten zu tun haben). Ohne diesen Schutz
                // crasht ein einzelnes unbekanntes Symbol die gesamte Subscription-Runde und damit
                // den kompletten Live-Algorithmus. Wir überspringen daher nur dieses eine Symbol.
                string subKey;
                try
                {
                    subKey = $"{NativeTicker(symbol)}_{tickType.ToString()}";
                }
                catch (Exception ex)
                {
                    Log.Trace($"{Name}.SubscribeSymbols: Skipping {symbol.Value} ({tickType}) — NativeTicker lookup failed: {ex.Message}");
                    continue;
                }

                if (_subscriptions.ContainsKey(subKey)) continue;

                _subRateGate.WaitToProceed();

                if (tickType == TickType.Trade)
                {
                    var sub = RunSync(() => _tradeSocket.SubscribeToTradeUpdatesAsync(
                        new SubscribeTradeRequest(shared, TradesExchangeParameters),
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

                    SetupSubscriptionEvents(sub?.Success ?? false, sub?.Data, _ => { }, $"{symbol.Value} Trade", $"Trade subscription failed for {symbol.Value}", sub?.Error?.ToString());
                    if (sub?.Success ?? false)
                    {
                        _subscriptions[subKey] = sub.Data;
                    }
                }
                else if (tickType == TickType.Quote)
                {
                    var sub = RunSync(() => _bookTickerSocket.SubscribeToBookTickerUpdatesAsync(
                        new SubscribeBookTickerRequest(shared, BookTickerExchangeParameters),
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

                    SetupSubscriptionEvents(sub?.Success ?? false, sub?.Data, _ => { }, $"{symbol.Value} Quote", $"Quote subscription failed for {symbol.Value}", sub?.Error?.ToString());
                    if (sub?.Success ?? false)
                    {
                        _subscriptions[subKey] = sub.Data;
                    }
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

                bool onFundingRate(DateTime now, decimal? fundingRate, DateTime? nextFundingTime)
                {
                    bool isFirstTick = false;
                    bool isRollover = false;
                    decimal rateToReport = 0m;

                    // Fixed cycle (e.g. HL 1h): next funding = current hour + interval
                    // Exchange-driven (e.g. Bybit): use nextFundingTime directly from socket
                    DateTime? currentNextFundingTime = FundingRolloverHours.HasValue
                        ? new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Utc)
                              .AddHours(FundingRolloverHours.Value)
                        : nextFundingTime;

                    _lastFundingState.AddOrUpdate(
                        symbol,
                        addValueFactory: _ =>
                        {
                            isFirstTick = true;
                            // Store null if we don't know nextFundingTime yet — will be set on next tick
                            return (currentNextFundingTime, fundingRate ?? 0m);
                        },
                        updateValueFactory: (_, state) =>
                        {
                            if (state.NextFundingTime == null)
                            {
                                // First tick with real nextFundingTime: set it, no rollover
                                return (currentNextFundingTime, fundingRate ?? state.Rate);
                            }

                            if ((currentNextFundingTime ?? now) > state.NextFundingTime)
                            {
                                isRollover = true;
                                // Default (z.B. Bybit/Hyperliquid, 8h-TWAP-Settlement): der alte,
                                // jetzt final abgeschlossene Wert wird gemeldet, da die neue Rate
                                // beim TWAP-Settlement-Exchange erst am Ende der Periode final feststeht.
                                // EmitFundingRateImmediately (z.B. Kraken): die Rate steht bereits zu
                                // Periodenbeginn fix, daher direkt den neuen (aktuell gültigen) Wert melden.
                                rateToReport = EmitFundingRateImmediately ? (fundingRate ?? state.Rate) : state.Rate;
                                return (currentNextFundingTime, fundingRate ?? state.Rate);
                            }

                            return (state.NextFundingTime, fundingRate ?? state.Rate);
                        });

                    if (isFirstTick) return false;
                    if (!isRollover) return false;

                    _aggregator.Update(new MarginInterestRate { Symbol = symbol, Time = now, InterestRate = rateToReport });
                    Log.Trace($"{Name} Funding Update: {symbol.Value} -> Rate: {rateToReport} (Rollover)");

                    return true;
                }

                var sub = RunSync(() => CreateFundingSubscriptionAsync(nativeTicker, symbol, onFundingRate));

                SetupSubscriptionEvents(sub?.Success ?? false, sub?.Data, _ => { },
                  $"MarginInterestRate {nativeTicker}",
                  $"SubscribeMarginInterestRate failed for {symbol}: {sub?.Error?.Message}",
                  sub?.Error?.ToString());

                if (sub?.Success ?? false)
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

        protected virtual Task<WebSocketResult<UpdateSubscription>> CreateFundingSubscriptionAsync(
            string nativeTicker, Symbol symbol, Func<DateTime, decimal?, DateTime?, bool> onFundingRate)
            => Task.FromResult(new WebSocketResult<UpdateSubscription>(Name, null, new InvalidOperationError("Funding subscription not supported by this exchange")));

        protected void EmitTick(Tick tick) => _aggregator?.Update(tick);
    }
}