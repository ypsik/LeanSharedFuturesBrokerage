using CryptoExchange.Net.Interfaces.Clients;
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

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    public abstract partial class SharedFuturesBrokerage
    {

        protected readonly ConcurrentDictionary<string, UpdateSubscription> _subscriptions = new();

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
            if (request.DataType == typeof(MarginInterestRate))
            {
                if (_fundingRateClient == null) yield break;
                var res = RunSync(() => _fundingRateClient.GetFundingRateHistoryAsync(
                    new GetFundingRateHistoryRequest(GetSharedSymbol(request.Symbol))
                    {
                        StartTime = request.StartTimeUtc,
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

                var klineReq = new GetKlinesRequest(shared, interval.Value) { StartTime = request.StartTimeUtc, EndTime = request.EndTimeUtc };
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
                var subKey = $"{symbol.Value}_{tickType}";
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
                if (_subscriptions.TryRemove($"{symbol.Value}_{tickType}", out var sub))
                {
                    RunSync(() => sub.CloseAsync());
                }
            }
            return true;
        }
        #endregion


        protected abstract bool SubscribeFunding(Symbol symbol);
        protected abstract bool UnsubscribeFunding(Symbol symbol);
        protected void EmitTick(Tick tick) => _aggregator?.Update(tick);
    }
}