using CryptoExchange.Net.SharedApis;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    public abstract partial class SharedFuturesBrokerage
    {
        #region IDataQueueHandler
        public virtual IEnumerator<BaseData> Subscribe(SubscriptionDataConfig config, EventHandler handler)
        {
            if (!_isInitialized || config.Symbol.Value.Contains("UNMAPPED")) return null;

            var enumerator = _aggregator.Add(config, handler);
            _subscriptionManager.Subscribe(config);
            return enumerator;
        }

        public virtual void Unsubscribe(SubscriptionDataConfig config)
        {
            if (_isInitialized)
            {
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
                PageRequest nextPage = null;
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

        protected abstract bool SubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType);
        protected abstract bool UnsubscribeSymbols(IEnumerable<Symbol> symbols, TickType tickType);
        protected void EmitTick(Tick tick) => _aggregator?.Update(tick);
    }
}