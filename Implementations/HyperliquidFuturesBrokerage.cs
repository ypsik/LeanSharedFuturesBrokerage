using CryptoExchange.Net.SharedApis;
using HyperLiquid.Net.Clients;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Logging;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    public class HyperliquidFuturesBrokerage : SharedFuturesBrokerage, IDataQueueHandler
    {
        private readonly HyperLiquidRestClient _restClient;
        private readonly ConcurrentQueue<BaseData> _ticks = new();

        public HyperliquidFuturesBrokerage(
            HyperLiquidRestClient restClient,
            HyperLiquidSocketClient socketClient,
            Func<List<Holding>> getHoldingsFunc)
            : base(
                "HyperLiquid",
                restClient.FuturesApi.SharedClient,
                restClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                socketClient.FuturesApi.SharedClient,
                getHoldingsFunc)
        {
            _restClient = restClient;
        }

        // ─────────────────────────────────────────────
        // SYMBOL MAPPING
        // ─────────────────────────────────────────────

        protected override string NormalizeSymbol(string rawSymbol)
            => rawSymbol.EndsWith("USDC", StringComparison.OrdinalIgnoreCase)
                ? rawSymbol.ToUpperInvariant()
                : rawSymbol.ToUpperInvariant() + "USDC";

        protected override SharedSymbol GetSharedSymbol(Symbol s)
        {
            var ticker = s.Value.ToUpperInvariant();

            var coin = ticker.EndsWith("USDC")
                ? ticker[..^4]
                : ticker;

            return new SharedSymbol(
                TradingMode.PerpetualLinear,
                coin,
                "USDC");
        }

        // ─────────────────────────────────────────────
        // SUBSCRIBE (WARMUP + LIVE)
        // ─────────────────────────────────────────────

        public IEnumerator<BaseData> Subscribe(
            SubscriptionDataConfig config,
            EventHandler newDataAvailable)
        {
            var symbol = config.Symbol;
            var shared = GetSharedSymbol(symbol);

            // 1️⃣ WARMUP (KLINES / CANDLES)
            WarmupHistory(symbol, shared);

            // 2️⃣ LIVE STREAM
            _tickerSocket.SubscribeToTickerUpdatesAsync(
                new SubscribeTickerRequest(shared),
                update =>
                {
                    if (update?.Data == null)
                        return;

                    var price = update.Data.LastPrice;
                    if (price == null)
                        return;

                    _ticks.Enqueue(new Tick
                    {
                        Symbol = symbol,
                        Time = update.DataTimeLocal ?? DateTime.UtcNow,
                        Value = price.Value,
                        TickType = TickType.Trade,
                        Quantity = 0
                    });

                    newDataAvailable?.Invoke(this, EventArgs.Empty);
                });

            return GetNextTicks().GetEnumerator();
        }

        // ─────────────────────────────────────────────
        // HISTORY (FIXED KLINES USAGE)
        // ─────────────────────────────────────────────

        private void WarmupHistory(Symbol symbol, SharedSymbol shared)
        {
            try
            {
                var end = DateTime.UtcNow;
                var start = end.AddMinutes(-200);

                var res = _restClient.FuturesApi.SharedClient.GetKlinesAsync(
                    new GetKlinesRequest(
                        shared,
                        SharedKlineInterval.OneMinute,
                        start,
                        end,
                        500
                    )
                ).GetAwaiter().GetResult();

                if (!res.Success || res.Data == null)
                {
                    Log.Trace($"{Name}: Warmup empty");
                    return;
                }

                foreach (var k in res.Data.OrderBy(x => x.OpenTime))
                {
                    _ticks.Enqueue(new Tick
                    {
                        Symbol = symbol,

                        // ✅ korrekt laut DTO
                        Time = k.OpenTime,

                        // ⚠️ bewusst: ClosePrice als Trade proxy
                        Value = k.ClosePrice,

                        TickType = TickType.Trade,
                        Quantity = 0
                    });
                }
            }
            catch (Exception ex)
            {
                Log.Error($"{Name}.WarmupHistory failed: {ex}");
            }
        }

        // ─────────────────────────────────────────────
        // LEAN QUEUE
        // ─────────────────────────────────────────────

        public IEnumerable<BaseData> GetNextTicks()
        {
            while (_ticks.TryDequeue(out var tick))
            {
                yield return tick;
            }
        }

        public void Unsubscribe(SubscriptionDataConfig config)
        {
            // optional: implement socket unsubscribe if SDK supports it
        }

        public void SetJob(LiveNodePacket job)
        {
        }
    }
}