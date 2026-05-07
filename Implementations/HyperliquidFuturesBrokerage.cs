using CryptoExchange.Net.SharedApis;
using HyperLiquid.Net.Clients;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using SilverQuant.Lean.Brokerages.Futures.Shared;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace SilverQuant.Lean.Brokerages.Futures.Implementations
{
    /// <summary>
    /// Hyperliquid futures brokerage with:
    /// - Order execution (base class)
    /// - Market data feed (IDataQueueHandler)
    /// - Tick buffer + thread-safe queue
    /// </summary>
    public class HyperliquidFuturesBrokerage : SharedFuturesBrokerage, IDataQueueHandler
    {
        private readonly ConcurrentQueue<BaseData> _ticks = new();
        private readonly object _tickLock = new();

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
        }

        // ─────────────────────────────────────────────────────────────
        // SYMBOL MAPPING
        // ─────────────────────────────────────────────────────────────

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

        // ─────────────────────────────────────────────────────────────
        // MARKET DATA (LEAN IDataQueueHandler)
        // ─────────────────────────────────────────────────────────────

        public IEnumerator<BaseData> Subscribe(
            SubscriptionDataConfig config,
            EventHandler newDataAvailable)
        {
            var symbol = config.Symbol;
            var shared = GetSharedSymbol(symbol);

            _tickerSocket.SubscribeToTickerUpdatesAsync(
                new SubscribeTickerRequest(shared),
                update =>
                {
                    if (update?.Data == null)
                        return;

                    if (update.Data.LastPrice == null)
                        return;

                    var tick = new Tick
                    {
                        Symbol = symbol,
                        Time = update.DataTimeLocal ?? DateTime.UtcNow,
                        Value = update.Data.LastPrice ?? 0m,
                        TickType = TickType.Trade,
                        Quantity = 0
                    };

                    _ticks.Enqueue(tick);
                    newDataAvailable?.Invoke(this, EventArgs.Empty);
                });

            return GetNextTicks().GetEnumerator();
        }

        public IEnumerable<BaseData> GetNextTicks()
        {
            while (_ticks.TryDequeue(out var tick))
            {
                yield return tick;
            }
        }
        public void Unsubscribe(SubscriptionDataConfig config)
        {
            // Optional: implement socket unsubscribe if SDK supports it
        }

        public void SetJob(LiveNodePacket job)
        {
            // Not required for Hyperliquid
        }
    }
}