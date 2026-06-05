using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Indicators;
using QuantConnect.Logging;
using QuantConnect.Securities;
using System;
using System.Collections.Generic;
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    public abstract partial class SharedFuturesBrokerage
    {
        private Timer? _cashBalanceTimer;

        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() => _balanceClient.GetBalancesAsync(new GetBalancesRequest()));
            return res.Success && res.Data != null
                ? res.Data.Select(x => new CashAmount(x.Total, x.Asset ?? SettleAsset)).ToList()
                : new List<CashAmount>();
        }

        protected virtual ExchangeParameters AccountHoldingsExchangeParameters => new ExchangeParameters();
        public override List<Holding> GetAccountHoldings()
        {
            var request = new GetPositionsRequest
            {
                ExchangeParameters = AccountHoldingsExchangeParameters
            };
            var res = RunSync(() => _orderClient.GetPositionsAsync(request));

            if (!res.Success)
            {
                Log.Error($"Fetch positions failed: {res.Error}");
            }
            else if (res.Data != null)
            {
                return res.Data.Where(f=>f.PositionSize != 0).Select(p =>
                {
                    var symbol = Symbol.Create(NormalizeSymbol(p.Symbol), SecurityType.CryptoFuture, Name);

                    var quantity = p.PositionSize;
                    if (p.PositionSide == SharedPositionSide.Short)
                    {
                        quantity *= -1;
                    }

                    if (quantity == 0)
                    {
                        return new Holding()
                        {
                            Symbol = symbol,
                        };
                    }

                    var openPrice = p.AverageOpenPrice ?? 0m;
                    var upnl = p.UnrealizedPnl ?? 0m;

                    var marketPrice = openPrice + (upnl / quantity);
                    return new Holding
                    {
                        Symbol = symbol,
                        Quantity = quantity,
                        AveragePrice = p.AverageOpenPrice ?? 0m,
                        MarketPrice = marketPrice,
                        UnrealizedPnL = upnl,
                        MarketValue = Math.Abs(quantity) * marketPrice
                    };
                })
                .Where(h => h != null)
                .ToList();
            }

            // Fallback auf die lokale Funktion
            return _getHoldingsFunc?.Invoke() ?? [];
        }

        private static TimeSpan GetNextCashRefreshDelay()
        {
            var now = DateTime.UtcNow;
            var next = now.Date.AddHours(now.Hour).AddMinutes(now.Minute < 23 ? 23 : now.Minute < 53 ? 53 : 83);
            if (now.Minute >= 53) next = now.Date.AddHours(now.Hour + 1).AddMinutes(23);
            return next - now;
        }

        private void RefreshCashBalance()
        {
            try
            {
                var cashAmounts = GetCashBalance();
                foreach (var cash in cashAmounts)
                    OnAccountChanged(new AccountEvent(cash.Currency, cash.Amount));
            }
            catch (Exception ex)
            {
                Log.Error($"{Name}: RefreshCashBalance failed: {ex.Message}");
            }
        }

    }
}
