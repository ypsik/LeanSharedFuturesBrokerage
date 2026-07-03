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
using System.Linq;
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
                return res.Data.Where(f => f.PositionSize != 0).Select(p =>
                {
                    var ticker = NormalizeSymbol(p.Symbol);
                    var security = _algorithm.Securities.Values
                        .FirstOrDefault(s => s.Symbol.Value == ticker
                                          && s.Symbol.ID.Market == Name);

                    var symbol = security?.Symbol ?? Symbol.Create(ticker, SecurityType.CryptoFuture, Name);

                    // FIX: PositionSize liegt bei Exchanges mit Contract-Notation (z.B. OKX) in Contracts
                    // vor, nicht in Base-Asset-Einheiten. Wir verpacken den rohen Wert in BEIDE Felder
                    // (BaseAsset und Contracts) und lassen das bereits vorhandene, pro Exchange korrekt
                    // überschriebene FromExchangeQuantity entscheiden, welches Feld tatsächlich gilt —
                    // Default-Exchanges lesen QuantityInBaseAsset (No-Op), OKX liest QuantityInContracts
                    // und rechnet via ContractMultiplier (ctVal) um. Ohne diese Umrechnung war Quantity
                    // um den Faktor ContractMultiplier verzerrt (z.B. XAU: 50 Contracts statt 0.05 XAU),
                    // was LEANs intern berechnetes UnrealizedProfit massiv verfälscht hat.
                    var rawPositionQuantity = new SharedOrderQuantity(baseAssetQuantity: p.PositionSize, contractQuantity: p.PositionSize);
                    var quantity = FromExchangeQuantity(symbol, rawPositionQuantity);
                    if (p.PositionSide == CryptoExchange.Net.SharedApis.SharedPositionSide.Short)
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