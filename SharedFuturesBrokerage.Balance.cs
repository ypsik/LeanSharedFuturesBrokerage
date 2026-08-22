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
                ? [.. res.Data.Select(x => new CashAmount(x.Total, x.Asset ?? SettleAsset))]
                : [];
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
                return [.. res.Data.Select(p =>
                {
                    var ticker = NormalizeSymbol(p.Symbol);
                    var security = _algorithm.Securities.Values
                        .FirstOrDefault(s => s.Symbol.Value == ticker
                                          && s.Symbol.ID.Market == Name);

                    var symbol = security?.Symbol ?? Symbol.Create(ticker, SecurityType.CryptoFuture, Name);

                    // PositionSizes liefert BaseAsset- und Contract-Menge getrennt (CryptoExchange.Net 12.5.0+,
                    // ersetzt das jetzt obsolete PositionSize). Gleiches Pattern wie in HandleUserTradeSocket:
                    // QuantityInBaseAsset hat Vorrang, falls die Exchange sie direkt mitliefert (keine Umrechnung
                    // nötig). Sonst greift der HasExchangeQuantity/FromExchangeQuantity-Hook als Fallback (Default:
                    // BaseAsset-Passthrough, OKX: rechnet QuantityInContracts via ContractMultiplier/ctVal um).
                    // Einzige Stelle, an der die Menge bestimmt wird - kein separater, abweichender Filter mehr davor.
                    decimal quantity = p.PositionSizes.QuantityInBaseAsset ?? FromExchangeQuantity(symbol, p.PositionSizes);

                    if (quantity == 0)
                    {
                        return null;
                    }

                    if (p.PositionSide == CryptoExchange.Net.SharedApis.SharedPositionSide.Short)
                    {
                        quantity *= -1;
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
                .OfType<Holding>()];
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