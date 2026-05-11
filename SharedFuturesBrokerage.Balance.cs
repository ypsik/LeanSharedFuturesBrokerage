using CryptoExchange.Net.SharedApis;
using QuantConnect;
using QuantConnect.Securities;
using System;
using System.Collections.Generic;
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    public abstract partial class SharedFuturesBrokerage
    {
        public override List<CashAmount> GetCashBalance()
        {
            var res = RunSync(() => _balanceClient.GetBalancesAsync(new GetBalancesRequest()));
            return res.Success && res.Data != null
                ? res.Data.Select(x => new CashAmount(x.Available, x.Asset ?? "USDC")).ToList()
                : new List<CashAmount>();
        }

        public override List<Holding> GetAccountHoldings()
        {
            var res = RunSync(() => _orderClient.GetPositionsAsync(new GetPositionsRequest()));

            if (res.Success && res.Data != null)
            {
                return res.Data.Select(p =>
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
            return _getHoldingsFunc?.Invoke() ?? new List<Holding>();
        }
        private async Task SubscribeToAccountUpdatesAsync()
        {
            var result = await _balanceSocket.SubscribeToBalanceUpdatesAsync(new SubscribeBalancesRequest(), update =>
            {
                foreach (var balance in update.Data)
                {
                    var accountEvent = new AccountEvent(balance.Asset, balance.Total);
                    OnAccountChanged(accountEvent);
                }

            });

            if (result.Success)
            {
                // Connection-Management
                result.Data.ConnectionLost += () => _isConnectedBalance = false;
                result.Data.ConnectionRestored += (d) =>
                {                    
                    _isConnectedBalance = true;
                };
            }
            else
                throw new Exception("Balance socket socket failed");

            _isConnectedBalance = true;
        }
    }   
}
