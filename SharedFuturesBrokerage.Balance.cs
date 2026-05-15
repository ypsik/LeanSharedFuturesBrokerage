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
        private decimal? _balance;
        private UpdateSubscription _balanceUpdatesSocketSub;
        bool _balanceUpdated;

        public override List<CashAmount> GetCashBalance()
        {
            if(_balance.HasValue)
                return new List<CashAmount> { new CashAmount(_balance.Value, "USDC") };

            var res = RunSync(() => _balanceClient.GetBalancesAsync(new GetBalancesRequest()));
            return res.Success && res.Data != null
                ? res.Data.Select(x => new CashAmount(x.Total, x.Asset ?? "USDC")).ToList()
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

        protected void OnBalanceUpdated()
        {
            _balanceUpdated = true;
        }

        private async Task SubscribeToBalanceUpdatesAsync()
        {
            _subRateGate.WaitToProceed();
            var sub = await _balanceSocket.SubscribeToBalanceUpdatesAsync(new SubscribeBalancesRequest(), update =>
            {
                foreach (var balance in update.Data)
                {
                    _balance = balance.Total;
                    if (_balanceUpdated)
                    {
                        OnAccountChanged(new AccountEvent("USDC", _balance.Value));
                        _balanceUpdated = false;
                    }
                }

            });

            if (sub.Success)
            {
                var subscription = sub.Data;

                subscription.ConnectionLost += () =>
                {
                    _isConnectedOrder = false;
                    Log.Error($"{Name}: Connection lost!");
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "Disconnect", "Balance updates stream lost."));
                };

                subscription.ConnectionRestored += (duration) =>
                {
                    _isConnectedOrder = true;
                    Log.Trace($"{Name}: Connection restored after {duration}.");
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Information, "Reconnect", $"Balance updates stream restored. Syncing..."));
                };
            }
            else
                throw new Exception("Balance updates socket failed");

            _balanceUpdatesSocketSub = sub.Data;
            _isConnectedBalance = true;
        }
    }   
}
