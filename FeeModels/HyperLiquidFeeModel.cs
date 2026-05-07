using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;
using System;
using System.Collections.Generic;
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.FeeModels
{
    public class HyperLiquidFeeModel : FeeModel
    {
        public override OrderFee GetOrderFee(OrderFeeParameters parameters)
        {
            // Hyperliquid Standard-Gebühren: Maker = 0.00%, Taker = 0.035%
            // Wir nehmen an: Limit-Orders sind Maker, Market-Orders sind Taker
            decimal feeRate = parameters.Order.Type == OrderType.Limit ? 0.0000m : 0.00035m;

            // Transaktionsvolumen berechnen (Preis * Menge)
            decimal tradeValue = parameters.Security.Price * Math.Abs(parameters.Order.Quantity);
            decimal feeAmount = tradeValue * feeRate;

            // Hyperliquid rechnet Gebühren in USDC ab
            return new OrderFee(new CashAmount(feeAmount, "USDC"));
        }
    }
}
