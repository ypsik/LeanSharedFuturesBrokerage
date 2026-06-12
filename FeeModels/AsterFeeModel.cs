using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;
using System;
using System.Collections.Generic;
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.FeeModels
{
    public class AsterFeeModel : FeeModel
    {
        public override OrderFee GetOrderFee(OrderFeeParameters parameters)
        {
            // Aster Standard-Gebühren: Maker = 0.00%, Taker = 0.04%
            // Wir nehmen an: Limit-Orders sind Maker, Market-Orders sind Taker
            decimal feeRate = parameters.Order.Type == OrderType.Limit ? 0.0000m : 0.0004m;

            // Transaktionsvolumen berechnen (Preis * Menge)
            decimal tradeValue = parameters.Security.Price * Math.Abs(parameters.Order.Quantity);
            decimal feeAmount = tradeValue * feeRate;

            var currency = parameters.Security.QuoteCurrency?.Symbol
               ?? "USDT";

            // Aster rechnet Gebühren in USDT ab
            return new OrderFee(new CashAmount(feeAmount, currency));
        }
    }
}
