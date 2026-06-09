using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;
using System;
using System.Collections.Generic;
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.FeeModels
{
    public class KrakenFuturesFeeModel : FeeModel
    {
        public override OrderFee GetOrderFee(OrderFeeParameters parameters)
        {
            // Kraken Standard-Gebühren: Maker = 0.02%, Taker = 0.05%
            // Wir nehmen an: Limit-Orders sind Maker, Market-Orders sind Taker
            decimal feeRate = parameters.Order.Type == OrderType.Limit ? 0.0002m : 0.0005m;

            // Transaktionsvolumen berechnen (Preis * Menge)
            decimal tradeValue = parameters.Security.Price * Math.Abs(parameters.Order.Quantity);
            decimal feeAmount = tradeValue * feeRate;

            // Aster rechnet Gebühren in USDT ab
            return new OrderFee(new CashAmount(feeAmount, "USD"));
        }
    }
}
