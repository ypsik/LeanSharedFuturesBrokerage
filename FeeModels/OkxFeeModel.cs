using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;
using System;
using System.Collections.Generic;
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.FeeModels
{
    public class OkxFuturesFeeModel : FeeModel
    {
        public override OrderFee GetOrderFee(OrderFeeParameters parameters)
        {
            decimal feeRate = parameters.Order.Type == OrderType.Limit ? 0.0002m : 0.0006m;

            decimal tradeValue = parameters.Security.Price * Math.Abs(parameters.Order.Quantity);
            decimal feeAmount = tradeValue * feeRate;

            return new OrderFee(new CashAmount(feeAmount, "USDT"));
        }
    }
}
