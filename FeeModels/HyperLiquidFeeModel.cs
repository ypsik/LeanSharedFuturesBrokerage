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
            var order = parameters.Order;
            var security = parameters.Security;

            var price = security.Price;

            var rate = order.Type == OrderType.Limit
                ? 0.0002m
                : 0.0005m;

            var fee = Math.Abs(order.Quantity) * price * rate;

            var currency = security.QuoteCurrency?.Symbol
                           ?? "USDC";

            return new OrderFee(
                new CashAmount(fee, currency)
            );
        }
    }
}
