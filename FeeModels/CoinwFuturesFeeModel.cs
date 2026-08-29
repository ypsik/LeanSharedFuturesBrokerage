using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;
using System;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.FeeModels
{
    public class CoinwFuturesFeeModel : FeeModel
    {
        public override OrderFee GetOrderFee(OrderFeeParameters parameters)
        {
            // Verifiziert gegen GET /v1/perpum/instruments am 2026-08-29: makerFee=0.0002,
            // takerFee=0.0006, identisch fuer BTC/TRX/HYPE/PAXG/ZEC. Gleiche Vereinfachung wie
            // beim bestehenden OkxFuturesFeeModel: Limit-Order -> Maker-Satz angenommen (keine
            // echte Maker/Taker-Erkennung anhand des tatsaechlichen Fills).
            decimal feeRate = parameters.Order.Type == OrderType.Limit ? 0.0002m : 0.0006m;

            decimal tradeValue = parameters.Security.Price * Math.Abs(parameters.Order.Quantity);
            decimal feeAmount = tradeValue * feeRate;
            var currency = parameters.Security.QuoteCurrency?.Symbol ?? "USDT";

            return new OrderFee(new CashAmount(feeAmount, currency));
        }
    }
}
