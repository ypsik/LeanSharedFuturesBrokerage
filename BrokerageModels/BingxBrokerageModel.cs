using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;
using QuantConnect.Securities.CryptoFuture;
using SilverQuant.Lean.Brokerages.Futures.Shared.FeeModels;
using System;
using System.Collections.Generic;
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageModels
{
    public class BingXBrokerageModel : DefaultBrokerageModel
    {
        protected virtual string MarketName => "bingx";

        public BingXBrokerageModel(AccountType accountType = AccountType.Margin)
            : base(accountType)
        {
        }

        // 1. Dein neues Gebührenmodell zuweisen
        public override IFeeModel GetFeeModel(Security security)
        {
            return new BingxFuturesFeeModel();
        }

        // 2. Den Standard-Hebel (Buying Power) definieren
        public override IBuyingPowerModel GetBuyingPowerModel(Security security)
        {
            return new CryptoFutureMarginModel(10m);
        }

        // 3. Optionale Feineinstellungen (z.B. Settlement)
        public override ISettlementModel GetSettlementModel(Security security)
        {
            return new ImmediateSettlementModel();
        }
    }
}
