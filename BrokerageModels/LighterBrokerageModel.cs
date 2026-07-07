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
    public class LighterBrokerageModel : DefaultBrokerageModel
    {
        protected virtual string MarketName => "aster";

        public LighterBrokerageModel(AccountType accountType = AccountType.Margin)
            : base(accountType)
        {
        }

        public override IFeeModel GetFeeModel(Security security)
        {
            return new LighterFeeModel();
        }

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
