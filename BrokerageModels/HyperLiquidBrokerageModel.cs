using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;
using SilverQuant.Lean.Brokerages.Futures.Shared.FeeModels;
using System;
using System.Collections.Generic;
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageModels
{
    public class HyperliquidBrokerageModel : DefaultBrokerageModel
    {
        public HyperliquidBrokerageModel(AccountType accountType = AccountType.Margin)
            : base(accountType)
        {
        }

        // 1. Dein neues Gebührenmodell zuweisen
        public override IFeeModel GetFeeModel(Security security)
        {
            return new HyperliquidFeeModel();
        }

        // 2. Den Standard-Hebel (Buying Power) definieren
        public override IBuyingPowerModel GetBuyingPowerModel(Security security)
        {
            // Setzt den Standard-Hebel für Hyperliquid (z.B. 20x)
            return new SecurityMarginModel(20m);
        }

        // 3. Optionale Feineinstellungen (z.B. Settlement)
        public override ISettlementModel GetSettlementModel(Security security)
        {
            return new ImmediateSettlementModel();
        }
    }
}
