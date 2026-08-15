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

        /// <summary>
        /// Gets the margin interest rate model for Hyperliquid
        /// </summary>
        /// <param name="security">The security to get margin interest rate model for</param>
        /// <returns>The margin interest rate model</returns>
        /// <remarks>
        /// CryptoFuture uses a funding rate mechanism for perpetual futures.
        /// This is handled separately from traditional margin interest.
        /// </remarks>
        public override IMarginInterestRateModel GetMarginInterestRateModel(Security security)
        {
            // Spot and index trading don't have margin interest
            if (security.Type == SecurityType.Crypto || security.Type == SecurityType.Index)
            {
                return MarginInterestRateModel.Null;
            }

            // Perpetual futures use funding rates, not traditional margin interest
            if (security.Type == SecurityType.CryptoFuture &&
                security.Symbol.ID.Date == SecurityIdentifier.DefaultDate)
            {
                // Return the null model which applies no interest
                // Funding rates are handled separately by the exchange
                return MarginInterestRateModel.Null;
            }

            return base.GetMarginInterestRateModel(security);
        }
    }
}
