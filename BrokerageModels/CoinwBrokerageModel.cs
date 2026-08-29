using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;
using QuantConnect.Securities.CryptoFuture;
using SilverQuant.Lean.Brokerages.Futures.Shared.FeeModels;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.BrokerageModels
{
    public class CoinwBrokerageModel : DefaultBrokerageModel
    {
        protected virtual string MarketName => "coinw";

        public CoinwBrokerageModel(AccountType accountType = AccountType.Margin)
            : base(accountType)
        {
        }

        public override IFeeModel GetFeeModel(Security security)
        {
            return new CoinwFuturesFeeModel();
        }

        public override IBuyingPowerModel GetBuyingPowerModel(Security security)
        {
            return new CryptoFutureMarginModel(10m);
        }

        public override ISettlementModel GetSettlementModel(Security security)
        {
            return new ImmediateSettlementModel();
        }

        /// <remarks>
        /// CryptoFuture nutzt einen Funding-Rate-Mechanismus statt klassischer Margin-Zinsen.
        /// </remarks>
        public override IMarginInterestRateModel GetMarginInterestRateModel(Security security)
        {
            if (security.Type == SecurityType.Crypto || security.Type == SecurityType.Index)
            {
                return MarginInterestRateModel.Null;
            }

            if (security.Type == SecurityType.CryptoFuture &&
                security.Symbol.ID.Date == SecurityIdentifier.DefaultDate)
            {
                return MarginInterestRateModel.Null;
            }

            return base.GetMarginInterestRateModel(security);
        }
    }
}
