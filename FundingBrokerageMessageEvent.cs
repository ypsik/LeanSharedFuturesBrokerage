using QuantConnect.Brokerages;
using System;
using System.Collections.Generic;
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    public class FundingBrokerageMessageEvent : BrokerageMessageEvent
    {
        public string Currency { get; }
        public decimal Amount { get; }

        public FundingBrokerageMessageEvent(string currency, decimal amount)
            : base(BrokerageMessageType.Information, "Funding", $"Funding payment received: {amount} {currency}")
        {
            Currency = currency;
            Amount = amount;
        }
    }
}
