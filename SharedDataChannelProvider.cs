using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.DataFeeds;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    [Export(typeof(IDataChannelProvider))]
    public class SharedDataChannelProvider : DataChannelProvider
    {
        public override bool ShouldStreamSubscription(SubscriptionDataConfig config)
        {
            if (config.Type == typeof(MarginInterestRate))
                return true;

            return base.ShouldStreamSubscription(config);
        }    
    }
}
