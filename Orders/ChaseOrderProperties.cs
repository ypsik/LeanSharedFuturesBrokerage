using QuantConnect.Interfaces;
using QuantConnect.Orders;
using System;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.Orders
{
    /// <summary>
    /// Order-Properties für eine Chase-Order: die Brokerage-Schicht hält den Preis der Order
    /// selbstständig an der BBO nach, unabhängig von der Algorithmus-Loop. Portiert 1:1 aus der
    /// bisherigen strategy-seitigen Reprice-Logik (AdaptiveMacroFlowAlgorithm.Buy/Sell/Reprice).
    /// </summary>
    public class ChaseOrderProperties : OrderProperties
    {
        /// <summary>
        /// 0 = Mid-Preis, 1 = am Bid/Ask selbst. Identisch zur bisherigen "aggression" in
        /// AdaptiveMacroFlowAlgorithm.GetAggressivePrice.
        /// </summary>
        public decimal Aggression { get; set; } = 0.4m;

        /// <summary>
        /// Mindestabstand zwischen zwei Reprice-Versuchen dieser Order (Rate-Limit-Schutz).
        /// </summary>
        public TimeSpan ChaseInterval { get; set; }

        public override IOrderProperties Clone() => (ChaseOrderProperties)MemberwiseClone();
    }
}