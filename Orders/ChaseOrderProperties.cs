using QuantConnect.Interfaces;
using QuantConnect.Orders;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.Orders
{
    public enum ChaseOffsetType
    {
        Absolute,
        Percentage
    }

    /// <summary>
    /// Order-Properties für eine Chase-Order: pegged an die BBO, wird vom Exchange-seitigen
    /// Strategy-Service laufend nachgeführt. Aktuell nur von AsterFuturesBrokerage unterstützt.
    /// </summary>
    public class ChaseOrderProperties : OrderProperties
    {
        public decimal? ChaseOffset { get; set; }
        public ChaseOffsetType? ChaseOffsetType { get; set; }
        public decimal? MaxChaseOffset { get; set; }
        public ChaseOffsetType? MaxChaseOffsetType { get; set; }
        public decimal? PriceLimit { get; set; }

        public override IOrderProperties Clone() => (ChaseOrderProperties)MemberwiseClone();
    }
}