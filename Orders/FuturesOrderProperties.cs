using QuantConnect.Interfaces;
using QuantConnect.Orders;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.Orders
{
    /// <summary>
    /// Positions-Seite, die die Strategie einer Futures-Order mitgibt. Bewusst ein eigener Enum und
    /// nicht der der Exchange-Bibliotheken (CryptoExchange.Net/JKorf) - die Strategie soll davon
    /// nichts wissen muessen.
    /// </summary>
    public enum FuturesPositionSide
    {
        Long,
        Short
    }

    /// <summary>
    /// Gemeinsame Basis fuer Futures-Order-Properties. Traegt die Absicht der Strategie, auf welcher
    /// Positions-Seite die Order wirkt (relevant im Hedge-Modus). Ohne Angabe gilt im Hedge-Modus Long.
    /// Im One-Way-Modus (IsHedgeMode == false) wird die Seite ignoriert; Short wird dort abgelehnt.
    /// </summary>
    public class FuturesOrderProperties : OrderProperties
    {
        /// <summary>
        /// Positions-Seite der Order. null = nicht angegeben (Hedge-Modus: Long).
        /// </summary>
        public FuturesPositionSide? PositionSide { get; set; }

        public override IOrderProperties Clone() => (FuturesOrderProperties)MemberwiseClone();
    }
}
