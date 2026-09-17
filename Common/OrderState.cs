using QuantConnect.Orders;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.Common
{
    public enum OrderLifeCycleState
    {
        Placing,    // Order ist lokal registriert, BrokerId noch ausstehend (REST-Call läuft)
        Submitted,
        Open,
        PartiallyFilled,
        Filled,
        Canceled,
        Replaced,
        Invalid
    }

    public sealed class OrderState
    {
        public OrderState(Order order, string clientOrderId)
        {
            Order = order;
            ClientOrderId = clientOrderId;
            LastUpdateUtc = DateTime.UtcNow;
        }

        public Order Order { get; }
        public decimal OriginalQuantity { get; set; }
        public decimal FilledQuantity { get; set; }

        // GEÄNDERT: Ersetzt die bisherigen Einzel-Felder FilledQuantityCurrentOrder,
        // CumulativeCostFilledCurrentOrder, CumulativeFeePaidCurrentOrder.
        //
        // Vorher wurde EINE Zahl pro State geführt, die bei jedem Cancel+Replace
        // (OrderStateManager.MapNewExchangeId) synchron auf 0 zurückgesetzt wurde. Das
        // Problem: zwischen "Replace abgeschickt" und "Fill-Bestätigung der ALTEN
        // Generation kommt per Socket an" liegt eine Zeitlücke (siehe DivideByZeroException
        // TRXUSDT 2026-09-17 00:04:03). Ein verspäteter Fill-Event der alten Generation
        // landete dabei im bereits zurückgesetzten Zähler der neuen Generation und erzeugte
        // ein Phantom-Delta mit negativer FillQuantity.
        //
        // Jetzt: pro BrokerId (=Exchange-Order-ID, ändert sich bei jedem Cancel+Replace)
        // ein eigener, isolierter Stand. Ein verspätetes Event einer alten BrokerId kann so
        // nie mehr das Delta einer anderen (neuen) BrokerId verfälschen. Kein Reset mehr
        // nötig - ein neuer Key startet automatisch bei 0 (GetValueOrDefault).
        public ConcurrentDictionary<string, decimal> FilledQuantityByBrokerId { get; } = new();
        public ConcurrentDictionary<string, decimal> CumulativeCostByBrokerId { get; } = new();
        public ConcurrentDictionary<string, decimal> FeePaidByBrokerId { get; } = new();

        // GEÄNDERT: eigener Helper statt dict.GetValueOrDefault(key, 0m) - letzteres ist an allen
        // Aufrufstellen mehrdeutig, weil sowohl System.Collections.Generic.CollectionExtensions als
        // auch QuantConnect.Util.LinqExtensions eine GetValueOrDefault-Extension-Methode für
        // ConcurrentDictionary anbieten (implementiert sowohl IDictionary<K,V> als auch
        // IReadOnlyDictionary<K,V>) und beide Namespaces per "using" im Scope stehen
        // ("Der Aufruf unterscheidet nicht eindeutig..."). TryGetValue ist eindeutig, da keine
        // Extension-Methode.
        public static decimal GetOrZero(ConcurrentDictionary<string, decimal> dict, string key)
            => dict.TryGetValue(key, out var value) ? value : 0m;

        public string? BrokerId { get; set; }
        public string ClientOrderId { get; set; }
        public OrderLifeCycleState State { get; set; }
        public DateTime LastUpdateUtc { get; set; }
        public bool IsUpdatePending { get; set; }
        public decimal CumulativeFeePaid { get; set; }
        public decimal CumulativeCostFilled { get; set; }

        // --- Chase-Order-Tracking (portiert aus AdaptiveMacroFlowAlgorithm.AggressiveOrder) ---
        public decimal? ChaseAggression { get; set; }
        public TimeSpan? ChaseInterval { get; set; }
        public decimal LastBid { get; set; }
        public decimal LastAsk { get; set; }
        // NEU: der zuletzt AM MARKT bestätigte Limit-Preis, bevor der laufende Reprice-Request
        // rausging (gesetzt im ChaseOrderLoop unmittelbar vor UpdateOrder()). Solange
        // IsUpdatePending true ist, vergleicht HandleOrderSocket eingehende Preis-Updates gegen
        // diesen Wert statt gegen (Order as LimitOrder).LimitPrice - letzterer wird von
        // ApplyUpdateOrderRequest sofort lokal überschrieben, noch bevor die Exchange den Edit
        // bestätigt hat, und wäre daher kein verlässlicher Vergleichswert.
        public decimal? LimitPrice { get; set; }
        public decimal Remaining => OriginalQuantity - FilledQuantity;

        public bool IsClosed => State is OrderLifeCycleState.Filled
                                      or OrderLifeCycleState.Canceled
                                      or OrderLifeCycleState.Invalid
                                      or OrderLifeCycleState.Replaced;
    }
}