using QuantConnect.Orders;
using System;
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
        // NEU: Fill-Menge nur für die AKTUELLE BrokerId-Generation. Wird von
        // OrderStateManager.MapNewExchangeId bei jedem BrokerId-Wechsel (Cancel+Replace)
        // auf 0 zurückgesetzt, während FilledQuantity kumulativ über die gesamte Order
        // (über alle BrokerId-Generationen hinweg) weiterläuft. Verhindert Phantom-
        // Fill-Events mit negativer FillQuantity direkt nach einem Replace, wenn die
        // Exchange QuantityFilled für die neue BrokerId wieder bei 0 beginnt.
        public decimal FilledQuantityCurrentOrder { get; set; }
        public string? BrokerId { get; set; }
        public string ClientOrderId { get; set; }
        public OrderLifeCycleState State { get; set; }
        public DateTime LastUpdateUtc { get; set; }
        public bool IsUpdatePending { get; set; }
        public decimal CumulativeFeePaid { get; set; }
        public decimal CumulativeCostFilledCurrentOrder { get; set; }
        public decimal CumulativeFeePaidCurrentOrder { get; set; }
        public decimal CumulativeCostFilled { get; set; }

        // --- Chase-Order-Tracking (portiert aus AdaptiveMacroFlowAlgorithm.AggressiveOrder) ---
        public decimal? ChaseAggression { get; set; }
        public TimeSpan? ChaseInterval { get; set; }
        public decimal LastBid { get; set; }
        public decimal LastAsk { get; set; }
        public decimal Remaining => OriginalQuantity - FilledQuantity;

        public bool IsClosed => State is OrderLifeCycleState.Filled
                                      or OrderLifeCycleState.Canceled
                                      or OrderLifeCycleState.Invalid
                                      or OrderLifeCycleState.Replaced;
    }
}
