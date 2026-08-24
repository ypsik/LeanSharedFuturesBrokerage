using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.SharedApis;
using QuantConnect;
using QuantConnect.Brokerages;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;
using QuantConnect.Statistics;
using QuantConnect.Util;
using SilverQuant.Lean.Brokerages.Futures.Shared.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using System.Threading.Tasks;

using CxCancelOrderRequest = CryptoExchange.Net.SharedApis.CancelOrderRequest;

namespace SilverQuant.Lean.Brokerages.Futures.Shared
{
    public abstract partial class SharedFuturesBrokerage
    {
        #region Socket / Reconcile

        private void HandleUserTradeSocket(DataEvent<SharedUserTrade[]> update)
        {
            foreach (var trade in update.Data)
            {
                try
                {
                    // =======================================================
                    // 🔥 RAW DIAGNOSTIC LOGGING 🔥
                    // =======================================================
                    Log.Trace($"{Name}.HandleUserTradeSocket RAW PAYLOAD: " +
                              $"UpdateTimeTicks='{trade.Timestamp.Ticks}', " +
                              $"OrderId='{trade.OrderId}', " +
                              $"ClientOrderId='{trade.ClientOrderId}', " +
                              $"Symbol='{trade.Symbol}', " +
                              $"QuantityInBaseAsset='{trade.Quantities.QuantityInBaseAsset}', " +
                              $"QuantityInContracts='{trade.Quantities.QuantityInContracts}', " +
                              $"Side='{trade.Side}', " +
                              $"Fee='{trade.Fee}', " +
                              $"Price='{trade.Price}'");

                    if (string.IsNullOrEmpty(trade.OrderId)) continue;

                    OrderState? state = null;

                    // =======================================================
                    // 1. VERSUCH: O(1) Lookup via Exchange-ID (Hidden Index)
                    // =======================================================
                    if (!_orderStateManager.TryGetByExchangeId(trade.OrderId, out state))
                    {
                        // =======================================================
                        // 2. VERSUCH: Fallback via ClientOrderId (Master-Dict)
                        // Deckt ab:
                        //   Fall A – Order noch im Placing-State (BrokerId = clientId, daher kein Hit oben)
                        //   Fall B – Cancel+Replace: MapNewExchangeId noch nicht gelaufen,
                        //            aber _statesByClientId[clientOrderId] zeigt immer auf den aktuellen State
                        // =======================================================
                        if (!string.IsNullOrEmpty(trade.ClientOrderId))
                        {
                            if (_orderStateManager.TryGetValue(trade.ClientOrderId, out state))
                            {
                                // Verhalten unverändert zu vorher (kein OnOrderIdChangedEvent hier) -
                                // Rückgabewert aktuell ungenutzt, siehe Rückfrage zu Site 3 im Chat.
                                _orderStateManager.MapNewExchangeId(trade.ClientOrderId, trade.OrderId);

                                // Alias-Cleanup falls neue clientOrderId (z.B. Bitget Edit)
                                if (trade.ClientOrderId != state.ClientOrderId)
                                {
                                    _orderStateManager.RemoveAlias(state.ClientOrderId);
                                    state.ClientOrderId = trade.ClientOrderId;
                                }
                            }
                        }
                    }

                    if (state == null)
                    {
                        if (state == null)
                        {
                            state = _orderStateManager.GetAllStates().FirstOrDefault(s =>
                                NativeTicker(s.Order.Symbol) == trade.Symbol &&
                                (s.Order.Direction == (trade.Side == SharedOrderSide.Buy ? OrderDirection.Buy : OrderDirection.Sell)) &&
                                (
                                    // Fall A: Reguläre neue Order im Transit (BrokerId ist leer)
                                    // JEDER erste Teil-Fill (kleiner oder gleich der Gesamtmenge) wird akzeptiert!
                                    (
                                        (s.State == OrderLifeCycleState.Placing || s.State == OrderLifeCycleState.Submitted) &&
                                        string.IsNullOrEmpty(s.BrokerId) &&
                                        (!HasExchangeQuantity(trade.Quantities) || Math.Abs(trade.Quantities.QuantityInBaseAsset ?? FromExchangeQuantity(s.Order.Symbol, trade.Quantities)) <= Math.Abs(s.Remaining))
                                    )
                                    ||
                                    // Fall B: Schwebendes Update (IsUpdatePending ist aktiv)
                                    // JEDER Fill (egal wie groß) wird geschluckt, da Kontext eindeutig!
                                    (
                                        s.IsUpdatePending &&
                                        (s.State == OrderLifeCycleState.Open || s.State == OrderLifeCycleState.PartiallyFilled || s.State == OrderLifeCycleState.Submitted)
                                    )
                                ));

                            if (state != null)
                            {
                                Log.Trace($"{Name}: Heuristic match successful! Linking unknown Trade {trade.OrderId} (Qty: {trade.Quantities.QuantityInBaseAsset ?? FromExchangeQuantity(state.Order.Symbol, trade.Quantities)}) to ClientOrder {state.ClientOrderId}");

                                // Der Trade-Socket mappt die neue ID sofort! 
                                // Folge-Teil-Fills laufen ab jetzt instantan über den O(1) Exchange-ID Index.
                                // MapNewExchangeId ergänzt Order.BrokerId bereits selbst (einzige Add-Stelle) -
                                // kein separates brokerId.Add hier mehr. Vorher stand hier zusätzlich
                                // brokerId.Add(trade.Id) - das war ein Bug (trade.Id ist die Execution-/
                                // Trade-ID, NICHT die Order-ID) UND ein Doppel-Add von trade.OrderId
                                // über MapNewExchangeId.
                                var mapped = _orderStateManager.MapNewExchangeId(state.ClientOrderId, trade.OrderId);

                                if (mapped)
                                {
                                    OnOrderIdChangedEvent(new BrokerageOrderIdChangedEvent
                                    {
                                        OrderId = state.Order.Id,
                                        BrokerId = state.Order.BrokerId
                                    });
                                }
                            }
                            else
                            {
                                Log.Trace($"{Name}.HandleUserTradeSocket: Ignoring trade {trade.OrderId}. Neither OrderId nor ClientOrderId {trade.ClientOrderId} found. " +
                                    $"Active states: [{string.Join(", ", _orderStateManager.GetAllStates().Select(s => $"ClientId={s.ClientOrderId} Symbol={NativeTicker(s.Order.Symbol)} State={s.State} IsUpdatePending={s.IsUpdatePending} BrokerId={s.BrokerId}"))}]");
                                continue;
                            }
                        }
                    }

                    // =======================================================
                    // TRADE VERARBEITEN (Da 'state' eine Referenz ist, updaten wir das richtige Objekt!)
                    // trade.Quantity (deprecated plain-decimal field) is no longer used here.
                    // Since CryptoExchange.Net 12.4.0, SharedUserTrade only exposes quantities via
                    // trade.Quantities (SharedOrderQuantity). Mirrors the existing pattern in
                    // SharedFuturesBrokerage.Data.cs (e.g. trade tick subscription, kline volume):
                    // use QuantityInBaseAsset directly if the exchange provides it (some send both
                    // values, no conversion needed) - only fall back to the
                    // HasExchangeQuantity/FromExchangeQuantity hook and convert from
                    // QuantityInContracts (exchange-specific, e.g. OKX/Kraken) if it's not set. If no
                    // usable quantity is present at all, the trade is discarded instead of being
                    // booked with a wrong 0.
                    // Hinweis FilledQuantityCurrentOrder: dieser Pfad arbeitet mit einem echten
                    // Delta aus dem Trade selbst (kein QuantityFilled-Snapshot einer Exchange-Order).
                    // WICHTIG: Trotzdem muss FilledQuantityCurrentOrder hier mitgeführt werden, da
                    // MapNewExchangeId es bei jedem Replace auf 0 resettet. Würde es hier nicht
                    // fortgeschrieben, bliebe es nach einem Replace dauerhaft bei 0, während
                    // ReconcileLoop/ReconcileOrderImmediateAsync ihre brokerOrder.QuantityFilled
                    // (relativ zur aktuellen BrokerId, inkl. aller bereits über diesen Trade-Socket
                    // verarbeiteten Fills) dagegen vergleichen - das würde bereits verarbeitete
                    // Fills doppelt zählen.
                    // =======================================================
                    // QuantityInBaseAsset hat Vorrang, falls die Exchange sie direkt mitliefert (keine
                    // Umrechnung nötig). HasExchangeQuantity prüft je nach Exchange nur eines der beiden
                    // Felder (Default: BaseAsset, OKX/Kraken: Contracts) - daher erst explizit auf
                    // QuantityInBaseAsset prüfen, bevor der HasExchangeQuantity/FromExchangeQuantity-Hook
                    // als Fallback greift.
                    decimal tradeQuantity;
                    if (trade.Quantities.QuantityInBaseAsset.HasValue)
                    {
                        tradeQuantity = trade.Quantities.QuantityInBaseAsset.Value;
                    }
                    else if (HasExchangeQuantity(trade.Quantities))
                    {
                        tradeQuantity = FromExchangeQuantity(state.Order.Symbol, trade.Quantities);
                    }
                    else
                    {
                        Log.Error($"{Name}.HandleUserTradeSocket: Trade {trade.OrderId} (ClientOrderId={state.ClientOrderId}) has no usable quantity " +
                                  $"(QuantityInBaseAsset/QuantityInContracts both empty). Discarding trade to avoid a wrong booking.");
                        continue;
                    }

                    var sign = trade.Side == SharedOrderSide.Buy ? 1m : -1m;
                    var signedFill = tradeQuantity * sign;
                    var fee = trade.Fee ?? 0m;

                    state.FilledQuantity += signedFill;
                    state.FilledQuantityCurrentOrder += signedFill;
                    state.CumulativeFeePaid += fee;
                    state.CumulativeCostFilledCurrentOrder += tradeQuantity * trade.Price;
                    state.CumulativeCostFilled += tradeQuantity * trade.Price;
                    state.LastUpdateUtc = DateTime.UtcNow;

                    var leanStatus = Math.Abs(state.FilledQuantity) >= Math.Abs(state.OriginalQuantity)
                        ? QuantConnect.Orders.OrderStatus.Filled
                        : QuantConnect.Orders.OrderStatus.PartiallyFilled;

                    state.State = leanStatus == QuantConnect.Orders.OrderStatus.Filled ? OrderLifeCycleState.Filled : OrderLifeCycleState.PartiallyFilled;

                    // Wenn der Trade die Order schließt, räumen wir ab.
                    // TryRemove bereinigt beide internen Dicts (_statesByClientId + _statesByExchangeId).
                    if (state.IsClosed)
                    {
                        _orderStateManager.TryRemove(state.ClientOrderId, out _);
                    }

                    OnOrderEvent(new OrderEvent(state.Order, DateTime.UtcNow, new OrderFee(new CashAmount(fee, trade.FeeAsset ?? SettleAsset)))
                    {
                        Status = leanStatus,
                        FillPrice = trade.Price,
                        FillQuantity = signedFill,
                        Message = "User trade socket"
                    });
                }
                catch (Exception ex)
                {
                    Log.Error($"{Name}.HandleUserTradeSocket: Unhandled exception processing OrderId='{trade.OrderId}' " +
                              $"ClientOrderId='{trade.ClientOrderId}' Symbol='{trade.Symbol}': {ex.ToString()}");
                }
            }
        }

        private void HandleOrderSocket(DataEvent<SharedFuturesOrder[]> update)
        {
            // =======================================================
            // 🔥 UNZERSTÖRBARER BATCH-FIX: Unabhängig von ClientOrderId
            // =======================================================
            var newOrderUpdates = update.Data.Where(o =>
                o.Status == SharedOrderStatus.Open ||
                o.Status == SharedOrderStatus.Filled).ToList();

            var cancelUpdates = update.Data.Where(o => o.Status == SharedOrderStatus.Canceled).ToList();

            var cancelsToDrop = new HashSet<SharedFuturesOrder>();

            if (newOrderUpdates.Any() && cancelUpdates.Any())
            {
                foreach (var newPayload in newOrderUpdates)
                {
                    // Match rein über Symbol und exakte Exchange-Zeitstempel
                    var match = cancelUpdates.FirstOrDefault(c =>
                        c.Symbol == newPayload.Symbol &&
                        c.UpdateTime == newPayload.UpdateTime);

                    if (match != null)
                    {
                        // Fall A: NewPayload hat KEINE ClientOrderId (Der HL-Standardfehler)
                        // -> Wir holen sie uns von der alten ID aus dem State-Manager
                        if (string.IsNullOrEmpty(newPayload.ClientOrderId))
                        {
                            if (_orderStateManager.TryGetByExchangeId(match.OrderId, out var state))
                            {
                                Log.Trace($"{Name}: Multi-Update Match (Naked)! Injecting ClientOrderId {state.ClientOrderId} into new {newPayload.Status} Order {newPayload.OrderId}");
                                newPayload.ClientOrderId = state.ClientOrderId;
                                cancelsToDrop.Add(match); // Altes Cancel vernichten
                            }
                        }
                        // Fall B: NewPayload HAT bereits eine ClientOrderId
                        // -> Perfekt, aber wir müssen das alte Cancel TROTZDEM vernichten, 
                        // damit es in der Schleife keinen Schaden anrichtet!
                        else
                        {
                            Log.Trace($"{Name}: Multi-Update Match (Identified)! Dropping redundant Cancel for old ID {match.OrderId}");
                            cancelsToDrop.Add(match); // Altes Cancel trotzdem vernichten!
                        }
                    }
                }
            }

            var cleanPayload = update.Data.Where(o => !cancelsToDrop.Contains(o));

            foreach (var o in cleanPayload)
            {
                try
                {
                    // =======================================================
                    // 🔥 RAW DIAGNOSTIC LOGGING 🔥
                    // =======================================================
                    Log.Trace($"{Name}.HandleOrderSocket RAW PAYLOAD: " +
                              $"UpdateTimeTicks='{o.UpdateTime?.Ticks}', " +
                              $"OrderId='{o.OrderId}', " +
                              $"ClientOrderId='{o.ClientOrderId}', " +
                              $"Symbol='{o.Symbol}', " +
                              $"Status='{o.Status}', " +
                              $"Qty='{o.OrderQuantity?.QuantityInBaseAsset ?? o.OrderQuantity?.QuantityInContracts}', " +
                              $"QtyFilled='{o.QuantityFilled?.QuantityInBaseAsset ?? o.QuantityFilled?.QuantityInContracts ?? 0m}', " +
                              $"Price='{o.OrderPrice}'" +
                              (!ExchangeSupportsUserTradeStream
                                  ? $", Fee='{o.Fee}', FeeAsset='{o.FeeAsset}', AvgPrice='{o.AveragePrice}', LastTradeFee='{o.LastTrade?.Fee}'"
                                  : ""));

                    if (string.IsNullOrEmpty(o.OrderId)) continue;

                    // -------------------------------------------------------
                    // PLACING STATE: Instantaner Fill während PlaceOrder()
                    // Order liegt in _statesByClientId[clientOrderId]
                    // -------------------------------------------------------
                    if (!string.IsNullOrEmpty(o.ClientOrderId) &&
                        _orderStateManager.TryGetValue(o.ClientOrderId, out var placingCandidate) &&
                        placingCandidate.State == OrderLifeCycleState.Placing &&
                        !_orderStateManager.TryGetByExchangeId(o.OrderId, out _))
                    {
                        var mapped = _orderStateManager.MapNewExchangeId(o.ClientOrderId, o.OrderId);

                        if (mapped)
                        {
                            placingCandidate.State = OrderLifeCycleState.Submitted;
                            placingCandidate.LastUpdateUtc = DateTime.UtcNow;

                            Log.Trace($"{Name}.HandleOrderSocket: Placing→Submitted for {o.OrderId} via socket. Fill (if any) follows via trade socket.");
                            OnOrderEvent(new OrderEvent(placingCandidate.Order, DateTime.UtcNow, OrderFee.Zero) { Status = QuantConnect.Orders.OrderStatus.Submitted });
                        }
                    }

                    // -------------------------------------------------------
                    // MODIFY / REPLACEMENT DETECTION (Cancel + Replace)
                    // -------------------------------------------------------
                    if (!string.IsNullOrEmpty(o.ClientOrderId) &&
                        _orderStateManager.TryGetValue(o.ClientOrderId, out var existingState) &&
                        existingState.BrokerId != o.OrderId)
                    {
                        // 🔥 THE ANTI-ZOMBIE GUARD 🔥
                        // Verhindert Rückwärts-Swaps, falls das Cancel-Event der ALTEN Order
                        // nach dem New-Event der NEUEN Order eintrifft.
                        if (existingState.Order.BrokerId.Contains(o.OrderId))
                        {
                            Log.Trace($"{Name}.HandleOrderSocket: Ignoring backwards swap to old ID {o.OrderId}.");
                        }
                        else
                        {
                            var oldBrokerId = existingState.BrokerId;

                            // 1. State-Properties aktualisieren
                            existingState.LastUpdateUtc = DateTime.UtcNow;

                            // 2. Exchange-ID atomar tauschen:
                            //    - entfernt alte BrokerId aus _statesByExchangeId
                            //    - setzt state.BrokerId = o.OrderId
                            //    - trägt unter o.OrderId in _statesByExchangeId ein
                            //    - ergänzt Order.BrokerId
                            //    - _statesByClientId[o.ClientOrderId] bleibt unverändert
                            //    - resettet FilledQuantityCurrentOrder für die neue BrokerId-Generation
                            var mapped = _orderStateManager.MapNewExchangeId(o.ClientOrderId, o.OrderId);

                            // Bitget-Style: neue clientOrderId war temporärer Alias → alten Key entfernen und State updaten
                            if (o.ClientOrderId != existingState.ClientOrderId)
                            {
                                _orderStateManager.RemoveAlias(existingState.ClientOrderId);
                                existingState.ClientOrderId = o.ClientOrderId;
                            }
                            existingState.IsUpdatePending = false;
                            existingState.LimitPrice = o.OrderPrice ?? existingState.LimitPrice;
                            var prevState = existingState.State;
                            existingState.State = existingState.FilledQuantity != 0m
                                ? (Math.Abs(existingState.FilledQuantity) >= Math.Abs(existingState.OriginalQuantity)
                                    ? OrderLifeCycleState.Filled
                                    : OrderLifeCycleState.PartiallyFilled)
                                : OrderLifeCycleState.Open;

                            if (existingState.State == OrderLifeCycleState.Open && prevState == OrderLifeCycleState.Submitted)
                            {
                                OnOrderEvent(new OrderEvent(existingState.Order, DateTime.UtcNow, OrderFee.Zero)
                                {
                                    Status = QuantConnect.Orders.OrderStatus.UpdateSubmitted,
                                    Message = "Order modified"
                                });
                            }

                            // KEIN separates brokerid.Add(o.OrderId) mehr - MapNewExchangeId hat
                            // Order.BrokerId bereits ergänzt (einzige Add-Stelle). Vorher garantiertes
                            // Duplikat von o.OrderId in Order.BrokerId bei jedem Durchlauf dieses Zweigs.
                            if (mapped)
                            {
                                OnOrderIdChangedEvent(new BrokerageOrderIdChangedEvent
                                {
                                    OrderId = existingState.Order.Id,
                                    BrokerId = existingState.Order.BrokerId
                                });
                            }

                            Log.Trace($"{Name}.HandleOrderSocket: Modify mapped via Socket | Old: {oldBrokerId} → New: {o.OrderId}");
                        }
                    }

                    // -------------------------------------------------------
                    // 🔥 SENSEMANN-CHECK: Fills aus dem Order-Stream vernichten 🔥
                    // -------------------------------------------------------
                    // Dies MUSS vor dem State-Lookup passieren, damit es auch greift, 
                    // wenn der Trade-Socket die Order bereits gelöscht hat!
                    if (o.Status == SharedOrderStatus.Filled || o.Status == SharedOrderStatus.Open)
                    {
                        if (ExchangeSupportsUserTradeStream && o.Status == SharedOrderStatus.Filled)
                        {
                            Log.Trace($"{Name}.HandleOrderSocket: Hard-ignoring {o.Status} for {o.OrderId} in Order-Stream. Trade-Stream owns this.");

                            // Wir setzen nur das Pending-Flag zurück, falls die Order noch modifiziert wurde.
                            if (_orderStateManager.TryGetByExchangeId(o.OrderId, out var pendingState))
                            {
                                pendingState.IsUpdatePending = false;
                            }
                            continue;
                        }
                        // Wenn kein Trade-Stream unterstützt wird ODER es sich um eine Teilfüllung handelt
                        // (SharedOrderStatus kennt kein eigenes PartiallyFilled, siehe ParseOrderStatus in
                        // BingX.Net → kommt hier als Open mit QuantityFilled > 0 an), verarbeiten wir den
                        // Fill-Delta hier direkt 1:1 mit echten Fee-Daten aus dem Order-Payload.
                        else if (!ExchangeSupportsUserTradeStream && _orderStateManager.TryGetByExchangeId(o.OrderId, out var fillState))
                        {
                            var sign = fillState.OriginalQuantity > 0 ? 1m : -1m;
                            var absFilled = HasExchangeQuantity(o.QuantityFilled)
                                ? FromExchangeQuantity(fillState.Order.Symbol, o.QuantityFilled)
                                : (o.Status == SharedOrderStatus.Filled ? Math.Abs(fillState.OriginalQuantity) : 0m);

                            // GEÄNDERT: absFilled/o.QuantityFilled ist relativ zur AKTUELLEN BrokerId
                            // (startet bei 0 nach jedem Cancel+Replace). Delta daher gegen
                            // FilledQuantityCurrentOrder bilden, nicht gegen die über alle
                            // BrokerId-Generationen kumulierte FilledQuantity - sonst entsteht direkt
                            // nach einem Replace ein Phantom-Event mit negativer FillQuantity
                            // (siehe DivideByZeroException-Fall vom 2026-07-18 14:16:00, XMRUSDT).
                            var currentOrderSignedFilled = absFilled * sign;
                            var signedFill = currentOrderSignedFilled - fillState.FilledQuantityCurrentOrder;

                            if (Math.Abs(signedFill) > 0)
                            {
                                var totalFeeForCurrentOrder = o.Fee ?? 0m;
                                var deltaFee = Math.Max(0m, totalFeeForCurrentOrder - fillState.CumulativeFeePaidCurrentOrder);
                                fillState.CumulativeFeePaidCurrentOrder = totalFeeForCurrentOrder;

                                fillState.FilledQuantityCurrentOrder = currentOrderSignedFilled;
                                fillState.FilledQuantity += signedFill;
                                fillState.CumulativeFeePaid += deltaFee;

                                // NEU: Fill-Preis dieses einzelnen Deltas aus AveragePrice × kumulierte Menge
                                // ableiten, statt fälschlich OrderPrice (Limit-/Original-Preis) zu nehmen.
                                // Nur wenn die Exchange AveragePrice für dieses Event liefert (nicht für
                                // alle Shared-Exchanges wie Aster/OKX garantiert) - sonst unverändertes
                                // OrderPrice-Fallback-Verhalten wie vorher, CumulativeCostFilled bleibt
                                // dann unangetastet (kein Corruption durch angenommene 0).
                                decimal fillPrice;
                                if (o.AveragePrice.HasValue && o.AveragePrice.Value > 0m)
                                {
                                    var previousCost = fillState.CumulativeCostFilled;
                                    var newCumulativeCost = o.AveragePrice.Value * absFilled;
                                    var deltaCost = newCumulativeCost - fillState.CumulativeCostFilledCurrentOrder;   // GEÄNDERT: nicht mehr CumulativeCostFilled

                                    fillPrice = deltaCost != 0m ? Math.Abs(deltaCost / signedFill) : o.AveragePrice.Value;

                                    // Fill-Preis kann durch die Cost-Delta-Division (deltaCost / signedFill)
                                    // bis zu 28 signifikante Nachkommastellen bekommen (decimal-Division rundet
                                    // in .NET nicht automatisch). Auf die Tick-Size des Symbols runden, analog
                                    // zu GetAggressivePrice - ein Fill-Preis kann exchange-seitig ohnehin nie
                                    // feiner als die Tick-Size sein, die Nachkommastellen sind reines Artefakt
                                    // der Division, keine echte Präzision.
                                    var priceTick = _algorithm.Securities[fillState.Order.Symbol].SymbolProperties.MinimumPriceVariation;
                                    if (priceTick > 0m)
                                        fillPrice = Math.Round(fillPrice / priceTick) * priceTick;

                                    fillState.CumulativeCostFilledCurrentOrder = newCumulativeCost;   // GEÄNDERT
                                    fillState.CumulativeCostFilled += (deltaCost != 0m ? deltaCost : o.AveragePrice.Value * signedFill);  // Lifetime-Kumulator weiterhin korrekt fortschreiben
                                }
                                else
                                {
                                    fillPrice = o.OrderPrice ?? 0m;
                                }

                                fillState.LastUpdateUtc = DateTime.UtcNow;

                                var leanStatus = Math.Abs(fillState.FilledQuantity) >= Math.Abs(fillState.OriginalQuantity) || o.Status == SharedOrderStatus.Filled
                                    ? QuantConnect.Orders.OrderStatus.Filled
                                    : QuantConnect.Orders.OrderStatus.PartiallyFilled;

                                fillState.State = leanStatus == QuantConnect.Orders.OrderStatus.Filled ? OrderLifeCycleState.Filled : OrderLifeCycleState.PartiallyFilled;

                                if (fillState.IsClosed)
                                {
                                    _orderStateManager.TryRemove(fillState.ClientOrderId, out _);
                                }

                                OnOrderEvent(new OrderEvent(fillState.Order, DateTime.UtcNow, new OrderFee(new CashAmount(deltaFee, o.FeeAsset ?? SettleAsset)))
                                {
                                    Status = leanStatus,
                                    FillPrice = fillPrice,
                                    FillQuantity = signedFill,
                                    Message = "Order socket stream (Execution fallback)"
                                });

                                if (leanStatus == QuantConnect.Orders.OrderStatus.Filled)
                                {
                                    continue;
                                }
                            }
                        }
                    }

                    // -------------------------------------------------------
                    // NORMAL STATUS UPDATE (Nur für Canceled / Open etc.)
                    // -------------------------------------------------------
                    if (_orderStateManager.TryGetByExchangeId(o.OrderId, out var state))
                    {
                        // Ignoriere Status-Events von alten, ersetzten Tickets
                        if (o.OrderId != state.BrokerId)
                        {
                            Log.Trace($"{Name}.HandleOrderSocket: Ignoring status '{o.Status}' for old replaced ticket {o.OrderId}. Current active ticket is {state.BrokerId}.");
                            continue;
                        }

                        state.LastUpdateUtc = DateTime.UtcNow;
                        var absFilled = FromExchangeQuantity(state.Order.Symbol, o.QuantityFilled);
                        var leanStatus = MapStatus(o.Status, absFilled);

                        if (leanStatus is QuantConnect.Orders.OrderStatus.Canceled or QuantConnect.Orders.OrderStatus.Invalid)
                        {
                            if (state.IsUpdatePending)
                            {
                                Log.Trace($"{Name}.HandleOrderSocket: Suppressing Cancel event for {state.BrokerId} because an Update is pending.");
                                continue;
                            }

                            state.State = OrderLifeCycleState.Canceled;
                            // TryRemove bereinigt beide internen Dicts.
                            _orderStateManager.TryRemove(state.ClientOrderId, out _);

                            OnOrderEvent(new OrderEvent(state.Order, DateTime.UtcNow, OrderFee.Zero)
                            {
                                Status = leanStatus,
                                Message = "Order socket update"
                            });
                        }
                        else
                        {
                            // Order ist auf der Exchange weiterhin aktiv - ein Status-Event für die
                            // aktuelle BrokerId (egal ob sauber auf Submitted/Open gemappt oder ein von
                            // MapStatus nicht abgebildeter Status wie Krakens "edited"/Unknown nach einem
                            // In-Place-Edit) KANN bestätigen, dass ein evtl. pending Update angekommen
                            // ist - muss es aber nicht: verspätete/doppelte Events können noch den alten
                            // Preis tragen. Deshalb erst gegen state.LimitPrice (Preis VOR dem Reprice-
                            // Request, siehe ChaseOrderLoop) prüfen: nur ein Event mit einem ANDEREN
                            // Preis bestätigt, dass der Edit tatsächlich angekommen ist. Ohne diesen
                            // Reset bleibt IsUpdatePending bei In-Place-Edit-Exchanges
                            // (ExchangeModifiesOrdersInPlace: Kraken, Bybit, OKX, Aster, Lighter) nach dem
                            // ersten Reprice für immer true, weil der BrokerId-Wechsel-Zweig weiter oben
                            // (MODIFY/REPLACEMENT DETECTION) bei gleichbleibender BrokerId nie greift -
                            // ChaseOrderLoop würde dann jeden weiteren Tick per `if (state.IsUpdatePending)
                            // continue;` überspringen (beobachtet: Kraken XAUTUSDC, ein Reprice und dann
                            // stundenlang nichts mehr).
                            if (state.IsUpdatePending
                                && (!o.OrderPrice.HasValue || !state.LimitPrice.HasValue || o.OrderPrice.Value != state.LimitPrice.Value))
                            {
                                Log.Trace($"{Name}.HandleOrderSocket: Resetting IsUpdatePending for {state.BrokerId} (RawStatus={o.Status}, LeanStatus={leanStatus}, OldPrice={state.LimitPrice}, EventPrice={o.OrderPrice}).");
                                state.IsUpdatePending = false;

                                // state.LimitPrice auf den jetzt bestätigten Preis nachziehen, damit das
                                // Feld auch außerhalb von ChaseOrderLoop (z.B. für Debugging/Logging)
                                // jederzeit den validen, aktuell bestätigten Preis zeigt statt bis zum
                                // nächsten Reprice-Tick auf dem alten Stand zu bleiben.
                                if (o.OrderPrice.HasValue)
                                {
                                    state.LimitPrice = o.OrderPrice.Value;
                                }
                            }

                            if (leanStatus == QuantConnect.Orders.OrderStatus.Submitted) // SharedOrderStatus.Open ohne Fill
                            {
                                state.State = OrderLifeCycleState.Open;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Log.Error($"{Name}.HandleOrderSocket: Unhandled exception processing OrderId='{o.OrderId}' " +
                              $"ClientOrderId='{o.ClientOrderId}' Status='{o.Status}': {ex.ToString()}");
                }
            }
        }

        private async Task ReconcileLoop(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(_reconciliationInterval, ct).ConfigureAwait(false);

                    var openRes = await _orderClient.GetOpenFuturesOrdersAsync(
                        new GetOpenOrdersRequest(tradingMode: OpenOrdersTradingMode, exchangeParameters: OpenOrdersExchangeParameters)
                    ).ConfigureAwait(false);

                    if (!openRes.Success || openRes.Data == null)
                    {
                        Log.Error($"{Name}.ReconcileLoop: Failed to fetch open orders: {openRes.Error}");
                        continue;
                    }

                    var openExchangeOrders = openRes.Data
                        .GroupBy(x => x.OrderId)
                        .ToDictionary(g => g.Key, g => g.First());

                    foreach (var state in _orderStateManager.GetAllStates().ToArray())
                    {
                        // Skip orders still in Placing phase – they have no real exchange ID yet.
                        // The REST call hasn't returned; no point querying the exchange for a temp ID.
                        if (state.State == OrderLifeCycleState.Placing) continue;
                        var brokerId = state.BrokerId;
                        if (string.IsNullOrEmpty(brokerId))
                        {
                            Log.Trace($"{Name}.ReconcileLoop: Skipping state with empty BrokerId. ClientOrderId: {state.ClientOrderId}");
                            continue;
                        }

                        var updateStillPending = state.IsUpdatePending &&
                            (DateTime.UtcNow - state.LastUpdateUtc).TotalSeconds < 10;

                        if (updateStillPending ||
                            openExchangeOrders.ContainsKey(brokerId) ||
                            (DateTime.UtcNow - state.LastUpdateUtc).TotalSeconds < 5)
                            continue;

                        var sharedSymbol = GetSharedSymbol(state.Order.Symbol);
                        var statusCheck = await _orderClient
                            .GetFuturesOrderAsync(new GetOrderRequest(sharedSymbol, brokerId))
                                .ConfigureAwait(false);

                        if (!statusCheck.Success || statusCheck.Data == null)
                        {
                            Log.Error($"{Name}.ReconcileLoop: Failed to verify order {brokerId}. Error: {statusCheck.Error}");
                            continue;
                        }

                        var brokerOrder = statusCheck.Data;

                        // SAFE REMOVE via ClientOrderId (bereinigt beide internen Dicts).
                        if (!_orderStateManager.TryRemove(state.ClientOrderId, out var removedState))
                            continue;

                        // CASE 1: FILLED
                        if (brokerOrder.Status == SharedOrderStatus.Filled)
                        {
                            // WICHTIG: State setzen, bevor das Event gefeuert wird. Ohne das bleibt
                            // state.IsClosed für immer false - ein ChaseOrderLoop, der noch seine
                            // eigene Referenz auf dieses OrderState-Objekt hält, würde sonst ewig
                            // weiterlaufen und gegen eine längst aus dem Manager entfernte Order
                            // reprice-Versuche schicken (beobachtet: BingX, Fill kam über Reconcile
                            // statt Socket, ChaseOrderLoop lief >20min gegen "old state missing" weiter).
                            removedState.State = OrderLifeCycleState.Filled;

                            var finalFillAbsQty = HasExchangeQuantity(brokerOrder.QuantityFilled)
                                ? FromExchangeQuantity(state.Order.Symbol, brokerOrder.QuantityFilled)
                                : Math.Abs(removedState.OriginalQuantity);

                            var finalSignedFillQty = finalFillAbsQty * (removedState.OriginalQuantity > 0 ? 1m : -1m);
                            // GEÄNDERT: brokerOrder.QuantityFilled kommt relativ zur AKTUELLEN BrokerId
                            // (brokerId, siehe oben) → gegen FilledQuantityCurrentOrder vergleichen,
                            // nicht gegen die kumulierte FilledQuantity über alle Generationen.
                            var remainingToFill = finalSignedFillQty - removedState.FilledQuantityCurrentOrder;
                            if (Math.Abs(remainingToFill) > 0)
                            {
                                // FIX 3: Doppelbuchungen der Gebühren verhindern!
                                var totalExchangeFee = brokerOrder.Fee ?? 0m;
                                var remainingFee = Math.Max(0m, totalExchangeFee - removedState.CumulativeFeePaid);

                                OnOrderEvent(new OrderEvent(removedState.Order, DateTime.UtcNow, OrderFee.Zero)
                                {
                                    Status = QuantConnect.Orders.OrderStatus.Filled,
                                    FillPrice = brokerOrder.AveragePrice ?? 0,
                                    FillQuantity = remainingToFill,
                                    OrderFee = new OrderFee(new CashAmount(remainingFee, brokerOrder.FeeAsset ?? SettleAsset)),
                                    Message = "Reconciled Fill"
                                });
                            }
                        }

                        // CASE 2: STILL OPEN
                        else if (brokerOrder.Status == SharedOrderStatus.Open)
                        {
                            // Falls IsUpdatePending seit >10s (Timer oben, updateStillPending) noch true
                            // ist UND der per REST abgefragte Preis vom Preis abweicht, der vor dem
                            // letzten Reprice-Request galt (state.LimitPrice, siehe ChaseOrderLoop), ist
                            // das Update auf der Exchange offensichtlich angekommen - nur die
                            // Socket-Bestätigung ist nie oder zu spät eingetroffen. Reset hier
                            // nachholen, sonst bleibt die Order für immer im ChaseOrderLoop-Throttle
                            // hängen (`if (state.IsUpdatePending) continue;`).
                            if (removedState.IsUpdatePending && brokerOrder.OrderPrice.HasValue
                                && removedState.LimitPrice.HasValue
                                && brokerOrder.OrderPrice.Value != removedState.LimitPrice.Value)
                            {
                                Log.Trace($"{Name}.ReconcileLoop: Resetting IsUpdatePending for {brokerId} via REST reconcile " +
                                          $"(OldPrice={removedState.LimitPrice}, RestPrice={brokerOrder.OrderPrice}).");
                                removedState.IsUpdatePending = false;
                                // state.LimitPrice auf den REST-bestätigten Preis nachziehen - siehe
                                // gleiche Begründung wie in HandleOrderSocket.
                                removedState.LimitPrice = brokerOrder.OrderPrice.Value;
                            }

                            // Re-register: TryAdd indexes by both ClientOrderId and BrokerId (exchange ID).
                            removedState.LastUpdateUtc = DateTime.UtcNow;
                            _orderStateManager.TryAdd(removedState.ClientOrderId, removedState);
                            Log.Trace($"{Name}.ReconcileLoop: Order {brokerId} still open on exchange, re-registered.");
                        }
                        // CASE 3: CANCELED / UNKNOWN
                        else
                        {
                            // Gleicher Grund wie bei CASE 1: State setzen, damit ein evtl. noch
                            // laufender ChaseOrderLoop erkennt, dass die Order terminal ist.
                            removedState.State = OrderLifeCycleState.Canceled;

                            OnOrderEvent(new OrderEvent(removedState.Order, DateTime.UtcNow, OrderFee.Zero)
                            {
                                Status = QuantConnect.Orders.OrderStatus.Canceled,
                                Message = $"Order {brokerOrder.OrderId} reconciled cancel"
                            });
                        }
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    Log.Error($"{Name}.ReconcileLoop Error: {ex}");
                }
            }
        }

        #endregion
    }
}