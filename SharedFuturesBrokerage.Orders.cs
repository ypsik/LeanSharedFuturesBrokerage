using Accord.Statistics.Distributions.Univariate;
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
        public virtual decimal MinimumOrderNotionalValue => 0m;

        /// <summary>
        /// Gibt an, ob die Börse Orders in-place ändert (z.B. Bybit) oder Cancel+Replace nutzt (z.B. Hyperliquid).
        /// </summary>
        public virtual bool ExchangeModifiesOrdersInPlace => false;

        protected virtual bool RequiresExplicitCancelBeforeReplace => false;

        /// <summary>
        /// Gibt an, ob die Börse einen dedizierten User-Trade-Stream (Fills) unterstützt.
        /// Wenn false, werden Fills direkt im Order-Stream verarbeitet.
        /// </summary>
        public virtual bool ExchangeSupportsUserTradeStream => true;
        protected virtual SharedMarginMode? SharedMarginMode => null;
        protected virtual SharedPositionSide? SharedPositionSide => null;

        #region Quantity Unit Conversion

        /// <summary>
        /// Wandelt eine LEAN Base-Asset-Menge (z.B. 1.5 HYPE) in die von der Exchange erwartete
        /// SharedQuantity-Repräsentation um. Default: 1:1 Passthrough als BaseAsset, für Exchanges
        /// wie Hyperliquid, Bybit, Bitget, die Orders direkt in Base-Asset-Einheiten entgegennehmen.
        /// Exchanges mit Contract-Notation (z.B. OKX) überschreiben dies, um in Contracts umzurechnen
        /// (base_qty / ContractMultiplier) und geben zusätzlich die tatsächlich gerundete Base-Menge
        /// über <paramref name="roundedBaseQuantity"/> zurück, damit die State-Machine mit der real
        /// an der Exchange platzierten Menge arbeitet statt mit der ungerundeten Zielmenge.
        /// </summary>
        protected virtual SharedQuantity ToExchangeQuantity(Symbol symbol, decimal absBaseQuantity, out decimal roundedBaseQuantity)
        {
            roundedBaseQuantity = absBaseQuantity;
            return new SharedQuantity { QuantityInBaseAsset = absBaseQuantity };
        }

        /// <summary>
        /// Wandelt eine von der Exchange gelieferte SharedOrderQuantity zurück in eine LEAN
        /// Base-Asset-Menge um. Default: liest QuantityInBaseAsset direkt.
        /// Exchanges mit Contract-Notation (z.B. OKX) überschreiben dies, um Contracts zurück
        /// in Base-Asset-Einheiten umzurechnen (contracts * ContractMultiplier).
        /// </summary>
        protected virtual decimal FromExchangeQuantity(Symbol symbol, SharedOrderQuantity? quantity)
            => quantity?.QuantityInBaseAsset ?? 0m;

        /// <summary>
        /// Prüft ob die Exchange für dieses SharedOrderQuantity-Objekt tatsächlich einen Wert geliefert
        /// hat (im Unterschied zu einer echten, gemeldeten Menge von 0). Wird an Stellen benötigt, die
        /// ursprünglich per Null-Coalescing (?. ... ?? Fallback) nur bei FEHLENDEM Wert auf einen Fallback
        /// (meist OriginalQuantity) zurückfielen, nicht bei einer echten 0-Meldung. FromExchangeQuantity
        /// allein kann das nicht unterscheiden (liefert in beiden Fällen 0m), daher dieser separate Hook.
        /// Default prüft QuantityInBaseAsset.HasValue. Exchanges mit Contract-Notation (z.B. OKX)
        /// überschreiben dies auf QuantityInContracts.HasValue.
        /// </summary>
        protected virtual bool HasExchangeQuantity(SharedOrderQuantity? quantity)
            => quantity?.QuantityInBaseAsset.HasValue == true;

        #endregion


        // --- SINGLE SOURCE OF TRUTH ---
        // Primary key: clientOrderId (permanent, never changes).
        // Exchange ID is indexed separately via _orderStateManager for O(1) socket lookups.
        protected readonly OrderStateManager _orderStateManager = new();


        #region Order Management

        protected virtual ExchangeParameters OpenOrdersExchangeParameters => new ExchangeParameters();

        /// <summary>
        /// Überschreiben um für GetOpenOrders/ReconcileLoop einen expliziten TradingMode zu erzwingen,
        /// statt den Shared-Client-Default (PerpetualLinear → i.d.R. Swap-Instrumente) zu nutzen.
        /// Relevant für Exchanges, bei denen der Shared-Client anhand von TradingMode.IsPerpetual()
        /// zwischen unterschiedlichen Instrument-Kategorien verzweigt (z.B. OKX: Swap vs. Futures),
        /// und der Default-Zweig für bestimmte Instrumente falsch ist (z.B. OKX X-Perp läuft nativ
        /// unter InstrumentType.Futures, obwohl wirtschaftlich ein Perpetual).
        /// </summary>
        protected virtual TradingMode? OpenOrdersTradingMode => null;

        public override List<Order> GetOpenOrders()
        {
            var res = RunSync(() => _orderClient.GetOpenFuturesOrdersAsync(
                new GetOpenOrdersRequest(tradingMode: OpenOrdersTradingMode, exchangeParameters: OpenOrdersExchangeParameters)));

            if (!res.Success || res.Data == null) return new List<Order>();

            return res.Data.Select(o =>
            {
                var symbol = Symbol.Create(NormalizeSymbol(o.Symbol), SecurityType.CryptoFuture, Name);
                var sign = o.Side == SharedOrderSide.Sell ? -1m : 1m;
                var qty = FromExchangeQuantity(symbol, o.OrderQuantity) * sign;
                var filledQty = FromExchangeQuantity(symbol, o.QuantityFilled) * sign;
                var price = o.OrderPrice ?? 0m;

                Order order;

                // FIX: Explizite Trennung der Order-Typen um Portfolio-Korruption beim Startup zu verhindern.
                if (o.OrderType == SharedOrderType.Limit)
                {
                    order = new LimitOrder(symbol, qty, price, DateTime.UtcNow);
                }
                else if (o.OrderType == SharedOrderType.Market)
                {
                    order = new MarketOrder(symbol, qty, DateTime.UtcNow);
                }
                else
                {
                    // Fallback für StopMarket, StopLimit, TrailingStop, etc.
                    // Durch das Mappen auf StopMarketOrder weiß LEAN, dass diese Order
                    // bedingungsgeknüpft ist und führt sie nicht sofort als Market-Order aus.
                    order = new StopMarketOrder(symbol, qty, price, DateTime.UtcNow);
                }

                order.BrokerId.Add(o.OrderId);
                order.Status = MapStatus(o.Status, Math.Abs(filledQty));

                var state = new OrderState(order, o.ClientOrderId ?? string.Empty)
                {
                    OriginalQuantity = qty,
                    FilledQuantity = filledQty,
                    FilledQuantityCurrentOrder = filledQty,
                    BrokerId = o.OrderId,
                    State = order.Status == QuantConnect.Orders.OrderStatus.PartiallyFilled ? OrderLifeCycleState.PartiallyFilled : OrderLifeCycleState.Open,
                    LimitPrice = o.OrderPrice
                };

                // TryAdd: nop if clientId already registered (idempotent on reconnect).
                // BrokerId is already set → manager auto-indexes in _statesByExchangeId.
                if (!string.IsNullOrEmpty(state.ClientOrderId))
                    _orderStateManager.TryAdd(state.ClientOrderId, state);

                return order;
            }).ToList();
        }

        public override bool PlaceOrder(Order order)
        {
            decimal executionQuantity = order.Quantity;

            if (MinimumOrderNotionalValue > 0m)
            {
                decimal price = 0m;
                if (order is LimitOrder limitOrder)
                    price = limitOrder.LimitPrice;
                else if (order is StopMarketOrder stopMarketOrder)
                    price = stopMarketOrder.StopPrice;
                else
                    price = _algorithm.Securities[order.Symbol].Price;

                if (price > 0m)
                {
                    decimal currentNotional = Math.Abs(executionQuantity * price);

                    if (currentNotional < MinimumOrderNotionalValue && executionQuantity != 0m)
                    {
                        var props = _spdb.GetSymbolProperties(order.Symbol.ID.Market, order.Symbol, order.Symbol.SecurityType, SettleAsset);
                        decimal baseLotSize = props?.LotSize ?? 0.01m;
                        decimal minUnitsRequired = MinimumOrderNotionalValue / price;
                        decimal adjustedQuantity = Math.Ceiling(minUnitsRequired / baseLotSize) * baseLotSize;

                        if (executionQuantity < 0)
                            adjustedQuantity = -adjustedQuantity;

                        Log.Trace($"{Name}.PlaceOrder: Adjusting execution quantity for {order.Symbol.Value} from {executionQuantity} to {adjustedQuantity} to meet the minimum of ${MinimumOrderNotionalValue}.");
                        OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero) { Quantity = adjustedQuantity });
                        executionQuantity = adjustedQuantity;
                    }
                }
            }

            var clientOrderId = GenerateClientId(order.Id);

            // Chase-Orders: Einstiegspreis passiv über dieselbe GetAggressivePrice/ApplyCrossGuard-
            // Logik wie beim Reprice berechnen, statt den vom Aufrufer übergebenen (ggf. crossing)
            // Preis unverändert zu senden - sonst matcht die Order sofort als Taker, noch bevor der
            // ChaseOrderLoop überhaupt einmal laufen konnte.
            decimal chaseInitialBid = 0m, chaseInitialAsk = 0m;
            if (order.Properties is Orders.ChaseOrderProperties initialChaseProps && order.Type == OrderType.Limit
                && _quoteCache.TryGetValue(order.Symbol, out var initialQuote) && initialQuote.Bid != 0m && initialQuote.Ask != 0m)
            {
                chaseInitialBid = initialQuote.Bid;
                chaseInitialAsk = initialQuote.Ask;

                bool isBuyInit = order.Quantity > 0;
                decimal originalPrice = (order as LimitOrder)?.LimitPrice ?? 0m;
                decimal initialPrice = GetAggressivePrice(order.Symbol, isBuyInit, initialChaseProps.Aggression, initialQuote.Bid, initialQuote.Ask);

                Log.Trace($"{Name}.PlaceOrder: initial chase price for {order.Symbol.Value} (orderId={order.Id}) " +
                    $"overridden from {originalPrice} to {initialPrice} (bid={initialQuote.Bid}, ask={initialQuote.Ask}, " +
                    $"aggression={initialChaseProps.Aggression}).");

                order.ApplyUpdateOrderRequest(new UpdateOrderRequest(
                    DateTime.UtcNow, order.Id, new UpdateOrderFields { LimitPrice = initialPrice }));
            }

            // Base-Menge in die von der Exchange erwartete Einheit umrechnen (BaseAsset oder Contracts).
            // roundedExecutionQuantity ist die tatsächlich gültige Base-Menge NACH Rundung auf
            // Contract-/Lot-Steps, damit die State-Machine mit der real platzierten Menge arbeitet.
            var sharedQuantity = ToExchangeQuantity(order.Symbol, Math.Abs(executionQuantity), out var roundedAbsQuantity);
            var signedRoundedQuantity = executionQuantity >= 0 ? roundedAbsQuantity : -roundedAbsQuantity;

            var request = new PlaceFuturesOrderRequest(
                GetSharedSymbol(order.Symbol),
                executionQuantity > 0 ? SharedOrderSide.Buy : SharedOrderSide.Sell,
                order.Type == OrderType.Limit ? SharedOrderType.Limit : SharedOrderType.Market,
                sharedQuantity)
            {
                Price = (order as LimitOrder)?.LimitPrice,
                ClientOrderId = clientOrderId,
                ExchangeParameters = PlaceFuturesOrderExchangeParameters,
                PositionSide = SharedPositionSide,
                MarginMode = SharedMarginMode
            };

            // State Machine: Order mit Placing-State registrieren bevor API-Call rausgeht.
            // Socket-Handler kann die Order damit sofort finden falls HL instantan füllt
            // und das WS-Event noch während RunSync() ankommt.
            // BrokerId bleibt null (noch keine Exchange-ID bekannt) → manager indiziert nur
            // unter clientOrderId, bis MapNewExchangeId die Exchange-ID nachträgt.
            // WICHTIG: OriginalQuantity nutzt die GERUNDETE Menge (signedRoundedQuantity), nicht die
            // ursprüngliche executionQuantity, damit Fill-Tracking/Remaining mit der real an der
            // Exchange platzierten Menge übereinstimmt (relevant für Contract-Exchanges wie OKX).
            var placingState = new OrderState(order, clientOrderId)
            {
                OriginalQuantity = signedRoundedQuantity,
                FilledQuantity = 0m,
                FilledQuantityCurrentOrder = 0m,
                State = OrderLifeCycleState.Placing,
                LimitPrice = (order as LimitOrder)?.LimitPrice
            };
            _orderStateManager.TryAdd(clientOrderId, placingState);

            var res = RunSync(() => ExecutePlaceOrderAsync(request));
            if (!res.Success)
            {
                // Placing-State wieder austragen
                _orderStateManager.TryRemove(clientOrderId, out _);

                var errorMsg = res.Error?.ToString() ?? "Unknown exchange error";
                Log.Error($"{Name}.PlaceOrder({order.Symbol.Value}): {errorMsg}");
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "PlaceOrder", errorMsg));
                return false;
            }

            // KEIN order.BrokerId.Add(res.Data.Id) hier mehr - das übernimmt ausschließlich
            // MapNewExchangeId weiter unten (bzw. HandleOrderSocket, falls der Socket schneller war).
            // Vorher stand hier ein unconditionales Add, das bei State != Placing (Socket war
            // schneller) zusätzlich zum Add in MapNewExchangeId lief -> res.Data.Id doppelt in
            // Order.BrokerId.

            // Prüfen ob der State noch im Placing-Zustand ist.
            // Falls nicht, hat 'HandleOrderSocket' bereits übernommen, den Exchange-ID-Swap
            // per MapNewExchangeId durchgeführt und das Submitted-Event gefeuert.
            if (_orderStateManager.TryGetValue(clientOrderId, out var currentState) &&
                currentState.State == OrderLifeCycleState.Placing)
            {

                if (!String.IsNullOrEmpty(res.Data.Id) && clientOrderId != res.Data.Id)
                {
                    var mapped = _orderStateManager.MapNewExchangeId(clientOrderId, res.Data.Id);

                    if (mapped)
                    {
                        placingState.State = OrderLifeCycleState.Submitted;
                        placingState.LastUpdateUtc = DateTime.UtcNow;
                        OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero) { Status = QuantConnect.Orders.OrderStatus.Submitted });
                    }
                }
            }
            // else: Socket hat Placing-State bereits umgebogen + Events gefeuert

            if (order.Properties is Orders.ChaseOrderProperties chaseProps && order.Type == OrderType.Limit)
            {
                // State kann inzwischen (Race mit HandleOrderSocket) unter derselben ClientOrderId
                // bereits durch den Socket-Pfad ersetzt worden sein - daher aktuellen State ziehen,
                // nicht den lokalen placingState-Verweis von oben verwenden.
                if (_orderStateManager.TryGetValue(clientOrderId, out var chaseState))
                {
                    chaseState.ChaseAggression = chaseProps.Aggression;
                    chaseState.ChaseInterval = chaseProps.ChaseInterval;
                    chaseState.LastBid = chaseInitialBid;
                    chaseState.LastAsk = chaseInitialAsk;
                    _ = Task.Run(() => ChaseOrderLoop(chaseState));
                }
            }

            return true;
        }

        // =====================================================================
        // CHASE ORDERS
        // =====================================================================
        // Portiert 1:1 aus AdaptiveMacroFlowAlgorithm (Buy/Sell/GetAggressivePrice/ApplyCrossGuard/
        // Reprice), nur dass hier die Brokerage selbst die Order nachführt statt der Algorithmus.
        // Trigger ist ein eigener Task pro Order statt eines gemeinsamen Loops/Timers - das
        // ChaseInterval aus den ChaseOrderProperties ist damit direkt der Throttle zwischen zwei
        // Reprice-Versuchen dieser einen Order.

        private async Task ChaseOrderLoop(OrderState state)
        {
            var order = state.Order;
            var symbol = order.Symbol;

            Log.Trace($"{Name}.ChaseOrderLoop: started for {symbol.Value} (orderId={order.Id}, clientOrderId={state.ClientOrderId}, interval={state.ChaseInterval}, aggression={state.ChaseAggression}, startPrice={((LimitOrder)order).LimitPrice}).");

            while (!state.IsClosed)
            {
                try
                {
                    await Task.Delay(state.ChaseInterval ?? TimeSpan.FromMilliseconds(1000), _chaseCts.Token);
                }
                catch (TaskCanceledException)
                {
                    Log.Trace($"{Name}.ChaseOrderLoop: cancelled for {symbol.Value} (orderId={order.Id}).");
                    return;
                }

                if (state.IsClosed) break;
                if (state.IsUpdatePending) continue;

                if (!_quoteCache.TryGetValue(symbol, out var quote) || quote.Bid == 0m || quote.Ask == 0m)
                    continue;

                // Markt hat sich seit dem letzten Reprice nicht bewegt -> nichts zu tun.
                if (quote.Bid == state.LastBid && quote.Ask == state.LastAsk)
                    continue;

                bool isBuy = order.Quantity > 0;
                decimal currentLimit = ((LimitOrder)order).LimitPrice;

                // Bin ich noch Top of Book (am oder besser als Best Bid/Ask)? Dann kein Chase,
                // auch wenn sich der Markt Richtung anderer Seite bewegt hat - Order bleibt stehen,
                // solange sie noch die beste im Buch ist.
                if (isBuy && currentLimit >= quote.Bid || !isBuy && currentLimit <= quote.Ask)
                {
                    state.LastBid = quote.Bid;
                    state.LastAsk = quote.Ask;
                    continue;
                }

                decimal targetPrice = GetAggressivePrice(symbol, isBuy, state.ChaseAggression ?? 0, quote.Bid, quote.Ask);
                decimal tick = _algorithm.Securities[symbol].SymbolProperties.MinimumPriceVariation;

                // Bei Cancel+Replace-Exchanges wird die Restmenge als neue Order-Größe geschickt und
                // kann unter die Minimum-Notional-Grenze fallen -> UpdateOrder würde jeden weiteren
                // Reprice-Versuch dauerhaft ablehnen (siehe MinimumOrderNotionalValue-Check dort).
                // Loop dann sauber beenden statt sinnlos weiterzupollen. Bei In-Place-Edit-Exchanges
                // wird dort nie quantity mitgeschickt (quantity = null), der Check greift also nie -
                // dort bleibt der Chase daher unangetastet.
                if (!ExchangeModifiesOrdersInPlace && MinimumOrderNotionalValue > 0m
                    && Math.Abs(state.Remaining) * targetPrice < MinimumOrderNotionalValue)
                {
                    Log.Trace($"{Name}.ChaseOrderLoop: stopping for {symbol.Value} (orderId={order.Id}) - " +
                              $"remaining {state.Remaining} (~{Math.Abs(state.Remaining) * targetPrice:F2}$) " +
                              $"below minimum ${MinimumOrderNotionalValue}. Order stays resting at {currentLimit}.");
                    return;
                }

                if (Math.Abs(currentLimit - targetPrice) > tick)
                {
                    Log.Trace($"{Name}.ChaseOrderLoop: repricing {symbol.Value} (orderId={order.Id}) from {currentLimit} to {targetPrice} (bid={quote.Bid}, ask={quote.Ask}, remaining={state.Remaining}).");

                    // LimitPrice VOR ApplyUpdateOrderRequest setzen, solange currentLimit noch der
                    // aktuell bestätigte (alte) Preis ist - danach überschreibt
                    // ApplyUpdateOrderRequest LimitPrice sofort lokal mit targetPrice.
                    state.LimitPrice = currentLimit;

                    // LimitPrice ist read-only - Order.ApplyUpdateOrderRequest ist der offizielle
                    // Weg, das trotzdem zu setzen (LEAN nutzt denselben Mechanismus intern u.a.
                    // für readonly Tag-Updates). Läuft NICHT über die OrderTicket/Transaction-
                    // Manager-Pipeline (kein ticket.UpdateRequests-Eintrag), daher liest UpdateOrder
                    // gleich danach den neuen Preis über den order.Price-Fallback.
                    order.ApplyUpdateOrderRequest(new UpdateOrderRequest(
                        DateTime.UtcNow, order.Id, new UpdateOrderFields { LimitPrice = targetPrice }));

                    if (!UpdateOrder(order))
                    {
                        Log.Error($"{Name}.ChaseOrderLoop: reprice update REJECTED for {symbol.Value} (orderId={order.Id}), stayed near {currentLimit}.");
                    }
                }

                state.LastBid = quote.Bid;
                state.LastAsk = quote.Ask;
            }

            Log.Trace($"{Name}.ChaseOrderLoop: stopped for {symbol.Value} (orderId={order.Id}), final state={state.State}, filled={state.FilledQuantity}/{state.OriginalQuantity}.");
        }

        private decimal GetAggressivePrice(Symbol symbol, bool isBuy, decimal aggression, decimal bid, decimal ask)
        {
            if (bid == 0m || ask == 0m) return _algorithm.Securities[symbol].Price;

            decimal spread = ask - bid;
            decimal tick = _algorithm.Securities[symbol].SymbolProperties.MinimumPriceVariation;

            // aggression=0 -> eigene Seite (bid bei Buy, ask bei Sell), aggression=1 -> Gegenseite (Cross)
            decimal rawPrice = isBuy
                ? bid + aggression * spread
                : ask - aggression * spread;

            decimal roundedPrice = Math.Round(rawPrice / tick) * tick;

            // aggression=1 -> bewusst sofort crossen (quasi Market), Guard hier nicht anwenden
            decimal guarded = aggression >= 1m ? roundedPrice : ApplyCrossGuard(isBuy, roundedPrice, bid, ask, tick);
            return Math.Round(guarded / tick) * tick;
        }

        private static decimal ApplyCrossGuard(bool isBuy, decimal price, decimal bid, decimal ask, decimal tick)
        {
            if (bid == 0m || ask == 0m) return price;

            if (isBuy)
            {
                if (price >= ask) return ask - tick;
            }
            else
            {
                if (price <= bid) return bid + tick;
            }

            return price;
        }

        public override bool CancelOrder(Order order)
        {
            if (!order.BrokerId.Any()) return false;
            var id = order.BrokerId.Last();

            var res = RunSync(() => ExecuteCancelOrderAsync(new CxCancelOrderRequest(GetSharedSymbol(order.Symbol), id, CancelFuturesOrderExchangeParameters)));
            if (!res.Success)
            {
                var errorMsg = res.Error?.ToString() ?? "Unknown exchange error";
                Log.Error($"{Name}.CancelOrder({order.Symbol.Value}): {errorMsg}");
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "CancelOrder", errorMsg));
                return false;
            }

            // FIX Bug 1: State zuerst aus dem Manager entfernen, dann Event feuern.
            // Damit schlägt der Socket-Handler fehl (TryGetByExchangeId miss) → kein doppeltes Event.
            // Lookup via Exchange-ID → ClientOrderId → TryRemove (bereinigt beide internen Dicts).
            if (_orderStateManager.TryGetByExchangeId(id, out var state))
            {
                _orderStateManager.TryRemove(state.ClientOrderId, out _);
                state.State = OrderLifeCycleState.Canceled;
                state.LastUpdateUtc = DateTime.UtcNow;

                OnOrderEvent(new OrderEvent(state.Order, DateTime.UtcNow, OrderFee.Zero)
                {
                    Status = QuantConnect.Orders.OrderStatus.Canceled,
                    Message = "Cancel confirmed"
                });
            }

            return true;
        }

        public override bool UpdateOrder(Order order)
        {
            if (!order.BrokerId.Any())
                return false;

            var ticket = _algorithm.Transactions.GetOrderTicket(order.Id);
            var updates = ticket?.UpdateRequests;
            var lastUpdate = updates?.Count > 0 ? updates[updates.Count - 1] : null;

            decimal price = lastUpdate?.LimitPrice ?? (order as LimitOrder)?.LimitPrice ?? order.Price;
            decimal? quantity = order.Quantity;

            // FIX: Suche via BrokerId statt GenerateClientId
            // GenerateClientId funktioniert nicht mehr nach Bitget-Style Modify
            // weil state.ClientOrderId auf Bitget-generierte ID umgebogen wurde.
            var activeBrokerId = order.BrokerId.LastOrDefault();

            if (string.IsNullOrEmpty(activeBrokerId))
            {
                Log.Error($"{Name}.UpdateOrder: No active BrokerId for {order.Symbol.Value} (Order Id {order.Id}). Cannot update.");

                OnMessage(new BrokerageMessageEvent(
                        BrokerageMessageType.Warning,
                        "UpdateOrderInvalid",
                        $"No active broker order id found for {order.Symbol.Value}. Update cancelled."));

                return false;
            }

            OrderState? state = null;

            if (_orderStateManager.TryGetByExchangeId(activeBrokerId, out state))
            {
                if (ExchangeModifiesOrdersInPlace)
                {
                    quantity = null;
                }
                else
                {
                    quantity = lastUpdate?.Quantity ?? state.Remaining;
                }

                // Minimum notional check
                if (MinimumOrderNotionalValue > 0m && price > 0m && quantity.HasValue)
                {
                    decimal currentNotional = Math.Abs(quantity.Value) * price;

                    if (currentNotional < MinimumOrderNotionalValue)
                    {
                        Log.Trace($"{Name}.UpdateOrder: Rejecting update for {order.Symbol.Value}. " +
                                  $"Remaining quantity {quantity} (~{currentNotional:F2}$) " +
                                  $"is below minimum ${MinimumOrderNotionalValue}. Returning false.");

                        OnMessage(new BrokerageMessageEvent(
                                BrokerageMessageType.Warning,
                                "UpdateOrderInvalid",
                                $"Order remaining size too small ({currentNotional:F2}$). Update cancelled."));

                        return false;
                    }
                }

                state.IsUpdatePending = true;
            }

            var res = RunSync(() => ExecuteUpdateOrderAsync(order, price, quantity));

            if (res?.Success != true)
            {
                var errorMsg = res?.Error?.ToString() ?? "Unknown exchange error";

                // Reject-Check ZUERST: IsUpdatePending darf hier NICHT zurückgesetzt werden,
                // sonst greift der Cancel-Schutz in HandleOrderSocket nicht mehr und LEAN
                // bekommt fälschlich ein Cancel-Event für die (durch den Workaround ersetzte) Order.
                // ExecuteReplaceWorkaround verwaltet IsUpdatePending selbst bis zum Abschluss.
                if (IsRejectedUpdateError(errorMsg) && quantity.HasValue)
                {
                    Log.Trace($"{Name}.UpdateOrder: Exchange rejected in-place modify (would have matched immediately). " +
                              $"Falling back to Cancel+Replace workaround for {order.Symbol.Value}.");

                    return ExecuteReplaceWorkaround(order, price, quantity.Value, activeBrokerId, state);
                }

                if (_orderStateManager.TryGetByExchangeId(activeBrokerId, out var errorState))
                {
                    errorState.IsUpdatePending = false;
                }

                if (errorMsg.Contains("canceled or filled") || errorMsg.Contains("Cannot modify"))
                {
                    Log.Trace($"{Name}.UpdateOrder: Race condition detected. Order was already filled or canceled on exchange. Suppressing LEAN ghost event.");

                    _ = Task.Run(() => ReconcileOrderImmediateAsync(activeBrokerId, order));

                    return true;
                }

                Log.Error($"{Name}.UpdateOrder({order.Symbol.Value}): {errorMsg}");

                OnMessage(new BrokerageMessageEvent(
                    BrokerageMessageType.Warning,
                    "UpdateOrder",
                    errorMsg));

                return false;
            }

            if (_orderStateManager.TryGetByExchangeId(activeBrokerId, out var activeState))
            {
                activeState.LastUpdateUtc = DateTime.UtcNow;
            }

            return true;
        }

        protected virtual ExchangeParameters PlaceFuturesOrderExchangeParameters => new ExchangeParameters();
        protected virtual async Task<HttpResult<SharedId>> ExecutePlaceOrderAsync(PlaceFuturesOrderRequest request)
        {
            if (_orderManagementSocket != null)
            {
                var res = await _orderManagementSocket.PlaceFuturesOrderAsync(request).ConfigureAwait(false);
                return new HttpResult<SharedId>(Name, res.Data, res.Error);
            }

            return await _orderClient.PlaceFuturesOrderAsync(request).ConfigureAwait(false);
        }

        protected virtual Task<HttpResult<SharedId>> ExecuteUpdateOrderAsync(Order order, decimal price, decimal? quantity)
            => Task.FromResult<HttpResult<SharedId>>(new HttpResult<SharedId>(Name, null, new InvalidOperationError("Update order not supported by this exchange")));
        protected virtual ExchangeParameters CancelFuturesOrderExchangeParameters => new ExchangeParameters();
        protected virtual async Task<HttpResult<SharedId>> ExecuteCancelOrderAsync(CxCancelOrderRequest request)
        {
            if (_orderManagementSocket != null)
            {
                var res = await _orderManagementSocket.CancelFuturesOrderAsync(request).ConfigureAwait(false);
                return new HttpResult<SharedId>(Name, res.Data, res.Error);
            }

            return await _orderClient.CancelFuturesOrderAsync(request).ConfigureAwait(false);
        }

        /// <summary>
        /// Überschreiben um exchange-spezifische "Order ist bereits terminal"-Fehler zu erkennen.
        /// Wenn true zurückgegeben wird, löst UpdateOrder sofort einen Reconcile für die Order aus.
        /// Beispiel Hyperliquid: errorMsg.Contains("canceled or filled")
        /// </summary>
        protected virtual bool IsTerminalUpdateError(string errorMsg) => false;

        /// <summary>
        /// Überschreiben um exchange-spezifische "Modify würde sofort matchen, daher rejected"-Fehler
        /// zu erkennen. Tritt z.B. bei Hyperliquid mit ALO/Post-Only-Modifies auf (Netzwerk-Upgrade Juni 2026).
        /// Wenn true zurückgegeben wird, führt UpdateOrder einen Cancel+Replace-Workaround aus
        /// (alte Order ist auf der Exchange bereits tot, neue Order wird mit denselben Parametern platziert),
        /// ohne dass LEAN ein Cancel-Event für die ursprüngliche Order sieht.
        /// Beispiel Hyperliquid: errorMsg.Contains("would have immediately matched")
        /// </summary>
        protected virtual bool IsRejectedUpdateError(string errorMsg) => false;

        /// <summary>
        /// Workaround für Exchanges, die einen In-Place-Modify ablehnen können, weil die neue
        /// Order-Konfiguration sofort gematcht hätte (z.B. Hyperliquid Post-Only/ALO Modify-Reject,
        /// Netzwerk-Upgrade Juni 2026). Die alte Order ist auf der Exchange in diesem Fall bereits
        /// storniert (der Modify-Call war intern ein Cancel+Replace, dessen Replace-Teil fehlschlug).
        ///
        /// Pattern identisch zu BitgetFuturesBrokerage.ExecuteUpdateOrderAsync: der bestehende
        /// OrderState bleibt unverändert bestehen, eine neue ClientOrderId wird lediglich als ALIAS
        /// auf diesen State registriert (_orderStateManager.TryAdd), BEVOR die neue Order via
        /// ExecutePlaceOrderAsync rausgeschickt wird. Trifft die Socket-Bestätigung mit dieser
        /// ClientOrderId ein, greift automatisch der bestehende "MODIFY / REPLACEMENT DETECTION"-Pfad
        /// in HandleOrderSocket (gleiche ClientOrderId, neue BrokerId) – inkl. korrektem
        /// IsUpdatePending-Reset und ohne Submitted-Event, da der State schon im Status Open/PartiallyFilled war.
        /// IsUpdatePending bleibt bis zum Abschluss true, damit ein eventuell nachgeliefertes
        /// Cancel-Event für die alte (jetzt tote) BrokerId unterdrückt wird.
        /// </summary>
        private bool ExecuteReplaceWorkaround(Order order, decimal price, decimal quantity, string activeBrokerId, OrderState? state)
        {
            // state wird von UpdateOrder bereits VOR dem REST-Call erfasst (Referenztyp). Falls die
            // Order zwischen Reprice-Request und Fehlerauswertung über den Trade-/Order-Socket komplett
            // gefüllt oder storniert wurde (Race), mutiert der jeweilige Handler genau dieses Objekt und
            // entfernt es danach aus dem OrderStateManager-Dictionary. Der hier mitgegebene state-Verweis
            // bleibt davon unberührt und zeigt weiterhin den aktuellen (bereits finalen) Zustand - deshalb
            // hier direkt prüfen statt per activeBrokerId erneut nachzuschlagen (das schlägt nach dem
            // Entfernen aus dem Dictionary fehl, obwohl gar kein echter Fehler vorliegt).
            if (state == null)
            {
                Log.Error($"{Name}.ExecuteReplaceWorkaround: Old state for {activeBrokerId} not found. Aborting workaround.");
                return false;
            }

            // Nur bei FILLED ist "nichts tun" sicher richtig - Zielexposure wurde erreicht, der Fill
            // wurde bereits über HandleUserTradeSocket korrekt gebucht (dieser Handler prüft
            // IsUpdatePending NICHT, kommt also immer sofort durch). Für Canceled/Invalid geht es
            // NICHT genauso, weil HandleOrderSocket Cancel-Events aktiv unterdrückt, solange
            // IsUpdatePending true ist (siehe Kommentar oben) - state.State bleibt in diesem Fall lokal
            // auf Open/PartiallyFilled stehen, selbst wenn die Exchange die Order längst storniert hat
            // (z.B. Hyperliquid, das beim gescheiterten Modify die alte Order intern immer cancelt).
            // Ein lokaler Canceled-Shortcut würde daher nie greifen und wäre irreführend - für diesen
            // Fall bleibt der bestehende explizite Cancel+Reconcile-Roundtrip unten die einzig
            // verlässliche Quelle.
            if (state.State == OrderLifeCycleState.Filled)
            {
                Log.Trace($"{Name}.ExecuteReplaceWorkaround: Order {activeBrokerId} was already FILLED " +
                          $"(race between reprice and fill). Skipping replace for {order.Symbol.Value}, target exposure already reached.");
                state.IsUpdatePending = false;
                return true;
            }

            // FIX (XMR-Overfill-Incident 2026-08-08): Vorher wurde hier direkt eine neue Order
            // platziert, während die alte (activeBrokerId) noch live im Buch lag -> beide konnten
            // gleichzeitig matchen (Overfill). Jetzt: alte Order ZUERST canceln und den Ausgang
            // synchron abwarten, bevor überhaupt an ein Replace gedacht wird.
            if (RequiresExplicitCancelBeforeReplace)
            {
                var cancelRes = RunSync(() => ExecuteCancelOrderAsync(
                    new CxCancelOrderRequest(GetSharedSymbol(order.Symbol), activeBrokerId, CancelFuturesOrderExchangeParameters)));

                if (!cancelRes.Success)
                {
                    // Cancel kann fehlschlagen, weil die alte Order zwischenzeitlich schon gefüllt oder
                    // bereits anderweitig storniert wurde (Race). Statt Fehlertexte zu raten: echten,
                    // synchron abgewarteten Status von der Exchange holen.
                    var finalStatus = RunSync(() => ReconcileOrderImmediateAsync(activeBrokerId, order));

                    if (finalStatus == SharedOrderStatus.Filled)
                    {
                        // Alte Order hat inzwischen komplett/final gefüllt - Ziel-Exposure erreicht.
                        // ReconcileOrderImmediateAsync hat den Fill bereits sauber gebucht. Keine neue
                        // Order platzieren, sonst genau das Overfill-Risiko, das wir vermeiden wollen.
                        Log.Trace($"{Name}.ExecuteReplaceWorkaround: Old order {activeBrokerId} turned out FILLED during cancel race. " +
                                  $"Skipping replace for {order.Symbol.Value}, target exposure already reached.");
                        state.IsUpdatePending = false;
                        return true;
                    }

                    if (finalStatus != SharedOrderStatus.Canceled)
                    {
                        // finalStatus ist Open (Cancel hat aus unbekanntem Grund nicht gegriffen) oder
                        // null (Status nicht zweifelsfrei bestimmbar, z.B. Request-Fehler oder Socket
                        // parallel schon durch). In beiden Fällen: NICHT platzieren. Ein verpasster
                        // Re-Chase kostet im schlimmsten Fall etwas Preis-Drift und wird beim nächsten
                        // Zyklus nachgeholt - ein Overfill kostet real Geld und Risiko.
                        Log.Error($"{Name}.ExecuteReplaceWorkaround: Could not confirm old order {activeBrokerId} is terminal " +
                                  $"(status={finalStatus?.ToString() ?? "unknown"}). Aborting replace for {order.Symbol.Value} " +
                                  "to avoid two simultaneously live orders.");
                        state.IsUpdatePending = false;
                        return false;
                    }

                    // finalStatus == Canceled -> alte Order ist sicher weg, sicher weiter unten neu platzieren.
                    Log.Trace($"{Name}.ExecuteReplaceWorkaround: Old order {activeBrokerId} confirmed canceled via reconcile, proceeding to replace.");
                }
                else
                {
                    Log.Trace($"{Name}.ExecuteReplaceWorkaround: Old order {activeBrokerId} canceled cleanly, proceeding to replace for {order.Symbol.Value}.");
                }
            }

            var newClientOrderId = GenerateClientId(order.Id);

            // Alias VOR dem Place-Call registrieren, damit der Socket-Handler die neue
            // ClientOrderId sofort auf den bestehenden State auflösen kann (instant fill/open).
            _orderStateManager.TryAdd(newClientOrderId, state);

            // Base-Menge in die von der Exchange erwartete Einheit umrechnen (analog PlaceOrder).
            var sharedQuantity = ToExchangeQuantity(order.Symbol, Math.Abs(quantity), out var roundedAbsQuantity);
            var signedRoundedQuantity = quantity >= 0 ? roundedAbsQuantity : -roundedAbsQuantity;

            var request = new PlaceFuturesOrderRequest(
                GetSharedSymbol(order.Symbol),
                quantity > 0 ? SharedOrderSide.Buy : SharedOrderSide.Sell,
                order.Type == OrderType.Limit ? SharedOrderType.Limit : SharedOrderType.Market,
                sharedQuantity)
            {
                Price = price,
                ClientOrderId = newClientOrderId,
                ExchangeParameters = PlaceFuturesOrderExchangeParameters,
                PositionSide = SharedPositionSide
            };

            var placeRes = RunSync(() => ExecutePlaceOrderAsync(request));

            if (!placeRes.Success)
            {
                _orderStateManager.RemoveAlias(newClientOrderId);
                state.IsUpdatePending = false;

                var errorMsg = placeRes.Error?.ToString() ?? "Unknown exchange error";
                Log.Error($"{Name}.ExecuteReplaceWorkaround({order.Symbol.Value}): Replace order failed: {errorMsg}");
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "UpdateOrderReplaceFailed", errorMsg));
                return false;
            }
            // Falls der Socket die neue ClientOrderId noch nicht selbst verarbeitet hat
            // (HandleOrderSocket-Pfad "MODIFY / REPLACEMENT DETECTION"): jetzt manuell nachziehen.
            if (!String.IsNullOrEmpty(placeRes.Data.Id) && state.BrokerId != placeRes.Data.Id)
            {
                var oldBrokerId = state.BrokerId;

                // MapNewExchangeId resettet FilledQuantityCurrentOrder für die neue BrokerId-Generation
                // und ergänzt Order.BrokerId als einzige Add-Stelle (kein separates order.BrokerId.Add
                // mehr hier - das führte vorher garantiert zu einem Duplikat von placeRes.Data.Id).
                var mapped = _orderStateManager.MapNewExchangeId(newClientOrderId, placeRes.Data.Id);
                _orderStateManager.RemoveAlias(state.ClientOrderId); // alte ClientOrderId-Eintragung entfernen
                state.ClientOrderId = newClientOrderId;
                state.LastUpdateUtc = DateTime.UtcNow;
                state.IsUpdatePending = false;

                if (mapped)
                {
                    OnOrderIdChangedEvent(new BrokerageOrderIdChangedEvent
                    {
                        OrderId = order.Id,
                        BrokerId = order.BrokerId
                    });
                }

                Log.Trace($"{Name}.ExecuteReplaceWorkaround: Replace mapped manually | Old: {oldBrokerId} -> New: {placeRes.Data.Id}.");
            }
            else
            {
                // Socket hat es bereits übernommen (Zeile 828ff Pfad).
                state.IsUpdatePending = false;
                Log.Trace($"{Name}.ExecuteReplaceWorkaround: Replace already mapped via socket for {order.Symbol.Value}.");
            }

            return true;
        }

        /// <summary>
        /// Holt den echten Order-Status von der Exchange und feuert das korrekte LEAN-Event.
        /// Wird aufgerufen wenn UpdateOrder einen Terminal-Fehler erkennt (Order bereits filled/canceled).
        /// </summary>
        /// <returns>
        /// Der final bestätigte Status der Order (Open/Filled/Canceled), damit Aufrufer wie
        /// ExecuteReplaceWorkaround entscheiden können, ob eine neue Order gefahrlos platziert
        /// werden darf. Null = Status konnte nicht zweifelsfrei bestimmt werden (z.B. Request-Fehler
        /// oder Socket hat parallel bereits aufgeräumt) -> vom Aufrufer als "unsicher" zu behandeln.
        /// </returns>
        private async Task<SharedOrderStatus?> ReconcileOrderImmediateAsync(string brokerId, Order order)
        {
            try
            {
                var sharedSymbol = GetSharedSymbol(order.Symbol);
                var statusCheck = await _orderClient
                    .GetFuturesOrderAsync(new GetOrderRequest(sharedSymbol, brokerId))
                    .ConfigureAwait(false);

                if (!statusCheck.Success || statusCheck.Data == null)
                {
                    Log.Error($"{Name}.ReconcileOrderImmediateAsync: Failed to fetch status for {brokerId}: {statusCheck.Error}");
                    return null;
                }

                var brokerOrder = statusCheck.Data;

                // 🔥 FIX 3: DER RECONCILER-MORD 🔥
                // Wenn die Order auf der Börse noch lebt, darf der Reconciler sie nicht anfassen!
                if (brokerOrder.Status == SharedOrderStatus.Open)
                {
                    Log.Trace($"{Name}.ReconcileOrderImmediateAsync: Order {brokerId} is still OPEN on exchange. Reconciler stands down.");
                    return SharedOrderStatus.Open;
                }

                // Ab hier wissen wir: Die Order ist wirklich tot (Terminal). Jetzt dürfen wir sie aus dem State löschen.
                if (!_orderStateManager.TryGetByExchangeId(brokerId, out var removedState))
                {
                    Log.Trace($"{Name}.ReconcileOrderImmediateAsync: State for {brokerId} already removed (socket beat us).");
                    // Wir wissen zwar, dass die Order terminal ist (brokerOrder.Status), aber nicht mehr
                    // sicher, ob der Socket-Pfad bereits alles korrekt gebucht hat. Sicherheitshalber
                    // als unbestimmt melden statt zu raten - Aufrufer muss dann konservativ reagieren.
                    return null;
                }

                _orderStateManager.TryRemove(removedState.ClientOrderId, out _);

                if (brokerOrder.Status == SharedOrderStatus.Filled)
                {
                    var finalFillAbsQty = HasExchangeQuantity(brokerOrder.QuantityFilled)
                        ? FromExchangeQuantity(order.Symbol, brokerOrder.QuantityFilled)
                        : Math.Abs(removedState.OriginalQuantity);

                    var sign = removedState.OriginalQuantity > 0 ? 1m : -1m;
                    var finalSignedFillQty = finalFillAbsQty * sign;
                    // GEÄNDERT: brokerOrder.QuantityFilled ist relativ zur AKTUELLEN BrokerId (brokerId
                    // startet bei 0 nach jedem Replace) → gegen FilledQuantityCurrentOrder vergleichen,
                    // nicht gegen die über alle Generationen kumulierte FilledQuantity.
                    var remainingToFill = finalSignedFillQty - removedState.FilledQuantityCurrentOrder;

                    if (Math.Abs(remainingToFill) > 0)
                    {
                        // FIX 3: Doppelbuchungen der Gebühren verhindern!
                        var totalExchangeFee = brokerOrder.Fee ?? 0m;
                        var remainingFee = Math.Max(0m, totalExchangeFee - removedState.CumulativeFeePaid);

                        Log.Trace($"{Name}.ReconcileOrderImmediateAsync: Order {brokerId} confirmed FILLED. Emitting fill event.");
                        OnOrderEvent(new OrderEvent(removedState.Order, DateTime.UtcNow, OrderFee.Zero)
                        {
                            Status = QuantConnect.Orders.OrderStatus.Filled,
                            FillPrice = brokerOrder.AveragePrice ?? 0,
                            FillQuantity = remainingToFill,
                            OrderFee = new OrderFee(new CashAmount(remainingFee, brokerOrder.FeeAsset ?? SettleAsset)),
                            Message = "Immediate Reconcile – Fill"
                        });
                    }

                    return SharedOrderStatus.Filled;
                }
                else
                {
                    Log.Trace($"{Name}.ReconcileOrderImmediateAsync: Order {brokerId} confirmed CANCELED. Emitting cancel event.");
                    OnOrderEvent(new OrderEvent(removedState.Order, DateTime.UtcNow, OrderFee.Zero)
                    {
                        Status = QuantConnect.Orders.OrderStatus.Canceled,
                        Message = "Immediate Reconcile – Cancel"
                    });

                    return brokerOrder.Status;
                }
            }
            catch (Exception ex)
            {
                Log.Error($"{Name}.ReconcileOrderImmediateAsync Error for {brokerId}: {ex.Message}");
                return null;
            }
        }

        #endregion

        #region Order Helpers

        protected virtual string GenerateClientId(int orderId)
        {
            return $"0x{((ulong)(StartTime.Ticks + orderId)).ToString("x16").PadLeft(32, '0')}";
        }

        protected virtual string NativeTicker(Symbol symbol) => symbol.Value;

        protected virtual string NormalizeSymbol(string rawSymbol) => rawSymbol;

        protected virtual SharedSymbol GetSharedSymbol(Symbol s)
        {
            CurrencyPairUtil.DecomposeCurrencyPair(s, out var baseAsset, out var quoteAsset);
            return new SharedSymbol(TradingMode.PerpetualLinear, baseAsset, quoteAsset);
        }

        private QuantConnect.Orders.OrderStatus MapStatus(SharedOrderStatus status, decimal filled)
        {
            if (status == SharedOrderStatus.Open)
                return filled > 0 ? QuantConnect.Orders.OrderStatus.PartiallyFilled : QuantConnect.Orders.OrderStatus.Submitted;

            return status switch
            {
                SharedOrderStatus.Filled => QuantConnect.Orders.OrderStatus.Filled,
                SharedOrderStatus.Canceled => QuantConnect.Orders.OrderStatus.Canceled,
                _ => QuantConnect.Orders.OrderStatus.None
            };
        }

        #endregion
    }
}