using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using static SilverQuant.Lean.Brokerages.Futures.Shared.SharedFuturesBrokerage;

namespace SilverQuant.Lean.Brokerages.Futures.Shared.Common
{
    internal class OrderStateManager
    {
        // 1. Dein Master-Dictionary (Außenwelt / LEAN / REST)
        private readonly ConcurrentDictionary<string, OrderState> _statesByClientId = new();

        // 2. Dein versteckter High-Speed-Index (Nur für WebSockets)
        private readonly ConcurrentDictionary<string, OrderState> _statesByExchangeId = new();

        public OrderState this[string clientId]
        {
            get => _statesByClientId[clientId];
            set => TryAdd(clientId, value);
        }

        // =========================================================
        // STANDARD DICTIONARY METHODEN (Für LEAN / Hauptcode)
        // =========================================================
        public bool TryAdd(string clientId, OrderState state)
        {
            if (_statesByClientId.TryAdd(clientId, state))
            {
                // Falls der State schon eine Exchange-ID hat, direkt mit indizieren
                if (!string.IsNullOrEmpty(state.BrokerId))
                {
                    _statesByExchangeId[state.BrokerId] = state;
                }
                return true;
            }
            return false;
        }

        public bool TryGetValue(string clientId, out OrderState state)
        {
            return _statesByClientId.TryGetValue(clientId, out state);
        }

        public bool TryRemove(string clientId, out OrderState state)
        {
            if (_statesByClientId.TryRemove(clientId, out state))
            {
                // Garantiert synchrones Löschen im geheimen Index!
                if (!string.IsNullOrEmpty(state.BrokerId))
                {
                    _statesByExchangeId.TryRemove(state.BrokerId, out _);
                }
                return true;
            }
            return false;
        }

        // =========================================================
        // HFT SONDER-METHODEN (Für Order-Socket & Trade-Socket)
        // =========================================================

        /// <summary>
        /// O(1) Lichtgeschwindigkeits-Suche für den Trade-Socket
        /// </summary>
        public bool TryGetByExchangeId(string exchangeId, out OrderState state)
        {
            return _statesByExchangeId.TryGetValue(exchangeId, out state);
        }

        /// <summary>
        /// Wird vom Order-Socket aufgerufen, wenn die Exchange-ID endlich bekannt ist
        /// </summary>
        public void MapNewExchangeId(string clientId, string newExchangeId)
        {
            if (string.IsNullOrEmpty(newExchangeId)) return;

            if (_statesByClientId.TryGetValue(clientId, out var state))
            {
                // ATOMARE SPERRE
                lock (state)
                {
                    // 🔥 DOUBLE-CHECKED LOCKING 🔥
                    // War ein anderer Thread schneller, während wir an der lock-Tür warten mussten?
                    if (state.BrokerId == newExchangeId)
                    {
                        // Arbeit ist bereits erledigt! Nichts weiter zu tun.
                        return;
                    }

                    // 1. Die primäre aktive ID im State aktualisieren
                    state.BrokerId = newExchangeId;

                    // 2. LEAN Liste synchronisieren
                    if (!state.Order.BrokerId.Contains(newExchangeId))
                    {
                        state.Order.BrokerId.Add(newExchangeId);
                    }

                    // 3. Den neuen Key im Index registrieren
                    _statesByExchangeId[newExchangeId] = state;
                }
            }
        }

        // Praktisch für Reconcile-Loops
        public IEnumerable<OrderState> GetAllStates() => _statesByClientId.Values;
    }
}
