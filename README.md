# LeanSharedFuturesBrokerage

Crypto futures (perpetuals) brokerage integrations for [QuantConnect LEAN](https://github.com/QuantConnect/Lean), built on a single shared base class instead of one independent plugin per exchange.

All exchange clients are built on JKorf's `CryptoExchange.Net` ecosystem (`Bybit.Net`, `Bitget.Net`, `Aster.Net`, `HyperLiquid.Net`, `BingX.Net`, `Kraken.Net`, `OKX.Net`), which exposes a common "Shared" interface layer (`IFuturesOrderRestClient`, `IFuturesOrderSocketClient`, `IBookTickerSocketClient`, `ITradeSocketClient`, `IUserTradeSocketClient`, `IFundingRateRestClient`, `IKlineRestClient`, `IBalanceRestClient`). `SharedFuturesBrokerage` drives all exchanges through these shared interfaces, so order management, reconciliation, and funding-rate handling are implemented **once**, with per-exchange classes only overriding what's genuinely exchange-specific.

## Status

| Exchange | Status | Notes |
|---|---|---|
| Bybit | ✅ Live | In-place order modify, funding fees via user-trade stream |
| Hyperliquid | ✅ Live | Vault trading, Builder Code support, cancel+replace workaround for post-June-2026 modify rejects |
| AsterDEX | ✅ Live | ListenKey user stream, hedge mode, Builder Code support, no native user-trade stream |
| Bitget | ✅ Live | In-place EditOrder, funding fees via ledger polling |
| BingX | ✅ Live | ListenKey user stream, hedge mode, funding rate via polling loop (not socket) |
| Kraken | ⚠️ Blocked | Underlying library (`Kraken.Net`) does not return PnL data — cash/equity tracking unreliable. Waiting on upstream fix. |
| OKX | ⚠️ Blocked | Underlying library's `SharedClient` constructs native symbols as `BASE-QUOTE-SWAP`, breaking subscriptions for any non-BTC pair (`error 60018`). Waiting on upstream fix. |

## Architecture

### One base class, split by concern

`SharedFuturesBrokerage` is an `abstract partial class` implementing both `Brokerage` and `IDataQueueHandler` — order execution and market data/history live in the same class, split across files by concern:

- `SharedFuturesBrokerage.cs` — connection lifecycle, base wiring (`InitializeBase`), reconciliation timer setup
- `SharedFuturesBrokerage.Orders.cs` — `PlaceOrder` / `UpdateOrder` / `CancelOrder`, order state machine, socket handlers, reconciliation loop
- `SharedFuturesBrokerage.Data.cs` — `Subscribe`/`Unsubscribe` (`IDataQueueHandler`), `GetHistory` (klines + funding rate history), funding-rate rollover detection

Each exchange implementation (`BybitFuturesBrokerage`, `HyperliquidFuturesBrokerage`, etc.) extends `SharedFuturesBrokerage` and only overrides:
- Client wiring (`InitializeFromJob`) — passing the exchange's `SharedClient` instances into `InitializeBase`
- `GetCashBalance()` — every exchange has a different equity/PnL field combination
- `ExecuteUpdateOrderAsync` — in-place modify vs. cancel+replace differs per exchange
- `CreateFundingSubscriptionAsync` — funding rate delivery differs per exchange (ticker socket, mark-price socket, user-data socket)
- Exchange-specific `ExchangeParameters` (e.g. Bitget's `ProductType`, Bybit's `category`/`settleCoin`)

### Order state machine & `OrderStateManager`

Single source of truth keyed by `ClientOrderId` (permanent, assigned before the order is even sent), with a secondary O(1) index by exchange order ID for socket lookups. This exists because exchanges disagree on how a "modify" behaves:

- **In-place modify** (Bybit, Bitget): exchange order ID stays the same after an edit.
- **Cancel+replace** (AsterDEX, and Hyperliquid as a fallback): the exchange order ID changes after an edit, so the old `ClientOrderId` is re-pointed to the new exchange ID without LEAN ever seeing a spurious cancel event.

Lifecycle: `Placing → Submitted → Open/PartiallyFilled → Filled/Canceled/Replaced/Invalid`.

Trade matching (`HandleUserTradeSocket`) falls back through three tiers when a fill arrives:
1. Exact match by exchange order ID (O(1))
2. Fallback match by `ClientOrderId` (covers orders still in `Placing` state, or cancel+replace where the new exchange ID hasn't been mapped yet)
3. Heuristic match by symbol + side + quantity, for races where a fill socket message arrives before the placing/replace REST call has returned

A reconciliation loop runs every 30s, cross-checking in-memory order state against the exchange's actual open orders — catches missed socket events without continuously polling REST.

### Funding rate handling

Unified through `SubscribeFunding` / `CreateFundingSubscriptionAsync`, exposed to LEAN as `MarginInterestRate` data (forced into the live streaming path via `SharedDataChannelProvider`, an `IDataChannelProvider` override — LEAN doesn't stream this type live by default).

Rollover detection supports both:
- **Fixed-cycle exchanges** (Hyperliquid, 1h cycle): next funding time computed from wall-clock hour + `FundingRolloverHours`
- **Exchange-driven exchanges** (Bybit): `nextFundingTime` taken directly from the socket payload

Funding *fee* settlement (the actual cash movement, separate from the funding *rate* feed) is exchange-specific:
- **Bybit**: extracted from the user-trade-update stream, filtered to `TradeType.Funding`
- **Bitget**: no push event for fees — detected via a ticker-socket rollover trigger, then polls the account ledger (idempotent via last-processed-ledger-ID) after a short delay
- **AsterDEX**: pushed via the account-update stream (`AccountUpdateReason.FundingFee`), filtered from the ListenKey user stream

### Minimum order notional

`MinimumOrderNotionalValue` is enforced per exchange (e.g. Bybit/Bitget $5, Hyperliquid $10). Quantities below the minimum are automatically rounded up to the nearest lot size that clears it, both on initial placement and on update.

### Symbol Properties Database (SPDB)

Populated dynamically at startup from each exchange's live instrument list (tick size, lot size) — not hardcoded. Hyperliquid additionally self-corrects tick size at runtime from live oracle-price ticks, since HL ties price/quantity decimal precision together (`szDecimals + pxDecimals = 5`) and tick size needs to track the asset's current price magnitude.

## Per-exchange notes

**Hyperliquid**
- Vault address trading supported (trades on behalf of a vault rather than the main account)
- Builder Code support: optional builder address + fee percentage (config-driven; defaults to the top of the allowed 0.001–0.1% range if an address is set without an explicit fee)
- June 2026 Hyperliquid network upgrade: in-place GTC modifies that would cross the book are now silently converted server-side to ALO/Post-Only and rejected (`"would have immediately matched"`). Worked around via cancel+replace that preserves the existing `OrderState` and avoids a spurious LEAN cancel event for the old order.

**AsterDEX**
- No native user-trade stream (`ExchangeSupportsUserTradeStream = false`) — fills are handled entirely through the order socket
- ListenKey-based user stream with a 45-minute keep-alive loop and automatic reconnect on expiry
- Optional hedge mode
- Builder Code support: optional builder address + fee percentage (same config pattern as Hyperliquid; defaults to the top of the allowed range if an address is set without an explicit fee)

**Bitget**
- No funding-fee push event — detected via ticker-socket rollover, then polls the account ledger
- In-place `EditOrder` modify with rotating client order ID per edit

**Bybit**
- In-place order modify; exchange order ID is stable across edits
- Funding fees extracted from the user-trade-update stream

**BingX**
- No native user-trade stream (`ExchangeSupportsUserTradeStream = false`) — fills handled via the order socket, like AsterDEX
- ListenKey-based user stream with 45-minute keep-alive loop and automatic reconnect on expiry
- Hedge mode by default, with `positionSide` (Long/Short) mapping per side
- Funding *rate* is not pushed via socket — handled by a dedicated polling loop that fetches the next funding timestamp, sleeps until settlement, then re-fetches rate + next timestamp (distinct from the socket+rollover pattern used by the other exchanges)
- Funding *fee* settlement pushed via the account-update stream, filtered to `Trigger == "FUNDING_FEE"`
- Order updates via `CancelReplaceOrderAsync`, which returns a new exchange order ID (cancel+replace under the hood, not a true in-place edit)

## Known blockers (not exchange-side, library-side)

- **Kraken**: `Kraken.Net` does not surface PnL in the account/balance response needed for LEAN's cash/equity tracking. No workaround currently in place — not used live.
- **OKX**: `OKX.Net`'s `SharedClient` constructs native symbols as `BASE-QUOTE-SWAP`, which breaks subscriptions for any pair other than BTC (`error 60018`). Paused pending an upstream fix.

Both are JKorf library issues, not implementation bugs in this repo.

## Status of this repository

This is maintained for the author's own live production use. It's public for transparency/reference, not as a community-supported project.

- Bug reports are welcome via Issues.
- Feature requests are not prioritized.

## Disclaimer

This code is provided as-is, without warranty of any kind. It is shared for transparency and reference purposes, not as a certified or audited trading product.

Using this code to trade — with real or simulated funds — is entirely at your own risk. The author assumes no liability for financial losses, missed trades, incorrect fills, exchange API behavior, or any other damages resulting from the use of this software, including but not limited to bugs in this repository or in the underlying third-party libraries it depends on.

Always test thoroughly (paper trading, small size) before relying on this for live trading, and review the code yourself rather than trusting it blindly.