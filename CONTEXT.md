# Connector Factory

This context describes reusable public market-data connectors and the readiness language used to decide whether a connector can be handed off to downstream Rust users or AFK agents.

## Language

**Connector**:
A reusable exchange-specific market-data module that exposes public no-key market-data behavior for downstream consumers.
_Avoid_: Exchange wrapper, endpoint string, adapter

**Connector Factory**:
The project practice for turning exchange-specific public market-data behavior into reusable connectors with repeatable readiness evidence.
_Avoid_: Exchange collection, connector scaffold generator

**Scaffold-Only Connector**:
A connector that exists as a crate or placeholder but has no repeatable public readiness evidence yet.
_Avoid_: Ready connector, unfinished exchange

**Partial Connector**:
A connector with some promotion evidence, but not enough to be handed off as ready.
_Avoid_: Broken connector, almost ready

**Handoff-Ready Connector**:
A connector whose public market-data behavior is proven enough for a downstream user or AFK agent to evaluate and reuse without relying on chat context.
_Avoid_: Complete connector, production connector

**Promotion Proof**:
Evidence that moves a connector toward handoff readiness, such as public REST proof, public WebSocket proof, coverage proof, and downstream handoff proof.
_Avoid_: Drift result, smoke test

**Promotion State**:
The readiness position of a connector based only on promotion proof: scaffold-only, partial, or handoff-ready.
_Avoid_: Drift state, display status

**Drift Audit**:
A live check that looks for exchange-side or connector-side drift after a connector has a readiness baseline.
_Avoid_: Promotion proof, CI check

**Drift Overlay**:
Runtime drift information layered over a connector's promotion state without changing the underlying promotion state.
_Avoid_: Promotion state, readiness status

**Drift Warning**:
A user-facing warning that a connector has a handoff-ready promotion state but currently shows drift risk.
_Avoid_: Partial connector, failed promotion

**Downstream Handoff**:
Evidence that a connector can be consumed through the public connector surface by a separate downstream-style caller.
_Avoid_: Internal example, crate compile
