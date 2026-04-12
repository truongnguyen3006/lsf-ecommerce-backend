# Phase 6: Saga Hardening / Evidence

## Current strategy

- `order-service` keeps `app.order.workflow.mode=legacy` as the default.
- `lsf-saga` remains opt-in for controlled testing and demo validation only.
- Saga runtime stays on `jdbc + direct`.
- Inventory fan-out is still bridged locally inside `order-service`; the framework saga definition only orchestrates order-level state.

## What Phase 6 proves

- The saga spike can complete the success path with persisted JDBC state.
- Inventory rejection leads to terminal failure on the saga path.
- Payment failure leads to compensation on the saga path.
- Payment timeout can be triggered and observed as a compensation flow.
- Delayed inventory replies can resume a persisted waiting saga instance.
- Duplicate bridge starts are ignored without creating duplicate side effects.
- Late inventory replies are ignored after the bridge session is gone.
- Aggregator pending sessions, expired sessions, recent failures, recent compensations, and overdue saga instances can be observed from `/admin/saga` and `/api/system/saga`.

## What is still unproven

- End-to-end production resilience under real process restart with MySQL + Redis infrastructure instead of test doubles.
- Outbox-backed saga transport for this workflow.
- A framework-native replacement for the local inventory aggregation bridge.
- Clean sequential modeling for multi-SKU fan-out/join without local adapters.
- Large-scale retry pressure and duplicate event storms across service boundaries.

## Remaining mismatches and blockers

- `lsf-saga-starter` is still a sequential saga orchestrator, not a general workflow engine.
- The checkout flow still needs an order-level aggregator because inventory replies arrive per SKU.
- `jdbc + direct` has stronger evidence in the framework source than `outbox` for this repo right now, so the spike intentionally does not switch transport.
- The current proof is enough for controlled rollout and demo evidence, but not enough to retire the legacy orchestration.

## Read-only evidence surfaces

- `order-service`:
  - `/admin/saga`
- `api-gateway`:
  - `/api/system/saga`

The snapshot now includes:

- workflow mode
- saga enabled/store/transport mode
- status summary
- timeout summary
- runtime counters since boot
- recent instances
- recent failures
- recent compensations
- overdue instances
- pending inventory bridge sessions

## Decision checklist before any full cutover

- Keep `legacy` as default until all checklist items below are green.
- Validate saga path with real shared infrastructure, not only embedded test runtime.
- Prove restart/recovery behavior with the actual JDBC store and Redis session state.
- Decide whether the inventory bridge remains an accepted local adapter or needs a framework-level answer.
- Validate duplicate/retry behavior with more production-like event replay scenarios.
- Confirm operational dashboards and alerts are good enough to replace legacy visibility.

## Recommendation after Phase 6

- Do not full-cutover yet.
- Keep the current controlled rollout.
- Use the new evidence surfaces and tests to run more opt-in validation before changing the default mode.
