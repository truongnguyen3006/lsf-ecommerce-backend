# Phase 8: Pragmatic Default-On Saga Cutover

## Current default

- `order-service` now defaults to `app.order.workflow.mode=lsf-saga`.
- Saga runtime remains `jdbc + direct`.
- The inventory multi-SKU bridge remains local to `order-service`.

## Rollback safety

- `legacy` mode is still available through the existing workflow mode flag.
- The runtime still chooses exactly one workflow mode before side effects are emitted.
- This cutover does not enable outbox-backed saga transport and does not run both orchestration paths in parallel.

## Why legacy is still kept

- `lsf-saga-starter` is still a `partial support` module in framework docs.
- The order workflow still needs a local fan-out/fan-in adapter for inventory replies.
- Keeping `legacy` available gives the team a fast rollback path if a real integration mismatch appears after default-on rollout.

## What Phase 8 changes operationally

- Admin visibility now shows:
  - active workflow mode
  - default workflow mode
  - rollback mode
  - rollback availability
- Frontend demo surface highlights that the system is default-on `lsf-saga`, while still exposing `legacy` as a fallback.

## When legacy can be retired later

- Production-like validation confirms the default saga path is stable over time, not only in embedded tests.
- The team is comfortable keeping the local inventory bridge as an accepted adapter, or has a better framework-backed answer.
- Rollback drills are no longer needed as a normal operating control.
- Ops dashboards and runbooks are considered good enough without relying on legacy-only troubleshooting paths.

## Recommendation after Phase 8

- Treat this as a pragmatic cutover, not as a full retirement of legacy orchestration.
- Keep rollback documented and tested until the next phase explicitly decides to remove it.
