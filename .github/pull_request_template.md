## What & why

<!-- Link the Jira story for this sprint, e.g. CCM-123. -->

## How it was tested

- [ ] Unit tests (`make test`)
- [ ] Integration tests (`make test-integration`) if storage, Kafka or Redis behaviour changed
- [ ] Manually against `docker compose` if the API changed

## Rollout

- [ ] Backwards-compatible migration (expand/contract) or none
- [ ] Message schema unchanged, or version bumped in `internal/stream`
