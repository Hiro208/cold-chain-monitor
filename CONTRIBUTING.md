# Contributing

## Workflow

The team works in two-week sprints. Each story in Jira (`CCM-<n>`) maps to
one short-lived branch and one pull request.

1. Branch from `main`: `feat/CCM-123-breach-hysteresis`.
2. Keep commits small and use [Conventional Commits](https://www.conventionalcommits.org/)
   (`feat(ingest): ...`, `fix(api): ...`, `infra(terraform): ...`).
3. Open a PR using the template. CI must be green — lint, unit tests,
   integration tests against Kafka and Postgres containers, Terraform
   validation and image builds are all required checks.
4. One approving review, then squash-merge. `main` deploys to production
   after the `production` environment approval.

## Local checks before pushing

```bash
make lint test
make test-integration   # when touching store, ingest, cache or Kafka code
```

## Database migrations

Add a new `internal/store/migrations/NNNN_description.up.sql`; never edit an
applied migration. Changes must be backwards compatible with the previous
release (expand, deploy, then contract) because the migration Job runs before
pods roll.

## Message schema changes

Kafka payloads are versioned (`v` field in `internal/stream`). Additive
fields are fine; breaking changes need a new version that consumers accept
before producers start emitting it.
