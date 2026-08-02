# Cold-Chain Container Monitoring Service

Backend for tracking refrigerated shipping containers ("reefers"): it ingests
temperature telemetry, keeps the latest state of every unit hot in Redis,
flags temperature breaches within seconds, and serves history over a REST API.

> Work in progress — see the commit history for how the service is built up.
