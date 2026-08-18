CREATE TABLE breaches (
    id           BIGSERIAL PRIMARY KEY,
    container_id TEXT             NOT NULL REFERENCES containers (id),
    kind         TEXT             NOT NULL CHECK (kind IN ('TOO_WARM', 'TOO_COLD')),
    started_at   TIMESTAMPTZ      NOT NULL,
    ended_at     TIMESTAMPTZ,
    peak_temp_c  DOUBLE PRECISION NOT NULL,
    limit_c      DOUBLE PRECISION NOT NULL,
    CONSTRAINT breaches_period_chk CHECK (ended_at IS NULL OR ended_at >= started_at)
);

-- A replayed Kafka batch re-opens the same breach; this key makes that a no-op.
CREATE UNIQUE INDEX breaches_container_start_uq ON breaches (container_id, started_at);

-- At most one open breach per container.
CREATE UNIQUE INDEX breaches_one_open_uq ON breaches (container_id) WHERE ended_at IS NULL;

-- "Recent breaches" and "currently open breaches" listings.
CREATE INDEX breaches_started_idx ON breaches (started_at DESC);
CREATE INDEX breaches_open_idx ON breaches (started_at DESC) WHERE ended_at IS NULL;
