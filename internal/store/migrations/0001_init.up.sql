CREATE TABLE containers (
    id          TEXT PRIMARY KEY,
    owner       TEXT             NOT NULL,
    commodity   TEXT             NOT NULL DEFAULT '',
    setpoint_c  DOUBLE PRECISION NOT NULL,
    min_temp_c  DOUBLE PRECISION NOT NULL,
    max_temp_c  DOUBLE PRECISION NOT NULL,
    created_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),
    CONSTRAINT containers_band_chk CHECK (min_temp_c < max_temp_c)
);

CREATE INDEX containers_owner_idx ON containers (owner, id);

-- One row per telemetry sample. The composite primary key doubles as the
-- index that serves "history of container X between t1 and t2" range scans,
-- and makes ingest idempotent: a Kafka redelivery hits ON CONFLICT DO NOTHING.
CREATE TABLE readings (
    container_id  TEXT             NOT NULL REFERENCES containers (id),
    recorded_at   TIMESTAMPTZ      NOT NULL,
    temperature_c DOUBLE PRECISION NOT NULL,
    humidity_pct  REAL             NOT NULL,
    door_open     BOOLEAN          NOT NULL DEFAULT false,
    ingested_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (container_id, recorded_at)
);

-- Cheap time-ordered index for retention jobs that delete by age across all
-- containers; BRIN stays tiny because rows arrive roughly in time order.
CREATE INDEX readings_recorded_at_brin ON readings USING BRIN (recorded_at);
