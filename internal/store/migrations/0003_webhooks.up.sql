-- Which container owners' events a partner (identified by its API Gateway
-- API key id) may subscribe to. Managed by operations, not via the API.
CREATE TABLE partner_grants (
    partner_id TEXT NOT NULL,
    owner      TEXT NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (partner_id, owner)
);

CREATE TABLE webhook_subscriptions (
    id          UUID PRIMARY KEY,
    partner_id  TEXT        NOT NULL,
    owner       TEXT        NOT NULL,
    url         TEXT        NOT NULL,
    events      TEXT[]      NOT NULL,
    -- AES-256-GCM(nonce || ciphertext), AAD = id. Never returned by the API.
    secret_enc  BYTEA       NOT NULL,
    active      BOOLEAN     NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT webhook_events_nonempty CHECK (cardinality(events) > 0)
);

CREATE INDEX webhook_partner_idx ON webhook_subscriptions (partner_id, created_at);
-- Fan-out lookup used by the alert Lambda.
CREATE INDEX webhook_owner_active_idx ON webhook_subscriptions (owner) WHERE active;
