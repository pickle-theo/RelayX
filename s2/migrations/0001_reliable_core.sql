-- S2 reliable core schema. Semantic contract v1, frozen at S2 acceptance.
-- All expiry/retry predicates use SQL now(); no Go-side timestamp is trusted.

CREATE TYPE message_state AS ENUM ('ACCEPTED', 'SETTLED', 'EXPIRED');
CREATE TYPE delivery_state AS ENUM
    ('PENDING', 'SENT', 'RECEIPT_ACKED', 'FAILED', 'EXPIRED');
CREATE TYPE producer_kind AS ENUM ('user', 'service');
CREATE TYPE delivery_mode AS ENUM ('all_endpoints', 'last_active');
CREATE TYPE fact_type AS ENUM (
    'message.accepted',
    'message.settled',
    'message.expired',
    'delivery.created',
    'delivery.sent',
    'delivery.receipt_acked',
    'delivery.failed',
    'delivery.expired'
);

CREATE TABLE messages (
    id                TEXT        PRIMARY KEY,
    tenant_id         TEXT        NOT NULL,
    producer_id       TEXT        NOT NULL,
    producer_kind     producer_kind NOT NULL,
    sender            TEXT,
    source_device_id  TEXT        NOT NULL,
    client_msg_id     TEXT        NOT NULL,
    payload           BYTEA       NOT NULL,
    payload_size      INTEGER     NOT NULL CHECK (payload_size > 0),
    delivery_mode     delivery_mode NOT NULL DEFAULT 'all_endpoints',
    fanout            INTEGER     NOT NULL DEFAULT 0 CHECK (fanout >= 0),
    ttl_seconds       INTEGER     NOT NULL CHECK (ttl_seconds > 0),
    max_attempts      INTEGER     NOT NULL CHECK (max_attempts > 0),
    platform_state    message_state NOT NULL DEFAULT 'ACCEPTED',
    state_version     INTEGER     NOT NULL DEFAULT 0,
    fanout_done_at    TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at        TIMESTAMPTZ NOT NULL,
    settled_at        TIMESTAMPTZ,

    CONSTRAINT uq_messages_idem
        UNIQUE (tenant_id, producer_id, client_msg_id),
    CONSTRAINT ck_payload_limit CHECK (payload_size <= 61440),
    CONSTRAINT ck_ttl_limit CHECK (ttl_seconds <= 2592000),
    CONSTRAINT ck_attempts_limit CHECK (max_attempts <= 10)
);

CREATE TABLE deliveries (
    id                TEXT        PRIMARY KEY,
    tenant_id         TEXT        NOT NULL,
    message_id        TEXT        NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    user_id           TEXT        NOT NULL,
    device_id         TEXT        NOT NULL,
    endpoint_id       TEXT        NOT NULL,
    delivery_state    delivery_state NOT NULL DEFAULT 'PENDING',
    state_version     INTEGER     NOT NULL DEFAULT 0,
    attempts          INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at   TIMESTAMPTZ,
    application_status TEXT,
    application_acked_at TIMESTAMPTZ,
    last_error        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    terminal_at       TIMESTAMPTZ,

    CONSTRAINT uq_deliveries_idem
        UNIQUE (tenant_id, message_id, user_id, device_id)
);

CREATE TABLE endpoints (
    tenant_id         TEXT        NOT NULL,
    user_id           TEXT        NOT NULL,
    device_id         TEXT        NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'OFFLINE'
                                  CHECK (status IN ('ONLINE','OFFLINE')),
    session_id        TEXT,
    last_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, user_id, device_id)
);

CREATE TABLE delivery_facts (
    event_id          TEXT        PRIMARY KEY,
    tenant_id         TEXT        NOT NULL,
    message_id        TEXT        NOT NULL,
    endpoint_id       TEXT,
    fact_type         fact_type   NOT NULL,
    state_version     INTEGER     NOT NULL,
    occurred_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload_summary   JSONB,
    published_at      TIMESTAMPTZ,
    UNIQUE (tenant_id, message_id, fact_type, endpoint_id, state_version)
);

CREATE INDEX idx_messages_expiry
    ON messages (expires_at) WHERE platform_state = 'ACCEPTED';
CREATE INDEX idx_deliveries_retry
    ON deliveries (next_attempt_at)
    WHERE delivery_state IN ('PENDING', 'SENT') AND next_attempt_at IS NOT NULL;
CREATE INDEX idx_deliveries_message ON deliveries (message_id);
CREATE INDEX idx_facts_message      ON delivery_facts (message_id, occurred_at);
CREATE INDEX idx_deliveries_endpoint
    ON deliveries (tenant_id, user_id, device_id, delivery_state);
CREATE INDEX idx_facts_unpublished
    ON delivery_facts (occurred_at, event_id) WHERE published_at IS NULL;
