-- Phase 5: 事务性 outbox、多租户配额、租户维度幂等键与投递租约字段。
-- 由 internal/store 在单个事务内执行，版本记录写入 schema_migrations。
-- 所有语句都使用 IF NOT EXISTS / IF EXISTS，可重复执行。

CREATE TABLE IF NOT EXISTS tenants (
    id           TEXT        PRIMARY KEY,
    rate_per_sec INT         NOT NULL DEFAULT 0,
    burst        INT         NOT NULL DEFAULT 0,
    max_inflight INT         NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tenants_rate_non_negative CHECK (rate_per_sec >= 0),
    CONSTRAINT tenants_burst_non_negative CHECK (burst >= 0),
    CONSTRAINT tenants_inflight_non_negative CHECK (max_inflight >= 0)
);

INSERT INTO tenants (id) VALUES ('default') ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS outbox (
    id              BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic           TEXT        NOT NULL,
    partition_key   TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    state           SMALLINT    NOT NULL DEFAULT 0,
    attempts        INT         NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
);

-- relay 扫描：按可发布时间领取待发布事件，FOR UPDATE SKIP LOCKED 避免多实例抢同一行。
CREATE INDEX IF NOT EXISTS outbox_pending_idx
    ON outbox (next_attempt_at, id)
    WHERE state = 0;

ALTER TABLE messages ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';

-- BREAKING：幂等键加入租户维度，不同租户可以复用同一个 client_msg_id。
ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_from_user_client_msg_id_key;

CREATE UNIQUE INDEX IF NOT EXISTS messages_tenant_client_msg_idx
    ON messages (tenant_id, from_user, client_msg_id);

ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS worker_id TEXT;

-- 租约扫描：按下次可调度时间领取 PENDING/SENT 行，租约字段只影响调度。
CREATE INDEX IF NOT EXISTS deliveries_due_idx
    ON deliveries (next_retry_at, id)
    WHERE state IN (0, 1);

CREATE INDEX IF NOT EXISTS messages_tenant_expire_idx
    ON messages (tenant_id, expire_at)
    WHERE state IN (0, 1);
