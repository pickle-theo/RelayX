-- Phase 2 初始 schema：endpoints / messages / deliveries 与三个扫描索引。
-- 由 internal/store 在单个事务内执行，版本记录写入 schema_migrations。

CREATE TABLE IF NOT EXISTS endpoints (
    user_id      TEXT        NOT NULL,
    device_id    TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, device_id)
);

CREATE TABLE IF NOT EXISTS messages (
    id            BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    client_msg_id BIGINT      NOT NULL,
    from_user     TEXT        NOT NULL,
    from_device   TEXT        NOT NULL,
    target_type   SMALLINT    NOT NULL,
    target        TEXT        NOT NULL,
    payload       BYTEA       NOT NULL,
    state         SMALLINT    NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expire_at     TIMESTAMPTZ NOT NULL,
    CONSTRAINT messages_from_user_client_msg_id_key UNIQUE (from_user, client_msg_id)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id            BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id    BIGINT      NOT NULL REFERENCES messages (id) ON DELETE CASCADE,
    user_id       TEXT        NOT NULL,
    device_id     TEXT        NOT NULL,
    state         SMALLINT    NOT NULL DEFAULT 0,
    attempts      INT         NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT deliveries_message_user_device_key UNIQUE (message_id, user_id, device_id)
);

-- 离线拉取：按 endpoint 顺序扫描未确认的投递。
CREATE INDEX IF NOT EXISTS deliveries_pull_idx
    ON deliveries (user_id, device_id, id)
    WHERE state IN (0, 1);

-- 重试扫描：按下次重试时间扫描已推送但未确认的投递。
CREATE INDEX IF NOT EXISTS deliveries_retry_idx
    ON deliveries (next_retry_at)
    WHERE state = 1;

-- 过期扫描：按过期时间扫描非终态消息。
CREATE INDEX IF NOT EXISTS messages_expire_idx
    ON messages (expire_at)
    WHERE state IN (0, 1);
