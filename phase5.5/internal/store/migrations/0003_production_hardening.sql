-- Phase 5.5：生产化加固 —— 死信队列、outbox 失败原因与数据生命周期索引。
--
-- 由 internal/store 在单个事务内执行，版本记录写入 schema_migrations。
-- 所有语句都使用 IF NOT EXISTS / IF EXISTS，可重复执行。

-- outbox 发布失败时记录最后一次错误原因，便于定位积压根因。
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS last_error TEXT;

-- 死信：无法处理的事件落到这里，既写 Kafka 主题也写库，保证可审计、可重放。
CREATE TABLE IF NOT EXISTS dead_letters (
    id           BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_topic TEXT        NOT NULL,
    partition    INT         NOT NULL,
    offset       BIGINT      NOT NULL,
    record_key   BYTEA,
    payload      BYTEA       NOT NULL,
    error        TEXT        NOT NULL,
    error_kind   TEXT        NOT NULL,
    attempts     INT         NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at  TIMESTAMPTZ
);

-- 同一来源记录只会死信一次，重放后不重复落库。
CREATE UNIQUE INDEX IF NOT EXISTS dead_letters_source_idx
    ON dead_letters (source_topic, partition, offset);

-- 待重放扫描：只看尚未重放的死信。
CREATE INDEX IF NOT EXISTS dead_letters_pending_idx
    ON dead_letters (created_at)
    WHERE replayed_at IS NULL;

-- 数据生命周期：终态投递行按完成时间批量归档。
CREATE INDEX IF NOT EXISTS deliveries_terminal_idx
    ON deliveries (state, updated_at)
    WHERE state IN (2, 3, 4, 5, 6);

-- 数据生命周期：终态消息按完成时间批量归档。
CREATE INDEX IF NOT EXISTS messages_terminal_idx
    ON messages (state, updated_at)
    WHERE state IN (1, 2, 3);

-- 数据生命周期：已发布 outbox 行按发布时间批量归档。
CREATE INDEX IF NOT EXISTS outbox_published_idx
    ON outbox (published_at)
    WHERE state = 1;
