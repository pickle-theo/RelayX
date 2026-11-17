-- S4 event projection cursor (LLD §5.4).
--
-- 投影器按冻结的键集顺序读取事实：
--   SELECT * FROM delivery_facts
--    WHERE fact_type IN (...)
--      AND (occurred_at, event_id) > ($1, $2)
--    ORDER BY occurred_at, event_id
--    LIMIT $3
-- 一批事实全部路由成功后才推进游标；崩溃只会导致重放，
-- 重复事件由确定性 event_id 在端侧幂等吸收。

CREATE TABLE IF NOT EXISTS projection_cursor (
    projector        TEXT        PRIMARY KEY,
    last_occurred_at TIMESTAMPTZ NOT NULL,
    last_event_id    TEXT        NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 键集分页索引（交付 S2 的 delivery_facts，不修改事实表结构）。
CREATE INDEX IF NOT EXISTS idx_facts_keyset
    ON delivery_facts (occurred_at, event_id);

COMMENT ON TABLE projection_cursor IS
    'S4 客户端事件投影游标：projector="client_event"；S7 由 Kafka consumer offset 取代。';
