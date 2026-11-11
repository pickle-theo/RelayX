-- Phase 5.5：虚拟分片与在线再平衡。
--
-- Phase 5 用 mod(id, shards) 静态取模分片，扩容必须停机统一切换配置。
-- 这里改成固定的虚拟分片空间：每一行投递按 message_id 落到一个稳定的虚拟分片，
-- Worker 领取的是区间而不是取模结果。区间可以随时劈裂或合并，行的归属由
-- shard_id 决定，因此再平衡只是元数据操作，不需要搬数据。

-- 虚拟分片：同一逻辑消息的所有投递行落在同一分片，保证同消息的处理顺序。
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS shard_id INT NOT NULL DEFAULT 0;

-- 分片分配：Worker 与虚拟分片区间的映射，带租约，租约过期的分配视为失效。
CREATE TABLE IF NOT EXISTS shard_assignments (
    id          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    worker_id   TEXT        NOT NULL,
    shard_start INT         NOT NULL,
    shard_end   INT         NOT NULL,
    version     BIGINT      NOT NULL DEFAULT 1,
    lease_until TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT shard_assignment_range CHECK (shard_start >= 0 AND shard_end > shard_start)
);

-- 一个 Worker 只持有一份有效分配。
CREATE UNIQUE INDEX IF NOT EXISTS shard_assignments_worker_idx
    ON shard_assignments (worker_id);

-- 活跃分配扫描：按租约判断分配是否仍然有效。
CREATE INDEX IF NOT EXISTS shard_assignments_lease_idx
    ON shard_assignments (lease_until);

-- 虚拟分片扫描：按分片区间领取到期投递行。
CREATE INDEX IF NOT EXISTS deliveries_shard_due_idx
    ON deliveries (shard_id, next_retry_at, id)
    WHERE state IN (0, 1);

-- Worker 存活探测：Worker 定期续租，租约过期即视为离线。
-- 分片分配与存活探测分开，避免"区间变化"和"心跳"耦合在一起。
CREATE TABLE IF NOT EXISTS shard_workers (
    worker_id   TEXT        PRIMARY KEY,
    lease_until TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS shard_workers_lease_idx
    ON shard_workers (lease_until);