package projector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Cursor 是投影进度：最后一条已路由事实的 (occurred_at, event_id)。
// 键集分页以此为准，绝不用 OFFSET。
type Cursor struct {
	At      time.Time `json:"at"`
	EventID string    `json:"event_id"`
}

// CursorStore 持久化投影游标（LLD §2.3 EventCursor / §5.4）。
type CursorStore interface {
	// Load 返回游标；首次运行返回零值。
	Load(ctx context.Context, projector string) (Cursor, error)
	// Save 在一批事实全部路由成功后推进游标；重复读取由 event_id 幂等吸收。
	Save(ctx context.Context, projector string, c Cursor) error
}

// MemoryCursor 是进程内游标：进程重启后从头重放（端侧 event_id 去重吸收）。
type MemoryCursor struct {
	mu sync.RWMutex
	m  map[string]Cursor
}

func NewMemoryCursor() *MemoryCursor {
	return &MemoryCursor{m: make(map[string]Cursor)}
}

func (c *MemoryCursor) Load(_ context.Context, projector string) (Cursor, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.m[projector], nil
}

func (c *MemoryCursor) Save(_ context.Context, projector string, cur Cursor) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[projector] = cur
	return nil
}

// FileCursor 把游标写入本地 JSON 文件，跨进程重启保留。
// 生产部署使用 migrations/0003_projection_cursor.sql 的 projection_cursor 表；
// 本阶段 module 不引入数据库依赖，file 模式提供等价的持久化语义。
type FileCursor struct {
	mu   sync.Mutex
	path string
	m    map[string]Cursor
}

func NewFileCursor(path string) (*FileCursor, error) {
	c := &FileCursor{path: path, m: make(map[string]Cursor)}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return c, nil
		}
		return nil, err
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c.m); err != nil {
			return nil, fmt.Errorf("projector: parse cursor file: %w", err)
		}
	}
	return c, nil
}

func (c *FileCursor) Load(_ context.Context, projector string) (Cursor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[projector], nil
}

func (c *FileCursor) Save(_ context.Context, projector string, cur Cursor) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[projector] = cur
	raw, err := json.MarshalIndent(c.m, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(c.path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}
