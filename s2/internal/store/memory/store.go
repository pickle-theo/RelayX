// Package memory 以进程内语义实现 S2 store 契约。
// 它逐条复刻 LLD 中的 SQL 规则：相同的 CAS 守卫、唯一键，
// 以及扮演 SQL now() 角色的 store 自有时钟。用于单元/集成测试
// 与默认本机运行模式；生产使用 store/postgres。
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"relayx/s2/internal/facts"
	"relayx/s2/internal/store"
)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

// SkewClock 用独立于进程时钟的方式模拟数据库时钟。
// 移动进程时钟不会改变基于该时钟的判定。
type SkewClock struct {
	mu     sync.RWMutex
	offset time.Duration
	base   time.Time
}

func NewSkewClock() *SkewClock {
	return &SkewClock{base: time.Now().UTC()}
}

func (c *SkewClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.base.Add(c.offset)
}

func (c *SkewClock) SetOffset(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offset = d
}

// Store 是进程内语义 store。
type Store struct {
	mu    sync.Mutex
	clock store.Clock

	messages   map[string]*store.Message
	deliveries map[string]*store.Delivery
	facts      map[string]*store.Fact
	endpoints  map[string]*store.Endpoint
	byIdem     map[string]string
	byDelKey   map[string]string
}

func New(clock store.Clock) *Store {
	if clock == nil {
		clock = systemClock{}
	}
	return &Store{
		clock:      clock,
		messages:   make(map[string]*store.Message),
		deliveries: make(map[string]*store.Delivery),
		facts:      make(map[string]*store.Fact),
		endpoints:  make(map[string]*store.Endpoint),
		byIdem:     make(map[string]string),
		byDelKey:   make(map[string]string),
	}
}

func (s *Store) now() time.Time { return s.clock.Now().UTC() }

func idemKey(tenant, producer, clientMsgID string) string {
	return tenant + "\x00" + producer + "\x00" + clientMsgID
}

func delKey(tenant, msgID, user, device string) string {
	return tenant + "\x00" + msgID + "\x00" + user + "\x00" + device
}

func epKey(tenant, user, device string) string {
	return tenant + "\x00" + user + "\x00" + device
}

func cloneMessage(m *store.Message) *store.Message {
	if m == nil {
		return nil
	}
	c := *m
	c.Payload = append([]byte(nil), m.Payload...)
	if m.FanoutDoneAt != nil {
		t := *m.FanoutDoneAt
		c.FanoutDoneAt = &t
	}
	if m.SettledAt != nil {
		t := *m.SettledAt
		c.SettledAt = &t
	}
	return &c
}

func cloneDelivery(d *store.Delivery) *store.Delivery {
	if d == nil {
		return nil
	}
	c := *d
	if d.NextAttemptAt != nil {
		t := *d.NextAttemptAt
		c.NextAttemptAt = &t
	}
	if d.TerminalAt != nil {
		t := *d.TerminalAt
		c.TerminalAt = &t
	}
	if d.ApplicationStatus != nil {
		v := *d.ApplicationStatus
		c.ApplicationStatus = &v
	}
	if d.ApplicationAckedAt != nil {
		t := *d.ApplicationAckedAt
		c.ApplicationAckedAt = &t
	}
	if d.LastError != nil {
		v := *d.LastError
		c.LastError = &v
	}
	return &c
}

type snapshot struct {
	messages   map[string]*store.Message
	deliveries map[string]*store.Delivery
	facts      map[string]*store.Fact
	endpoints  map[string]*store.Endpoint
	byIdem     map[string]string
	byDelKey   map[string]string
}

func (s *Store) capture() *snapshot {
	snap := &snapshot{
		messages:   make(map[string]*store.Message, len(s.messages)),
		deliveries: make(map[string]*store.Delivery, len(s.deliveries)),
		facts:      make(map[string]*store.Fact, len(s.facts)),
		endpoints:  make(map[string]*store.Endpoint, len(s.endpoints)),
		byIdem:     make(map[string]string, len(s.byIdem)),
		byDelKey:   make(map[string]string, len(s.byDelKey)),
	}
	for k, v := range s.messages {
		snap.messages[k] = cloneMessage(v)
	}
	for k, v := range s.deliveries {
		snap.deliveries[k] = cloneDelivery(v)
	}
	for k, v := range s.facts {
		c := *v
		c.PayloadSummary = cloneMap(v.PayloadSummary)
		snap.facts[k] = &c
	}
	for k, v := range s.endpoints {
		c := *v
		snap.endpoints[k] = &c
	}
	for k, v := range s.byIdem {
		snap.byIdem[k] = v
	}
	for k, v := range s.byDelKey {
		snap.byDelKey[k] = v
	}
	return snap
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (s *Store) restore(snap *snapshot) {
	s.messages = snap.messages
	s.deliveries = snap.deliveries
	s.facts = snap.facts
	s.endpoints = snap.endpoints
	s.byIdem = snap.byIdem
	s.byDelKey = snap.byDelKey
}

type txn struct {
	s      *Store
	snap   *snapshot
	done   bool
	rolled bool
}

func (t *txn) Messages() store.MessageStore    { return &messageView{s: t.s, tx: t} }
func (t *txn) Deliveries() store.DeliveryStore { return &deliveryView{s: t.s, tx: t} }
func (t *txn) Facts() store.FactStore          { return &factView{s: t.s, tx: t} }
func (t *txn) Commit() error {
	t.done = true
	return nil
}
func (t *txn) Rollback() error {
	if !t.done && !t.rolled {
		t.s.restore(t.snap)
		t.rolled = true
	}
	t.done = true
	return nil
}

func (s *Store) WithinTx(_ context.Context, fn func(store.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := &txn{s: s, snap: s.capture()}
	err := fn(t)
	if err != nil {
		_ = t.Rollback()
		return err
	}
	if !t.done {
		_ = t.Commit()
	}
	return nil
}

func (s *Store) Messages() store.MessageStore      { return &messageView{s: s} }
func (s *Store) Deliveries() store.DeliveryStore   { return &deliveryView{s: s} }
func (s *Store) Endpoints() store.EndpointRegistry { return &endpointView{s: s} }
func (s *Store) Facts() store.FactStore            { return &factView{s: s} }
func (s *Store) Close() error                      { return nil }

// SetClockOffset 移动 store 时钟，用于确定性的失败/过期测试。
// 若 store 构建时未使用 SkewClock，则为空操作。
func (s *Store) SetClockOffset(d time.Duration) {
	if c, ok := s.clock.(*SkewClock); ok {
		c.SetOffset(d)
	}
}

type view struct {
	s  *Store
	tx *txn
}

func (v *view) lock() {
	if v.tx == nil {
		v.s.mu.Lock()
	}
}
func (v *view) unlock() {
	if v.tx == nil {
		v.s.mu.Unlock()
	}
}

// ---------- 消息 ----------

type messageView struct{ view }

func (v *messageView) Insert(_ context.Context, _ store.Tx, m *store.Message) (bool, *store.Message, error) {
	v.lock()
	defer v.unlock()
	key := idemKey(m.TenantID, m.ProducerID, m.ClientMsgID)
	if id, ok := v.s.byIdem[key]; ok {
		return false, cloneMessage(v.s.messages[id]), nil
	}
	c := cloneMessage(m)
	now := v.s.now()
	if c.PlatformState == "" {
		c.PlatformState = store.MessageAccepted
	}
	c.CreatedAt = now
	c.ExpiresAt = now.Add(time.Duration(c.TTLSeconds) * time.Second)
	v.s.byIdem[key] = c.ID
	v.s.messages[c.ID] = c
	return true, nil, nil
}

func (v *messageView) GetByID(_ context.Context, id string) (*store.Message, error) {
	v.lock()
	defer v.unlock()
	m, ok := v.s.messages[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return cloneMessage(m), nil
}

func (v *messageView) GetByIdempotencyKey(_ context.Context, tenant, producerID, clientMsgID string) (*store.Message, error) {
	v.lock()
	defer v.unlock()
	id, ok := v.s.byIdem[idemKey(tenant, producerID, clientMsgID)]
	if !ok {
		return nil, store.ErrNotFound
	}
	return cloneMessage(v.s.messages[id]), nil
}

func (v *messageView) ListExpired(_ context.Context, limit int) ([]*store.Message, error) {
	v.lock()
	defer v.unlock()
	now := v.s.now()
	out := make([]*store.Message, 0)
	for _, m := range v.s.messages {
		if m.PlatformState == store.MessageAccepted && !m.ExpiresAt.After(now) {
			out = append(out, cloneMessage(m))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ExpiresAt.Before(out[j].ExpiresAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (v *messageView) MarkState(_ context.Context, _ store.Tx, id, state string, version int) error {
	v.lock()
	defer v.unlock()
	m, ok := v.s.messages[id]
	if !ok {
		return store.ErrNotFound
	}
	if version >= 0 && m.StateVersion != version {
		return store.ErrConcurrentConflict
	}
	m.PlatformState = state
	m.StateVersion++
	return nil
}

func (v *messageView) MarkFanoutDone(_ context.Context, _ store.Tx, id string) (bool, error) {
	v.lock()
	defer v.unlock()
	m, ok := v.s.messages[id]
	if !ok {
		return false, store.ErrNotFound
	}
	if m.FanoutDoneAt != nil {
		return false, nil
	}
	now := v.s.now()
	m.FanoutDoneAt = &now
	return true, nil
}

func finalCounts(ds []*store.Delivery) store.FinalCounts {
	var c store.FinalCounts
	c.Total = len(ds)
	for _, d := range ds {
		switch d.DeliveryState {
		case store.DeliveryReceiptAcked:
			c.ReceiptAcked++
		case store.DeliveryFailed:
			c.Failed++
		case store.DeliveryExpired:
			c.Expired++
		}
	}
	return c
}

func deliveriesOf(s *Store, msgID string) []*store.Delivery {
	out := make([]*store.Delivery, 0)
	for _, d := range s.deliveries {
		if d.MessageID == msgID {
			out = append(out, d)
		}
	}
	return out
}

func (v *messageView) SettleIfEligible(_ context.Context, _ store.Tx, id string) (store.CASResult, bool, error) {
	v.lock()
	defer v.unlock()
	m, ok := v.s.messages[id]
	if !ok {
		return store.CASResult{}, false, store.ErrNotFound
	}
	if m.PlatformState != store.MessageAccepted || m.FanoutDoneAt == nil {
		return store.CASResult{}, false, nil
	}
	ds := deliveriesOf(v.s, id)
	for _, d := range ds {
		if d.DeliveryState == store.DeliveryPending || d.DeliveryState == store.DeliverySent {
			return store.CASResult{}, false, nil
		}
	}
	now := v.s.now()
	m.PlatformState = store.MessageSettled
	m.StateVersion++
	m.SettledAt = &now
	return store.CASResult{
		StateVersion: m.StateVersion,
		TerminalAt:   now,
		Counts:       finalCounts(ds),
	}, true, nil
}

func (v *messageView) ExpireIfEligible(_ context.Context, _ store.Tx, id string) (store.CASResult, bool, error) {
	v.lock()
	defer v.unlock()
	m, ok := v.s.messages[id]
	if !ok {
		return store.CASResult{}, false, store.ErrNotFound
	}
	now := v.s.now()
	if m.PlatformState != store.MessageAccepted || m.ExpiresAt.After(now) {
		return store.CASResult{}, false, nil
	}
	m.PlatformState = store.MessageExpired
	m.StateVersion++
	ds := deliveriesOf(v.s, id)
	return store.CASResult{
		StateVersion: m.StateVersion,
		TerminalAt:   m.ExpiresAt,
		Counts:       finalCounts(ds),
	}, true, nil
}

// ---------- 投递 ----------

type deliveryView struct{ view }

func (v *deliveryView) InsertBatch(_ context.Context, _ store.Tx, ds []*store.Delivery) error {
	v.lock()
	defer v.unlock()
	now := v.s.now()
	for _, d := range ds {
		key := delKey(d.TenantID, d.MessageID, d.UserID, d.DeviceID)
		if _, ok := v.s.byDelKey[key]; ok {
			continue
		}
		c := cloneDelivery(d)
		if c.DeliveryState == "" {
			c.DeliveryState = store.DeliveryPending
		}
		c.CreatedAt = now
		c.UpdatedAt = now
		if c.NextAttemptAt == nil && c.Online && c.DeliveryState == store.DeliveryPending {
			t := now
			c.NextAttemptAt = &t
		}
		v.s.byDelKey[key] = c.ID
		v.s.deliveries[c.ID] = c
	}
	return nil
}

func (v *deliveryView) GetByKey(_ context.Context, tenant, msgID, user, device string) (*store.Delivery, error) {
	v.lock()
	defer v.unlock()
	id, ok := v.s.byDelKey[delKey(tenant, msgID, user, device)]
	if !ok {
		return nil, store.ErrNotFound
	}
	return cloneDelivery(v.s.deliveries[id]), nil
}

func (v *deliveryView) ListByMessage(_ context.Context, msgID string) ([]*store.Delivery, error) {
	v.lock()
	defer v.unlock()
	out := make([]*store.Delivery, 0)
	for _, d := range v.s.deliveries {
		if d.MessageID == msgID {
			out = append(out, cloneDelivery(d))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (v *deliveryView) ListDue(_ context.Context, limit int) ([]*store.Delivery, error) {
	v.lock()
	defer v.unlock()
	now := v.s.now()
	out := make([]*store.Delivery, 0)
	for _, d := range v.s.deliveries {
		if (d.DeliveryState == store.DeliveryPending || d.DeliveryState == store.DeliverySent) &&
			d.NextAttemptAt != nil && !d.NextAttemptAt.After(now) {
			out = append(out, cloneDelivery(d))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NextAttemptAt.Before(*out[j].NextAttemptAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (v *deliveryView) ListPendingByEndpoint(_ context.Context, tenant, user, device string) ([]*store.Delivery, error) {
	v.lock()
	defer v.unlock()
	out := make([]*store.Delivery, 0)
	for _, d := range v.s.deliveries {
		if d.TenantID == tenant && d.UserID == user && d.DeviceID == device &&
			(d.DeliveryState == store.DeliveryPending || d.DeliveryState == store.DeliverySent) {
			out = append(out, cloneDelivery(d))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (v *deliveryView) ActivatePending(_ context.Context, _ store.Tx, tenant, user, device string) error {
	v.lock()
	defer v.unlock()
	now := v.s.now()
	for _, d := range v.s.deliveries {
		if d.TenantID == tenant && d.UserID == user && d.DeviceID == device &&
			(d.DeliveryState == store.DeliveryPending || d.DeliveryState == store.DeliverySent) &&
			d.NextAttemptAt == nil {
			t := now
			d.NextAttemptAt = &t
			d.UpdatedAt = now
		}
	}
	return nil
}

func applyPatch(d *store.Delivery, p store.DeliveryPatch, now time.Time) {
	if p.State != "" {
		d.DeliveryState = p.State
	}
	if p.ClearNextAttemptAt {
		d.NextAttemptAt = nil
	} else if p.NextAttemptAt != nil {
		t := *p.NextAttemptAt
		d.NextAttemptAt = &t
	}
	if p.Attempts != nil {
		d.Attempts = *p.Attempts
	}
	if p.ClearApplicationStatus {
		d.ApplicationStatus = nil
	} else if p.ApplicationStatus != nil {
		v := *p.ApplicationStatus
		d.ApplicationStatus = &v
	}
	if p.ApplicationAckedAt != nil {
		t := *p.ApplicationAckedAt
		d.ApplicationAckedAt = &t
	}
	if p.ClearLastError {
		d.LastError = nil
	} else if p.LastError != nil {
		v := *p.LastError
		d.LastError = &v
	}
	if p.TerminalAt != nil {
		t := *p.TerminalAt
		d.TerminalAt = &t
	}
	d.UpdatedAt = now
}

func (v *deliveryView) MarkSent(_ context.Context, _ store.Tx, id string, attempts int32, backoffSeconds int32) (int, bool, error) {
	v.lock()
	defer v.unlock()
	d, ok := v.s.deliveries[id]
	if !ok {
		return 0, false, store.ErrNotFound
	}
	if d.DeliveryState != store.DeliveryPending && d.DeliveryState != store.DeliverySent {
		return d.StateVersion, false, nil
	}
	now := v.s.now()
	next := now.Add(time.Duration(backoffSeconds) * time.Second)
	d.DeliveryState = store.DeliverySent
	d.Attempts = attempts
	d.NextAttemptAt = &next
	d.StateVersion++
	d.UpdatedAt = now
	return d.StateVersion, true, nil
}

func isTerminalState(s string) bool {
	return s == store.DeliveryReceiptAcked || s == store.DeliveryFailed || s == store.DeliveryExpired
}

func (v *deliveryView) Transition(_ context.Context, _ store.Tx, id string, from []string, to string, patch store.DeliveryPatch, version int) (bool, error) {
	v.lock()
	defer v.unlock()
	d, ok := v.s.deliveries[id]
	if !ok {
		return false, store.ErrNotFound
	}
	if !contains(from, d.DeliveryState) || d.StateVersion != version {
		return false, nil
	}
	now := v.s.now()
	patch.State = to
	if isTerminalState(to) && patch.TerminalAt == nil {
		t := now
		patch.TerminalAt = &t
	}
	applyPatch(d, patch, now)
	d.StateVersion++
	return true, nil
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func (v *deliveryView) ExpireIfEligible(_ context.Context, _ store.Tx, id string) (int, bool, error) {
	v.lock()
	defer v.unlock()
	d, ok := v.s.deliveries[id]
	if !ok {
		return 0, false, store.ErrNotFound
	}
	if d.DeliveryState != store.DeliveryPending && d.DeliveryState != store.DeliverySent {
		return d.StateVersion, false, nil
	}
	now := v.s.now()
	d.DeliveryState = store.DeliveryExpired
	d.StateVersion++
	d.NextAttemptAt = nil
	d.TerminalAt = &now
	d.UpdatedAt = now
	return d.StateVersion, true, nil
}

func (v *deliveryView) SetApplicationAck(_ context.Context, _ store.Tx, id string, status string) error {
	v.lock()
	defer v.unlock()
	d, ok := v.s.deliveries[id]
	if !ok {
		return store.ErrNotFound
	}
	s := status
	t := v.s.now()
	d.ApplicationStatus = &s
	d.ApplicationAckedAt = &t
	return nil
}

// ---------- 事实 ----------

type factView struct{ view }

func (v *factView) InsertBatch(_ context.Context, _ store.Tx, facts []*store.Fact) error {
	v.lock()
	defer v.unlock()
	now := v.s.now()
	for _, f := range facts {
		c := *f
		if c.EventID == "" {
			c.EventID = deriveEventID(c.TenantID, c.MessageID, c.FactType, c.EndpointID, c.StateVersion)
		}
		if c.OccurredAt.IsZero() {
			c.OccurredAt = now
		} else {
			c.OccurredAt = c.OccurredAt.UTC()
		}
		c.PayloadSummary = cloneMap(c.PayloadSummary)
		if _, ok := v.s.facts[c.EventID]; ok {
			continue
		}
		v.s.facts[c.EventID] = &c
	}
	return nil
}

func (v *factView) ListByMessage(_ context.Context, msgID string) ([]*store.Fact, error) {
	v.lock()
	defer v.unlock()
	out := make([]*store.Fact, 0)
	for _, f := range v.s.facts {
		if f.MessageID == msgID {
			c := *f
			c.PayloadSummary = cloneMap(f.PayloadSummary)
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].OccurredAt.Equal(out[j].OccurredAt) {
			return out[i].EventID < out[j].EventID
		}
		return out[i].OccurredAt.Before(out[j].OccurredAt)
	})
	return out, nil
}

func deriveEventID(tenant, messageID, factType string, endpointID *string, version int) string {
	return facts.Derive(tenant, messageID, factType, endpointID, version)
}

// ---------- 端点 ----------

type endpointView struct{ view }

func (v *endpointView) Resolve(_ context.Context, tenant, user string, devices []string, mode string, limit int32) ([]store.Endpoint, error) {
	v.lock()
	defer v.unlock()
	want := make(map[string]bool, len(devices))
	for _, d := range devices {
		want[d] = true
	}
	out := make([]store.Endpoint, 0)
	for _, e := range v.s.endpoints {
		if e.TenantID != tenant || e.UserID != user {
			continue
		}
		if len(want) > 0 && !want[e.DeviceID] {
			continue
		}
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LastSeenAt.Equal(out[j].LastSeenAt) {
			return out[i].DeviceID < out[j].DeviceID
		}
		return out[i].LastSeenAt.After(out[j].LastSeenAt)
	})
	if mode == store.ModeAllEndpoints {
		sort.Slice(out, func(i, j int) bool { return out[i].DeviceID < out[j].DeviceID })
	}
	if limit > 0 && int32(len(out)) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (v *endpointView) MarkOnline(_ context.Context, tenant, user, device, sessionID string) error {
	v.lock()
	defer v.unlock()
	k := epKey(tenant, user, device)
	e, ok := v.s.endpoints[k]
	if !ok {
		e = &store.Endpoint{TenantID: tenant, UserID: user, DeviceID: device}
		v.s.endpoints[k] = e
	}
	e.Status = store.EndpointOnline
	e.SessionID = sessionID
	e.LastSeenAt = v.s.now()
	return nil
}

func (v *endpointView) MarkOffline(_ context.Context, tenant, user, device, sessionID string) error {
	v.lock()
	defer v.unlock()
	e, ok := v.s.endpoints[epKey(tenant, user, device)]
	if !ok || (sessionID != "" && e.SessionID != sessionID) {
		return nil
	}
	e.Status = store.EndpointOffline
	e.SessionID = ""
	e.LastSeenAt = v.s.now()
	return nil
}

func (v *endpointView) ResetAllOffline(_ context.Context) error {
	v.lock()
	defer v.unlock()
	now := v.s.now()
	for _, e := range v.s.endpoints {
		e.Status = store.EndpointOffline
		e.SessionID = ""
		e.LastSeenAt = now
	}
	return nil
}

func (v *endpointView) ListInconsistentOnline(_ context.Context, isOnline func(tenant, user, device string) bool) ([]store.Endpoint, error) {
	v.lock()
	defer v.unlock()
	out := make([]store.Endpoint, 0)
	for _, e := range v.s.endpoints {
		if e.Status == store.EndpointOnline && !isOnline(e.TenantID, e.UserID, e.DeviceID) {
			out = append(out, *e)
		}
	}
	return out, nil
}
