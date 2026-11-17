package core

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"relayx/s4/protocol"
)

var (
	// ErrPolicyLimit 表示策略/payload 超出平台硬上限。
	ErrPolicyLimit = errors.New("core: policy limit exceeded")
	// ErrDuplicateMismatch 表示幂等键被复用但 payload 不同。
	ErrDuplicateMismatch = errors.New("core: idempotency key reused with different payload")
	// ErrUnknownMessage 表示 ACK 引用了未知 message_id。
	ErrUnknownMessage = errors.New("core: unknown message id")
	// ErrNotFound 表示消息/投递不存在。
	ErrNotFound = errors.New("core: not found")
)

// Clock 是内核的时间源。它模拟数据库时钟：移动进程时钟不会改变
// 已经依据内核时钟做出的判定（S2 语义契约 §4）。
type Clock interface {
	Now() time.Time
}

// SystemClock 是生产时钟。
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

// SkewClock 是可注入偏移的时钟，用于 TTL 过期测试。
type SkewClock struct {
	mu     sync.RWMutex
	base   time.Time
	offset time.Duration
}

func NewSkewClock() *SkewClock { return &SkewClock{base: time.Now().UTC()} }

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

// Sink 是单条活跃连接的 P1 可靠通道入口（连接层负责编码与序号分配）。
type Sink interface {
	EnqueueReliable(ctx context.Context, f protocol.Frame) error
}

// OnlineResolver 把端点映射到其活跃 Sink；会话注册表是权威来源。
type OnlineResolver interface {
	Lookup(tenant, user, device string) (Sink, bool)
}

// Kernel 是进程内事实生产者。所有状态读写都在同一把锁下完成，
// 事实与状态变更原子地一起提交（不存在"状态变了事实没写"的窗口）。
type Kernel struct {
	mu         sync.Mutex
	dispatchMu sync.Mutex

	opts     Options
	clock    Clock
	resolver OnlineResolver

	messages   map[string]*Message
	deliveries map[string]*Delivery
	facts      []*Fact
	endpoints  map[string]*Endpoint
	byIdem     map[string]string
	byDelKey   map[string]string

	seq    uint64
	notify chan struct{}

	// lastFactAt 保证事实的 occurred_at 与插入顺序严格一致。
	// 若允许两个批次共享同一时间戳，前一批可能已把游标推进到
	// 同时间戳但 event_id 更大的事实，后一批中 event_id 更小的新事实
	// 将永远落在 (occurred_at, event_id) > cursor 之外 —— 即漏读。
	lastFactAt time.Time
}

// New 创建内核。clock 为 nil 时使用 SystemClock。
func New(opts Options, clock Clock) *Kernel {
	if clock == nil {
		clock = SystemClock{}
	}
	return &Kernel{
		opts:       opts,
		clock:      clock,
		messages:   make(map[string]*Message),
		deliveries: make(map[string]*Delivery),
		endpoints:  make(map[string]*Endpoint),
		byIdem:     make(map[string]string),
		byDelKey:   make(map[string]string),
		notify:     make(chan struct{}, 1),
	}
}

// SetOnlineResolver 注入在线判定（Gateway 的会话注册表）。
func (k *Kernel) SetOnlineResolver(r OnlineResolver) {
	k.mu.Lock()
	k.resolver = r
	k.mu.Unlock()
}

func (k *Kernel) now() time.Time { return k.clock.Now().UTC() }

func (k *Kernel) nextID(prefix string) string {
	k.seq++
	return prefix + "-" + u64(k.seq)
}

func u64(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// notifyLocked 发出"有新事实"信号（低延迟优化，正确性仍由游标轮询保证）。
func (k *Kernel) notifyLocked() {
	select {
	case k.notify <- struct{}{}:
	default:
	}
}

// Signal 返回事实写入信号（投影循环可 select 它立即扫描）。
func (k *Kernel) Signal() <-chan struct{} { return k.notify }

func (k *Kernel) appendFactsLocked(fs ...*Fact) {
	if len(fs) == 0 {
		return
	}
	at := k.now()
	if !at.After(k.lastFactAt) {
		at = k.lastFactAt.Add(time.Nanosecond)
	}
	k.lastFactAt = at
	for _, f := range fs {
		if f.OccurredAt.IsZero() {
			f.OccurredAt = at
		}
		k.facts = append(k.facts, f)
	}
	k.notifyLocked()
}

// Submit 走唯一受理路径：幂等判定与消息/投递/事实写入在同一临界区完成。
func (k *Kernel) Submit(_ context.Context, in Input) (Result, error) {
	if in.TenantID == "" || in.ProducerID == "" || in.ClientMsgID == "" || in.TargetUserID == "" {
		return Result{}, ErrPolicyLimit
	}
	if len(in.Payload) == 0 || int32(len(in.Payload)) > k.opts.MaxPayload {
		return Result{}, ErrPolicyLimit
	}
	policy, ok := k.normalizePolicy(in.Policy)
	if !ok {
		return Result{}, ErrPolicyLimit
	}
	kind := in.ProducerKind
	if kind != ProducerUser && kind != ProducerService {
		kind = ProducerUser
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	key := in.TenantID + "\x00" + in.ProducerID + "\x00" + in.ClientMsgID
	if id, exists := k.byIdem[key]; exists {
		m := k.messages[id]
		if !bytes.Equal(m.Payload, in.Payload) {
			return Result{}, ErrDuplicateMismatch
		}
		// AcceptedAt 必须是首次受理时刻：SEND_ACK 与 HTTP 201 都直接透出它，
		// 留零值会让端侧收到公元 1 年的时间戳（S2 同样回填 existing.CreatedAt）。
		return Result{
			MessageID:  m.ID,
			State:      m.PlatformState,
			Duplicate:  true,
			Counts:     k.countsLocked(m.ID),
			AcceptedAt: m.CreatedAt,
		}, nil
	}

	targets := k.resolveLocked(in.TenantID, in.TargetUserID, in.TargetDevices, policy.DeliveryMode, policy.Fanout)
	now := k.now()
	m := &Message{
		ID:             k.nextID("msg"),
		TenantID:       in.TenantID,
		ProducerID:     in.ProducerID,
		ProducerKind:   kind,
		Sender:         in.Sender,
		SourceDeviceID: in.SourceDeviceID,
		ClientMsgID:    in.ClientMsgID,
		Payload:        append([]byte(nil), in.Payload...),
		DeliveryMode:   policy.DeliveryMode,
		Fanout:         policy.Fanout,
		TTLSeconds:     policy.TTLSeconds,
		MaxAttempts:    policy.MaxAttempts,
		PlatformState:  MessageAccepted,
		CreatedAt:      now,
		ExpiresAt:      now.Add(time.Duration(policy.TTLSeconds) * time.Second),
	}
	k.messages[m.ID] = m
	k.byIdem[key] = m.ID

	facts := []*Fact{messageFact(m, FactMessageAccepted, 0, protocol.DeliveryCounts{Total: len(targets)})}
	for _, t := range targets {
		d := &Delivery{
			ID:            k.nextID("del"),
			TenantID:      m.TenantID,
			MessageID:     m.ID,
			UserID:        t.UserID,
			DeviceID:      t.DeviceID,
			EndpointID:    t.UserID + ":" + t.DeviceID,
			DeliveryState: DeliveryPending,
		}
		k.deliveries[d.ID] = d
		k.byDelKey[deliveryKey(d.TenantID, d.MessageID, d.UserID, d.DeviceID)] = d.ID
		facts = append(facts, deliveryFact(d, FactDeliveryCreated, 0))
	}
	fanoutDone := now
	m.FanoutDoneAt = &fanoutDone
	k.appendFactsLocked(facts...)

	// 扇出完成即是允许结算的信号；零端点消息在这里同事务结算。
	k.trySettleLocked(m)

	return Result{
		MessageID:  m.ID,
		State:      m.PlatformState,
		Counts:     k.countsLocked(m.ID),
		AcceptedAt: now,
	}, nil
}

func (k *Kernel) normalizePolicy(p Policy) (Policy, bool) {
	if p.TTLSeconds == 0 {
		p.TTLSeconds = k.opts.DefaultTTL
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = k.opts.DefaultMaxAttempts
	}
	if p.TTLSeconds <= 0 || p.TTLSeconds > k.opts.MaxTTL {
		return p, false
	}
	if p.MaxAttempts <= 0 || p.MaxAttempts > k.opts.MaxAttempts {
		return p, false
	}
	if p.Fanout < 0 {
		return p, false
	}
	switch p.DeliveryMode {
	case "", ModeAllEndpoints:
		p.DeliveryMode = ModeAllEndpoints
	case ModeLastActive:
	default:
		return p, false
	}
	return p, true
}

type target struct {
	UserID   string
	DeviceID string
}

// resolveLocked 从端点目录解析投递目标；只返回已知端点（与 S2 一致）。
// last_active 按 LastSeenAt 降序取第一个，all_endpoints 按 device 升序。
func (k *Kernel) resolveLocked(tenant, user string, devices []string, mode string, limit int32) []target {
	want := make(map[string]bool, len(devices))
	for _, d := range devices {
		want[d] = true
	}
	out := make([]target, 0)
	for _, e := range k.endpoints {
		if e.TenantID != tenant || e.UserID != user {
			continue
		}
		if len(want) > 0 && !want[e.DeviceID] {
			continue
		}
		out = append(out, target{UserID: e.UserID, DeviceID: e.DeviceID})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DeviceID < out[j].DeviceID })
	if mode == ModeLastActive && len(out) > 1 {
		var best *target
		var bestAt time.Time
		for i := range out {
			e := k.endpoints[endpointKey(tenant, out[i].UserID, out[i].DeviceID)]
			if e == nil {
				continue
			}
			if best == nil || e.LastSeenAt.After(bestAt) {
				best, bestAt = &out[i], e.LastSeenAt
			}
		}
		if best != nil {
			out = []target{*best}
		}
	}
	if limit > 0 && int32(len(out)) > limit {
		out = out[:limit]
	}
	return out
}

// Dispatch 把所有 PENDING 且在线端点推进到 SENT，并写入 delivery.sent 事实。
// 离线端点保持 PENDING（不重试、不写事实），由重连或查询兜底。
func (k *Kernel) Dispatch(ctx context.Context, messageID string) error {
	k.dispatchMu.Lock()
	defer k.dispatchMu.Unlock()

	type pending struct {
		delivery *Delivery
		payload  []byte
		sink     Sink
	}
	k.mu.Lock()
	m := k.messages[messageID]
	if m == nil {
		k.mu.Unlock()
		return ErrNotFound
	}
	if m.PlatformState != MessageAccepted {
		k.mu.Unlock()
		return nil
	}
	var todo []pending
	for _, d := range k.deliveries {
		if d.MessageID != messageID || d.DeliveryState != DeliveryPending {
			continue
		}
		if k.resolver == nil {
			continue
		}
		sink, ok := k.resolver.Lookup(d.TenantID, d.UserID, d.DeviceID)
		if !ok {
			continue // 离线：停在 PENDING，不产生事实
		}
		todo = append(todo, pending{
			delivery: cloneDelivery(d),
			payload:  append([]byte(nil), m.Payload...),
			sink:     sink,
		})
	}
	producerID, sender, expiresAt := m.ProducerID, m.Sender, m.ExpiresAt
	k.mu.Unlock()

	for _, it := range todo {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		frame := protocol.Frame{Type: protocol.TypeDeliver, Body: protocol.EncodeDeliver(protocol.DeliverBody{
			MessageID: messageID, ProducerID: producerID, Sender: sender,
			Payload: it.payload, ExpiresAt: expiresAt.UnixMilli(),
		})}
		if err := it.sink.EnqueueReliable(ctx, frame); err != nil {
			continue // 保持 PENDING，下一轮重试或查询兜底
		}
		k.mu.Lock()
		d := k.deliveries[it.delivery.ID]
		if d != nil && d.DeliveryState == DeliveryPending {
			d.DeliveryState = DeliverySent
			d.StateVersion++
			d.Attempts++
			k.appendFactsLocked(deliveryFact(d, FactDeliverySent, d.StateVersion))
		}
		k.mu.Unlock()
	}
	return nil
}

// ReceiptAck 是投递进入 RECEIPT_ACKED 的唯一路径。端点身份由会话提供。
func (k *Kernel) ReceiptAck(_ context.Context, tenant, messageID, user, device string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	d, ok := k.deliveries[k.byDelKey[deliveryKey(tenant, messageID, user, device)]]
	if !ok {
		return ErrUnknownMessage
	}
	// 与 S2 冻结状态机一致：只有 SENT → RECEIPT_ACKED 合法。
	// 允许 PENDING 会让客户端对尚未投递的投递（message_id 可猜）抢先 ACK：
	// 该投递随后被 Dispatch 跳过，消息以 receipt_acked 计数结算但 payload 从未送达。
	if d.DeliveryState != DeliverySent {
		return nil // duplicate / late ACK：不改状态、不写事实
	}
	d.DeliveryState = DeliveryReceiptAcked
	d.StateVersion++
	terminal := k.now()
	d.TerminalAt = &terminal
	k.appendFactsLocked(deliveryFact(d, FactDeliveryReceiptAcked, d.StateVersion))
	if m := k.messages[d.MessageID]; m != nil {
		k.trySettleLocked(m)
	}
	return nil
}

// BusinessAck 只写业务透传列；不进状态机、不改重试计划、不产生事实。
func (k *Kernel) BusinessAck(_ context.Context, tenant, messageID, user, device, status string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	d, ok := k.deliveries[k.byDelKey[deliveryKey(tenant, messageID, user, device)]]
	if !ok {
		return ErrUnknownMessage
	}
	s := status
	d.ApplicationStatus = &s
	ackedAt := k.now()
	d.ApplicationAckedAt = &ackedAt
	return nil
}

// ExpireMessage 应用过期 CAS：仅当消息仍为 ACCEPTED 且已过 TTL 时生效，
// 同一临界区内把 PENDING/SENT 投递置为 EXPIRED 并写入 message.expired。
func (k *Kernel) ExpireMessage(messageID string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	m := k.messages[messageID]
	if m == nil {
		return ErrNotFound
	}
	if m.PlatformState != MessageAccepted || k.now().Before(m.ExpiresAt) {
		return nil
	}
	for _, d := range k.deliveries {
		if d.MessageID != messageID {
			continue
		}
		if d.DeliveryState != DeliveryPending && d.DeliveryState != DeliverySent {
			continue
		}
		d.DeliveryState = DeliveryExpired
		d.StateVersion++
		terminal := k.now()
		d.TerminalAt = &terminal
		k.appendFactsLocked(deliveryFact(d, FactDeliveryExpired, d.StateVersion))
	}
	counts := k.countsLocked(messageID)
	m.PlatformState = MessageExpired
	m.StateVersion++
	k.appendFactsLocked(messageFact(m, FactMessageExpired, m.StateVersion, counts))
	return nil
}

// RunExpiryScanner 是单 goroutine 扫描器；本地 ticker 只决定扫描频率，
// 终态判定全部来自内核时钟与状态谓词。
func (k *Kernel) RunExpiryScanner(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			k.ExpireDue()
		}
	}
}

// ExpireDue 扫描一轮到期消息（供扫描器与确定性测试使用）。
func (k *Kernel) ExpireDue() {
	k.mu.Lock()
	var due []string
	for id, m := range k.messages {
		if m.PlatformState == MessageAccepted && !k.now().Before(m.ExpiresAt) {
			due = append(due, id)
		}
	}
	k.mu.Unlock()
	for _, id := range due {
		_ = k.ExpireMessage(id)
	}
}

// trySettleLocked 是聚合守卫：消息结算必须满足 fanout_done 已置位且
// 所有投递进入终态。零端点消息在扇出完成时立即结算。
func (k *Kernel) trySettleLocked(m *Message) bool {
	if m == nil || m.PlatformState != MessageAccepted || m.FanoutDoneAt == nil {
		return false
	}
	for _, d := range k.deliveries {
		if d.MessageID != m.ID {
			continue
		}
		switch d.DeliveryState {
		case DeliveryReceiptAcked, DeliveryFailed, DeliveryExpired:
		default:
			return false
		}
	}
	counts := k.countsLocked(m.ID)
	m.PlatformState = MessageSettled
	m.StateVersion++
	settled := k.now()
	m.SettledAt = &settled
	k.appendFactsLocked(messageFact(m, FactMessageSettled, m.StateVersion, counts))
	return true
}

func (k *Kernel) countsLocked(messageID string) protocol.DeliveryCounts {
	var c protocol.DeliveryCounts
	for _, d := range k.deliveries {
		if d.MessageID != messageID {
			continue
		}
		c.Total++
		switch d.DeliveryState {
		case DeliveryReceiptAcked:
			c.ReceiptAcked++
		case DeliveryFailed:
			c.Failed++
		case DeliveryExpired:
			c.Expired++
		}
	}
	return c
}

func deliveryKey(tenant, messageID, user, device string) string {
	return tenant + "\x00" + messageID + "\x00" + user + "\x00" + device
}

func endpointKey(tenant, user, device string) string {
	return tenant + "\x00" + user + "\x00" + device
}

// MarkOnline 记录端点上线（登录时调用）。
func (k *Kernel) MarkOnline(tenant, user, device, sessionID string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	key := endpointKey(tenant, user, device)
	e := k.endpoints[key]
	if e == nil {
		e = &Endpoint{TenantID: tenant, UserID: user, DeviceID: device}
		k.endpoints[key] = e
	}
	e.Online = true
	e.SessionID = sessionID
	e.LastSeenAt = k.now()
}

// MarkOffline 记录端点离线（断连时调用）。
func (k *Kernel) MarkOffline(tenant, user, device, sessionID string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	e := k.endpoints[endpointKey(tenant, user, device)]
	if e == nil {
		return
	}
	if sessionID != "" && e.SessionID != sessionID {
		return // 新会话已接管，旧连接的清理不得覆盖
	}
	e.Online = false
	e.SessionID = ""
}

// PendingMessagesFor 返回该端点仍有 PENDING 投递的消息 ID（重连恢复用）。
func (k *Kernel) PendingMessagesFor(tenant, user, device string) []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	seen := make(map[string]bool)
	out := make([]string, 0)
	for _, d := range k.deliveries {
		if d.TenantID != tenant || d.UserID != user || d.DeviceID != device {
			continue
		}
		if d.DeliveryState != DeliveryPending || seen[d.MessageID] {
			continue
		}
		seen[d.MessageID] = true
		out = append(out, d.MessageID)
	}
	return out
}

// GetMessage 返回消息副本；不存在返回 ErrNotFound。
func (k *Kernel) GetMessage(messageID string) (Message, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	m := k.messages[messageID]
	if m == nil {
		return Message{}, ErrNotFound
	}
	return *cloneMessage(m), nil
}

// ListDeliveries 返回某消息的全部投递副本。
func (k *Kernel) ListDeliveries(messageID string) []Delivery {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]Delivery, 0)
	for _, d := range k.deliveries {
		if d.MessageID == messageID {
			out = append(out, *cloneDelivery(d))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Counts 从投递表重算计数（与 payload_summary.counts 的双源比对用）。
func (k *Kernel) Counts(messageID string) protocol.DeliveryCounts {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.countsLocked(messageID)
}

// Route 返回消息的来源路由键（producer_id, source_device_id）。
func (k *Kernel) Route(messageID string) (producerID, sourceDeviceID string, ok bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	m := k.messages[messageID]
	if m == nil {
		return "", "", false
	}
	return m.ProducerID, m.SourceDeviceID, true
}

// ListFactsAfter 按 (occurred_at, event_id) 键集分页读取事实。
// types 为空表示不过滤；返回严格升序，相同 (occurred_at, event_id) 不重不漏。
func (k *Kernel) ListFactsAfter(afterAt time.Time, afterID string, types []string, limit int) []Fact {
	k.mu.Lock()
	defer k.mu.Unlock()
	want := make(map[string]bool, len(types))
	for _, t := range types {
		want[t] = true
	}
	out := make([]Fact, 0)
	for _, f := range k.facts {
		if len(want) > 0 && !want[f.FactType] {
			continue
		}
		if !afterCursor(f, afterAt, afterID) {
			continue
		}
		out = append(out, *cloneFact(f))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].OccurredAt.Equal(out[j].OccurredAt) {
			return out[i].EventID < out[j].EventID
		}
		return out[i].OccurredAt.Before(out[j].OccurredAt)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// ListFactsByMessage 返回某消息的全部事实（升序）。
func (k *Kernel) ListFactsByMessage(messageID string) []Fact {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]Fact, 0)
	for _, f := range k.facts {
		if f.MessageID == messageID {
			out = append(out, *cloneFact(f))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].OccurredAt.Equal(out[j].OccurredAt) {
			return out[i].EventID < out[j].EventID
		}
		return out[i].OccurredAt.Before(out[j].OccurredAt)
	})
	return out
}

func afterCursor(f *Fact, at time.Time, id string) bool {
	if f.OccurredAt.After(at) {
		return true
	}
	if f.OccurredAt.Equal(at) {
		return f.EventID > id
	}
	return false
}

func cloneMessage(m *Message) *Message {
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

func cloneDelivery(d *Delivery) *Delivery {
	c := *d
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
	return &c
}

func cloneFact(f *Fact) *Fact {
	c := *f
	if f.EndpointID != nil {
		ep := *f.EndpointID
		c.EndpointID = &ep
	}
	if f.PayloadSummary != nil {
		c.PayloadSummary = make(map[string]any, len(f.PayloadSummary))
		for k, v := range f.PayloadSummary {
			c.PayloadSummary[k] = v
		}
	}
	return &c
}
