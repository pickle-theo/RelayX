package global

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// grantTokensScript refills the token bucket and hands out up to the
// requested number of tokens.
//
// The bucket state lives entirely in Redis, so the tokens granted to the
// workers are the only tokens the cluster owns: the total delivery rate can
// never exceed the configured rate.
var grantTokensScript = redis.NewScript(`
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local want = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)

local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end
local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = tokens + (elapsed / 1000.0) * rate
if tokens > burst then tokens = burst end

local granted = 0
if tokens >= want then
  granted = want
else
  granted = math.floor(tokens)
  if granted < 0 then granted = 0 end
end
tokens = tokens - granted

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], ttl)
return granted
`)

// acquireSlotScript reserves one in-flight slot. Every reservation refreshes
// the expiry, so the counter only disappears when the whole fleet stops
// using it.
var acquireSlotScript = redis.NewScript(`
local limit = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local count = redis.call('INCR', KEYS[1])
redis.call('PEXPIRE', KEYS[1], ttl)
if limit > 0 and count > limit then
  redis.call('DECR', KEYS[1])
  return 0
end
return 1
`)

// releaseSlotScript gives one in-flight slot back and removes the counter when
// it reaches zero.
var releaseSlotScript = redis.NewScript(`
local count = redis.call('DECR', KEYS[1])
if count <= 0 then
  redis.call('DEL', KEYS[1])
  return 0
end
return count
`)

// inflightSlotTTL is the lifetime of the global in-flight counter. A worker
// that dies without releasing its slot therefore blocks the tenant for at most
// this long.
const inflightSlotTTL = 60 * time.Second

func (l *Limiter) bucketKey(tenantID string) string {
	return l.opts.Prefix + "tokens:" + tenantID
}

func (l *Limiter) inflightKey(tenantID string) string {
	return l.opts.Prefix + "inflight:" + tenantID
}

// requestTokens asks Redis for up to want tokens and returns how many were
// granted.
func (l *Limiter) requestTokens(ctx context.Context, tenantID string, rate int, burst int, want int) (int, error) {
	if burst <= 0 {
		burst = int(rate)
	}
	if burst <= 0 {
		burst = 1
	}
	// The bucket expires once it had the time to refill completely, so an idle
	// tenant leaves no key behind.
	ttl := time.Duration(float64(burst)/float64(rate)*2000) * time.Millisecond
	if ttl < 2*time.Second {
		ttl = 2 * time.Second
	}
	granted, err := grantTokensScript.Run(ctx, l.redis,
		[]string{l.bucketKey(tenantID)},
		rate, burst, want, ttl.Milliseconds()).Int()
	if err != nil {
		return 0, fmt.Errorf("global: grant tokens: %w", err)
	}
	return granted, nil
}

// acquireGlobalSlot reserves one in-flight slot of the tenant.
func (l *Limiter) acquireGlobalSlot(ctx context.Context, tenantID string, limit int) (bool, error) {
	granted, err := acquireSlotScript.Run(ctx, l.redis,
		[]string{l.inflightKey(tenantID)}, limit, inflightSlotTTL.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("global: acquire in-flight slot: %w", err)
	}
	return granted == 1, nil
}

// releaseGlobalSlot gives one in-flight slot back.
func (l *Limiter) releaseGlobalSlot(ctx context.Context, tenantID string) {
	if l.redis == nil {
		return
	}
	if _, err := releaseSlotScript.Run(ctx, l.redis,
		[]string{l.inflightKey(tenantID)}).Int(); err != nil {
		l.logger.Warn("release global in-flight slot failed", "tenant_id", tenantID, "error", err)
	}
}

// LogRedisFailure reports a Redis problem that the limiter absorbed.
func (l *Limiter) LogRedisFailure(tenantID string, err error) {
	l.logger.Warn("tenant quota backend degraded", "tenant_id", tenantID, "error", err)
}
