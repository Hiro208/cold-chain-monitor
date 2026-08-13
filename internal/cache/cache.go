// Package cache keeps the latest state of every container in Redis so that
// "what is container X doing right now" never touches Postgres.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
)

// ErrMiss is returned when no state is cached for a container.
var ErrMiss = errors.New("cache miss")

// State is the cached view of a container.
type State struct {
	Latest    domain.Reading     `json:"latest"`
	Breach    domain.BreachState `json:"breach"`
	UpdatedAt time.Time          `json:"updated_at"`
}

// Entry is one container's update produced by the ingest worker.
type Entry struct {
	Reading domain.Reading
	Breach  domain.BreachState
}

// Each container is a hash at reefer:{<id>}:state with fields:
//
//	ts      recorded_at of the cached reading, unix microseconds
//	reading JSON-encoded domain.Reading
//	breach  JSON-encoded domain.BreachState
//	upd     wall-clock time of the write, unix microseconds
//
// The braces form a Redis Cluster hash tag so the key routes by container ID.
func stateKey(id string) string { return "reefer:{" + id + "}:state" }

// upsertScript writes the entry only if it is newer than what is cached. A
// redelivered or out-of-order Kafka message must never roll the "latest"
// state backwards. Doing the compare-and-set in Lua keeps it atomic without
// WATCH/MULTI round trips.
var upsertScript = redis.NewScript(`
local cur = redis.call('HGET', KEYS[1], 'ts')
if cur and tonumber(cur) >= tonumber(ARGV[1]) then
  return 0
end
redis.call('HSET', KEYS[1], 'ts', ARGV[1], 'reading', ARGV[2], 'breach', ARGV[3], 'upd', ARGV[4])
if tonumber(ARGV[5]) > 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[5])
end
return 1
`)

// Cache is a thin typed wrapper over a Redis client.
type Cache struct {
	rdb redis.UniversalClient
	ttl time.Duration
	now func() time.Time
}

// New returns a Cache. ttl expires state for containers that stop reporting
// (0 disables expiry).
func New(rdb redis.UniversalClient, ttl time.Duration) *Cache {
	return &Cache{rdb: rdb, ttl: ttl, now: time.Now}
}

// Ping checks connectivity.
func (c *Cache) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }

// Upsert writes a batch of entries in one pipelined round trip and returns
// how many were applied (stale ones are skipped).
func (c *Cache) Upsert(ctx context.Context, entries []Entry) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	applied, err := c.upsert(ctx, entries)
	if err != nil && isNoScript(err) {
		// Redis was restarted or failed over and lost its script cache.
		if err := upsertScript.Load(ctx, c.rdb).Err(); err != nil {
			return 0, fmt.Errorf("load script: %w", err)
		}
		applied, err = c.upsert(ctx, entries)
	}
	return applied, err
}

func (c *Cache) upsert(ctx context.Context, entries []Entry) (int, error) {
	now := c.now().UnixMicro()
	ttlMs := c.ttl.Milliseconds()
	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.Cmd, 0, len(entries))
	for _, e := range entries {
		reading, err := json.Marshal(e.Reading)
		if err != nil {
			return 0, err
		}
		breach, err := json.Marshal(e.Breach)
		if err != nil {
			return 0, err
		}
		cmds = append(cmds, upsertScript.EvalSha(ctx, pipe,
			[]string{stateKey(e.Reading.ContainerID)},
			e.Reading.RecordedAt.UnixMicro(), reading, breach, now, ttlMs))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return 0, err
	}
	applied := 0
	for _, cmd := range cmds {
		n, err := cmd.Int()
		if err != nil {
			return 0, err
		}
		applied += n
	}
	return applied, nil
}

func isNoScript(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT")
}

// Latest returns the cached state of a container.
func (c *Cache) Latest(ctx context.Context, id string) (State, error) {
	vals, err := c.rdb.HMGet(ctx, stateKey(id), "reading", "breach", "upd").Result()
	if err != nil {
		return State{}, err
	}
	if vals[0] == nil {
		return State{}, ErrMiss
	}
	var st State
	if err := json.Unmarshal([]byte(vals[0].(string)), &st.Latest); err != nil {
		return State{}, fmt.Errorf("decode reading: %w", err)
	}
	if s, ok := vals[1].(string); ok {
		if err := json.Unmarshal([]byte(s), &st.Breach); err != nil {
			return State{}, fmt.Errorf("decode breach: %w", err)
		}
	}
	if s, ok := vals[2].(string); ok {
		var us int64
		if _, err := fmt.Sscan(s, &us); err == nil {
			st.UpdatedAt = time.UnixMicro(us).UTC()
		}
	}
	return st, nil
}

// BreachStates loads the detector state of the given containers. The ingest
// worker calls it when it is assigned Kafka partitions, so that a breach that
// was open before a rebalance stays open afterwards.
func (c *Cache) BreachStates(ctx context.Context, ids []string) (map[string]domain.BreachState, error) {
	out := make(map[string]domain.BreachState, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGet(ctx, stateKey(id), "breach")
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	for i, cmd := range cmds {
		s, err := cmd.Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var bs domain.BreachState
		if err := json.Unmarshal([]byte(s), &bs); err != nil {
			return nil, fmt.Errorf("decode breach state for %s: %w", ids[i], err)
		}
		out[ids[i]] = bs
	}
	return out, nil
}
