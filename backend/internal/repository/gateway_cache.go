package repository

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const stickySessionPrefix = "sticky_session:"

type gatewayCache struct {
	rdb *redis.Client
}

func NewGatewayCache(rdb *redis.Client) service.GatewayCache {
	return &gatewayCache{rdb: rdb}
}

// buildSessionKey 构建 session key，包含 groupID 实现分组隔离
// 格式: sticky_session:{groupID}:{sessionHash}
func buildSessionKey(groupID int64, sessionHash string) string {
	return fmt.Sprintf("%s%d:%s", stickySessionPrefix, groupID, sessionHash)
}

// GetSessionAccountID 获取粘性会话绑定的账号 ID。
// 支持两种存储格式：旧格式 "accountID"，新格式 "accountID:priority"。
func (c *gatewayCache) GetSessionAccountID(ctx context.Context, groupID int64, sessionHash string) (int64, error) {
	key := buildSessionKey(groupID, sessionHash)
	val, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	// 兼容新格式 "accountID:priority"
	if idx := strings.Index(val, ":"); idx >= 0 {
		return strconv.ParseInt(val[:idx], 10, 64)
	}
	return strconv.ParseInt(val, 10, 64)
}

func (c *gatewayCache) SetSessionAccountID(ctx context.Context, groupID int64, sessionHash string, accountID int64, ttl time.Duration) error {
	key := buildSessionKey(groupID, sessionHash)
	return c.rdb.Set(ctx, key, accountID, ttl).Err()
}

// setSessionIfBetterScript 原子地将 sticky 账号更新为优先级更低（更好）的账号。
// 若 Redis 中已存储优先级 <= 新值，则放弃写入，避免并发下更差账号覆盖更好账号。
// 存储格式: "accountID:priority"
// 旧格式（无 ':'）视为最高优先级数值（maxint），始终允许覆盖。
var setSessionIfBetterScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
local newPriority = tonumber(ARGV[2])
local newVal = ARGV[1] .. ':' .. ARGV[2]
local ttl = tonumber(ARGV[3])

if current == false then
    redis.call('SET', KEYS[1], newVal, 'EX', ttl)
    return 1
end

local sep = string.find(current, ':')
local curPriority
if sep then
    curPriority = tonumber(string.sub(current, sep + 1))
else
    -- 旧格式（无优先级后缀）：无法比较，拒绝覆写，等 TTL 自然过期。
    -- 避免新请求用已知优先级账号覆盖旧格式中可能更优的绑定。
    return 0
end

if newPriority < curPriority then
    redis.call('SET', KEYS[1], newVal, 'EX', ttl)
    return 1
end
return 0
`)

// SetSessionAccountIDIfBetter 仅当新账号优先级低于（更好于）当前绑定账号时才更新 sticky。
// 返回 true 表示 Redis 实际完成了写入，false 表示当前已有更优 sticky 而拒绝写入。
// 用于抢占迁移场景，防止并发请求用更差账号覆盖更好账号。
func (c *gatewayCache) SetSessionAccountIDIfBetter(ctx context.Context, groupID int64, sessionHash string, accountID int64, priority int, ttl time.Duration) (bool, error) {
	key := buildSessionKey(groupID, sessionHash)
	ttlSeconds := int64(ttl.Seconds())
	result, err := setSessionIfBetterScript.Run(ctx, c.rdb, []string{key},
		strconv.FormatInt(accountID, 10),
		strconv.Itoa(priority),
		strconv.FormatInt(ttlSeconds, 10),
	).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (c *gatewayCache) RefreshSessionTTL(ctx context.Context, groupID int64, sessionHash string, ttl time.Duration) error {
	key := buildSessionKey(groupID, sessionHash)
	return c.rdb.Expire(ctx, key, ttl).Err()
}

// DeleteSessionAccountID 删除粘性会话与账号的绑定关系。
// 当检测到绑定的账号不可用（如状态错误、禁用、不可调度等）时调用，
// 以便下次请求能够重新选择可用账号。
//
// DeleteSessionAccountID removes the sticky session binding for the given session.
// Called when the bound account becomes unavailable (e.g., error status, disabled,
// or unschedulable), allowing subsequent requests to select a new available account.
func (c *gatewayCache) DeleteSessionAccountID(ctx context.Context, groupID int64, sessionHash string) error {
	key := buildSessionKey(groupID, sessionHash)
	return c.rdb.Del(ctx, key).Err()
}
