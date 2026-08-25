package whatsapp

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ikermy/air-logger/v2/pkg/logger"
	"github.com/redis/go-redis/v9"
)

const (
	knownResponderTTL       = 168 * time.Hour
	knownResponderServiceID = 3 // WhatsAppUserBot
)

type CacheMethods interface {
	Has(ctx context.Context, userID uint32, senderID int64) (bool, error)
	Set(ctx context.Context, userID uint32, senderID int64) error
	LoadUser(ctx context.Context, userID uint32) ([]int64, error)
}

type cache struct {
	client redis.UniversalClient
}

func newRedisKnownResponderCache(client redis.UniversalClient) CacheMethods {
	if client == nil {
		return nil
	}
	return &cache{client: client}
}

func (c *cache) Has(ctx context.Context, userID uint32, senderID int64) (bool, error) {
	result, err := c.client.Exists(ctx, knownResponderKey(userID, senderID)).Result()
	if err != nil {
		return false, err
	}
	return result > 0, nil
}

func (c *cache) Set(ctx context.Context, userID uint32, senderID int64) error {
	return c.client.Set(ctx, knownResponderKey(userID, senderID), "1", knownResponderTTL).Err()
}

func (c *cache) LoadUser(ctx context.Context, userID uint32) ([]int64, error) {
	pattern := fmt.Sprintf("%s*", knownResponderUserPrefix(userID))
	iter := c.client.Scan(ctx, 0, pattern, 0).Iterator()

	ids := make([]int64, 0)
	for iter.Next(ctx) {
		senderID, ok := parseKnownResponderKey(userID, iter.Val())
		if !ok {
			continue
		}
		ids = append(ids, senderID)
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return ids, nil
}

func knownResponderKey(userID uint32, senderID int64) string {
	return fmt.Sprintf("known_responder:%d:%d:%d", knownResponderServiceID, userID, senderID)
}

func knownResponderUserPrefix(userID uint32) string {
	return fmt.Sprintf("known_responder:%d:%d:", knownResponderServiceID, userID)
}

func parseKnownResponderKey(userID uint32, key string) (int64, bool) {
	prefix := knownResponderUserPrefix(userID)
	if !strings.HasPrefix(key, prefix) {
		return 0, false
	}

	senderPart := strings.TrimPrefix(key, prefix)
	senderID, err := strconv.ParseInt(senderPart, 10, 64)
	if err != nil {
		logger.Warn("Redis: пропускаю некорректный ключ knownResponder %s: %v", key, err)
		return 0, false
	}

	return senderID, true
}
