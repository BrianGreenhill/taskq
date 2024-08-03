package queue

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Queue struct {
	redis *redis.Client
}

func New(redis *redis.Client) *Queue {
	return &Queue{redis: redis}
}

func (q *Queue) Push(ctx context.Context, task []byte) error {
	if err := q.redis.Publish(ctx, "tasks", task).Err(); err != nil {
		return err
	}

	return nil
}
