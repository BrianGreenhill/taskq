package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"taskq/internal/queue"
	"time"

	"github.com/go-redis/redis/v8"
)

func main() {
	if err := realMain(); err != nil {
		slog.Error("error running worker", "error", err)
		os.Exit(1)
	}
}

func realMain() error {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	sub := c.Subscribe(ctx, "tasks")
	store := queue.NewStore(&queue.Config{
		DBName:      "queue",
		Addr:        "localhost:3306",
		User:        "root",
		MemFallback: true,
	})

	startWorkerPool(10, sub, store)

	<-signalChan
	slog.Info("received shut down signal", "event", os.Interrupt)
	if err := sub.Close(); err != nil {
		return err
	}
	return nil
}

func startWorkerPool(size int, sub *redis.PubSub, store queue.Store) {
	for i := 0; i < size; i++ {
		go func(workerID int) {
			for {
				slog.Info("waiting for messages", "worker.id", workerID)
				for msg := range sub.Channel() {
					slog.Info("Worker received message", "worker.id", workerID)
					processTask(msg, store)
				}
			}
		}(i)
	}
}

func processTask(msg *redis.Message, store queue.Store) {
	var t queue.Task
	if err := t.UnmarshalBinary([]byte(msg.Payload)); err != nil {
		slog.Error("error unmarshalling task", "error", err)
		return
	}
	slog.Info("received task", "task", t)
	time.Sleep(5 * time.Second)
	if err := store.UpdateStatus(t.ID.String(), queue.StatusDone); err != nil {
		slog.Error("error updating task", "error", err)
	}
	t.Status = queue.StatusDone
	slog.Info("task completed", "task", t)
}
