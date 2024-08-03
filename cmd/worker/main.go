package main

import (
	"context"
	"os"
	"os/signal"
	"taskq/internal/metrics"
	"taskq/internal/queue"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/log/global"
)

var logger = otelslog.NewLogger("taskq.worker")

func main() {
	if err := realMain(context.Background()); err != nil {
		logger.Error("error running worker", "exception.message", err.Error())
		os.Exit(1)
	}
}

func realMain(ctx context.Context) error {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	otelShutdown, err := metrics.SetupOtel(ctx)
	if err != nil {
		return err
	}
	defer func(ctx context.Context) {
		if err := otelShutdown(ctx); err != nil {
			panic(err)
		}
	}(ctx)

	logger := otelslog.NewLogger("taskq", otelslog.WithLoggerProvider(global.GetLoggerProvider()))

	c := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := redisotel.InstrumentTracing(c); err != nil {
		logger.Error("error instrumenting redis", "error", err)
		return err
	}
	if err := redisotel.InstrumentMetrics(c); err != nil {
		logger.Error("error instrumenting redis", "error", err)
		return err
	}
	sub := c.Subscribe(ctx, "tasks")
	store := queue.NewStore(&queue.Config{
		DBName:      "queue",
		Addr:        "localhost:3306",
		User:        "root",
		MemFallback: true,
	})

	startWorkerPool(ctx, 10, sub, store)

	<-signalChan
	logger.Info("received shut down signal", "event", os.Interrupt)
	if err := sub.Close(); err != nil {
		return err
	}
	return nil
}

func startWorkerPool(ctx context.Context, size int, sub *redis.PubSub, store queue.Store) {
	for i := 0; i < size; i++ {
		go func(workerID int) {
			for {
				logger.Info("waiting for messages", "worker.id", workerID)
				for msg := range sub.Channel() {
					logger.Info("Worker received message", "worker.id", workerID)
					processTask(ctx, msg, store)
				}
			}
		}(i)
	}
}

func processTask(ctx context.Context, msg *redis.Message, store queue.Store) {
	var t queue.Task
	if err := t.UnmarshalBinary([]byte(msg.Payload)); err != nil {
		logger.Error("error unmarshalling task", "error", err)
		return
	}
	logger.Info("received task", "task", t)
	time.Sleep(5 * time.Second)
	if err := store.UpdateStatus(ctx, t.ID.String(), queue.StatusDone); err != nil {
		logger.Error("error updating task", "error", err)
	}
	t.Status = queue.StatusDone
	logger.Info("task completed", "task", t)
}
