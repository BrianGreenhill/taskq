package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"taskq/internal/metrics"
	"taskq/internal/queue"
	"taskq/internal/server"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/log/global"
)

func main() {
	ctx := context.Background()
	if err := realMain(ctx); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func realMain(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	otelShutdown, err := metrics.SetupOtel(ctx)
	if err != nil {
		return err
	}

	logger := otelslog.NewLogger("taskq", otelslog.WithLoggerProvider(global.GetLoggerProvider()))

	defer func(ctx context.Context) {
		if err := otelShutdown(ctx); err != nil {
			logger.Error("error shutting down otel", "error", err)
			return
		}
	}(ctx)

	cfg := queue.Config{
		DBName:      "queue",
		Addr:        "localhost:3306",
		User:        "root",
		MemFallback: true,
	}
	qs := queue.NewStore(&cfg)
	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := redisotel.InstrumentTracing(redisClient); err != nil {
		logger.Error("error instrumenting redis", "error", err)
		return err
	}
	if err := redisotel.InstrumentMetrics(redisClient); err != nil {
		logger.Error("error instrumenting redis", "error", err)
		return err
	}
	q := queue.New(redisClient)

	s := server.New(qs, q)
	srv := &http.Server{
		Addr:              ":8080",
		BaseContext:       func(_ net.Listener) context.Context { return ctx },
		ReadTimeout:       time.Second,
		WriteTimeout:      time.Second * 10,
		Handler:           s.Handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	srvErr := make(chan error, 1)
	go func() {
		logger.Info("server listening", "address", srv.Addr)
		srvErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-srvErr:
		logger.Error("server error", "error", err)
		return err
	case <-ctx.Done():
		logger.Info("shutting down server")
		stop()
	}

	return srv.Shutdown(ctx)
}
