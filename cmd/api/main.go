package main

import (
	"log/slog"
	"net/http"
	"os"
	"taskq/internal/queue"
	"taskq/internal/server"
	"time"

	"github.com/go-redis/redis/v8"
)

func main() {
	if err := realMain(); err != nil {
		slog.Error("error running API", "error", err)
		os.Exit(1)
	}
}

func realMain() error {
	cfg := queue.Config{
		DBName:      "queue",
		Addr:        "localhost:3306",
		User:        "root",
		MemFallback: true,
	}
	qs := queue.NewStore(&cfg)
	q := queue.New(redis.NewClient(&redis.Options{Addr: "localhost:6379"}))

	s := server.New(qs, q)
	srv := &http.Server{
		Addr:              ":8080",
		Handler:           s.Handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	slog.Info("Server started on port 8080")
	return srv.ListenAndServe()
}
