package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"taskq/internal/queue"
	"time"

	"github.com/google/uuid"
)

type Server struct {
	Handler http.Handler
	qs      queue.Store
	q       *queue.Queue
}

func New(qs queue.Store, q *queue.Queue) *Server {
	s := &Server{qs: qs, q: q}
	mux := http.NewServeMux()
	mux.Handle("/tasks", http.HandlerFunc(s.createTaskHandler))
	mux.Handle("/tasks/{id}", http.HandlerFunc(s.getTaskHandler))
	mux.Handle("/healthz", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	s.Handler = mux

	return s
}

func (s *Server) getTaskHandler(w http.ResponseWriter, r *http.Request) {
	go func(ctx context.Context) {
		for x := range ctx.Done() {
			slog.Info("context done", "done", x)
		}
	}(r.Context())
	start := time.Now()
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	t, err := s.qs.GetTask(r.PathValue("id"))
	if err != nil {
		slog.Error("error getting task", "error", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(t.Status.String())); err != nil {
		slog.Error("error writing response", "error", err)
	}
	slog.Info("request completed", "http.method", r.Method, "duration", time.Since(start), "message", r.URL.Path, "http.status", http.StatusOK)
}

func (s *Server) createTaskHandler(w http.ResponseWriter, r *http.Request) {
	go func(ctx context.Context) {
		for x := range ctx.Done() {
			slog.Info("context done", "done", x)
		}
	}(r.Context())
	start := time.Now()
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var task queue.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		slog.Error("error parsing payload", "error", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	task.ID = uuid.New()
	if err := s.qs.CreateTask(task); err != nil {
		slog.Error("error creating task", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := w.Write([]byte(err.Error())); err != nil {
			slog.Error("error writing response", "error", err)
		}
		return
	}
	data, err := json.Marshal(task)
	if err != nil {
		slog.Error("error marshalling task", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := w.Write([]byte(err.Error())); err != nil {
			slog.Error("error writing response", "error", err)
		}
		return
	}

	if err := s.q.Push(r.Context(), data); err != nil {
		slog.Error("error pushing task", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := w.Write([]byte(err.Error())); err != nil {
			slog.Error("error writing response", "error", err)
		}
		return
	}
	w.WriteHeader(http.StatusCreated)
	if _, err := w.Write([]byte(task.ID.String())); err != nil {
		slog.Error("error writing response", "error", err)
	}
	slog.Info("request completed", "http.method", r.Method, "duration", time.Since(start), "message", r.URL.Path, "http.status", http.StatusCreated)
}
