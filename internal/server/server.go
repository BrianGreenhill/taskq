package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"taskq/internal/queue"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/log/global"
)

type Server struct {
	Handler http.Handler
	qs      queue.Store
	q       *queue.Queue
	logger  *slog.Logger
}

func New(qs queue.Store, q *queue.Queue) *Server {
	logger := otelslog.NewLogger("taskq", otelslog.WithLoggerProvider(global.GetLoggerProvider()))
	s := &Server{
		qs:     qs,
		q:      q,
		logger: logger,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", s.createTaskHandler)
	mux.HandleFunc("GET /tasks/{id}", s.getTaskHandler)
	mux.Handle("GET /healthz", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	s.Handler = otelhttp.NewHandler(mux, "/")

	return s
}

func (s *Server) getTaskHandler(w http.ResponseWriter, r *http.Request) {
	t, err := s.qs.GetTask(r.Context(), r.PathValue("id"))
	if err != nil {
		s.logger.Error("error getting task", "error", err)
		return
	}

	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(t.Status.String())); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Server) createTaskHandler(w http.ResponseWriter, r *http.Request) {
	var task queue.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		s.logger.Error("error parsing payload", "error", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	task.ID = uuid.New()
	if err := s.qs.CreateTask(r.Context(), task); err != nil {
		s.logger.Error("error creating task", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := w.Write([]byte(err.Error())); err != nil {
			s.logger.Error("error writing response", "error", err)
		}
		return
	}
	data, err := json.Marshal(task)
	if err != nil {
		s.logger.Error("error marshalling task", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := w.Write([]byte(err.Error())); err != nil {
			s.logger.Error("error writing response", "error", err)
		}
		return
	}

	if err := s.q.Push(r.Context(), data); err != nil {
		s.logger.Error("error pushing task", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := w.Write([]byte(err.Error())); err != nil {
			s.logger.Error("error writing response", "error", err)
		}
		return
	}
	w.WriteHeader(http.StatusCreated)
	if _, err := w.Write([]byte(task.ID.String())); err != nil {
		s.logger.Error("error writing response", "error", err)
	}
}
