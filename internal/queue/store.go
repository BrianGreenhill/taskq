package queue

import (
	"context"
	"database/sql"
	"errors"

	"github.com/XSAM/otelsql"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
)

var ErrTaskNotFound = errors.New("task not found")

type Store interface {
	CreateTask(ctx context.Context, task Task) error
	GetTask(ctx context.Context, id string) (*Task, error)
	UpdateStatus(ctx context.Context, id string, status TaskStatus) error
}

type MemoryStore struct{ tasks map[string]Task }

func (m *MemoryStore) CreateTask(_ context.Context, task Task) error {
	m.tasks[task.ID.String()] = task
	return nil
}

func (m *MemoryStore) GetTask(_ context.Context, id string) (*Task, error) {
	t := m.tasks[id]
	return &t, nil
}

func (m *MemoryStore) UpdateStatus(_ context.Context, id string, status TaskStatus) error {
	t, ok := m.tasks[id]
	if !ok {
		return ErrTaskNotFound
	}
	t.Status = status
	m.tasks[id] = t
	return nil
}

type SQLStore struct {
	db *sql.DB
}

type Config struct {
	DBName, Addr, User string

	// MemFallback is a flag to enable memory store fallback
	MemFallback bool
}

func NewStore(c *Config) Store {
	var fallback MemoryStore
	cfg := mysql.Config{
		User:   c.User,
		Net:    "tcp",
		DBName: c.DBName,
		Addr:   c.Addr,
	}
	db, err := otelsql.Open("mysql", cfg.FormatDSN(), otelsql.WithAttributes(
		semconv.DBSystemMySQL,
	))
	if err != nil {
		if c.MemFallback {
			return &fallback
		}
		panic(err)
	}

	if err = otelsql.RegisterDBStatsMetrics(db, otelsql.WithAttributes(
		semconv.DBSystemMySQL,
	)); err != nil {
		if c.MemFallback {
			return &fallback
		}
		panic(err)
	}
	return &SQLStore{db: db}
}

func (s *SQLStore) UpdateStatus(ctx context.Context, id string, status TaskStatus) error {
	_, err := s.db.ExecContext(ctx, "UPDATE tasks SET status = ? WHERE uuid = ?", status, id)
	return err
}

func (s *SQLStore) CreateTask(ctx context.Context, task Task) error {
	_, err := s.db.ExecContext(ctx, "INSERT INTO tasks (uuid) VALUES (?)", task.ID.String())
	return err
}

func (s *SQLStore) GetTask(ctx context.Context, id string) (*Task, error) {
	idVal, err := uuid.Parse(id)
	if err != nil {
		return nil, ErrTaskNotFound
	}
	var task Task
	err = s.db.QueryRowContext(ctx, "SELECT uuid, status FROM tasks WHERE uuid = ?", idVal).Scan(&task.ID, &task.Status)
	if err != nil {
		return nil, ErrTaskNotFound
	}
	return &task, nil
}
