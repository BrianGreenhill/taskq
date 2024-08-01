package queue

import (
	"database/sql"
	"errors"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

var ErrTaskNotFound = errors.New("task not found")

type Store interface {
	CreateTask(task Task) error
	GetTask(id string) (*Task, error)
	UpdateStatus(id string, status TaskStatus) error
}

type MemoryStore struct{ tasks map[string]Task }

func (m *MemoryStore) CreateTask(task Task) error {
	m.tasks[task.ID.String()] = task
	return nil
}

func (m *MemoryStore) GetTask(id string) (*Task, error) {
	t := m.tasks[id]
	return &t, nil
}

func (m *MemoryStore) UpdateStatus(id string, status TaskStatus) error {
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
	cfg := mysql.Config{
		User:   c.User,
		Net:    "tcp",
		DBName: c.DBName,
		Addr:   c.Addr,
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		if c.MemFallback {
			return &MemoryStore{tasks: make(map[string]Task)}
		}
		panic(err)
	}
	return &SQLStore{db: db}
}

func (s *SQLStore) UpdateStatus(id string, status TaskStatus) error {
	_, err := s.db.Exec("UPDATE tasks SET status = ? WHERE uuid = ?", status, id)
	return err
}

func (s *SQLStore) CreateTask(task Task) error {
	_, err := s.db.Exec("INSERT INTO tasks (uuid) VALUES (?)", task.ID.String())
	return err
}

func (s *SQLStore) GetTask(id string) (*Task, error) {
	idVal, err := uuid.Parse(id)
	if err != nil {
		return nil, ErrTaskNotFound
	}
	var task Task
	err = s.db.QueryRow("SELECT uuid, status FROM tasks WHERE uuid = ?", idVal).Scan(&task.ID, &task.Status)
	if err != nil {
		return nil, ErrTaskNotFound
	}
	return &task, nil
}
