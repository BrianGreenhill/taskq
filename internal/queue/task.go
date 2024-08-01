package queue

import (
	"encoding/json"
	"errors"

	"github.com/google/uuid"
)

var ErrInvalidTaskData = errors.New("invalid task data")

type TaskStatus int

const (
	StatusPending TaskStatus = iota
	StatusProcessing
	StatusDone
	StatusFailed
)

func (s TaskStatus) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusProcessing:
		return "processing"
	case StatusDone:
		return "done"
	case StatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Task represents a task in the queue
// implements encoding.BinaryMarshaler
// and encoding.BinaryUnmarshaler
type Task struct {
	ID      uuid.UUID  `json:"uuid"`
	Payload string     `json:"payload"`
	Status  TaskStatus `json:"status"`
}

func (t Task) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

func (t *Task) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}
