// Package store persists the control plane's migration audit trail.
package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"trading-platform/control-plane/internal/node"
)

// Statuses used by the control plane. "in-flight" is written before the node
// call so a crash leaves a visible trace; on reload it becomes "unknown" and
// requires operator reconciliation against current placements.
const (
	StatusInFlight   = "in-flight"
	StatusCompleted  = "completed"
	StatusRolledBack = "rolled-back"
	StatusDegraded   = "degraded"
	StatusFailed     = "failed"
	StatusUnknown    = "unknown"
)

// Record is one migration audit entry. Summary fields duplicate the node
// report so list queries do not need to unpack it.
type Record struct {
	ID             string       `json:"id"`
	Shard          uint16       `json:"shard"`
	IdempotencyKey string       `json:"idempotency_key,omitempty"`
	Status         string       `json:"status"`
	Plan           node.Plan    `json:"plan"`
	CreatedAt      time.Time    `json:"created_at"`
	UpdatedAt      time.Time    `json:"updated_at"`
	EpochBefore    uint64       `json:"epoch_before"`
	EpochAfter     uint64       `json:"epoch_after"`
	LeaderBefore   *uint64      `json:"leader_before"`
	LeaderAfter    *uint64      `json:"leader_after"`
	VotersBefore   []uint64     `json:"voters_before"`
	VotersAfter    []uint64     `json:"voters_after"`
	RetiredNodes   []uint64     `json:"retired_nodes"`
	Timings        node.Timings `json:"timings"`
	Events         []node.Event `json:"events"`
	Error          string       `json:"error,omitempty"`
	NodeHTTPStatus int          `json:"node_http_status,omitempty"`
	NodeReport     *node.Report `json:"node_report,omitempty"`
}

// Store is an append-only JSONL audit log with an in-memory index. Every
// update appends the full record; the last line for an id wins on reload.
type Store struct {
	mu      sync.Mutex
	path    string
	file    *os.File
	records []*Record
	byID    map[string]*Record
	byKey   map[string]*Record
}

// Open loads an existing audit log or creates one in dir.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	path := filepath.Join(dir, "audit.jsonl")
	s := &Store{
		path:  path,
		byID:  make(map[string]*Record),
		byKey: make(map[string]*Record),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	// A migration that was in flight when the process died has no trustworthy
	// outcome. Keep it visible as unknown; an operator reconciles it against
	// the node's current placements before retrying.
	for _, record := range s.records {
		if record.Status == StatusInFlight {
			record.Status = StatusUnknown
			record.UpdatedAt = time.Now().UTC()
		}
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open audit log: %w", err)
	}
	s.file = file
	return s, nil
}

func (s *Store) load() error {
	file, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open audit log: %w", err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			var record Record
			if decodeErr := json.Unmarshal(line, &record); decodeErr != nil {
				return fmt.Errorf("corrupt audit log line: %w", decodeErr)
			}
			s.apply(&record)
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (s *Store) apply(record *Record) {
	existing, ok := s.byID[record.ID]
	if ok {
		// Replace the in-memory entry but keep its position in the list.
		*existing = *record
		if record.IdempotencyKey != "" {
			s.byKey[record.IdempotencyKey] = existing
		}
		return
	}
	cloned := *record
	s.records = append(s.records, &cloned)
	s.byID[cloned.ID] = &cloned
	if cloned.IdempotencyKey != "" {
		s.byKey[cloned.IdempotencyKey] = &cloned
	}
}

// Begin persists a new in-flight record before the node is called.
func (s *Store) Begin(record *Record) error {
	record.CreatedAt = record.CreatedAt.UTC()
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	record.UpdatedAt = record.CreatedAt
	record.Status = StatusInFlight
	return s.append(record)
}

// Update persists a changed record.
func (s *Store) Update(record *Record) error {
	record.UpdatedAt = time.Now().UTC()
	return s.append(record)
}

func (s *Store) append(record *Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err := s.file.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("append audit log: %w", err)
	}
	// Audit records are rare; fsync them so a control-plane crash cannot lose
	// the trace of an orchestrator action.
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync audit log: %w", err)
	}
	s.apply(record)
	return nil
}

// Get returns a copy of one record.
func (s *Store) Get(id string) (Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.byID[id]
	if !ok {
		return Record{}, false
	}
	return *record, true
}

// ByIdempotencyKey returns an existing record for a replayed request.
func (s *Store) ByIdempotencyKey(key string) (Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.byKey[key]
	if !ok {
		return Record{}, false
	}
	return *record, true
}

// ActiveForShard reports whether a migration is currently in flight.
func (s *Store) ActiveForShard(shard uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, record := range s.records {
		if record.Shard == shard && record.Status == StatusInFlight {
			return true
		}
	}
	return false
}

// List returns all records in creation order.
func (s *Store) List() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, 0, len(s.records))
	for _, record := range s.records {
		out = append(out, *record)
	}
	return out
}

// Close releases the audit file.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}
