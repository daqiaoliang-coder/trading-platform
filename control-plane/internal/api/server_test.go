package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"trading-platform/control-plane/internal/node"
	"trading-platform/control-plane/internal/store"
)

type fakeNode struct {
	mu       sync.Mutex
	snapshot node.ShardsSnapshot
	calls    int
	outcome  string
}

func (f *fakeNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/admin/health":
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	case r.Method == http.MethodGet && r.URL.Path == "/admin/shards":
		_ = json.NewEncoder(w).Encode(f.snapshot)
	case r.Method == http.MethodGet && r.URL.Path == "/admin/routes":
		_ = json.NewEncoder(w).Encode(node.RoutesSnapshot{Version: f.snapshot.RouteVersion, Slots: []uint16{0, 1}})
	case r.Method == http.MethodPost && r.URL.Path == "/admin/shards/0/migrate":
		f.mu.Lock()
		f.calls++
		outcome := f.outcome
		f.mu.Unlock()
		switch outcome {
		case "conflict":
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"target replica 4 is already live"}`))
		case "rolled-back":
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(resultEnvelope("rolled-back", "target failed", 1, []uint64{1, 2, 3}, uint64Ptr(1)))
		case "degraded":
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(resultEnvelope("degraded", "leader unknown", 2, []uint64{2, 3, 4}, nil))
		default:
			_ = json.NewEncoder(w).Encode(resultEnvelope("completed", "", 2, []uint64{2, 3, 4}, uint64Ptr(4)))
		}
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeNode) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func resultEnvelope(status, message string, epochAfter uint64, voters []uint64, leader *uint64) map[string]any {
	var errorPtr any
	if message != "" {
		errorPtr = message
	}
	return map[string]any{
		"status": status,
		"error":  errorPtr,
		"report": map[string]any{
			"shard":                    0,
			"mode":                     "relocate-leader",
			"target_node":              4,
			"retired_nodes":            []uint64{1},
			"voters_before":            []uint64{1, 2, 3},
			"voters_after":             voters,
			"leader_before":            1,
			"leader_after":             leader,
			"epoch_before":             1,
			"epoch_after":              epochAfter,
			"catchup":                  nil,
			"positions_before_promote": []any{},
			"timings":                  map[string]any{"catchup_ms": 10, "freeze_ms": 5, "switch_ms": 2, "election_ms": 100, "total_ms": 120},
			"events":                   []any{map[string]any{"elapsed_ms": 1, "kind": "completed", "detail": "done"}},
		},
	}
}

func harness(t *testing.T, outcome string) (string, *store.Store, *fakeNode, string) {
	t.Helper()
	fake := &fakeNode{
		outcome: outcome,
		snapshot: node.ShardsSnapshot{
			RouteVersion: 1,
			Shards: []node.Placement{
				{Shard: 0, Epoch: 1, Voters: []uint64{1, 2, 3}, LiveReplicas: []uint64{1, 2, 3}, Leader: uint64Ptr(1)},
				{Shard: 1, Epoch: 1, Voters: []uint64{1, 2, 3}, LiveReplicas: []uint64{1, 2, 3}, Leader: uint64Ptr(1)},
			},
		},
	}
	nodeServer := httptest.NewServer(fake)
	dir := t.TempDir()
	audit, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open audit: %v", err)
	}
	server := httptest.NewServer(New(node.New(nodeServer.URL), audit).Handler())
	t.Cleanup(func() {
		server.Close()
		nodeServer.Close()
		_ = audit.Close()
	})
	return server.URL, audit, fake, dir
}

func request(t *testing.T, method, url, body, key string) (int, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = bytes.NewBufferString(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode body %q: %v", data, err)
	}
	return resp.StatusCode, decoded
}

func migrationBody(mode string, target, decommission *uint64) string {
	parts := fmt.Sprintf(`{"mode":%q`, mode)
	if target != nil {
		parts += fmt.Sprintf(`,"target_node":%d`, *target)
	}
	if decommission != nil {
		parts += fmt.Sprintf(`,"decommission_node":%d`, *decommission)
	}
	return parts + "}"
}

func TestCreateMigrationIsAuditedAndIdempotent(t *testing.T) {
	base, audit, fake, dir := harness(t, "completed")
	body := migrationBody("relocate-leader", uint64Ptr(4), nil)
	status, response := request(t, http.MethodPost, base+"/v1/shards/0/migrations", body, "key-1")
	if status != http.StatusCreated {
		t.Fatalf("create status = %d: %+v", status, response)
	}
	record := response["migration"].(map[string]any)
	id := record["id"].(string)
	if record["status"] != "completed" {
		t.Fatalf("record status = %v", record["status"])
	}
	if record["epoch_after"] != float64(2) {
		t.Fatalf("epoch_after = %v", record["epoch_after"])
	}
	if fake.callCount() != 1 {
		t.Fatalf("node calls = %d", fake.callCount())
	}

	status, replay := request(t, http.MethodPost, base+"/v1/shards/0/migrations", body, "key-1")
	if status != http.StatusOK || replay["replayed"] != true {
		t.Fatalf("replay status=%d body=%+v", status, replay)
	}
	if replay["migration"].(map[string]any)["id"] != id {
		t.Fatalf("replay returned a different migration: %+v", replay)
	}
	if fake.callCount() != 1 {
		t.Fatalf("replay called the node again: %d", fake.callCount())
	}

	status, list := request(t, http.MethodGet, base+"/v1/migrations", "", "")
	if status != http.StatusOK {
		t.Fatalf("list status = %d", status)
	}
	if len(list["migrations"].([]any)) != 1 {
		t.Fatalf("audit list = %+v", list)
	}
	status, one := request(t, http.MethodGet, base+"/v1/migrations/"+id, "", "")
	if status != http.StatusOK || one["migration"].(map[string]any)["id"] != id {
		t.Fatalf("get status=%d body=%+v", status, one)
	}
	if records := audit.List(); len(records) != 1 || records[0].NodeReport == nil {
		t.Fatalf("store record = %+v", records)
	}
	// The audit file must exist and contain at least the begin and update lines.
	content, err := os.ReadFile(dir + "/audit.jsonl")
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	if bytes.Count(content, []byte("\n")) < 2 {
		t.Fatalf("audit log has %d lines", bytes.Count(content, []byte("\n")))
	}
}

func TestCreateMigrationRejectsLiveTargetAndUnknownShard(t *testing.T) {
	base, _, fake, _ := harness(t, "completed")
	status, response := request(t, http.MethodPost, base+"/v1/shards/0/migrations",
		migrationBody("scale-out", uint64Ptr(1), nil), "")
	if status != http.StatusConflict {
		t.Fatalf("live target status = %d: %+v", status, response)
	}
	if fake.callCount() != 0 {
		t.Fatalf("node was called for an invalid plan")
	}
	status, _ = request(t, http.MethodPost, base+"/v1/shards/5/migrations",
		migrationBody("scale-out", uint64Ptr(9), nil), "")
	if status != http.StatusNotFound {
		t.Fatalf("unknown shard status = %d", status)
	}
}

func TestCreateMigrationMapsRollbackDegradedAndNodeConflict(t *testing.T) {
	cases := []struct {
		outcome    string
		wantHTTP   int
		wantStatus string
		wantEpoch  float64
	}{
		{"rolled-back", http.StatusConflict, "rolled-back", 1},
		{"degraded", http.StatusServiceUnavailable, "degraded", 2},
		{"conflict", http.StatusConflict, "failed", 0},
	}
	for _, tc := range cases {
		t.Run(tc.outcome, func(t *testing.T) {
			base, audit, _, _ := harness(t, tc.outcome)
			status, response := request(t, http.MethodPost, base+"/v1/shards/0/migrations",
				migrationBody("relocate-leader", uint64Ptr(4), nil), "")
			if status != tc.wantHTTP {
				t.Fatalf("status = %d, want %d: %+v", status, tc.wantHTTP, response)
			}
			record := response["migration"].(map[string]any)
			if record["status"] != tc.wantStatus {
				t.Fatalf("record status = %v, want %s", record["status"], tc.wantStatus)
			}
			if tc.wantEpoch != 0 && record["epoch_after"] != tc.wantEpoch {
				t.Fatalf("epoch_after = %v, want %v", record["epoch_after"], tc.wantEpoch)
			}
			if records := audit.List(); len(records) != 1 {
				t.Fatalf("audit records = %d", len(records))
			}
		})
	}
}

func TestActiveMigrationBlocksNewPlan(t *testing.T) {
	base, audit, fake, _ := harness(t, "completed")
	if err := audit.Begin(&store.Record{ID: "active", Shard: 0, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("begin active: %v", err)
	}
	status, response := request(t, http.MethodPost, base+"/v1/shards/0/migrations",
		migrationBody("scale-out", uint64Ptr(4), nil), "")
	if status != http.StatusConflict {
		t.Fatalf("status = %d: %+v", status, response)
	}
	if fake.callCount() != 0 {
		t.Fatal("node should not be called while another migration is in flight")
	}
}

func uint64Ptr(value uint64) *uint64 {
	return &value
}
