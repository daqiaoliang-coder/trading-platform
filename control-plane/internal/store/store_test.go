package store

import (
	"testing"

	"trading-platform/control-plane/internal/node"
)

func TestAuditLogReloadsAndRecoversInFlight(t *testing.T) {
	dir := t.TempDir()
	first, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	completed := Record{
		ID:             "mig-1",
		Shard:          0,
		IdempotencyKey: "key-1",
		EpochBefore:    1,
	}
	if err := first.Begin(&completed); err != nil {
		t.Fatalf("begin: %v", err)
	}
	completed.Status = StatusCompleted
	completed.EpochAfter = 2
	completed.LeaderAfter = uint64Ptr(4)
	completed.Timings = node.Timings{FreezeMS: 7, TotalMS: 120}
	if err := first.Update(&completed); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := first.Begin(&Record{ID: "mig-2", Shard: 1}); err != nil {
		t.Fatalf("begin in-flight: %v", err)
	}
	if !first.ActiveForShard(1) {
		t.Fatal("in-flight migration not visible")
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	second, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()
	records := second.List()
	if len(records) != 2 {
		t.Fatalf("records = %d", len(records))
	}
	if records[0].Status != StatusCompleted || records[0].EpochAfter != 2 {
		t.Fatalf("completed record lost: %+v", records[0])
	}
	if records[1].Status != StatusUnknown {
		t.Fatalf("in-flight record should recover as unknown: %+v", records[1])
	}
	if _, ok := second.Get("mig-1"); !ok {
		t.Fatal("get by id failed")
	}
	if record, ok := second.ByIdempotencyKey("key-1"); !ok || record.ID != "mig-1" {
		t.Fatalf("idempotency index lost: %+v, %v", record, ok)
	}
	if second.ActiveForShard(1) {
		t.Fatal("unknown record must not block a retry")
	}
}

func uint64Ptr(value uint64) *uint64 {
	return &value
}
