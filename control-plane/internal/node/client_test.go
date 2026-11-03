package node

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestShardsAndRoutes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/admin/health":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/admin/shards":
			_, _ = w.Write([]byte(`{"route_version":1,"shards":[{"shard":0,"epoch":1,"voters":[1,2,3],"learners":[],"leader":1,"live_replicas":[1,2,3]}]}`))
		case "/admin/routes":
			_, _ = w.Write([]byte(`{"version":1,"slots":[0,1]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := New(server.URL)
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("health: %v", err)
	}
	shards, err := client.Shards(context.Background())
	if err != nil {
		t.Fatalf("shards: %v", err)
	}
	if shards.RouteVersion != 1 || len(shards.Shards) != 1 {
		t.Fatalf("unexpected snapshot: %+v", shards)
	}
	if shards.Shards[0].Leader == nil || *shards.Shards[0].Leader != 1 {
		t.Fatalf("leader not decoded: %+v", shards.Shards[0])
	}
	routes, err := client.Routes(context.Background())
	if err != nil {
		t.Fatalf("routes: %v", err)
	}
	if len(routes.Slots) != 2 {
		t.Fatalf("unexpected route slots: %+v", routes)
	}
}

func TestMigratePreservesRollbackReport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"status":"rolled-back","error":"target failed","report":{"shard":0,"epoch_after":1,"voters_after":[1,2,3]}}`))
	}))
	defer server.Close()

	client := New(server.URL)
	result, status, err := client.Migrate(context.Background(), 0, Plan{Mode: "scale-out"})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if status != http.StatusConflict {
		t.Fatalf("status = %d", status)
	}
	if result.Status != "rolled-back" {
		t.Fatalf("result status = %q", result.Status)
	}
	if result.Error == nil || *result.Error != "target failed" {
		t.Fatalf("result error = %v", result.Error)
	}
	if result.Report.EpochAfter != 1 || len(result.Report.VotersAfter) != 3 {
		t.Fatalf("report not decoded: %+v", result.Report)
	}
}

func TestMigrateRejectionIsAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"scale-in requires decommission_node"}`))
	}))
	defer server.Close()

	client := New(server.URL)
	_, status, err := client.Migrate(context.Background(), 0, Plan{Mode: "scale-in"})
	if err == nil {
		t.Fatal("expected an API error")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error type = %T", err)
	}
	if apiErr.Status != http.StatusBadRequest {
		t.Fatalf("api error status = %d", apiErr.Status)
	}
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d", status)
	}
}
