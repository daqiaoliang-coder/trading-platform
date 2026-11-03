// Package api exposes the control plane's migration orchestration API and
// enforces operator-facing policy before a plan reaches a node.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"trading-platform/control-plane/internal/node"
	"trading-platform/control-plane/internal/store"
)

const (
	maxRequestBytes  = 1 << 20
	readNodeTimeout  = 10 * time.Second
	migrationTimeout = 2 * time.Minute
)

// Server wires the node client and the audit store into HTTP handlers.
type Server struct {
	client *node.Client
	store  *store.Store
}

func New(client *node.Client, audit *store.Store) *Server {
	return &Server{client: client, store: audit}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /v1/node", s.nodeStatus)
	mux.HandleFunc("GET /v1/shards", s.shards)
	mux.HandleFunc("GET /v1/routes", s.routes)
	mux.HandleFunc("POST /v1/shards/{shard}/migrations", s.createMigration)
	mux.HandleFunc("GET /v1/migrations", s.listMigrations)
	mux.HandleFunc("GET /v1/migrations/{id}", s.getMigration)
	return mux
}

type statusEnvelope struct {
	Status string `json:"status"`
	Node   string `json:"node"`
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := s.client.Health(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, statusEnvelope{Status: "degraded", Node: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, statusEnvelope{Status: "ok", Node: "ok"})
}

func (s *Server) nodeStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readNodeTimeout)
	defer cancel()
	healthErr := s.client.Health(ctx)
	shards, shardsErr := s.client.Shards(ctx)
	routes, routesErr := s.client.Routes(ctx)
	if shardsErr != nil || routesErr != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error":  "node unavailable",
			"shards": shardsErrString(shardsErr),
			"routes": shardsErrString(routesErr),
		})
		return
	}
	result := map[string]any{"shards": shards, "routes": routes}
	if healthErr != nil {
		result["health"] = healthErr.Error()
	} else {
		result["health"] = "ok"
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) shards(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readNodeTimeout)
	defer cancel()
	shards, err := s.client.Shards(ctx)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, shards)
}

func (s *Server) routes(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readNodeTimeout)
	defer cancel()
	routes, err := s.client.Routes(ctx)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, routes)
}

type createResponse struct {
	Migration store.Record `json:"migration"`
	Replayed  bool         `json:"replayed,omitempty"`
}

func (s *Server) createMigration(w http.ResponseWriter, r *http.Request) {
	shard, err := parseShard(r.PathValue("shard"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var plan node.Plan
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid plan: " + err.Error()})
		return
	}
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(key) > 128 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "idempotency key too long"})
		return
	}
	if key != "" {
		if existing, ok := s.store.ByIdempotencyKey(key); ok {
			writeJSON(w, http.StatusOK, createResponse{Migration: existing, Replayed: true})
			return
		}
	}
	if s.store.ActiveForShard(shard) {
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "a migration is already in flight for this shard",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), readNodeTimeout)
	snapshot, err := s.client.Shards(ctx)
	cancel()
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	placement, ok := findPlacement(snapshot, shard)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown shard"})
		return
	}
	if status, message := validatePlan(plan, placement); status != 0 {
		writeJSON(w, status, map[string]string{"error": message})
		return
	}

	now := time.Now().UTC()
	record := store.Record{
		ID:             newID(now),
		Shard:          shard,
		IdempotencyKey: key,
		Plan:           plan,
		CreatedAt:      now,
		EpochBefore:    snapshot.RouteVersion,
		LeaderBefore:   placement.Leader,
		VotersBefore:   append([]uint64(nil), placement.Voters...),
	}
	if err := s.store.Begin(&record); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Keep running the node call even if the client disconnects: the audit
	// record must reflect what actually happened.
	migrateCtx, migrateCancel := context.WithTimeout(context.Background(), migrationTimeout)
	defer migrateCancel()
	result, nodeStatus, err := s.client.Migrate(migrateCtx, shard, plan)
	if err != nil {
		record.Status = store.StatusFailed
		record.Error = err.Error()
		record.NodeHTTPStatus = nodeStatus
		if updateErr := s.store.Update(&record); updateErr != nil {
			log.Printf("audit update failed for %s: %v", record.ID, updateErr)
		}
		writeJSON(w, nodeStatusOrBadGateway(nodeStatus), createResponse{Migration: record})
		return
	}

	record.NodeHTTPStatus = nodeStatus
	record.NodeReport = &result.Report
	record.EpochAfter = result.Report.EpochAfter
	record.LeaderAfter = result.Report.LeaderAfter
	record.VotersAfter = append([]uint64(nil), result.Report.VotersAfter...)
	record.RetiredNodes = append([]uint64(nil), result.Report.RetiredNodes...)
	record.Timings = result.Report.Timings
	record.Events = append([]node.Event(nil), result.Report.Events...)
	if result.Error != nil {
		record.Error = *result.Error
	}
	switch result.Status {
	case store.StatusCompleted:
		record.Status = store.StatusCompleted
	case store.StatusRolledBack:
		record.Status = store.StatusRolledBack
	case store.StatusDegraded:
		record.Status = store.StatusDegraded
	default:
		// A 409 without a migration status is the node rejecting the plan
		// (for example the target is already live).
		record.Status = store.StatusFailed
		if record.Error == "" {
			record.Error = "node rejected the plan"
		}
	}
	if err := s.store.Update(&record); err != nil {
		log.Printf("audit update failed for %s: %v", record.ID, err)
	}
	status := http.StatusCreated
	switch record.Status {
	case store.StatusRolledBack:
		status = http.StatusConflict
	case store.StatusDegraded:
		status = http.StatusServiceUnavailable
	case store.StatusFailed:
		status = nodeStatusOrBadGateway(nodeStatus)
	}
	if status == http.StatusCreated {
		w.Header().Set("Location", "/v1/migrations/"+record.ID)
	}
	writeJSON(w, status, createResponse{Migration: record})
}

func (s *Server) listMigrations(w http.ResponseWriter, r *http.Request) {
	records := s.store.List()
	sort.SliceStable(records, func(i, j int) bool {
		return records[i].CreatedAt.After(records[j].CreatedAt)
	})
	writeJSON(w, http.StatusOK, map[string]any{"migrations": records})
}

func (s *Server) getMigration(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	record, ok := s.store.Get(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown migration"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"migration": record})
}

// validatePlan applies control-plane policy before the node is touched. The
// node repeats essential checks; this layer catches operator mistakes and
// rejects conflicting plans without creating an audit record.
func validatePlan(plan node.Plan, placement node.Placement) (int, string) {
	voters := toSet(placement.Voters)
	live := toSet(placement.LiveReplicas)
	if placement.Leader != nil {
		live[*placement.Leader] = struct{}{}
	}
	target := plan.TargetNode
	decommission := plan.DecommissionNode
	switch plan.Mode {
	case "scale-out", "relocate-leader":
		if target == nil {
			return http.StatusBadRequest, plan.Mode + " requires target_node"
		}
		if decommission != nil {
			return http.StatusBadRequest, plan.Mode + " does not take decommission_node"
		}
	case "scale-in":
		if target != nil {
			return http.StatusBadRequest, "scale-in does not take target_node"
		}
		if decommission == nil {
			return http.StatusBadRequest, "scale-in requires decommission_node"
		}
	case "replace":
		if target == nil || decommission == nil {
			return http.StatusBadRequest, "replace requires target_node and decommission_node"
		}
		if *target == *decommission {
			return http.StatusBadRequest, "target_node and decommission_node must differ"
		}
	default:
		return http.StatusBadRequest, fmt.Sprintf("unknown mode %q", plan.Mode)
	}
	if target != nil {
		if _, exists := live[*target]; exists {
			return http.StatusConflict, fmt.Sprintf("replica %d is already live on shard %d", *target, placement.Shard)
		}
	}
	if decommission != nil {
		if _, exists := voters[*decommission]; !exists {
			return http.StatusConflict, fmt.Sprintf("replica %d is not a voter on shard %d", *decommission, placement.Shard)
		}
		if len(voters)-1 < 2 {
			return http.StatusConflict, "migration would leave fewer than two voters"
		}
	}
	return 0, ""
}

func findPlacement(snapshot node.ShardsSnapshot, shard uint16) (node.Placement, bool) {
	for _, placement := range snapshot.Shards {
		if placement.Shard == shard {
			return placement, true
		}
	}
	return node.Placement{}, false
}

func toSet(values []uint64) map[uint64]struct{} {
	set := make(map[uint64]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func parseShard(value string) (uint16, error) {
	shard, err := strconv.ParseUint(value, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("invalid shard id %q", value)
	}
	return uint16(shard), nil
}

func newID(now time.Time) string {
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		suffix = []byte{0, 0, 0, byte(now.UnixNano())}
	}
	return "mig-" + now.Format("20060102T150405.000") + "-" + hex.EncodeToString(suffix)
}

func nodeStatusOrBadGateway(status int) int {
	switch status {
	case http.StatusBadRequest, http.StatusNotFound, http.StatusConflict:
		return status
	default:
		return http.StatusBadGateway
	}
}

func shardsErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		http.Error(w, `{"error":"encode response"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
