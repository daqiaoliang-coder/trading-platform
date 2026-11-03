// Package node is the client side of the Rust node admin API.
package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const maxResponseBytes = 4 << 20

// Plan mirrors the node's migration request contract. Only the fields valid
// for the selected mode are set by the control plane.
type Plan struct {
	Mode               string  `json:"mode"`
	TargetNode         *uint64 `json:"target_node,omitempty"`
	DecommissionNode   *uint64 `json:"decommission_node,omitempty"`
	PreferTargetLeader *bool   `json:"prefer_target_leader,omitempty"`
	CatchupTimeoutMS   *uint64 `json:"catchup_timeout_ms,omitempty"`
	SwitchTimeoutMS    *uint64 `json:"switch_timeout_ms,omitempty"`
	CatchupLag         *uint64 `json:"catchup_lag,omitempty"`
	// Fault is a drill-only injection understood by the node
	// ("crash-target-before-learner").
	Fault string `json:"fault,omitempty"`
}

// Placement is the physical location of one logical shard.
type Placement struct {
	Shard        uint16   `json:"shard"`
	Epoch        uint64   `json:"epoch"`
	Voters       []uint64 `json:"voters"`
	Learners     []uint64 `json:"learners"`
	Leader       *uint64  `json:"leader"`
	LiveReplicas []uint64 `json:"live_replicas"`
}

// ShardsSnapshot is a consistent-enough view of every shard for planning.
type ShardsSnapshot struct {
	RouteVersion uint64      `json:"route_version"`
	Shards       []Placement `json:"shards"`
}

// RoutesSnapshot exposes the logical mapping and its epoch.
type RoutesSnapshot struct {
	Version uint64   `json:"version"`
	Slots   []uint16 `json:"slots"`
}

// CatchUp is the position check performed before and after promotion.
type CatchUp struct {
	Target        uint64  `json:"target"`
	Leader        *uint64 `json:"leader"`
	LeaderApplied *uint64 `json:"leader_applied"`
	TargetApplied *uint64 `json:"target_applied"`
	TargetLastLog *uint64 `json:"target_last_log"`
	Lag           *uint64 `json:"lag"`
}

// Position is one replica's log position in the frozen window.
type Position struct {
	ID           uint64  `json:"id"`
	Leader       bool    `json:"leader"`
	Voter        bool    `json:"voter"`
	LastLogIndex *uint64 `json:"last_log_index"`
	LastApplied  *uint64 `json:"last_applied"`
}

// Timings are the audit-critical migration durations.
type Timings struct {
	CatchupMS  uint64 `json:"catchup_ms"`
	FreezeMS   uint64 `json:"freeze_ms"`
	SwitchMS   uint64 `json:"switch_ms"`
	ElectionMS uint64 `json:"election_ms"`
	TotalMS    uint64 `json:"total_ms"`
}

// Event is one audit entry emitted by the node orchestrator.
type Event struct {
	ElapsedMS uint64 `json:"elapsed_ms"`
	Kind      string `json:"kind"`
	Detail    string `json:"detail"`
}

// Report is the node's complete migration audit report.
type Report struct {
	Shard           uint16     `json:"shard"`
	Mode            string     `json:"mode"`
	TargetNode      *uint64    `json:"target_node"`
	RetiredNodes    []uint64   `json:"retired_nodes"`
	VotersBefore    []uint64   `json:"voters_before"`
	VotersAfter     []uint64   `json:"voters_after"`
	LeaderBefore    *uint64    `json:"leader_before"`
	LeaderAfter     *uint64    `json:"leader_after"`
	EpochBefore     uint64     `json:"epoch_before"`
	EpochAfter      uint64     `json:"epoch_after"`
	Catchup         *CatchUp   `json:"catchup"`
	PositionsBefore []Position `json:"positions_before_promote"`
	Timings         Timings    `json:"timings"`
	Events          []Event    `json:"events"`
}

// Result is the node migration response envelope, returned on 200/409/503.
type Result struct {
	Status string  `json:"status"`
	Error  *string `json:"error"`
	Report Report  `json:"report"`
}

// APIError is a non-migration HTTP rejection (400/404/413).
type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("node admin rejected request: %d %s", e.Status, e.Message)
}

// Client talks to one node's local admin listener.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// New returns a client with a timeout comfortably above a full migration.
func New(baseURL string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP:    &http.Client{Timeout: 90 * time.Second},
	}
}

func (c *Client) get(ctx context.Context, path string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return &APIError{Status: resp.StatusCode, Message: decodeError(body)}
	}
	return json.Unmarshal(body, target)
}

// Health verifies that the node admin listener is reachable.
func (c *Client) Health(ctx context.Context) error {
	var health struct {
		Status string `json:"status"`
	}
	if err := c.get(ctx, "/admin/health", &health); err != nil {
		return err
	}
	if health.Status != "ok" {
		return fmt.Errorf("node health status %q", health.Status)
	}
	return nil
}

// Shards returns current physical placements.
func (c *Client) Shards(ctx context.Context) (ShardsSnapshot, error) {
	var snapshot ShardsSnapshot
	err := c.get(ctx, "/admin/shards", &snapshot)
	return snapshot, err
}

// Routes returns the logical route table and its epoch.
func (c *Client) Routes(ctx context.Context) (RoutesSnapshot, error) {
	var routes RoutesSnapshot
	err := c.get(ctx, "/admin/routes", &routes)
	return routes, err
}

// Migrate executes one plan synchronously and returns the audit report. The
// HTTP status is preserved so callers can map 409 (rolled back) and 503
// (degraded) without losing the report body.
func (c *Client) Migrate(ctx context.Context, shard uint16, plan Plan) (Result, int, error) {
	payload, err := json.Marshal(plan)
	if err != nil {
		return Result{}, 0, err
	}
	url := fmt.Sprintf("%s/admin/shards/%d/migrate", c.BaseURL, shard)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return Result{}, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Result{}, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return Result{}, resp.StatusCode, err
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusConflict, http.StatusServiceUnavailable:
		var result Result
		if err := json.Unmarshal(body, &result); err != nil {
			return Result{}, resp.StatusCode, fmt.Errorf("decode migration result: %w", err)
		}
		return result, resp.StatusCode, nil
	default:
		return Result{}, resp.StatusCode, &APIError{
			Status:  resp.StatusCode,
			Message: decodeError(body),
		}
	}
}

func decodeError(body []byte) string {
	var envelope struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.Error != "" {
		return envelope.Error
	}
	message := strings.TrimSpace(string(body))
	if len(message) > 256 {
		message = message[:256]
	}
	if message == "" {
		return "empty error body"
	}
	return message
}
