package dashboard

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	gcapi "github.com/gastownhall/gascity/internal/api"
)

type servicesUnavailableError struct{}

func (servicesUnavailableError) Error() string { return "dashboard services endpoint unavailable" }
func (servicesUnavailableError) ServicesUnavailable() bool {
	return true
}

var errServicesEndpointUnavailable error = servicesUnavailableError{}

// APIFetcher implements ConvoyFetcher by calling the GC API server.
type APIFetcher struct {
	baseURL   string       // e.g. "http://127.0.0.1:8080"
	cityPath  string       // city directory path
	cityName  string       // for display
	cityScope string       // supervisor city scope; when set, /v0/x → /v0/city/{scope}/x
	client    *http.Client // shared client with timeout
	apiClient *gcapi.Client

	// sessionsOnce caches the session list for a single dashboard render.
	// Multiple panel fetchers run in parallel and all need session data;
	// this avoids redundant API calls.
	sessionsOnce   sync.Once
	sessionsCached []apiSessionResponse
}

var (
	_ ConvoyFetcher          = (*APIFetcher)(nil)
	_ ServiceFetcher         = (*APIFetcher)(nil)
	_ scopedDashboardFetcher = (*APIFetcher)(nil)
)

// NewAPIFetcher creates a new API-backed fetcher.
func NewAPIFetcher(baseURL, cityPath, cityName string) *APIFetcher {
	return &APIFetcher{
		baseURL:  strings.TrimRight(baseURL, "/"),
		cityPath: cityPath,
		cityName: cityName,
		apiClient: newFetcherTransportClient(strings.TrimRight(baseURL, "/"), ""),
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// WithScope returns a copy of the fetcher with the given city scope.
// The copy shares the HTTP client but routes API paths through
// /v0/city/{scope}/... for supervisor mode. The session cache is reset.
func (f *APIFetcher) WithScope(cityScope string) *APIFetcher {
	return &APIFetcher{
		baseURL:   f.baseURL,
		cityPath:  f.cityPath,
		cityName:  f.cityName,
		cityScope: cityScope,
		client:    f.client,
		apiClient: newFetcherTransportClient(f.baseURL, cityScope),
	}
}

func newFetcherTransportClient(baseURL, cityScope string) *gcapi.Client {
	if baseURL == "" {
		return nil
	}
	if cityScope != "" {
		return gcapi.NewCityScopedClient(baseURL, cityScope)
	}
	return gcapi.NewClient(baseURL)
}

// Scope returns a scoped dashboard fetcher without exposing the concrete type.
func (f *APIFetcher) Scope(cityScope string) dashboardFetcher {
	return f.WithScope(cityScope)
}

// --- API response types (matching internal/api JSON shapes) ---

// apiListResponse wraps list endpoint responses: {"items": [...], "total": N}
type apiListResponse struct {
	Items json.RawMessage `json:"items"`
	Total int             `json:"total"`
}

// apiSessionResponse mirrors the session API response (GET /v0/sessions).
type apiSessionResponse struct {
	ID          string `json:"id"`
	Template    string `json:"template"`
	State       string `json:"state"`
	Reason      string `json:"reason,omitempty"`
	Title       string `json:"title"`
	Provider    string `json:"provider"`
	DisplayName string `json:"display_name,omitempty"`
	SessionName string `json:"session_name"`
	CreatedAt   string `json:"created_at"`
	LastActive  string `json:"last_active,omitempty"`
	Attached    bool   `json:"attached"`
	Rig         string `json:"rig,omitempty"`
	Pool        string `json:"pool,omitempty"`
	Running     bool   `json:"running"`
	ActiveBead  string `json:"active_bead,omitempty"`
	LastOutput  string `json:"last_output,omitempty"`
	Model       string `json:"model,omitempty"`
	ContextPct  *int   `json:"context_pct,omitempty"`
}

type apiRigResponse struct {
	Name      string `json:"name"`
	Path      string `json:"path"`
	Suspended bool   `json:"suspended"`
	Prefix    string `json:"prefix,omitempty"`
}

type apiServiceResponse struct {
	ServiceName string `json:"service_name"`
	Kind        string `json:"kind,omitempty"`
	State       string `json:"state"`
	LocalState  string `json:"local_state"`
}

type apiConvoyDetail struct {
	Convoy   apiBead   `json:"convoy"`
	Children []apiBead `json:"children"`
	Progress struct {
		Total  int `json:"total"`
		Closed int `json:"closed"`
	} `json:"progress"`
}

type apiBead struct {
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	Status      string    `json:"status"`
	Type        string    `json:"type"`
	CreatedAt   time.Time `json:"created_at"`
	Assignee    string    `json:"assignee,omitempty"`
	From        string    `json:"from,omitempty"`
	ParentID    string    `json:"parent_id,omitempty"`
	Description string    `json:"description,omitempty"`
	Labels      []string  `json:"labels,omitempty"`
}

type apiEvent struct {
	Seq     uint64          `json:"seq"`
	Type    string          `json:"type"`
	Ts      time.Time       `json:"ts"`
	Actor   string          `json:"actor"`
	Subject string          `json:"subject,omitempty"`
	Message string          `json:"message,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type apiMailMessage struct {
	ID        string    `json:"id"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	Subject   string    `json:"subject"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
	Read      bool      `json:"read"`
	ThreadID  string    `json:"thread_id,omitempty"`
	ReplyTo   string    `json:"reply_to,omitempty"`
	Priority  int       `json:"priority,omitempty"`
}

type apiStatusResponse struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	AgentCount int    `json:"agent_count"`
	RigCount   int    `json:"rig_count"`
	Running    int    `json:"running"`
}

type apiHTTPError struct {
	Path       string
	StatusCode int
	Body       string
}

func (e *apiHTTPError) Error() string {
	return fmt.Sprintf("GET %s: status %d: %s", e.Path, e.StatusCode, e.Body)
}

// --- HTTP helpers ---

// get performs a GET request and decodes the JSON response into result.
func (f *APIFetcher) get(path string, result any) error {
	if f.apiClient != nil {
		body, err := f.apiClient.GetJSON(path)
		if err == nil {
			if err := json.Unmarshal(body, result); err != nil {
				return fmt.Errorf("GET %s: decode: %w", path, err)
			}
			return nil
		}
	}
	resp, err := f.client.Get(f.baseURL + scopedPath(path, f.cityScope))
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort close

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return &apiHTTPError{
			Path:       path,
			StatusCode: resp.StatusCode,
			Body:       string(body),
		}
	}

	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("GET %s: decode: %w", path, err)
	}
	return nil
}

// getList performs a GET request and unwraps the {"items": [...], "total": N} envelope.
func (f *APIFetcher) getList(path string, items any) error {
	var wrapper apiListResponse
	if err := f.get(path, &wrapper); err != nil {
		return err
	}
	if len(wrapper.Items) == 0 || string(wrapper.Items) == "null" {
		return nil
	}
	return json.Unmarshal(wrapper.Items, items)
}

// --- ConvoyFetcher implementation ---

// FetchRigs returns all registered rigs from the API.
func (f *APIFetcher) FetchRigs() ([]RigRow, error) {
	var rigs []apiRigResponse
	if err := f.getList("/v0/rigs", &rigs); err != nil {
		return nil, fmt.Errorf("fetching rigs: %w", err)
	}

	rows := make([]RigRow, 0, len(rigs))
	for _, r := range rigs {
		rows = append(rows, RigRow{
			Name: r.Name,
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Name < rows[j].Name
	})
	return rows, nil
}

// FetchServices returns all workspace services from the API.
func (f *APIFetcher) FetchServices() ([]ServiceRow, error) {
	var services []apiServiceResponse
	if err := f.getList("/v0/services", &services); err != nil {
		var httpErr *apiHTTPError
		if errors.As(err, &httpErr) && serviceEndpointUnavailable(httpErr.StatusCode) {
			return nil, errServicesEndpointUnavailable
		}
		return nil, fmt.Errorf("fetching services: %w", err)
	}

	rows := make([]ServiceRow, 0, len(services))
	for _, svc := range services {
		rows = append(rows, ServiceRow{
			Name:       svc.ServiceName,
			Kind:       svc.Kind,
			State:      svc.State,
			LocalState: svc.LocalState,
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Name < rows[j].Name
	})
	return rows, nil
}

func serviceEndpointUnavailable(statusCode int) bool {
	switch statusCode {
	case http.StatusNotFound:
		return true
	default:
		return false
	}
}

// FetchWorkers returns all running worker sessions with activity data.
// Uses the session API as the single source of truth.
func (f *APIFetcher) FetchWorkers() ([]WorkerRow, error) {
	sessions := f.fetchSessions()

	var workers []WorkerRow
	for _, sess := range sessions {
		if !sess.Running {
			continue
		}
		// Only show rig-scoped pool members in the workers panel.
		if sess.Pool == "" || sess.Rig == "" {
			continue
		}

		lastActivity := parseTime(sess.LastActive)
		activityAge := time.Duration(0)
		if !lastActivity.IsZero() {
			activityAge = time.Since(lastActivity)
		}

		issueID := sess.ActiveBead
		var issueTitle string
		if issueID != "" {
			var bead apiBead
			if err := f.get("/v0/bead/"+issueID, &bead); err == nil {
				issueTitle = bead.Title
			}
		}

		workStatus := calculateWorkerWorkStatus(activityAge, issueID, sess.Template,
			5*time.Minute, defaultGUPPViolationTimeout)

		workers = append(workers, WorkerRow{
			Name:         sess.Template,
			Rig:          sess.Rig,
			SessionID:    sess.SessionName,
			LastActivity: calculateActivity(lastActivity),
			IssueID:      issueID,
			IssueTitle:   issueTitle,
			WorkStatus:   workStatus,
			AgentType:    sess.Pool,
			StatusHint:   truncate(sess.LastOutput, 60),
		})
	}

	return workers, nil
}

// FetchDogs returns city-scoped pool sessions (rig == "").
func (f *APIFetcher) FetchDogs() ([]DogRow, error) {
	sessions := f.fetchSessions()

	var rows []DogRow
	for _, sess := range sessions {
		if sess.Rig != "" || sess.Pool == "" {
			continue
		}

		state := "idle"
		if sess.ActiveBead != "" {
			state = "working"
		}

		rows = append(rows, DogRow{
			Name:  sess.Template,
			State: state,
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Name < rows[j].Name
	})
	return rows, nil
}

// FetchMayor returns the coordinator session's status.
// The coordinator is a city-scoped non-pool session.
func (f *APIFetcher) FetchMayor() (*MayorStatus, error) {
	status := &MayorStatus{IsAttached: false}

	sessions := f.fetchSessions()

	// Find city-scoped non-pool session (the coordinator).
	var coordinator *apiSessionResponse
	for i := range sessions {
		if sessions[i].Pool == "" && sessions[i].Rig == "" {
			coordinator = &sessions[i]
			break
		}
	}
	if coordinator == nil {
		return status, nil
	}

	status.IsAttached = coordinator.Attached
	status.SessionName = coordinator.SessionName
	if status.SessionName == "" {
		status.SessionName = coordinator.Template
	}

	lastActive := parseTime(coordinator.LastActive)
	if !lastActive.IsZero() {
		age := time.Since(lastActive)
		status.LastActivity = formatTimestamp(lastActive)
		status.IsActive = age < 5*time.Minute
	}

	return status, nil
}

// FetchConvoys fetches all open convoys with progress data.
func (f *APIFetcher) FetchConvoys() ([]ConvoyRow, error) {
	// List convoys (they're beads with type=convoy)
	var convoys []apiBead
	if err := f.getList("/v0/convoys", &convoys); err != nil {
		return nil, fmt.Errorf("listing convoys: %w", err)
	}

	rows := make([]ConvoyRow, 0, len(convoys))
	for _, c := range convoys {
		if c.Status == "closed" {
			continue
		}

		// Get convoy detail with children and progress.
		var detail apiConvoyDetail
		if err := f.get("/v0/convoy/"+c.ID, &detail); err != nil {
			log.Printf("warning: skipping convoy %s: %v", c.ID, err)
			continue
		}

		row := ConvoyRow{
			ID:        c.ID,
			Title:     c.Title,
			Status:    c.Status,
			Total:     detail.Progress.Total,
			Completed: detail.Progress.Closed,
			Progress:  fmt.Sprintf("%d/%d", detail.Progress.Closed, detail.Progress.Total),
		}

		// Build tracked issues, compute work breakdown, and find most recent activity.
		var mostRecentUpdated time.Time
		tracked := make([]TrackedIssue, 0, len(detail.Children))
		assigneeSet := make(map[string]bool)
		for _, child := range detail.Children {
			tracked = append(tracked, TrackedIssue{
				ID:       child.ID,
				Title:    child.Title,
				Status:   child.Status,
				Assignee: child.Assignee,
			})
			if child.CreatedAt.After(mostRecentUpdated) {
				mostRecentUpdated = child.CreatedAt
			}
			if child.Status != "closed" {
				if child.Assignee != "" {
					row.InProgress++
					assigneeSet[child.Assignee] = true
				} else {
					row.ReadyBeads++
				}
			}
		}
		row.TrackedIssues = tracked
		if row.Total > 0 {
			row.ProgressPct = (row.Completed * 100) / row.Total
		}
		for a := range assigneeSet {
			row.Assignees = append(row.Assignees, a)
		}
		sort.Strings(row.Assignees)

		if !mostRecentUpdated.IsZero() {
			row.LastActivity = calculateActivity(mostRecentUpdated)
		} else {
			row.LastActivity = ActivityInfo{
				Display:    "idle",
				ColorClass: colorUnknown,
			}
		}

		row.WorkStatus = calculateWorkStatus(row.Completed, row.Total, row.LastActivity.ColorClass)
		rows = append(rows, row)
	}

	return rows, nil
}

// FetchMail fetches recent mail messages from the API.
func (f *APIFetcher) FetchMail() ([]MailRow, error) {
	var messages []apiMailMessage
	if err := f.getList("/v0/mail", &messages); err != nil {
		return nil, fmt.Errorf("fetching mail: %w", err)
	}

	rows := make([]MailRow, 0, len(messages))
	for _, m := range messages {
		var age string
		var sortKey int64
		if !m.CreatedAt.IsZero() {
			age = formatTimestamp(m.CreatedAt)
			sortKey = m.CreatedAt.Unix()
		}

		priorityStr := "normal"
		switch m.Priority {
		case 0:
			priorityStr = "urgent"
		case 1:
			priorityStr = "high"
		case 2:
			priorityStr = "normal"
		case 3, 4:
			priorityStr = "low"
		}

		rows = append(rows, MailRow{
			ID:        m.ID,
			From:      formatAgentAddress(m.From),
			FromRaw:   m.From,
			To:        formatAgentAddress(m.To),
			Subject:   m.Subject,
			Timestamp: m.CreatedAt.Format("15:04"),
			Age:       age,
			Priority:  priorityStr,
			Type:      "notification",
			Read:      m.Read,
			SortKey:   sortKey,
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].SortKey > rows[j].SortKey
	})
	return rows, nil
}

// FetchAssigned returns in-progress beads assigned to agents.
func (f *APIFetcher) FetchAssigned() ([]AssignedRow, error) {
	var beadList []apiBead
	if err := f.getList("/v0/beads?status=in_progress&limit=1000", &beadList); err != nil {
		return nil, nil
	}

	var rows []AssignedRow
	for _, b := range beadList {
		row := AssignedRow{
			ID:       b.ID,
			Title:    b.Title,
			Assignee: b.Assignee,
			Agent:    formatAgentAddress(b.Assignee),
		}

		if !b.CreatedAt.IsZero() {
			age := time.Since(b.CreatedAt)
			row.Age = formatTimestamp(b.CreatedAt)
			row.IsStale = age > time.Hour
		}

		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].IsStale != rows[j].IsStale {
			return rows[i].IsStale
		}
		return rows[i].Age > rows[j].Age
	})
	return rows, nil
}

// FetchIssues returns open and in-progress issues (the backlog), tagged by rig.
func (f *APIFetcher) FetchIssues() ([]IssueRow, error) {
	// Discover rigs so we can tag each bead with its source rig.
	var rigs []apiRigResponse
	if err := f.getList("/v0/rigs", &rigs); err != nil || len(rigs) == 0 {
		// Fallback: query without rig scoping.
		rigs = []apiRigResponse{{Name: ""}}
	}

	var allBeads []rigBead
	for _, rig := range rigs {
		rigFilter := ""
		if rig.Name != "" {
			rigFilter = "&rig=" + rig.Name
		}
		var openBeads []apiBead
		if err := f.getList("/v0/beads?status=open&limit=50"+rigFilter, &openBeads); err == nil {
			for _, b := range openBeads {
				allBeads = append(allBeads, rigBead{bead: b, rig: rig.Name})
			}
		}
		var inProgressBeads []apiBead
		if err := f.getList("/v0/beads?status=in_progress&limit=50"+rigFilter, &inProgressBeads); err == nil {
			for _, b := range inProgressBeads {
				allBeads = append(allBeads, rigBead{bead: b, rig: rig.Name})
			}
		}
	}

	var rows []IssueRow
	for _, rb := range allBeads {
		b := rb.bead
		if isInternalBead(b) {
			continue
		}

		row := IssueRow{
			ID:    b.ID,
			Title: b.Title,
			Type:  b.Type,
			Rig:   rb.rig,
		}

		var displayLabels []string
		for _, label := range b.Labels {
			if !strings.HasPrefix(label, "gc:") && !strings.HasPrefix(label, "internal:") {
				displayLabels = append(displayLabels, label)
			}
		}
		if len(displayLabels) > 0 {
			row.Labels = strings.Join(displayLabels, ", ")
			if len(row.Labels) > 25 {
				row.Labels = row.Labels[:22] + "..."
			}
		}

		if !b.CreatedAt.IsZero() {
			row.Age = formatTimestamp(b.CreatedAt)
		}

		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		pi, pj := rows[i].Priority, rows[j].Priority
		if pi == 0 {
			pi = 5
		}
		if pj == 0 {
			pj = 5
		}
		if pi != pj {
			return pi < pj
		}
		return rows[i].Age > rows[j].Age
	})
	return rows, nil
}

// rigBead pairs a bead with its source rig name.
type rigBead struct {
	bead apiBead
	rig  string
}

// isInternalBead returns true for beads that are internal infrastructure.
func isInternalBead(b apiBead) bool {
	switch b.Type {
	case "message", "convoy", "queue", "merge-request", "wisp", "agent":
		return true
	}
	for _, l := range b.Labels {
		switch l {
		case "gc:message", "gc:convoy", "gc:queue", "gc:merge-request", "gc:wisp", "gc:agent":
			return true
		}
	}
	return false
}

// FetchEscalations returns open escalations needing attention.
func (f *APIFetcher) FetchEscalations() ([]EscalationRow, error) {
	var beadList []apiBead
	if err := f.getList("/v0/beads?label=gc:escalation&status=open", &beadList); err != nil {
		return nil, nil
	}

	var rows []EscalationRow
	for _, b := range beadList {
		row := EscalationRow{
			ID:          b.ID,
			Title:       b.Title,
			EscalatedBy: formatAgentAddress(b.From),
			Severity:    "medium",
		}

		for _, label := range b.Labels {
			if strings.HasPrefix(label, "severity:") {
				row.Severity = strings.TrimPrefix(label, "severity:")
			}
			if label == "acked" {
				row.Acked = true
			}
		}

		if !b.CreatedAt.IsZero() {
			row.Age = formatTimestamp(b.CreatedAt)
		}

		rows = append(rows, row)
	}

	severityOrder := map[string]int{"critical": 0, "high": 1, "medium": 2, "low": 3}
	sort.Slice(rows, func(i, j int) bool {
		si, sj := severityOrder[rows[i].Severity], severityOrder[rows[j].Severity]
		return si < sj
	})
	return rows, nil
}

// FetchHealth returns system health from the API.
func (f *APIFetcher) FetchHealth() (*HealthRow, error) {
	row := &HealthRow{}

	var status apiStatusResponse
	if err := f.get("/v0/status", &status); err != nil {
		row.DeaconHeartbeat = "no heartbeat"
		return row, nil
	}

	// Count healthy/unhealthy from sessions.
	sessions := f.fetchSessions()
	for _, sess := range sessions {
		if sess.Running {
			row.HealthyAgents++
		} else {
			row.UnhealthyAgents++
		}
	}

	row.DeaconHeartbeat = "active"
	row.HeartbeatFresh = true
	return row, nil
}

// FetchQueues returns work queues.
func (f *APIFetcher) FetchQueues() ([]QueueRow, error) {
	var beadList []apiBead
	if err := f.getList("/v0/beads?label=gc:queue", &beadList); err != nil {
		return nil, nil
	}

	var rows []QueueRow
	for _, b := range beadList {
		row := QueueRow{
			Name:   b.Title,
			Status: b.Status,
		}

		// Parse counts from description.
		for _, line := range strings.Split(b.Description, "\n") {
			line = strings.TrimSpace(line)
			switch {
			case strings.HasPrefix(line, "available_count:"):
				_, _ = fmt.Sscanf(line, "available_count: %d", &row.Available)
			case strings.HasPrefix(line, "processing_count:"):
				_, _ = fmt.Sscanf(line, "processing_count: %d", &row.Processing)
			case strings.HasPrefix(line, "completed_count:"):
				_, _ = fmt.Sscanf(line, "completed_count: %d", &row.Completed)
			case strings.HasPrefix(line, "failed_count:"):
				_, _ = fmt.Sscanf(line, "failed_count: %d", &row.Failed)
			case strings.HasPrefix(line, "status:"):
				var s string
				_, _ = fmt.Sscanf(line, "status: %s", &s)
				if s != "" {
					row.Status = s
				}
			}
		}

		rows = append(rows, row)
	}
	return rows, nil
}

// FetchActivity returns recent events from the API.
func (f *APIFetcher) FetchActivity() ([]ActivityRow, error) {
	var events []apiEvent
	if err := f.getList("/v0/events?since=1h", &events); err != nil {
		return nil, nil
	}

	// Take last 50 events.
	start := 0
	if len(events) > 50 {
		start = len(events) - 50
	}

	var rows []ActivityRow
	for i := len(events) - 1; i >= start; i-- {
		event := events[i]

		// Subject holds the agent identity (e.g. "myrig/polecats/polecat-1");
		// Actor is who initiated the action (e.g. "gc", "controller", "human").
		// Use Subject for display when available, fall back to Actor.
		agent := event.Subject
		if agent == "" {
			agent = event.Actor
		}

		row := ActivityRow{
			Type:         event.Type,
			Category:     eventCategory(event.Type),
			Actor:        formatAgentAddress(agent),
			Rig:          extractRig(agent),
			Icon:         eventIcon(event.Type),
			RawTimestamp: event.Ts.Format(time.RFC3339),
		}

		if !event.Ts.IsZero() {
			row.Time = formatTimestamp(event.Ts)
		}

		row.Summary = eventSummary(event.Type, event.Actor, event.Subject, event.Message)
		rows = append(rows, row)
	}

	return rows, nil
}

// FetchMergeQueue fetches open PRs from registered rigs via the API + gh CLI.
func (f *APIFetcher) FetchMergeQueue() ([]MergeQueueRow, error) {
	// Get rig paths from the API.
	var rigs []apiRigResponse
	if err := f.getList("/v0/rigs", &rigs); err != nil {
		return nil, fmt.Errorf("fetching rigs for merge queue: %w", err)
	}

	ghTimeout := 10 * time.Second

	var result []MergeQueueRow
	for _, rig := range rigs {
		if rig.Path == "" {
			continue
		}
		repoPath := detectRepoFromPath(rig.Path, ghTimeout)
		if repoPath == "" {
			continue
		}

		prs, err := fetchPRsForRepo(repoPath, rig.Name, ghTimeout)
		if err != nil {
			continue
		}
		result = append(result, prs...)
	}

	return result, nil
}

// fetchSessions calls GET /v0/sessions?state=active&peek=true and returns all sessions.
// Results are cached via sync.Once for the lifetime of a single dashboard render
// so that parallel panel fetchers share one API call.
func (f *APIFetcher) fetchSessions() []apiSessionResponse {
	f.sessionsOnce.Do(func() {
		if err := f.getList("/v0/sessions?state=active&peek=true", &f.sessionsCached); err != nil {
			log.Printf("dashboard: fetchSessions: %v", err)
		}
	})
	return f.sessionsCached
}

// parseTime parses an RFC3339 timestamp, returning zero time on failure.
func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

// truncate shortens a string to maxLen, adding "..." if truncated.
func truncate(s string, maxLen int) string {
	// Take last non-empty line (for peek output).
	lines := strings.Split(s, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line != "" {
			s = line
			break
		}
	}
	if len(s) > maxLen {
		return s[:maxLen-3] + "..."
	}
	return s
}

// detectRepoFromPath tries to extract owner/repo from a git working directory.
func detectRepoFromPath(path string, timeout time.Duration) string {
	stdout, err := runCmd(timeout, "git", "-C", path, "remote", "get-url", "origin")
	if err != nil {
		return ""
	}
	return gitURLToRepoPath(strings.TrimSpace(stdout.String()))
}

// fetchPRsForRepo fetches open PRs for a single repo via gh CLI.
func fetchPRsForRepo(repoFull, repoShort string, timeout time.Duration) ([]MergeQueueRow, error) {
	stdout, err := runCmd(timeout, "gh", "pr", "list",
		"--repo", repoFull,
		"--state", "open",
		"--json", "number,title,url,mergeable,statusCheckRollup")
	if err != nil {
		return nil, fmt.Errorf("fetching PRs for %s: %w", repoFull, err)
	}

	var prs []prResponse
	if err := json.Unmarshal(stdout.Bytes(), &prs); err != nil {
		return nil, fmt.Errorf("parsing PRs for %s: %w", repoFull, err)
	}

	result := make([]MergeQueueRow, 0, len(prs))
	for _, pr := range prs {
		row := MergeQueueRow{
			Number: pr.Number,
			Repo:   repoShort,
			Title:  pr.Title,
			URL:    pr.URL,
		}
		row.CIStatus = determineCIStatus(pr.StatusCheckRollup)
		row.Mergeable = determineMergeableStatus(pr.Mergeable)
		row.ColorClass = determineColorClass(row.CIStatus, row.Mergeable)
		result = append(result, row)
	}

	return result, nil
}
