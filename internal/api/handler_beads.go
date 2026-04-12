package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gastownhall/gascity/internal/beads"
)

type beadCreateRequest struct {
	Rig         string   `json:"rig"`
	Title       string   `json:"title"`
	Type        string   `json:"type"`
	Priority    *int     `json:"priority"`
	Assignee    string   `json:"assignee"`
	Description string   `json:"description"`
	Labels      []string `json:"labels"`
}

func (s *Server) handleBeadList(w http.ResponseWriter, r *http.Request) {
	bp := parseBlockingParams(r)
	if bp.isBlocking() {
		waitForChange(r.Context(), s.state.EventProvider(), bp)
	}

	q := r.URL.Query()
	qStatus := q.Get("status")
	qType := q.Get("type")
	qLabel := q.Get("label")
	qAssignee := q.Get("assignee")
	qRig := q.Get("rig")
	pp := parsePagination(r, 50)

	all := s.listBeads(beads.ListQuery{
		Status:   qStatus,
		Type:     qType,
		Label:    qLabel,
		Assignee: qAssignee,
	}, qRig, r)
	if !pp.IsPaging {
		if pp.Limit < len(all) {
			all = all[:pp.Limit]
		}
		writeListJSON(w, s.latestIndex(), all, len(all))
		return
	}
	page, total, nextCursor := paginate(all, pp)
	if page == nil {
		page = []beads.Bead{}
	}
	writePagedJSON(w, s.latestIndex(), page, total, nextCursor)
}

func (s *Server) listBeads(query beads.ListQuery, rig string, r *http.Request) []beads.Bead {
	if !query.HasFilter() {
		query.AllowScan = true
	}
	stores := s.state.BeadStores()
	var rigNames []string
	if rig != "" {
		if _, ok := stores[rig]; ok {
			rigNames = []string{rig}
		}
	} else {
		rigNames = sortedRigNames(stores)
	}
	if r != nil {
		setDataSource(r, "bd_subprocess")
	}
	var all []beads.Bead
	for _, rigName := range rigNames {
		store := stores[rigName]
		list, err := store.List(query)
		if err != nil {
			continue
		}
		all = append(all, list...)
	}
	if all == nil {
		return []beads.Bead{}
	}
	return all
}

func (s *Server) handleBeadReady(w http.ResponseWriter, r *http.Request) {
	bp := parseBlockingParams(r)
	if bp.isBlocking() {
		waitForChange(r.Context(), s.state.EventProvider(), bp)
	}

	all, err := s.listReadyBeads()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeListJSON(w, s.latestIndex(), all, len(all))
}

func (s *Server) listReadyBeads() ([]beads.Bead, error) {
	stores := s.state.BeadStores()
	rigNames := sortedRigNames(stores)
	var all []beads.Bead
	for _, rigName := range rigNames {
		ready, err := stores[rigName].Ready()
		if err != nil {
			return nil, err
		}
		all = append(all, ready...)
	}

	if all == nil {
		all = []beads.Bead{}
	}
	return all, nil
}

func (s *Server) handleBeadGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	b, err := s.getBead(id)
	if err != nil {
		if errors.Is(err, beads.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "bead "+id+" not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeIndexJSON(w, s.latestIndex(), b)
}

func (s *Server) getBead(id string) (beads.Bead, error) {
	for _, store := range s.beadStoresForID(id) {
		b, err := store.Get(id)
		if err != nil {
			if errors.Is(err, beads.ErrNotFound) {
				continue
			}
			return beads.Bead{}, err
		}
		return b, nil
	}
	return beads.Bead{}, beads.ErrNotFound
}

func (s *Server) handleBeadDeps(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	resp, err := s.getBeadDeps(id)
	if err != nil {
		if errors.Is(err, beads.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "bead "+id+" not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeIndexJSON(w, s.latestIndex(), resp)
}

func (s *Server) getBeadDeps(id string) (map[string]any, error) {
	for _, store := range s.beadStoresForID(id) {
		parent, err := store.Get(id)
		if err != nil {
			if errors.Is(err, beads.ErrNotFound) {
				continue
			}
			return nil, err
		}
		children, err := store.List(beads.ListQuery{
			ParentID: id,
			Sort:     beads.SortCreatedAsc,
		})
		if err != nil {
			return nil, err
		}
		children = appendMetadataAttachedChildren(store, parent, children)
		if children == nil {
			children = []beads.Bead{}
		}
		return map[string]any{"children": children}, nil
	}
	return nil, beads.ErrNotFound
}

func appendMetadataAttachedChildren(store beads.Store, parent beads.Bead, children []beads.Bead) []beads.Bead {
	if store == nil {
		return children
	}
	seen := make(map[string]struct{}, len(children))
	for _, child := range children {
		seen[child.ID] = struct{}{}
	}
	for _, key := range []string{"molecule_id", "workflow_id"} {
		attachedID := strings.TrimSpace(parent.Metadata[key])
		if attachedID == "" {
			continue
		}
		if _, ok := seen[attachedID]; ok {
			continue
		}
		attached, err := store.Get(attachedID)
		if err != nil {
			continue
		}
		seen[attached.ID] = struct{}{}
		children = append(children, attached)
	}
	return children
}

func (s *Server) handleBeadCreate(w http.ResponseWriter, r *http.Request) {
	var body beadCreateRequest
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}

	// Idempotency check — key is scoped by method+path to prevent cross-endpoint collisions.
	idemKey := scopedIdemKey(r, r.Header.Get("Idempotency-Key"))
	var bodyHash string
	if idemKey != "" {
		bodyHash = hashBody(body)
		if s.idem.handleIdempotent(w, idemKey, bodyHash) {
			return
		}
	}

	b, err := s.createBead(body)
	if err != nil {
		s.idem.unreserve(idemKey)
		if herrStatus(err) != 0 {
			herr := asHTTPError(err)
			writeStructuredError(w, herr.status, herr.code, herr.message, herr.details)
		} else {
			writeError(w, http.StatusInternalServerError, "internal", err.Error())
		}
		return
	}
	s.idem.storeResponse(idemKey, bodyHash, http.StatusCreated, b)
	writeJSON(w, http.StatusCreated, b)
}

func (s *Server) createBead(body beadCreateRequest) (beads.Bead, error) {
	if body.Title == "" {
		return beads.Bead{}, httpError{status: http.StatusBadRequest, code: "invalid", message: "title is required"}
	}
	store := s.findStore(body.Rig)
	if store == nil {
		return beads.Bead{}, httpError{status: http.StatusBadRequest, code: "invalid", message: "rig is required when multiple rigs are configured"}
	}
	return store.Create(beads.Bead{
		Title:       body.Title,
		Type:        body.Type,
		Priority:    body.Priority,
		Assignee:    body.Assignee,
		Description: body.Description,
		Labels:      body.Labels,
	})
}

func (s *Server) handleBeadClose(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	resp, err := s.closeBead(id)
	if err != nil {
		if errors.Is(err, beads.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "bead "+id+" not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) closeBead(id string) (map[string]string, error) {
	for _, store := range s.beadStoresForID(id) {
		if err := store.Close(id); err != nil {
			if errors.Is(err, beads.ErrNotFound) {
				continue
			}
			return nil, err
		}
		return map[string]string{"status": "closed"}, nil
	}
	return nil, beads.ErrNotFound
}

func (s *Server) handleBeadUpdate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	payload, err := decodeBodyBytes(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	resp, err := s.updateBead(id, payload)
	if err != nil {
		if errors.Is(err, beads.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "bead "+id+" not found")
			return
		}
		if herrStatus(err) != 0 {
			herr := asHTTPError(err)
			writeStructuredError(w, herr.status, herr.code, herr.message, herr.details)
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) updateBead(id string, payload []byte) (map[string]string, error) {
	var raw map[string]json.RawMessage
	if len(bytes.TrimSpace(payload)) > 0 {
		if err := json.Unmarshal(payload, &raw); err != nil {
			return nil, httpError{status: http.StatusBadRequest, code: "invalid", message: err.Error()}
		}
	}
	var body struct {
		Title        *string           `json:"title"`
		Status       *string           `json:"status"`
		Type         *string           `json:"type"`
		Priority     *int              `json:"priority"`
		Assignee     *string           `json:"assignee"`
		Description  *string           `json:"description"`
		Labels       []string          `json:"labels"`
		RemoveLabels []string          `json:"remove_labels"`
		Metadata     map[string]string `json:"metadata"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil, httpError{status: http.StatusBadRequest, code: "invalid", message: err.Error()}
	}
	if rawPriority, ok := raw["priority"]; ok && bytes.Equal(bytes.TrimSpace(rawPriority), []byte("null")) {
		return nil, httpError{status: http.StatusBadRequest, code: "invalid", message: "clearing priority is not supported"}
	}

	opts := beads.UpdateOpts{
		Title:        body.Title,
		Status:       body.Status,
		Type:         body.Type,
		Priority:     body.Priority,
		Assignee:     body.Assignee,
		Description:  body.Description,
		Labels:       body.Labels,
		RemoveLabels: body.RemoveLabels,
	}
	for _, store := range s.beadStoresForID(id) {
		if err := store.Update(id, opts); err != nil {
			if errors.Is(err, beads.ErrNotFound) {
				continue
			}
			return nil, err
		}
		if len(body.Metadata) > 0 {
			if err := store.SetMetadataBatch(id, body.Metadata); err != nil {
				return nil, err
			}
		}
		return map[string]string{"status": "updated"}, nil
	}
	return nil, beads.ErrNotFound
}

func (s *Server) handleBeadReopen(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	resp, err := s.reopenBead(id)
	if err != nil {
		if errors.Is(err, beads.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "bead "+id+" not found")
			return
		}
		if herrStatus(err) != 0 {
			herr := asHTTPError(err)
			writeError(w, herr.status, herr.code, herr.message)
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleBeadAssign(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		Assignee string `json:"assignee"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	resp, err := s.assignBead(id, body.Assignee)
	if err != nil {
		if errors.Is(err, beads.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "bead "+id+" not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) reopenBead(id string) (map[string]string, error) {
	status := "open"
	for _, store := range s.beadStoresForID(id) {
		b, err := store.Get(id)
		if err != nil {
			if errors.Is(err, beads.ErrNotFound) {
				continue
			}
			return nil, err
		}
		if b.Status != "closed" {
			return nil, httpError{status: http.StatusConflict, code: "conflict", message: "bead " + id + " is not closed (status: " + b.Status + ")"}
		}
		if err := store.Update(id, beads.UpdateOpts{Status: &status}); err != nil {
			return nil, err
		}
		return map[string]string{"status": "reopened"}, nil
	}
	return nil, beads.ErrNotFound
}

func (s *Server) assignBead(id, assignee string) (map[string]string, error) {
	for _, store := range s.beadStoresForID(id) {
		if err := store.Update(id, beads.UpdateOpts{Assignee: &assignee}); err != nil {
			if errors.Is(err, beads.ErrNotFound) {
				continue
			}
			return nil, err
		}
		return map[string]string{"status": "assigned", "assignee": assignee}, nil
	}
	return nil, beads.ErrNotFound
}

func (s *Server) handleBeadDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	for _, store := range s.beadStoresForID(id) {
		if err := store.Close(id); err != nil {
			if errors.Is(err, beads.ErrNotFound) {
				continue
			}
			writeError(w, http.StatusInternalServerError, "internal", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
		return
	}
	writeError(w, http.StatusNotFound, "not_found", "bead "+id+" not found")
}

// findStore returns the bead store for the given rig. If rig is empty, returns
// the sole store when exactly one exists (after deduplication), or nil when
// multiple distinct stores exist (caller should require explicit rig).
func (s *Server) findStore(rig string) beads.Store {
	if rig != "" {
		return s.state.BeadStore(rig)
	}
	stores := s.state.BeadStores()
	names := sortedRigNames(stores)
	if len(names) == 1 {
		return stores[names[0]]
	}
	return nil
}

// beadStoresForID resolves the authoritative store for a bead ID using its
// prefix/routes mapping when possible. If there is no routed match, it falls
// back to the legacy store scan order.
func (s *Server) beadStoresForID(id string) []beads.Store {
	if prefix := beadPrefix(strings.TrimSpace(id)); prefix != "" {
		if store := s.resolveStoreByPrefix(prefix); store != nil {
			return []beads.Store{store}
		}
	}

	stores := s.state.BeadStores()
	rigNames := sortedRigNames(stores)
	candidates := make([]beads.Store, 0, len(rigNames)+1)
	if cityStore := s.state.CityBeadStore(); cityStore != nil {
		candidates = append(candidates, cityStore)
	}
	for _, rigName := range rigNames {
		candidates = append(candidates, stores[rigName])
	}
	return candidates
}

// resolveStoreByPrefix finds the store that owns a bead prefix by checking
// routes.jsonl files in the city and each rig's .beads/ directory, then
// mapping the resolved store path back to the correct store.
func (s *Server) resolveStoreByPrefix(prefix string) beads.Store {
	cfg := s.state.Config()
	if cfg == nil {
		return nil
	}
	stores := s.state.BeadStores()
	cityPath := strings.TrimSpace(s.state.CityPath())

	// Build rig path → name map for reverse lookup (used by both city
	// and rig route resolution below).
	rigPathToName := make(map[string]string, len(cfg.Rigs))
	for _, rig := range cfg.Rigs {
		rp := strings.TrimSpace(rig.Path)
		if rp == "" {
			continue
		}
		if !filepath.IsAbs(rp) && cityPath != "" {
			rp = filepath.Join(cityPath, rp)
		}
		rigPathToName[filepath.Clean(rp)] = rig.Name
	}

	// Check city-level routes first.
	if cityPath != "" {
		if storePath, ok := resolveRoutePrefix(cityPath, prefix); ok {
			cleanPath := filepath.Clean(storePath)
			// Route may point to a rig directory — resolve to the rig store.
			if rigName, found := rigPathToName[cleanPath]; found {
				if store, exists := stores[rigName]; exists {
					return store
				}
			}
			// Route points to the city itself (e.g. prefix "mc" → ".").
			if cleanPath == filepath.Clean(cityPath) {
				if cityStore := s.state.CityBeadStore(); cityStore != nil {
					return cityStore
				}
			}
		}
	}

	// Search routes.jsonl in each rig's .beads/ directory.
	for _, rig := range cfg.Rigs {
		rigPath := strings.TrimSpace(rig.Path)
		if rigPath == "" {
			continue
		}
		if !filepath.IsAbs(rigPath) && cityPath != "" {
			rigPath = filepath.Join(cityPath, rigPath)
		}
		storePath, ok := resolveRoutePrefix(rigPath, prefix)
		if !ok {
			continue
		}
		// The resolved store path might point to a different rig
		// (e.g., prefix "gb" in alpha's routes maps to ../beta).
		cleanPath := filepath.Clean(storePath)
		if rigName, found := rigPathToName[cleanPath]; found {
			if store, exists := stores[rigName]; exists {
				return store
			}
		}
		// Fallback: the route pointed to the same rig.
		if store, exists := stores[rig.Name]; exists {
			return store
		}
	}
	return nil
}

// sortedRigNames returns rig names from the store map in deterministic sorted order,
// deduplicating rigs that share the same underlying store (e.g. file provider mode).
func sortedRigNames(stores map[string]beads.Store) []string {
	names := make([]string, 0, len(stores))
	for name := range stores {
		names = append(names, name)
	}
	sort.Strings(names)
	// Deduplicate by store identity — when multiple rigs share the same
	// store instance (file provider), only keep the first rig name to
	// prevent duplicate results in aggregate queries.
	seen := make(map[beads.Store]bool, len(names))
	deduped := names[:0]
	for _, name := range names {
		s := stores[name]
		if seen[s] {
			continue
		}
		seen[s] = true
		deduped = append(deduped, name)
	}
	return deduped
}

// beadGraphResponseJSON is the response shape for GET /v0/beads/graph/{rootID}.
// Returns raw beads and deps — no status mapping, no presentation logic.
type beadGraphResponseJSON struct {
	Root  beads.Bead            `json:"root"`
	Beads []beads.Bead          `json:"beads"`
	Deps  []workflowDepResponse `json:"deps"`
}

func (s *Server) handleBeadGraph(w http.ResponseWriter, r *http.Request) {
	rootID := r.PathValue("rootID")
	resp, err := s.getBeadGraph(rootID)
	if err != nil {
		if herrStatus(err) != 0 {
			herr := asHTTPError(err)
			writeError(w, herr.status, herr.code, herr.message)
			return
		}
		if errors.Is(err, beads.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "bead "+rootID+" not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeIndexJSON(w, s.latestIndex(), resp)
}

func (s *Server) getBeadGraph(rootID string) (beadGraphResponseJSON, error) {
	if rootID == "" {
		return beadGraphResponseJSON{}, httpError{status: http.StatusBadRequest, code: "invalid", message: "rootID is required"}
	}

	var root beads.Bead
	var foundStore beads.Store
	for _, store := range s.beadStoresForID(rootID) {
		b, err := store.Get(rootID)
		if err != nil {
			if errors.Is(err, beads.ErrNotFound) {
				continue
			}
			return beadGraphResponseJSON{}, err
		}
		root = b
		foundStore = store
		break
	}
	if foundStore == nil {
		return beadGraphResponseJSON{}, beads.ErrNotFound
	}

	all, err := foundStore.List(beads.ListQuery{
		Metadata:      map[string]string{"gc.root_bead_id": rootID},
		IncludeClosed: true,
	})
	if err != nil {
		return beadGraphResponseJSON{}, err
	}

	graphBeads := []beads.Bead{root}
	beadIndex := map[string]beads.Bead{root.ID: root}
	for _, b := range all {
		if b.ID == root.ID {
			continue
		}
		graphBeads = append(graphBeads, b)
		beadIndex[b.ID] = b
	}

	deps, _ := collectWorkflowDeps(foundStore, beadIndex)
	return beadGraphResponseJSON{
		Root:  root,
		Beads: graphBeads,
		Deps:  deps,
	}, nil
}

// beadPrefix extracts the alphabetic prefix from a bead ID (e.g., "ga" from "ga-5b8i").
func beadPrefix(id string) string {
	for i, c := range id {
		if c == '-' {
			return id[:i]
		}
		if c < 'a' || c > 'z' {
			return ""
		}
	}
	return ""
}

// resolveRoutePrefix reads routes.jsonl from a rig's .beads/ directory and
// resolves the given prefix to an absolute store path.
func resolveRoutePrefix(rigPath, prefix string) (string, bool) {
	routesPath := filepath.Join(rigPath, ".beads", "routes.jsonl")
	data, err := os.ReadFile(routesPath)
	if err != nil {
		return "", false
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var entry struct {
			Prefix string `json:"prefix"`
			Path   string `json:"path"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if entry.Prefix == prefix {
			resolved := entry.Path
			if !filepath.IsAbs(resolved) {
				resolved = filepath.Join(rigPath, resolved)
			}
			return resolved, true
		}
	}
	return "", false
}

// decodeBody decodes JSON request body into v.
// Limits body size to 1 MiB to prevent OOM from oversized requests.
func decodeBody(r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, 1<<20) // 1 MiB
	return json.NewDecoder(r.Body).Decode(v)
}

func decodeBodyBytes(r *http.Request) ([]byte, error) {
	r.Body = http.MaxBytesReader(nil, r.Body, 1<<20) // 1 MiB
	return io.ReadAll(r.Body)
}
