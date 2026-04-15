package api

import (
	"github.com/gastownhall/gascity/internal/beads"
)

// BeadService is the domain interface for bead operations. The Server struct
// implements it via the existing listBeads/createBead/etc. methods. Extracting
// this interface breaks up the Server god object: each domain becomes an
// independently testable service with a small, stable contract.
type BeadService interface {
	List(query beads.ListQuery, rig string) []beads.Bead
	ListReady() ([]beads.Bead, error)
	Get(id string) (beads.Bead, error)
	Deps(id string) (beadDepsResponse, error)
	Graph(rootID string) (beadGraphResponseJSON, error)
	Create(req beadCreateRequest) (beads.Bead, error)
	Close(id string) (mutationStatusResponse, error)
	Update(body beadUpdateRequest) (mutationStatusResponse, error)
	Reopen(id string) (mutationStatusResponse, error)
	Assign(id, assignee string) (beadAssignResponse, error)
	Delete(id string) error
}

// beadService is the default BeadService implementation, delegating to
// Server's existing methods. This is a thin adapter — the real logic
// stays on *Server for now and can be extracted further later.
type beadService struct {
	s *Server
}

func (b *beadService) List(query beads.ListQuery, rig string) []beads.Bead {
	return b.s.listBeads(query, rig, nil)
}

func (b *beadService) ListReady() ([]beads.Bead, error) {
	return b.s.listReadyBeads()
}

func (b *beadService) Get(id string) (beads.Bead, error) {
	return b.s.getBead(id)
}

func (b *beadService) Deps(id string) (beadDepsResponse, error) {
	return b.s.getBeadDeps(id)
}

func (b *beadService) Graph(rootID string) (beadGraphResponseJSON, error) {
	return b.s.getBeadGraph(rootID)
}

func (b *beadService) Create(req beadCreateRequest) (beads.Bead, error) {
	return b.s.createBead(req)
}

func (b *beadService) Close(id string) (mutationStatusResponse, error) {
	return b.s.closeBead(id)
}

func (b *beadService) Update(body beadUpdateRequest) (mutationStatusResponse, error) {
	return b.s.updateBead(body)
}

func (b *beadService) Reopen(id string) (mutationStatusResponse, error) {
	return b.s.reopenBead(id)
}

func (b *beadService) Assign(id, assignee string) (beadAssignResponse, error) {
	return b.s.assignBead(id, assignee)
}

func (b *beadService) Delete(id string) error {
	return b.s.deleteBead(id)
}
