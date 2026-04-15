package api

import (
	"github.com/gastownhall/gascity/internal/beads"
)

// ConvoyService is the domain interface for convoy operations.
type ConvoyService interface {
	List() []beads.Bead
	Get(id string) (convoySnapshotResponse, error)
	Create(body convoyCreateRequest) (beads.Bead, error)
	AddItems(id string, items []string) error
	RemoveItems(id string, items []string) error
	Check(id string) (convoyCheckResponse, error)
	Close(id string) error
	Delete(id string) error
}

// convoyService is the default ConvoyService implementation.
type convoyService struct {
	s *Server
}

func (c *convoyService) List() []beads.Bead {
	return c.s.listConvoys()
}

func (c *convoyService) Get(id string) (convoySnapshotResponse, error) {
	return c.s.getConvoySnapshot(id)
}

func (c *convoyService) Create(body convoyCreateRequest) (beads.Bead, error) {
	return c.s.createConvoy(body)
}

func (c *convoyService) AddItems(id string, items []string) error {
	return c.s.convoyAddItems(id, items)
}

func (c *convoyService) RemoveItems(id string, items []string) error {
	return c.s.convoyRemoveItems(id, items)
}

func (c *convoyService) Check(id string) (convoyCheckResponse, error) {
	return c.s.convoyCheck(id)
}

func (c *convoyService) Close(id string) error {
	return c.s.convoyClose(id)
}

func (c *convoyService) Delete(id string) error {
	return c.s.convoyDelete(id)
}
