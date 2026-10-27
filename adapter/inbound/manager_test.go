package inbound

import (
	"context"
	"sync"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
)

type testInbound struct {
	tag    string
	access sync.Mutex
	stages []adapter.StartStage
	closed int
}

func (i *testInbound) Type() string { return "test" }
func (i *testInbound) Tag() string  { return i.tag }

func (i *testInbound) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	i.access.Lock()
	i.stages = append(i.stages, stage)
	i.access.Unlock()
	if stage == adapter.StartStateStart {
		scope.Add(func() error {
			i.access.Lock()
			i.closed++
			i.access.Unlock()
			return nil
		})
	}
	return nil
}

func (i *testInbound) state() (int, int) {
	i.access.Lock()
	defer i.access.Unlock()
	return len(i.stages), i.closed
}

type testRegistry struct {
	created []*testInbound
}

func (r *testRegistry) OptionTypes() []string            { return []string{"test"} }
func (r *testRegistry) CreateOptions(string) (any, bool) { return nil, true }
func (r *testRegistry) Create(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ string, _ any) (adapter.Inbound, error) {
	inbound := &testInbound{tag: tag}
	r.created = append(r.created, inbound)
	return inbound, nil
}

func startManager(t *testing.T, m *Manager, scope *adapter.Scope) {
	t.Helper()
	for _, stage := range adapter.ListStartStages {
		if err := m.Start(stage, scope); err != nil {
			t.Fatal(err)
		}
	}
}

func TestManagerHotReload(t *testing.T) {
	registry := &testRegistry{}
	m := NewManager(registry, nil)
	scope := adapter.NewScope(context.Background(), log.NewNOPFactory().Logger())

	if err := m.Create(context.Background(), nil, nil, "a", "test", nil); err != nil {
		t.Fatal(err)
	}
	if err := m.Create(context.Background(), nil, nil, "a", "test", nil); err == nil {
		t.Fatal("duplicate tag before start should fail")
	}
	startManager(t, m, scope)
	a := registry.created[0]
	if stages, closed := a.state(); stages != len(adapter.ListStartStages) || closed != 0 {
		t.Fatalf("a: stages=%d closed=%d", stages, closed)
	}

	// Created after start: started immediately through all stages
	if err := m.Create(context.Background(), nil, nil, "b", "test", nil); err != nil {
		t.Fatal(err)
	}
	b := registry.created[2]
	if stages, _ := b.state(); stages != len(adapter.ListStartStages) {
		t.Fatalf("b: stages=%d", stages)
	}

	// Remove closes only that inbound
	if err := m.Remove("b"); err != nil {
		t.Fatal(err)
	}
	if _, closed := b.state(); closed != 1 {
		t.Fatalf("b: closed=%d", closed)
	}
	if _, found := m.inboundByTag["b"]; found || len(m.Inbounds()) != 1 {
		t.Fatal("b still registered")
	}
	if _, closed := a.state(); closed != 0 {
		t.Fatal("a closed by removing b")
	}

	// Re-creating an existing tag after start replaces and closes the old one
	if err := m.Create(context.Background(), nil, nil, "a", "test", nil); err != nil {
		t.Fatal(err)
	}
	a2 := registry.created[3]
	if _, closed := a.state(); closed != 1 {
		t.Fatalf("old a: closed=%d", closed)
	}
	if got := m.inboundByTag["a"]; got != a2 || len(m.Inbounds()) != 1 {
		t.Fatal("a not replaced")
	}

	// Box exit closes the remaining inbound, and closing removed ones again is harmless
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if _, closed := a2.state(); closed != 1 {
		t.Fatalf("a2: closed=%d", closed)
	}
	if _, closed := b.state(); closed != 1 {
		t.Fatalf("b closed twice: %d", closed)
	}
	if err := m.Remove("missing"); err == nil {
		t.Fatal("removing a missing tag should fail")
	}
}
