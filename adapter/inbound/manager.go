package inbound

import (
	"context"
	"os"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
)

var _ adapter.InboundManager = (*Manager)(nil)

type Manager struct {
	registry     adapter.InboundRegistry
	endpoint     adapter.EndpointManager
	access       sync.Mutex
	inbounds     []adapter.Inbound
	inboundByTag map[string]adapter.Inbound

	// BXtest hot reload: inbounds can be created and removed after the box has started
	scope      *adapter.Scope
	started    bool
	lifecycles map[adapter.Inbound]*inboundLifecycle
}

// inboundLifecycle gives each inbound a scope of its own, so that Remove can close it alone.
// The parent scope keeps a reference to the closed child until the box exits.
type inboundLifecycle struct {
	inbound adapter.Inbound
	scope   *adapter.Scope
}

func (l *inboundLifecycle) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	l.scope = scope
	return l.inbound.Start(stage, scope)
}

func NewManager(registry adapter.InboundRegistry, endpoint adapter.EndpointManager) *Manager {
	return &Manager{
		registry:     registry,
		endpoint:     endpoint,
		inboundByTag: make(map[string]adapter.Inbound),
		lifecycles:   make(map[adapter.Inbound]*inboundLifecycle),
	}
}

func (m *Manager) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	m.access.Lock()
	m.scope = scope
	inbounds := m.inbounds
	m.access.Unlock()
	for _, inbound := range inbounds {
		err := m.startInbound(scope, inbound, stage)
		if err != nil {
			return err
		}
	}
	if stage == adapter.StartStateStarted {
		m.access.Lock()
		m.started = true
		m.access.Unlock()
	}
	return nil
}

func (m *Manager) startInbound(scope *adapter.Scope, inbound adapter.Inbound, stage adapter.StartStage) error {
	m.access.Lock()
	lifecycle, loaded := m.lifecycles[inbound]
	if !loaded {
		lifecycle = &inboundLifecycle{inbound: inbound}
		m.lifecycles[inbound] = lifecycle
	}
	m.access.Unlock()
	return scope.Start("inbound/"+inbound.Type()+"["+inbound.Tag()+"]", lifecycle, stage)
}

func (m *Manager) closeInbound(inbound adapter.Inbound) error {
	m.access.Lock()
	lifecycle := m.lifecycles[inbound]
	delete(m.lifecycles, inbound)
	m.access.Unlock()
	if lifecycle == nil || lifecycle.scope == nil {
		return nil
	}
	return lifecycle.scope.Close()
}

func (m *Manager) Remove(tag string) error {
	m.access.Lock()
	inbound, found := m.inboundByTag[tag]
	if !found {
		m.access.Unlock()
		return os.ErrInvalid
	}
	delete(m.inboundByTag, tag)
	inbounds := make([]adapter.Inbound, 0, len(m.inbounds))
	for _, it := range m.inbounds {
		if it != inbound {
			inbounds = append(inbounds, it)
		}
	}
	m.inbounds = inbounds
	m.access.Unlock()
	return m.closeInbound(inbound)
}

func (m *Manager) Inbounds() []adapter.Inbound {
	m.access.Lock()
	defer m.access.Unlock()
	return m.inbounds
}

func (m *Manager) Get(tag string) (adapter.Inbound, bool) {
	m.access.Lock()
	inbound, found := m.inboundByTag[tag]
	m.access.Unlock()
	if found {
		return inbound, true
	}
	return m.endpoint.Get(tag)
}

func (m *Manager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) error {
	inbound, err := m.registry.Create(ctx, router, logger, tag, outboundType, options)
	if err != nil {
		return err
	}
	m.access.Lock()
	started, scope := m.started, m.scope
	if _, loaded := m.inboundByTag[tag]; loaded && !started {
		m.access.Unlock()
		return E.New("duplicate inbound tag: ", tag)
	}
	m.access.Unlock()
	if started {
		for _, stage := range adapter.ListStartStages {
			err = m.startInbound(scope, inbound, stage)
			if err != nil {
				_ = m.closeInbound(inbound)
				return err
			}
		}
	}
	m.access.Lock()
	existing, loaded := m.inboundByTag[tag]
	inbounds := make([]adapter.Inbound, 0, len(m.inbounds)+1)
	for _, it := range m.inbounds {
		if !loaded || it != existing {
			inbounds = append(inbounds, it)
		}
	}
	m.inbounds = append(inbounds, inbound)
	m.inboundByTag[tag] = inbound
	m.access.Unlock()
	if loaded {
		return m.closeInbound(existing)
	}
	return nil
}
