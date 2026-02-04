package cluster

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"weak"
)

var lastProducerId = atomic.Int64{}

func newProducerId() int64 {
	return lastProducerId.Add(1)
}

func newHeaderManager[ActualHeader any](allowParallelSessions bool) headerManager[ActualHeader] {
	res := headerManager[ActualHeader]{}
	if allowParallelSessions {
		res.impl = &asyncHeaderManagerImpl[ActualHeader]{
			factory:         newHeaderManagerSessionFactory[ActualHeader](),
			currentSessions: make(map[int64]weak.Pointer[headerManagerSession[ActualHeader]]),
		}
	} else {
		res.impl = &syncHeaderManagerImpl[ActualHeader]{
			factory: newHeaderManagerSessionFactory[ActualHeader](),
		}
	}
	return res
}

// must be async safe
type headerManagerImpl[ActualHeader any] interface {
	NewSession() (*headerManagerSession[ActualHeader], error)
	GetSession(sequenceId *SequenceId) (*headerManagerSession[ActualHeader], error)
	GetAllSessions() []*headerManagerSession[ActualHeader]
}

type headerManager[ActualHeader any] struct {
	impl headerManagerImpl[ActualHeader]
}

func (m headerManager[ActualHeader]) NewSession() (HeaderManagerSession[ActualHeader], error) {
	return m.impl.NewSession()
}
func (m headerManager[ActualHeader]) AckHeader(header *Header, commitFunc func(headers []ActualHeader) error) error {
	session, err := m.impl.GetSession(header.SequenceId)
	if err != nil {
		return err
	}
	return session.Commit(header.NewState, commitFunc)
}
func (m headerManager[ActualHeader]) NackHeaderSource(header *Header, ackFunc func(headers []ActualHeader) error, nackFunc func(headers []ActualHeader) error) error {
	session, err := m.impl.GetSession(header.SequenceId)
	if err != nil {
		return err
	}
	if err := session.Commit(header.Source.Start, ackFunc); err != nil {
		return err
	}
	if err := session.Commit(header.Source.End, nackFunc); err != nil {
		return err
	}
	return nil
}
func (m headerManager[ActualHeader]) GetSource(header *Header) ([]ActualHeader, error) {
	session, err := m.impl.GetSession(header.SequenceId)
	if err != nil {
		return nil, err
	}
	return session.GetSource(header.Source.Start, header.Source.End)
}
func (m headerManager[ActualHeader]) CloseAll(nackFunc func(headers []ActualHeader) error) error {
	sessions := m.impl.GetAllSessions()
	for _, session := range sessions {
		if err := session.Close(nackFunc); err != nil {
			return err
		}
	}
	return nil
}

type asyncHeaderManagerImpl[ActualHeader any] struct {
	factory         headerManagerSessionFactory[ActualHeader]
	currentSessions map[int64]weak.Pointer[headerManagerSession[ActualHeader]]
	mu              sync.Mutex
}

func (m *asyncHeaderManagerImpl[ActualHeader]) NewSession() (*headerManagerSession[ActualHeader], error) {
	newSession := m.factory.NewSession()
	newSessionId := newSession.GetSequenceID().SessionId
	m.mu.Lock()
	defer m.mu.Unlock()
	if prevVal, ok := m.currentSessions[newSessionId]; ok {
		prevSession := prevVal.Value()
		if prevSession != nil || !prevSession.IsClosed() {
			return nil, fmt.Errorf("how did you create more than %v sessions without closing first one, lol", math.MaxInt64)
		}
	}
	m.currentSessions[newSessionId] = weak.Make(newSession)
	return newSession, nil
}
func (m *asyncHeaderManagerImpl[ActualHeader]) GetSession(sequenceId *SequenceId) (*headerManagerSession[ActualHeader], error) {
	if sequenceId.ProducerId != m.factory.GetProducerId() {
		return nil, fmt.Errorf("wrong producer id: provided=%v, expected=%v", sequenceId.ProducerId, m.factory.GetProducerId())
	}
	m.mu.Lock()
	var session *headerManagerSession[ActualHeader]
	if sessionPtr, ok := m.currentSessions[sequenceId.SessionId]; ok {
		session = sessionPtr.Value()
	}
	m.mu.Unlock()
	if session == nil || session.IsClosed() {
		return nil, fmt.Errorf("no active session with id %v", sequenceId.SessionId)
	}
	return session, nil
}
func (m *asyncHeaderManagerImpl[ActualHeader]) GetAllSessions() []*headerManagerSession[ActualHeader] {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]*headerManagerSession[ActualHeader], len(m.currentSessions))
	for _, sessionPtr := range m.currentSessions {
		session := sessionPtr.Value()
		if session == nil || session.IsClosed() {
			continue
		}
		res = append(res, session)
	}
	return res
}

type syncHeaderManagerImpl[ActualHeader any] struct {
	factory        headerManagerSessionFactory[ActualHeader]
	currentSession weak.Pointer[headerManagerSession[ActualHeader]]
	mu             sync.Mutex
}

func (m *syncHeaderManagerImpl[ActualHeader]) NewSession() (*headerManagerSession[ActualHeader], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur := m.currentSession.Value(); cur != nil && !cur.IsClosed() {
		return nil, fmt.Errorf("previous session is still active")
	}
	newSession := m.factory.NewSession()
	m.currentSession = weak.Make(newSession)
	return newSession, nil
}
func (m *syncHeaderManagerImpl[ActualHeader]) GetSession(sequenceId *SequenceId) (*headerManagerSession[ActualHeader], error) {
	if sequenceId.ProducerId != m.factory.GetProducerId() {
		return nil, fmt.Errorf("wrong producer id: provided=%v, expected=%v", sequenceId.ProducerId, m.factory.GetProducerId())
	}
	m.mu.Lock()
	currentSession := m.currentSession.Value()
	m.mu.Unlock()
	if currentSession == nil || currentSession.GetSequenceID().SessionId != sequenceId.SessionId || currentSession.IsClosed() {
		return nil, fmt.Errorf("no active session with id %v", sequenceId.SessionId)
	}
	return currentSession, nil
}
func (m *syncHeaderManagerImpl[ActualHeader]) GetAllSessions() []*headerManagerSession[ActualHeader] {
	m.mu.Lock()
	currentSession := m.currentSession.Value()
	m.mu.Unlock()
	if currentSession == nil || currentSession.IsClosed() {
		return nil
	}
	return []*headerManagerSession[ActualHeader]{currentSession}
}
