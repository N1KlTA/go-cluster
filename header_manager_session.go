package cluster

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
)

func newHeaderManagerSessionFactory[ActualHeader any]() headerManagerSessionFactory[ActualHeader] {
	return headerManagerSessionFactory[ActualHeader]{
		sessionIdFactory: atomic.Int64{},
		producerId:       newProducerId(),
	}
}

type headerManagerSessionFactory[ActualHeader any] struct {
	sessionIdFactory atomic.Int64
	producerId       int64
}

func (f *headerManagerSessionFactory[ActualHeader]) NewSession() *headerManagerSession[ActualHeader] {
	return &headerManagerSession[ActualHeader]{
		sequenceId: &SequenceId{
			SessionId:  f.sessionIdFactory.Add(1),
			ProducerId: f.producerId,
		},
	}
}
func (f *headerManagerSessionFactory[ActualHeader]) GetProducerId() int64 {
	return f.producerId
}

type headerManagerSession[ActualHeader any] struct {
	headerQueue []ActualHeader // protected by mu
	firstIndex  uint64         // protected by mu
	closed      bool           // protected by mu
	sequenceId  *SequenceId    // immutable
	mu          sync.Mutex
	commitMu    sync.Mutex
}

func (s *headerManagerSession[ActualHeader]) EmmitHeader(actual ActualHeader) (*Header, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("session closed")
	}
	prevState := s.firstIndex + uint64(len(s.headerQueue))
	s.headerQueue = append(s.headerQueue, actual)
	s.mu.Unlock()
	return &Header{
		NewState: prevState + 1,

		Source: struct {
			Start uint64
			End   uint64
		}{
			Start: prevState,
			End:   prevState + 1,
		},

		SequenceId: s.sequenceId,
	}, nil
}
func (s *headerManagerSession[ActualHeader]) GetSequenceID() *SequenceId {
	return s.sequenceId
}
func (s *headerManagerSession[ActualHeader]) GetSource(startState, endState uint64) ([]ActualHeader, error) {
	if startState > endState {
		return nil, fmt.Errorf("incorrect input start state (%v) > end state (%v)", startState, endState)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, fmt.Errorf("session closed")
	}
	if startState < s.firstIndex {
		return nil, fmt.Errorf("source already deleted: current first index = %v, wanted = %v", s.firstIndex, startState)
	}
	if curLast := s.firstIndex + uint64(len(s.headerQueue)); endState > curLast {
		return nil, fmt.Errorf("incorrect end index: current last index = %v, wanted = %v", curLast, endState)
	}
	start := int(startState - s.firstIndex)
	end := int(endState - s.firstIndex)
	return slices.Clone(s.headerQueue[start:end]), nil
}
func (s *headerManagerSession[ActualHeader]) getCommitData(newState uint64) ([]ActualHeader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, fmt.Errorf("session closed")
	}
	if newState < s.firstIndex {
		return []ActualHeader{}, fmt.Errorf("already commited")
	}
	if newState > s.firstIndex+uint64(len(s.headerQueue)) {
		return []ActualHeader{}, fmt.Errorf("commit state exceeds emmited messages count")
	}
	return slices.Clone(s.headerQueue[:int(newState-s.firstIndex)]), nil
}
func (s *headerManagerSession[ActualHeader]) commitState(newState uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range int(newState - s.firstIndex) { // to detach resources
		var nilHeader ActualHeader
		s.headerQueue[i] = nilHeader
	}
	s.headerQueue = s.headerQueue[int(newState-s.firstIndex):]
	s.firstIndex = newState
	return nil
}
func (s *headerManagerSession[ActualHeader]) Commit(newState uint64, commitFunc func([]ActualHeader) error) error {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	commitData, err := s.getCommitData(newState)
	if err != nil {
		return err
	}
	if err := commitFunc(commitData); err != nil {
		return err
	}
	return s.commitState(newState)
}
func (s *headerManagerSession[ActualHeader]) Close(closeFunc func([]ActualHeader) error) error {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed { // already closed
		return nil
	}
	sliceToCancel := slices.Clone(s.headerQueue)
	if err := closeFunc(sliceToCancel); err != nil {
		return err
	}
	sliceToCancel = nil
	s.closed = true
	return nil
}
func (s *headerManagerSession[ActualHeader]) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}
