package clusterpipes

import (
	"context"
	"fmt"

	"github.com/N1KlTA/go-cluster"
)

// This is a scratch implementation, it will be rewrited later
func RecoverMsgsAfter[T, PipeOut any](subpipe cluster.MsgPipe[T, PipeOut]) cluster.MsgPipe[T, T] {
	headerManager := cluster.NewHeaderManager[cluster.Msg[T]](true)
	return func(startupCtx context.Context, consumer cluster.MsgConsumer[T]) (cluster.MsgConsumer[T], error) {
		pipeOut, input := cluster.NewMsgConsumer[PipeOut]()
		pipeIn, err := subpipe.MapConsumer(startupCtx, pipeOut)
		if err != nil {
			consumer.Close(cluster.CauseCanceled)
			return nil, fmt.Errorf("failed to init subpipe: %w", err)
		}
		hmSession, err := headerManager.NewSession()
		if err != nil {
			consumer.Close(cluster.CauseCanceled)
			return nil, fmt.Errorf("failed to init header manager session: %w", err)
		}
		res := wrapConsumer(func(originalMsg Msg[T]) (Msg[T], bool) {
			newHeader, err := hmSession.EmmitHeader(originalMsg)
			if err != nil {
				pipeIn.Close(fmt.Errorf("failed to emmit subpipe header: %s", err))
				return Msg[T]{}, false
			}
			return Msg[T]{
				Header:  newHeader,
				Content: originalMsg.Content,
				Error:   originalMsg.Error,
			}, true
		}, pipeIn)

		session := &recoverSession[T, PipeOut]{
			hm:     headerManager,
			input:  input,
			output: consumer,
		}

		go func() {
			defer hmSession.Close(func(header []cluster.Msg[T]) error {
				return nil
			}) // just drop it
			stopCause := session.forwardRoutine()
			fmt.Println("GOT STOP FROM SUBPIPE")
			consumer.Close(stopCause)
		}()

		return res, nil
	}
}

type recoverSession[T, From any] struct {
	hm     cluster.HeaderManager[Msg[T]]
	input  cluster.Input[Msg[From]]
	output cluster.MsgConsumer[T]
}

func (s *recoverSession[T, From]) forwardRoutine() error {
	for transformedMsg := range s.input.Range() {
		var sendErr error
		if failCause := transformedMsg.Err(); failCause != nil {
			sendErr = s.hm.NackHeaderSource(transformedMsg.GetHeader(), s.ackMsgs, func(originalMsgs []Msg[T]) error {
				return s.nackMsgs(originalMsgs, failCause)
			})
		} else {
			sendErr = s.hm.AckHeader(transformedMsg.GetHeader(), s.ackMsgs)
		}
		if s.input.Context().Err() != nil {
			return s.input.GetStopCause()
		}
		if sendErr != nil {
			return fmt.Errorf("recover send failed unexpectedly: %s", sendErr)
		}
	}
	return s.input.GetStopCause()
}
func (s *recoverSession[T, From]) ackMsgs(originalMsgs []Msg[T]) error {
	for _, originalMsg := range originalMsgs {
		if err := s.output.Send(s.input.Context(), originalMsg); err != nil {
			return err
		}
	}
	return nil
}
func (s *recoverSession[T, From]) nackMsgs(originalMsgs []Msg[T], failCause error) error {
	for _, originalMsg := range originalMsgs {
		newMsg := cluster.MsgMap(originalMsg, func(T) (nilRes T, err error) {
			return nilRes, failCause
		})
		if err := s.output.Send(s.input.Context(), newMsg); err != nil {
			return err
		}
	}
	return nil
}
