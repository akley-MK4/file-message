package common

import (
	"errors"
	"sync/atomic"
	"time"
)

func NewMessageContext(msgInfo *MessageInfo) *MessageContext {
	return &MessageContext{
		msgInfo:   msgInfo,
		eventHook: make(chan uint8, 1),
	}
}

type MessageContext struct {
	msgInfo   *MessageInfo
	eventHook chan uint8
	hookFlag  uint32
}

func (t *MessageContext) GetMessageInfo() *MessageInfo {
	return t.msgInfo
}

func (t *MessageContext) PushEvent(event uint8) error {
	if !atomic.CompareAndSwapUint32(&t.hookFlag, 0, 1) {
		return errors.New("invalid event hook")
	}
	t.eventHook <- event
	return nil
}

func (t *MessageContext) GetEventHook() chan uint8 {
	return t.eventHook
}

func (t *MessageContext) PopEventWithTimeout(timeout time.Duration) (retEvent uint8, retErr error) {
	timer := time.NewTimer(timeout)

loopEnd:
	for {
		select {
		case event, ok := <-t.eventHook:
			if !ok {
				retErr = errors.New("invalid event hook")
			}
			retEvent = event
			break loopEnd
		case <-timer.C:
			retErr = errors.New("request timeout")
			break loopEnd
		}
	}

	timer.Stop()
	return
}
