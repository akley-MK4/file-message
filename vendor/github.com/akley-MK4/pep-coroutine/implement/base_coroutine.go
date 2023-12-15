package implement

import (
	"context"
	"github.com/akley-MK4/pep-coroutine/define"
	"time"
)

func newBaseCoroutine(coId define.CoId, coType define.CoType, coGroup define.CoGroup,
	interval time.Duration, handle define.CoroutineHandle, handleArgs ...interface{}) baseCoroutine {

	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	return baseCoroutine{
		id:                 coId,
		status:             uint32(define.UncreatedCoroutineStatus),
		tpy:                coType,
		group:              coGroup,
		interval:           interval,
		cancelCtx:          cancelCtx,
		cancelFunc:         cancelFunc,
		handle:             handle,
		handleArgs:         handleArgs,
		createdMillisecond: time.Now().UnixMilli(),
	}
}

type baseCoroutine struct {
	id         define.CoId
	status     uint32
	tpy        define.CoType
	group      define.CoGroup
	interval   time.Duration
	closed     bool
	cancelFunc context.CancelFunc
	cancelCtx  context.Context
	handle     define.CoroutineHandle
	handleArgs []interface{}

	createdMillisecond int64
}

func (t *baseCoroutine) GetId() define.CoId {
	return t.id
}

func (t *baseCoroutine) GetType() define.CoType {
	return t.tpy
}

func (t *baseCoroutine) GetGroup() define.CoGroup {
	return t.group
}

func (t *baseCoroutine) GetStatus() define.CoStatus {
	return define.CoStatus(t.status)
}

func (t *baseCoroutine) getStatusPtr() *uint32 {
	return &t.status
}

func (t *baseCoroutine) close() error {
	t.closed = true
	t.cancelFunc()
	return nil
}

func (t *baseCoroutine) GetCreatedMilliseconds() int64 {
	return t.createdMillisecond
}

func (t *baseCoroutine) run() {
	timer := time.NewTicker(t.interval)
loopEnd:
	for {
		select {
		case <-timer.C:
			if t.closed {
				break loopEnd
			}

			if !t.handle(t.id, t.handleArgs...) {
				break loopEnd
			}
			break
		case <-t.cancelCtx.Done():
			break loopEnd
		}
	}

	timer.Stop()
}

func (t *baseCoroutine) cleanUp() {
	t.handle = nil
	t.handleArgs = []interface{}{}
}
