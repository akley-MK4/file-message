package implement

import (
	"github.com/akley-MK4/pep-coroutine/define"
	"time"
)

type NewCoroutineFunc func(coId define.CoId, coGroup define.CoGroup, interval time.Duration,
	handle define.CoroutineHandle, handleArgs ...interface{}) (ICoroutine, error)

type ICoroutine interface {
	GetId() define.CoId
	GetType() define.CoType
	GetGroup() define.CoGroup
	GetStatus() define.CoStatus
	getStatusPtr() *uint32
	run()
	cleanUp()
	close() error
	GetCreatedMilliseconds() int64
}
