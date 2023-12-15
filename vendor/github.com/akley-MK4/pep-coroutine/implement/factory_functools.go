package implement

import (
	"errors"
	"fmt"
	"github.com/akley-MK4/pep-coroutine/define"
	"github.com/akley-MK4/pep-coroutine/logger"
	"runtime/debug"
	"sync/atomic"
	"time"
)

var (
	coIncId uint64
)

func newCoroutineId() define.CoId {
	return define.CoId(atomic.AddUint64(&coIncId, 1))
}

func CreateCoroutine(coType define.CoType, coGroup define.CoGroup, interval time.Duration,
	handle define.CoroutineHandle, handleArgs ...interface{}) (retCo ICoroutine, retErr error) {

	groupInfo := setDefaultCoroutineGroupInfo(coGroup)
	if groupInfo == nil {
		retErr = fmt.Errorf("unknown coroutine group %v", coGroup)
		return
	}

	defer func() {
		if retErr != nil || retCo == nil {
			groupInfo.baseStatsHandler.addTotalFailedCreatNum(1)
			return
		}
		groupInfo.baseStatsHandler.addTotalSuccessfulCreatedNum(1)
	}()

	newCoFunc := registerCoroutineTypesFunc[coType]
	if newCoFunc == nil {
		retErr = fmt.Errorf("unknown coroutine type %v", coType)
		return
	}

	co, newCoErr := newCoFunc(newCoroutineId(), coGroup, interval, handle, handleArgs...)
	if newCoErr != nil {
		retErr = newCoErr
		return
	}
	if coType != co.GetType() {
		retErr = errors.New("the coroutine type used is inconsistent with the coroutine type created")
		return
	}
	statusPtr := co.getStatusPtr()
	if statusPtr == nil {
		retErr = errors.New("the status of the coroutine is a nil pointer")
		return
	}

	if !atomic.CompareAndSwapUint32(statusPtr, uint32(define.UncreatedCoroutineStatus), uint32(define.CreatedCoroutineStatus)) {
		retErr = errors.New("unable to create, incorrect state")
		return
	}

	retCo = co
	return
}

func StartCoroutine(co ICoroutine) (retErr error) {
	if co == nil {
		retErr = errors.New("the co is a nil value")
		return
	}

	groupInfo := getCoroutineGroupInfo(co.GetGroup())
	if groupInfo == nil {
		retErr = fmt.Errorf("unknown coroutine group %v", co.GetGroup())
		return
	}

	defer func() {
		if retErr != nil {
			groupInfo.baseStatsHandler.addTotalFailedStartNum(1)
			return
		}
	}()

	statusPtr := co.getStatusPtr()
	if statusPtr == nil {
		retErr = errors.New("the status of the coroutine is a nil pointer")
		return
	}

	if !atomic.CompareAndSwapUint32(statusPtr, uint32(define.CreatedCoroutineStatus), uint32(define.StartingCoroutineStatus)) {
		retErr = errors.New("unable to start, incorrect state")
		return
	}

	go scheduleCoroutine(co, groupInfo)
	return
}

func CloseCoroutine(co ICoroutine) (retErr error) {
	if co == nil {
		retErr = errors.New("the co is a nil value")
		return
	}

	groupInfo := getCoroutineGroupInfo(co.GetGroup())
	if groupInfo == nil {
		retErr = fmt.Errorf("unknown coroutine group %v", co.GetGroup())
		return
	}

	defer func() {
		if retErr != nil {
			groupInfo.baseStatsHandler.addTotalFailedCloseNum(1)
			return
		}
		groupInfo.baseStatsHandler.addTotalSuccessfulClosedNum(1)
	}()

	statusPtr := co.getStatusPtr()
	if statusPtr == nil {
		retErr = errors.New("the status of the coroutine is a nil pointer")
		return
	}

	if !atomic.CompareAndSwapUint32(statusPtr, uint32(define.StartedCoroutineStatus), uint32(define.ClosingCoroutineStatus)) {
		retErr = errors.New("unable to close, incorrect status")
		return
	}

	if err := co.close(); err != nil {
		retErr = fmt.Errorf("unable to close, %v", err)
		return
	}

	return
}

func scheduleCoroutine(co ICoroutine, groupInfo *coroutineGroupInfo) {
	groupInfo.baseStatsHandler.addTotalSuccessfulStartedNum(1)
	begTime := time.Now()

	//logger.GetLoggerInstance().DebugF("Coroutine %v starts scheduling, CoType: %v, CoGroup: %v",
	//	co.GetId(), co.GetType(), co.GetGroup())

	statusPtr := co.getStatusPtr()

	defer func() {
		if r := recover(); r != nil {
			if statusPtr != nil {
				atomic.StoreUint32(statusPtr, uint32(define.CrashedCoroutineStatus))
			}
			groupInfo.baseStatsHandler.addTotalCrashedScheduleNum(1)
			logger.GetLoggerInstance().ErrorF("Catch the exception, CoId: %v, CoType: %v, CoGroup: %v, Recover: %v, Stack: %v",
				co.GetId(), co.GetType(), co.GetGroup(), r, string(debug.Stack()))
		} else {
			groupInfo.baseStatsHandler.addTotalCompletedScheduleNum(1)
		}

		co.cleanUp()

		endTime := time.Now()
		durationMilliseconds := endTime.UnixMilli() - begTime.UnixMilli()
		if durationMilliseconds > 0 {
			groupInfo.baseStatsHandler.addTotalRunningDurationMilliseconds(uint64(durationMilliseconds))
		}
		durationMicroseconds := endTime.UnixMicro() - begTime.UnixMicro()
		if durationMicroseconds > 0 {
			groupInfo.baseStatsHandler.addTotalTotalRunningDurationMicroseconds(uint64(durationMicroseconds))
		}
	}()

	atomic.StoreUint32(statusPtr, uint32(define.StartedCoroutineStatus))
	co.run()
	atomic.StoreUint32(statusPtr, uint32(define.CompletedCoroutineStatus))

	//logger.GetLoggerInstance().DebugF("Coroutine %v exit scheduling, CoType: %v, CoGroup: %v", co.GetId(), co.GetType(), co.GetGroup())
}

func CreateAndStartStatelessCoroutine(coGroup define.CoGroup, handle define.CoroutineHandle, handleArgs ...interface{}) (retErr error) {
	groupInfo := setDefaultCoroutineGroupInfo(coGroup)
	if groupInfo == nil {
		retErr = fmt.Errorf("unknown coroutine group %v", coGroup)
		return
	}

	defer func() {
		if retErr != nil {
			groupInfo.baseStatsHandler.addTotalFailedCreatNum(1)
			return
		}
		groupInfo.baseStatsHandler.addTotalSuccessfulCreatedNum(1)
	}()

	if handle == nil {
		retErr = errors.New("the handle is a nil value")
		return
	}

	go scheduleStatelessCoroutine(newCoroutineId(), coGroup, groupInfo, handle, handleArgs...)

	return
}

func scheduleStatelessCoroutine(coId define.CoId, coGroup define.CoGroup, groupInfo *coroutineGroupInfo, handle define.CoroutineHandle, handleArgs ...interface{}) {
	groupInfo.baseStatsHandler.addTotalSuccessfulStartedNum(1)
	begTime := time.Now()
	//logger.GetLoggerInstance().DebugF("Coroutine %v starts scheduling, CoType: Stateless, CoGroup: %v", coId, coGroup)

	defer func() {
		if r := recover(); r != nil {
			groupInfo.baseStatsHandler.addTotalCrashedScheduleNum(1)
			logger.GetLoggerInstance().ErrorF("Catch the exception, CoId: %v, CoType: Stateless, CoGroup: %v, Recover: %v, Stack: %v",
				coId, coGroup, r, string(debug.Stack()))
		} else {
			groupInfo.baseStatsHandler.addTotalCompletedScheduleNum(1)
		}

		endTime := time.Now()
		durationMilliseconds := endTime.UnixMilli() - begTime.UnixMilli()
		if durationMilliseconds > 0 {
			groupInfo.baseStatsHandler.addTotalRunningDurationMilliseconds(uint64(durationMilliseconds))
		}
		durationMicroseconds := endTime.UnixMicro() - begTime.UnixMicro()
		if durationMicroseconds > 0 {
			groupInfo.baseStatsHandler.addTotalTotalRunningDurationMicroseconds(uint64(durationMicroseconds))
		}
	}()

	handle(coId, handleArgs...)
	//logger.GetLoggerInstance().DebugF("Coroutine %v exit scheduling, CoType: Stateless, CoGroup: %v", coId, coGroup)
}
