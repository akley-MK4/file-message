package implement

import (
	"errors"
	"github.com/akley-MK4/pep-coroutine/define"
	"sync"
	"time"
)

type coroutineGroupInfo struct {
	baseStatsHandler baseGroupStatsHandler
}

var (
	coGroupInfoMap  = make(map[define.CoGroup]*coroutineGroupInfo)
	regGroupRWMutex sync.RWMutex
)

func getCoroutineGroupInfo(group define.CoGroup) *coroutineGroupInfo {
	regGroupRWMutex.RLock()
	defer regGroupRWMutex.RUnlock()
	return coGroupInfoMap[group]
}

func setDefaultCoroutineGroupInfo(group define.CoGroup) *coroutineGroupInfo {
	regGroupRWMutex.Lock()
	defer regGroupRWMutex.Unlock()
	_, exist := coGroupInfoMap[group]
	if !exist {
		coGroupInfoMap[group] = &coroutineGroupInfo{}
		coGroupInfoMap[group].baseStatsHandler.createTime = time.Now()
	}
	return coGroupInfoMap[group]
}

func GetAllRegisteredGroup() (retGroups []define.CoGroup) {
	regGroupRWMutex.RLock()
	defer regGroupRWMutex.RUnlock()

	for g := range coGroupInfoMap {
		retGroups = append(retGroups, g)
	}
	return
}

func AddCoroutineGroupInfo(group define.CoGroup) error {
	regGroupRWMutex.Lock()
	defer regGroupRWMutex.Unlock()

	_, exist := coGroupInfoMap[group]
	if exist {
		return errors.New("group information already exists")
	}

	coGroupInfoMap[group] = &coroutineGroupInfo{}
	coGroupInfoMap[group].baseStatsHandler.createTime = time.Now()
	return nil
}

var (
	registerCoroutineTypesFunc = map[define.CoType]NewCoroutineFunc{
		define.TimerCoroutineType: func(coId define.CoId, coGroup define.CoGroup, interval time.Duration,
			handle define.CoroutineHandle, handleArgs ...interface{}) (ICoroutine, error) {

			co := &timerCoroutine{}
			co.baseCoroutine = newBaseCoroutine(coId, define.TimerCoroutineType, coGroup, interval, handle, handleArgs...)
			return co, nil
		},
	}
)
