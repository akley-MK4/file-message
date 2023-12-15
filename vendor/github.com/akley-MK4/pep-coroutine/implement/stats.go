package implement

import (
	"github.com/akley-MK4/pep-coroutine/define"
	"runtime"
	"sync/atomic"
	"time"
)

func FetchStats() (retStats Stats) {
	regGroupRWMutex.RLock()
	for group, groupInfo := range coGroupInfoMap {
		groupStats := groupInfo.baseStatsHandler.GetStats()
		groupStats.Group = group
		retStats.GroupStats = append(retStats.GroupStats, groupStats)
		retStats.CurrentMonitoredGoroutinesCount += groupStats.CurrentRunningCount
	}
	regGroupRWMutex.RUnlock()

	retStats.CurrentGoroutinesCount = runtime.NumGoroutine()
	retStats.CurrentUnmonitoredGoroutinesCount = retStats.CurrentGoroutinesCount - int(retStats.CurrentMonitoredGoroutinesCount)
	return
}

type Stats struct {
	CurrentGoroutinesCount            int              `json:"currentGoroutinesCount"`
	CurrentMonitoredGoroutinesCount   uint64           `json:"currentMonitoredGoroutinesCount"`
	CurrentUnmonitoredGoroutinesCount int              `json:"currentUnmonitoredGoroutinesCount"`
	GroupStats                        []BaseGroupStats `json:"groupStats"`
}

type BaseGroupStats struct {
	Group define.CoGroup `json:"group,omitempty"`

	TotalSuccessfulCreatedNum uint64 `json:"totalSuccessfulCreatedNum,omitempty"`
	TotalFailedCreatNum       uint64 `json:"totalFailedCreatNum,omitempty"`
	TotalSuccessfulStartedNum uint64 `json:"totalSuccessfulStartedNum,omitempty"`
	TotalFailedStartNum       uint64 `json:"totalFailedStartNum,omitempty"`
	TotalCrashedScheduleNum   uint64 `json:"totalCrashedScheduleNum,omitempty"`
	TotalCompletedScheduleNum uint64 `json:"totalCompletedScheduleNum,omitempty"`
	TotalStoppedNum           uint64 `json:"totalStoppedNum,omitempty"`

	TotalSuccessfulClosedNum uint64 `json:"totalSuccessfulClosedNum,omitempty"`
	TotalFailedCloseNum      uint64 `json:"totalFailedCloseNum,omitempty"`

	TotalRunningDurationMilliseconds uint64 `json:"totalRunningDurationMilliseconds,omitempty"`
	TotalRunningDurationMicroseconds uint64 `json:"totalRunningDurationMicroseconds,omitempty"`

	//////////////////////////////////
	CurrentRunningCount uint64 `json:"currentRunningCount,omitempty"`
	//MaxRunningDuration  uint64
	AvgRunningDurationMilliseconds uint64 `json:"avgRunningDurationMilliseconds,omitempty"`
	AvgRunningDurationMicroseconds uint64 `json:"avgRunningDurationMicroseconds,omitempty"`
}

type baseGroupStatsHandler struct {
	createTime time.Time
	stats      BaseGroupStats
}

func (t *baseGroupStatsHandler) GetStats() BaseGroupStats {
	t.stats.TotalStoppedNum = t.stats.TotalCompletedScheduleNum + t.stats.TotalCrashedScheduleNum
	t.stats.CurrentRunningCount = t.stats.TotalSuccessfulStartedNum - t.stats.TotalStoppedNum
	if t.stats.TotalStoppedNum > 0 {
		t.stats.AvgRunningDurationMilliseconds = t.stats.TotalRunningDurationMilliseconds / t.stats.TotalStoppedNum
		t.stats.AvgRunningDurationMicroseconds = t.stats.TotalRunningDurationMicroseconds / t.stats.TotalStoppedNum
	} else if t.stats.TotalSuccessfulStartedNum > 0 {
		nowTime := time.Now()
		t.stats.AvgRunningDurationMilliseconds = uint64(nowTime.UnixMilli() - t.createTime.UnixMilli())
		t.stats.AvgRunningDurationMicroseconds = uint64(nowTime.UnixMicro() - t.createTime.UnixMicro())
	}

	return t.stats
}

func (t *baseGroupStatsHandler) addTotalSuccessfulCreatedNum(delta uint64) {
	atomic.AddUint64(&t.stats.TotalSuccessfulCreatedNum, delta)
}

func (t *baseGroupStatsHandler) addTotalFailedCreatNum(delta uint64) {
	atomic.AddUint64(&t.stats.TotalFailedCreatNum, delta)
}

func (t *baseGroupStatsHandler) addTotalSuccessfulStartedNum(delta uint64) {
	atomic.AddUint64(&t.stats.TotalSuccessfulStartedNum, delta)
}

func (t *baseGroupStatsHandler) addTotalFailedStartNum(delta uint64) {
	atomic.AddUint64(&t.stats.TotalFailedStartNum, delta)
}

func (t *baseGroupStatsHandler) addTotalSuccessfulClosedNum(delta uint64) {
	atomic.AddUint64(&t.stats.TotalSuccessfulClosedNum, delta)
}

func (t *baseGroupStatsHandler) addTotalFailedCloseNum(delta uint64) {
	atomic.AddUint64(&t.stats.TotalFailedCloseNum, delta)
}

func (t *baseGroupStatsHandler) addTotalCrashedScheduleNum(delta uint64) {
	atomic.AddUint64(&t.stats.TotalCrashedScheduleNum, delta)
}

func (t *baseGroupStatsHandler) addTotalCompletedScheduleNum(delta uint64) {
	atomic.AddUint64(&t.stats.TotalCompletedScheduleNum, delta)
}

func (t *baseGroupStatsHandler) addTotalRunningDurationMilliseconds(delta uint64) {
	atomic.AddUint64(&t.stats.TotalRunningDurationMilliseconds, delta)
}

func (t *baseGroupStatsHandler) addTotalTotalRunningDurationMicroseconds(delta uint64) {
	atomic.AddUint64(&t.stats.TotalRunningDurationMicroseconds, delta)
}
