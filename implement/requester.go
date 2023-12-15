package implement

import (
	"context"
	"errors"
	"fmt"
	"github.com/akley-MK4/file-message/common"
	"github.com/akley-MK4/file-message/logger"
	PCD "github.com/akley-MK4/pep-coroutine/define"
	PCI "github.com/akley-MK4/pep-coroutine/implement"
	"github.com/fsnotify/fsnotify"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

func NewRequester(basePath string) (retRequester *Requester, retErr error) {
	requestMsgDirPath := path.Join(basePath, common.RequestMsgDirName)
	responseMsgDirPath := path.Join(basePath, common.ResponseMsgDirName)

	for _, dirPath := range []string{
		requestMsgDirPath, responseMsgDirPath,
	} {
		_, statErr := os.Stat(dirPath)
		if os.IsNotExist(statErr) {
			retErr = fmt.Errorf("not existent directory path %v", dirPath)
			return
		}
	}

	w, newWatcherErr := fsnotify.NewWatcher()
	if newWatcherErr != nil {
		retErr = fmt.Errorf("unable to create watcher, %v", newWatcherErr)
		return
	}
	if err := w.Add(responseMsgDirPath); err != nil {
		retErr = err
		return
	}

	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	retRequester = &Requester{
		requestMsgDirPath:  requestMsgDirPath,
		responseMsgDirPath: responseMsgDirPath,
		reqMsgCtxMap:       make(map[string]*common.MessageContext),
		dirWatcher:         w,
		cancelCtx:          cancelCtx,
		cancelFunc:         cancelFunc,
	}

	return
}

type Requester struct {
	requestMsgDirPath  string
	responseMsgDirPath string
	incSeqNum          uint64

	cancelFunc context.CancelFunc
	cancelCtx  context.Context

	dirWatcher   *fsnotify.Watcher
	reqMsgCtxMap map[string]*common.MessageContext
	rwMutex      sync.RWMutex
}

func (t *Requester) Start() error {
	return PCI.CreateAndStartStatelessCoroutine(common.CoroutineGroupRequester1, func(_ PCD.CoId, _ ...interface{}) bool {
		if err := t.listenResponseMessageFile(); err != nil {
			logger.GetLoggerInstance().WarningF("The listening response message file failed and will exit the listening process, %v", err)
		}

		return false
	})
}

func (t *Requester) listenResponseMessageFile() error {
	for {
		triggered, continued, watchErr := t.watchDir()
		if watchErr != nil {
			return watchErr
		}
		if !continued {
			return nil
		}
		if !triggered {
			continue
		}

		if err := t.checkAndProcessResponseMessageFile(); err != nil {
			logger.GetLoggerInstance().WarningF("Processing request message file failed, %v", err)
		}
	}
}

func (t *Requester) watchDir() (retTriggered bool, retContinued bool, retErr error) {
	select {
	case <-t.cancelCtx.Done():
		retContinued = false
		return
	case err, ok := <-t.dirWatcher.Errors:
		if !ok {
			retErr = err
			return
		}
	case e, ok := <-t.dirWatcher.Events:
		if !ok {
			retErr = errors.New("watcher.Events not ok")
			return
		}

		switch e.Op {
		case fsnotify.Rename, fsnotify.Chmod, fsnotify.Create, fsnotify.Remove:
			retContinued = true
			return
		}
	}

	retTriggered = true
	retContinued = true
	return
}

func (t *Requester) checkAndProcessResponseMessageFile() (retErr error) {
	dirEntryList, readDirErr := os.ReadDir(t.responseMsgDirPath)
	if readDirErr != nil {
		return readDirErr
	}

	for _, dirEntry := range dirEntryList {
		if dirEntry.IsDir() {
			continue
		}

		info, infoErr := dirEntry.Info()
		if infoErr != nil {
			logger.GetLoggerInstance().WarningF("Failed to get file info, DirEntry: %v, Err: %v", dirEntry.Name(), infoErr)
			continue
		}

		fileName := info.Name()
		opType, seqNum, nanosecond, parseMsgFileNameErr := common.ParseMessageFileName(fileName)
		if parseMsgFileNameErr != nil {
			logger.GetLoggerInstance().WarningF("Failed to parse response message file name, %v", parseMsgFileNameErr)
			continue
		}
		if opType != common.ResponseMsgOpType {
			logger.GetLoggerInstance().WarningF("Wrong response message file name OpType, FileName: %v", fileName)
			continue
		}

		ctx := t.getRequestMessageContext(common.FmtRequestMessageFileName(seqNum, nanosecond))
		if ctx == nil {
			logger.GetLoggerInstance().WarningF("The response message file does not have a request message context, FilePath: %v",
				path.Join(t.responseMsgDirPath, fileName))
			continue
		}

		if err := ctx.PushEvent(1); err != nil {
			logger.GetLoggerInstance().WarningF("Failed to publish event in response to message file, %v", err)
			continue
		}
	}

	return
}

func (t *Requester) newSeqNum() uint64 {
	return atomic.AddUint64(&t.incSeqNum, 1)
}

func (t *Requester) getRequestMessageContext(fileName string) *common.MessageContext {
	t.rwMutex.RLock()
	defer t.rwMutex.RUnlock()
	return t.reqMsgCtxMap[fileName]
}

func (t *Requester) addNewRequestMessageContext() (*common.MessageContext, error) {
	reqMsgInfo := t.newRequestMessageInfo()

	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	if _, exist := t.reqMsgCtxMap[reqMsgInfo.FileName]; exist {
		return nil, errors.New("repeatedly adding specified context")
	}

	t.reqMsgCtxMap[reqMsgInfo.FileName] = common.NewMessageContext(reqMsgInfo)
	return t.reqMsgCtxMap[reqMsgInfo.FileName], nil
}

func (t *Requester) newRequestMessageInfo() *common.MessageInfo {
	seqNum := t.newSeqNum()
	nanosecond := time.Now().UnixNano()
	reqMsgFileName := common.FmtRequestMessageFileName(seqNum, nanosecond)
	reqMsgFilePath := path.Join(t.requestMsgDirPath, reqMsgFileName)

	reqMsgInfo := &common.MessageInfo{
		FileName:   reqMsgFileName,
		FilePath:   reqMsgFilePath,
		OpType:     common.RequestMsgOpType,
		SeqNum:     seqNum,
		Nanosecond: nanosecond,
	}

	return reqMsgInfo
}

func (t *Requester) Send() error {
	return errors.New("not implemented")
}

func (t *Requester) Call(reqMsgData []byte, timeout time.Duration) (retRespData []byte, retErr error) {
	if len(reqMsgData) <= 0 {
		retErr = errors.New("empty request message data")
		return
	}

	ctx, addCtxErr := t.addNewRequestMessageContext()
	if addCtxErr != nil {
		retErr = addCtxErr
		return
	}

	if err := t.writeRequestMessageInfo(ctx.GetMessageInfo(), reqMsgData); err != nil {
		retErr = err
		return
	}

	_, popErr := ctx.PopEventWithTimeout(timeout)
	if popErr != nil {
		retErr = popErr
		return
	}

	retRespData, retErr = t.processResponseMessageFile(ctx)
	return
}

func (t *Requester) writeRequestMessageInfo(msgInfo *common.MessageInfo, msgData []byte) error {
	f, openErr := os.OpenFile(msgInfo.FilePath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if openErr != nil {
		return openErr
	}
	defer func() {
		if err := f.Close(); err != nil {
			logger.GetLoggerInstance().WarningF("Failed to close request message file %v, %v", msgInfo.FilePath, err)
		}
	}()

	_, retErr := f.Write(msgData)
	return retErr
}

func (t *Requester) processResponseMessageFile(ctx *common.MessageContext) (retRespData []byte, retErr error) {
	msgInfo := ctx.GetMessageInfo()

	respMsgFileName := path.Join(t.responseMsgDirPath, common.FmtResponseMessageFileName(msgInfo.SeqNum, msgInfo.Nanosecond))
	msgData, readErr := ioutil.ReadFile(respMsgFileName)
	if readErr != nil {
		retErr = readErr
		return
	}

	retRespData = msgData
	if err := os.Remove(respMsgFileName); err != nil {
		logger.GetLoggerInstance().WarningF("Failed to remove the response message file path %v, %v", msgInfo.FilePath, err)
		return
	}
	logger.GetLoggerInstance().DebugF("Removed the response message file path %v", msgInfo.FilePath)
	return
}
