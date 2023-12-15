package implement

import (
	"errors"
	"fmt"
	"github.com/akley-MK4/file-message/common"
	"github.com/akley-MK4/file-message/logger"
	"github.com/akley-MK4/go-tools-box/filehandle"
	PCD "github.com/akley-MK4/pep-coroutine/define"
	PCI "github.com/akley-MK4/pep-coroutine/implement"
	"github.com/fsnotify/fsnotify"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

func NewResponder(basePath string, onReceivedMessage common.OnReceivedMessage) (retResponder *Responder, retErr error) {
	if onReceivedMessage == nil {
		retErr = errors.New("the onReceivedMessage function is a nil value")
		return
	}

	_, createBaseErr := filehandle.CreateDirectory(basePath)
	if createBaseErr != nil {
		retErr = fmt.Errorf("unable to create base directory %s, %v", basePath, createBaseErr)
		return
	}

	requestMsgDirPath := path.Join(basePath, common.RequestMsgDirName)
	_, createReqDirErr := filehandle.CreateDirectory(requestMsgDirPath)
	if createReqDirErr != nil {
		retErr = fmt.Errorf("unable to create request message directory %s, %v", requestMsgDirPath, createReqDirErr)
		return
	}

	responseMsgDirPath := path.Join(basePath, common.ResponseMsgDirName)
	_, createRespDirErr := filehandle.CreateDirectory(responseMsgDirPath)
	if createRespDirErr != nil {
		retErr = fmt.Errorf("unable to create response message directory %s, %v", responseMsgDirPath, createRespDirErr)
		return
	}

	w, newWatcherErr := fsnotify.NewWatcher()
	if newWatcherErr != nil {
		retErr = fmt.Errorf("unable to create watcher, %v", newWatcherErr)
		return
	}
	if err := w.Add(requestMsgDirPath); err != nil {
		retErr = err
		return
	}

	retResponder = &Responder{
		dirWatcher:         w,
		reqMsgInfoMap:      make(map[string]common.MessageInfo),
		onReceivedMessage:  onReceivedMessage,
		responseMsgDirPath: responseMsgDirPath,
		requestMsgDirPath:  requestMsgDirPath,
	}

	return
}

type Responder struct {
	requestMsgDirPath  string
	responseMsgDirPath string
	dirWatcher         *fsnotify.Watcher
	reqMsgInfoMap      map[string]common.MessageInfo
	rwMutex            sync.RWMutex
	onReceivedMessage  common.OnReceivedMessage
}

func (t *Responder) Start() error {
	return PCI.CreateAndStartStatelessCoroutine(common.CoroutineGroupResponder1, func(_ PCD.CoId, _ ...interface{}) bool {
		if err := t.listenRequestMessageFile(); err != nil {
			logger.GetLoggerInstance().WarningF("The listening request message file failed and will exit the listening process, %v", err)
		}

		return false
	})
}

func (t *Responder) listenRequestMessageFile() error {
	for {
		triggered, watchErr := t.watchDir()
		if watchErr != nil {
			return watchErr
		}
		if !triggered {
			continue
		}

		if err := t.checkAndProcessRequestMessageFile(); err != nil {
			logger.GetLoggerInstance().WarningF("Processing request message file failed, %v", err)
		}
	}
}

func (t *Responder) watchDir() (bool, error) {
	select {
	case err, ok := <-t.dirWatcher.Errors:
		if !ok {
			return false, err
		}
	case e, ok := <-t.dirWatcher.Events:
		if !ok {
			return false, errors.New("watcher.Events not ok")
		}

		switch e.Op {
		case fsnotify.Rename, fsnotify.Chmod, fsnotify.Create:
			return false, nil
		}
	}

	return true, nil
}

func (t *Responder) checkAndProcessRequestMessageFile() (retErr error) {
	var addedMsgInfos []common.MessageInfo
	dirEntryList, readDirErr := os.ReadDir(t.requestMsgDirPath)
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
			logger.GetLoggerInstance().WarningF("Failed to parse request message file name, %v", parseMsgFileNameErr)
			continue
		}
		if opType != common.RequestMsgOpType {
			logger.GetLoggerInstance().WarningF("Wrong request message file name OpType, FileName: %v", fileName)
			continue
		}

		msgInfo := common.MessageInfo{
			FileName:   fileName,
			FilePath:   path.Join(t.requestMsgDirPath, fileName),
			OpType:     opType,
			SeqNum:     seqNum,
			Nanosecond: nanosecond,
		}

		if !t.addReqMessageInfo(msgInfo) {
			logger.GetLoggerInstance().WarningF("Failed to add request message file with filename %v, It already exists", fileName)
			continue
		}
		addedMsgInfos = append(addedMsgInfos, msgInfo)
	}

	for _, msgInfo := range addedMsgInfos {
		if err := PCI.CreateAndStartStatelessCoroutine(common.CoroutineGroupResponder2, func(coID PCD.CoId, args ...interface{}) bool {
			inMsgInfo := args[0].(common.MessageInfo)
			if err := t.processRequestMessage(inMsgInfo); err != nil {
				logger.GetLoggerInstance().WarningF("Failed to process request message, %v", err)
			}
			return false
		}, msgInfo); err != nil {
			logger.GetLoggerInstance().WarningF("Failed to create coroutine for the processRequestMessage function, %v", err)
			continue
		}
	}

	return
}

func (t *Responder) addReqMessageInfo(msgInfo common.MessageInfo) bool {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	if _, exist := t.reqMsgInfoMap[msgInfo.FileName]; exist {
		return false
	}

	t.reqMsgInfoMap[msgInfo.FileName] = msgInfo
	return true
}

func (t *Responder) cleanUpRequestMessageInfo(fileName string) (retErr error) {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()
	msgInfo, exist := t.reqMsgInfoMap[fileName]
	if !exist {
		return
	}

	if err := os.Remove(msgInfo.FilePath); err != nil {
		retErr = err
		//return err
	}

	delete(t.reqMsgInfoMap, fileName)
	return
}

func (t *Responder) processRequestMessage(msgInfo common.MessageInfo) error {
	logger.GetLoggerInstance().DebugF("Start processing request message, FileName: %v, FilePath: %v, SeqNum: %v, Nanosecond: %v",
		msgInfo.FileName, msgInfo.FilePath, msgInfo.SeqNum, msgInfo.Nanosecond)
	msgData, readErr := ioutil.ReadFile(msgInfo.FilePath)
	if readErr != nil {
		return readErr
	}
	if len(msgData) <= 0 {
		return errors.New("empty file")
	}

	return t.dispatchMessage(msgInfo, msgData)
}

func (t *Responder) dispatchMessage(msgInfo common.MessageInfo, msgData []byte) error {
	respData := t.onReceivedMessage(msgData, msgInfo.SeqNum, msgInfo.Nanosecond)

	defer func() {
		if cleanErr := t.cleanUpRequestMessageInfo(msgInfo.FileName); cleanErr != nil {
			logger.GetLoggerInstance().WarningF("Cleaning request message file failed, %v", cleanErr)
			return
		}
		logger.GetLoggerInstance().DebugF("Successfully cleared the request message, FilePath: %v", msgInfo.FilePath)
	}()

	respMsgFileName := common.FmtResponseMessageFileName(msgInfo.SeqNum, msgInfo.Nanosecond)
	respMsgFilePath := path.Join(t.responseMsgDirPath, respMsgFileName)
	f, openErr := os.OpenFile(respMsgFilePath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if openErr != nil {
		return openErr
	}
	defer func() {
		if err := f.Close(); err != nil {
			logger.GetLoggerInstance().WarningF("Failed to close response message file %v, %v", respMsgFileName, err)
		}
	}()

	if _, err := f.Write(respData); err != nil {
		return err
	}

	logger.GetLoggerInstance().DebugF("Successfully written response file message file, %v", respMsgFilePath)
	return nil
}
