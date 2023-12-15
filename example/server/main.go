package main

import (
	"fmt"
	"github.com/akley-MK4/file-message/common"
	"github.com/akley-MK4/file-message/implement"
	"github.com/akley-MK4/file-message/logger"
	PCL "github.com/akley-MK4/pep-coroutine/logger"
	"log"
	"os"
	"runtime"
	"time"
)

func main() {

	if err := logger.SetLoggerInstance(newExampleLogger("[example-server]")); err != nil {
		log.Println("Failed to set logger instance, ", err.Error())
		os.Exit(1)
	}

	if err := PCL.SetLoggerInstance(logger.GetLoggerInstance()); err != nil {
		log.Println("Failed to set pep-coroutine lib logger instance, ", err.Error())
		os.Exit(1)
	}

	var cb common.OnReceivedMessage = func(msgData []byte, seqNum uint64, nanosecond int64) []byte {
		logger.GetLoggerInstance().DebugF("OnReceivedMessage. SeqNum: %v, Nanosecond: %v, ReqMsgData: %s",
			seqNum, nanosecond, string(msgData))

		respMsgData := []byte(fmt.Sprintf("server-resp-%d", time.Now().UnixNano()))
		return respMsgData
	}
	svr, newSvrErr := implement.NewResponder("/tmp/filemsg_test", cb)
	if newSvrErr != nil {
		logger.GetLoggerInstance().ErrorF("Failed to create server, %v", newSvrErr.Error())
		os.Exit(1)
	}

	printMemoryStatus()

	if err := svr.Start(); err != nil {
		logger.GetLoggerInstance().ErrorF("Failed to start server, %v", err.Error())
		os.Exit(1)
	}

	time.Sleep(time.Second * 60)
	runtime.GC()
	time.Sleep(time.Second)
	printMemoryStatus()

}
