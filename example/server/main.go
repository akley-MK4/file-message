package main

import (
	"github.com/akley-MK4/file-message/common"
	"github.com/akley-MK4/file-message/logger"
	"github.com/akley-MK4/file-message/server"
	"log"
	"os"
	"time"
)

func main() {

	if err := logger.SetLoggerInstance(newExampleLogger("[example]")); err != nil {
		log.Println("Failed to set logger instance, ", err.Error())
		os.Exit(1)
	}

	var cb common.OnReceivedMessage = func(reqMsgData []byte, seqNum uint64, nanosecond int) []byte {
		logger.GetLoggerInstance().DebugF("OnReceivedMessage. SeqNum: %v, Nanosecond: %v, ReqMsgData: %s",
			seqNum, nanosecond, string(reqMsgData))

		return []byte("res[333222")
	}
	svr, newSvrErr := server.NewServer("/tmp/filemsg_test", cb)
	if newSvrErr != nil {
		logger.GetLoggerInstance().ErrorF("Failed to create server, %v", newSvrErr.Error())
		os.Exit(1)
	}

	if err := svr.Start(); err != nil {
		logger.GetLoggerInstance().ErrorF("Failed to start server, %v", err.Error())
		os.Exit(1)
	}

	time.Sleep(time.Hour)
}
