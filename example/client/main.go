package main

import (
	"fmt"
	"github.com/akley-MK4/file-message/implement"
	"github.com/akley-MK4/file-message/logger"
	PCL "github.com/akley-MK4/pep-coroutine/logger"
	"log"
	"os"
	"runtime"
	"time"
)

func main() {
	if err := logger.SetLoggerInstance(newExampleLogger("[example-client]")); err != nil {
		log.Println("Failed to set logger instance, ", err.Error())
		os.Exit(1)
	}

	if err := PCL.SetLoggerInstance(logger.GetLoggerInstance()); err != nil {
		log.Println("Failed to set pep-coroutine lib logger instance, ", err.Error())
		os.Exit(1)
	}

	requester, newRequesterErr := implement.NewRequester("/tmp/filemsg_test")
	if newRequesterErr != nil {
		logger.GetLoggerInstance().ErrorF("Failed to create a request, %v", newRequesterErr)
		os.Exit(1)
	}

	if err := requester.Start(); err != nil {
		logger.GetLoggerInstance().ErrorF("Failed to start the requester, %v", err)
		os.Exit(1)
	}

	printMemoryStatus()

	for i := 0; i < 100; i++ {
		reqMsgData := []byte(fmt.Sprintf("client-req-%d", time.Now().UnixNano()))
		respData, callErr := requester.Call(reqMsgData, time.Second*5)
		if callErr != nil {
			logger.GetLoggerInstance().ErrorF("Failed to call server, %v", callErr)
			continue
		}
		logger.GetLoggerInstance().DebugF("Received response message: %v", string(respData))
	}

	runtime.GC()
	time.Sleep(time.Second * 5)
	printMemoryStatus()

	time.Sleep(time.Second)
	os.Exit(0)
}
