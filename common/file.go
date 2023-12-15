package common

import (
	"fmt"
	"strconv"
	"strings"
)

func FmtRequestMessageFileName(seqNum uint64, nanosecond int64) string {
	return fmt.Sprintf("%v-%d-%d", RequestMsgDirName, seqNum, nanosecond)
}

func FmtResponseMessageFileName(seqNum uint64, nanosecond int64) string {
	return fmt.Sprintf("%v-%d-%d", ResponseMsgDirName, seqNum, nanosecond)
}

func ParseMessageFileName(fileName string) (retOpType string, retSeqNum uint64, retNanosecond int64, retErr error) {
	splitList := strings.Split(fileName, "-")
	if len(splitList) < 3 {
		retErr = fmt.Errorf("invalied file name format, %v", fileName)
		return
	}

	seqNum, seqNumErr := strconv.Atoi(splitList[1])
	if seqNumErr != nil {
		retErr = fmt.Errorf("invalied seq num format, Name: %v, Err: %v", fileName, seqNumErr)
		return
	}

	nanosecond, nanosecondErr := strconv.Atoi(splitList[2])
	if nanosecondErr != nil {
		retErr = fmt.Errorf("invalied nanosecond format, Name: %v, Err: %v", fileName, nanosecondErr)
		return
	}

	retOpType = splitList[0]
	retSeqNum = uint64(seqNum)
	retNanosecond = int64(nanosecond)
	return
}
