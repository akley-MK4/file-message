package common

type MessageInfo struct {
	FileName   string
	FilePath   string
	OpType     string
	SeqNum     uint64
	Nanosecond int64
}

type OnReceivedMessage func(msgData []byte, seqNum uint64, nanosecond int64) []byte
