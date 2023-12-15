package define

type CoId uint64
type CoType uint8
type CoGroup string
type CoStatus uint8

type CoroutineHandle func(coID CoId, args ...interface{}) bool
