package define

const (
	UncreatedCoroutineStatus CoStatus = iota
	CreatedCoroutineStatus
	StartingCoroutineStatus
	StartedCoroutineStatus
	ClosingCoroutineStatus
	CompletedCoroutineStatus
	CrashedCoroutineStatus
)

const (
	TimerCoroutineType CoType = iota
)
