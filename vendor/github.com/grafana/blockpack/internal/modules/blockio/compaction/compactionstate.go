package compaction

type compactionState struct {
	current      *writerState
	stagingDir   string
	stagedFiles  []string
	seenSpans    map[[24]byte]struct{}
	cfg          Config
	maxSpans     int
	outputSeq    int
	droppedSpans int64
}
