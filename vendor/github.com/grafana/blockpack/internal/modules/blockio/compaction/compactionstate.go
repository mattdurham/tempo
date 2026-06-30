package compaction

type compactionState struct {
	current      *writerState
	seenSpans    map[[24]byte]struct{}
	stagingDir   string
	stagedFiles  []string
	cfg          Config
	maxSpans     int
	outputSeq    int
	droppedSpans int64
}
