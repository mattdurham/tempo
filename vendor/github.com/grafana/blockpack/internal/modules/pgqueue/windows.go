package pgqueue

import "time"

// viBackfillWindowSizeSec is the fixed width of every vi_backfill job's window (issue #529).
const viBackfillWindowSizeSec = 60

// WindowsForRetention generates one WindowSpec per 1-minute window covering
// [newestEnd-retention, newestEnd), newest-first (Priority 0 for the newest window, increasing
// by 1 for each older one -- see WindowSpec's own doc comment for why that ordering can never
// starve other job types).
//
// Windows are aligned to a fixed epoch-second grid (every boundary is a multiple of 60),
// deterministically derived from now -- NOT "60 seconds before the exact instant this function
// was called". This is load-bearing: InsertViBackfillWindows is called repeatedly for the same
// trailing range on every compaction-planner tick (issue #529's "ongoing coverage" mechanism,
// no separate watermark), and relies on producing the IDENTICAL WindowSpec.EndSec for "the same
// real-world minute" every time so the dedup key correctly no-ops on repeat rather than
// inserting a slightly-offset near-duplicate window each tick.
//
// The newest window ends at the latest FULLY-ELAPSED minute boundary at or before now, never the
// still-in-progress current minute (which has incomplete source data).
func WindowsForRetention(retention time.Duration, now time.Time) []WindowSpec {
	newestEnd := (now.Unix() / viBackfillWindowSizeSec) * viBackfillWindowSizeSec
	return WindowsBefore(newestEnd, retention)
}

// WindowsBefore generates one WindowSpec per 1-minute window covering [newestEndSec-retention,
// newestEndSec), newest-first. newestEndSec must already be aligned to viBackfillWindowSizeSec
// (WindowsForRetention's own caller-facing wrapper handles that alignment; this lower-level
// entry point exists so a caller inserting a specific trailing range -- e.g.
// compactionplanner's periodic "last 5 minutes" top-up -- doesn't need to duplicate the
// alignment arithmetic).
func WindowsBefore(newestEndSec int64, retention time.Duration) []WindowSpec {
	windowCount := int64(retention.Seconds()) / viBackfillWindowSizeSec
	if int64(retention.Seconds())%viBackfillWindowSizeSec != 0 {
		windowCount++ // a partial trailing window still needs its own job
	}
	if windowCount <= 0 {
		return nil
	}

	windows := make([]WindowSpec, 0, windowCount)
	for i := int64(0); i < windowCount; i++ {
		end := newestEndSec - i*viBackfillWindowSizeSec
		windows = append(windows, WindowSpec{
			StartSec: end - viBackfillWindowSizeSec,
			EndSec:   end,
			Priority: i,
		})
	}
	return windows
}
