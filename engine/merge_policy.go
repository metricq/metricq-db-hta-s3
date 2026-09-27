package engine

import "time"

// Large growing tails should gain at least 25% before being recompressed.
// A prefix which cannot fit the next block is finished immediately; sparse
// streams finish within one hour once their newest source is eligible. Zero
// cooldown explicitly retains aggressive consolidation for benchmarks/tools.
func mergeWorthwhile(group []BlockInfo, nextRecords int, newest int64, now time.Time, cooldown int64) bool {
	if cooldown == 0 || len(group) < 2 {
		return true
	}
	largest, total := 0, 0
	for _, block := range group {
		n := block.Entry.Records
		largest = max(largest, n)
		total += n
	}
	if largest < 256 || total-largest >= (largest+3)/4 || total == maxDataBlockRecords {
		return true
	}
	if nextRecords > 0 && total+nextRecords > maxDataBlockRecords {
		return true
	}
	return now.Sub(time.Unix(0, newest)) >= max(time.Hour, time.Duration(cooldown)*4*time.Second)
}
