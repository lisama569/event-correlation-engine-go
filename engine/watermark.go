package engine

import "time"

// watermarkTracker 跟踪事件时间水位：watermark = maxSeen - allowedLateness。
// 事件时间 <= watermark 的事件视为超出容忍范围的迟到事件。
type watermarkTracker struct {
	maxSeen         time.Time
	allowedLateness time.Duration
	initialized     bool
}

func newWatermarkTracker(allowedLateness time.Duration) *watermarkTracker {
	return &watermarkTracker{allowedLateness: allowedLateness}
}

// observe 观察一个事件时间，返回更新后的水位。
func (w *watermarkTracker) observe(ts time.Time) time.Time {
	if !w.initialized || ts.After(w.maxSeen) {
		w.maxSeen = ts
		w.initialized = true
	}
	return w.watermark()
}

// watermark 返回当前水位。
func (w *watermarkTracker) watermark() time.Time {
	if !w.initialized {
		return time.Time{}
	}
	return w.maxSeen.Add(-w.allowedLateness)
}

// isBeyondLateness 判断事件是否超出迟到容忍范围（早于或等于水位）。
func (w *watermarkTracker) isBeyondLateness(ts time.Time) bool {
	if !w.initialized {
		return false
	}
	return !ts.After(w.watermark())
}
