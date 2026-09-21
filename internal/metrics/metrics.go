package metrics

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects and calculates throughput and latency statistics.
type Metrics struct {
	mu           sync.Mutex
	startTime    time.Time
	endTime      time.Time
	sentMessages atomic.Int64
	sentBytes    atomic.Int64
	failedMsgs   atomic.Int64
	lastError    atomic.Value // string

	// Latencies recorded (sample reservoir for percentile computation)
	latencies []time.Duration

	// Window tracking for instantaneous rates
	lastTickTime     time.Time
	lastTickMessages int64
	lastTickBytes    int64
}

// Snapshot represents a point-in-time view of the test metrics.
type Snapshot struct {
	Elapsed         time.Duration
	TotalSent       int64
	TotalBytes      int64
	TotalErrors     int64
	LastError       string
	CurrentMsgRate  float64
	CurrentMBRate   float64
	AverageMsgRate  float64
	AverageMBRate   float64
	P50Latency      time.Duration
	P90Latency      time.Duration
	P95Latency      time.Duration
	P99Latency      time.Duration
	MinLatency      time.Duration
	MaxLatency      time.Duration
	AvgLatency      time.Duration
}

// NewMetrics initializes a new Metrics tracker.
func NewMetrics() *Metrics {
	now := time.Now()
	m := &Metrics{
		startTime:    now,
		lastTickTime: now,
		latencies:    make([]time.Duration, 0, 500000),
	}
	m.lastError.Store("")
	return m
}

// RecordSuccess records a successfully produced message.
func (m *Metrics) RecordSuccess(latency time.Duration, bytesLen int) {
	m.sentMessages.Add(1)
	m.sentBytes.Add(int64(bytesLen))

	m.mu.Lock()
	// Cap reservoir sample size to prevent unbounded memory growth in long tests
	if len(m.latencies) < 1000000 {
		m.latencies = append(m.latencies, latency)
	}
	m.mu.Unlock()
}

// RecordFailure records a failure to produce a message.
func (m *Metrics) RecordFailure(err error) {
	m.failedMsgs.Add(1)
	if err != nil {
		m.lastError.Store(err.Error())
	}
}

// Stop marks the test as finished.
func (m *Metrics) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.endTime.IsZero() {
		m.endTime = time.Now()
	}
}

// Tick calculates the instantaneous rates since the last tick and returns a Snapshot.
func (m *Metrics) Tick() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(m.startTime)
	tickDuration := now.Sub(m.lastTickTime).Seconds()

	totalSent := m.sentMessages.Load()
	totalBytes := m.sentBytes.Load()
	totalErrors := m.failedMsgs.Load()

	deltaMsgs := totalSent - m.lastTickMessages
	deltaBytes := totalBytes - m.lastTickBytes

	var curMsgRate, curMBRate float64
	if tickDuration > 0 {
		curMsgRate = float64(deltaMsgs) / tickDuration
		curMBRate = (float64(deltaBytes) / (1024 * 1024)) / tickDuration
	}

	var avgMsgRate, avgMBRate float64
	if elapsed.Seconds() > 0 {
		avgMsgRate = float64(totalSent) / elapsed.Seconds()
		avgMBRate = (float64(totalBytes) / (1024 * 1024)) / elapsed.Seconds()
	}

	m.lastTickTime = now
	m.lastTickMessages = totalSent
	m.lastTickBytes = totalBytes

	lastErr, _ := m.lastError.Load().(string)

	snap := Snapshot{
		Elapsed:        elapsed,
		TotalSent:      totalSent,
		TotalBytes:     totalBytes,
		TotalErrors:    totalErrors,
		LastError:      lastErr,
		CurrentMsgRate: curMsgRate,
		CurrentMBRate:  curMBRate,
		AverageMsgRate: avgMsgRate,
		AverageMBRate:  avgMBRate,
	}

	m.calculateLatencyPercentiles(&snap)
	return snap
}

// FinalSnapshot computes the summary over the whole run.
func (m *Metrics) FinalSnapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	end := m.endTime
	if end.IsZero() {
		end = time.Now()
	}
	elapsed := end.Sub(m.startTime)

	totalSent := m.sentMessages.Load()
	totalBytes := m.sentBytes.Load()
	totalErrors := m.failedMsgs.Load()

	var avgMsgRate, avgMBRate float64
	if elapsed.Seconds() > 0 {
		avgMsgRate = float64(totalSent) / elapsed.Seconds()
		avgMBRate = (float64(totalBytes) / (1024 * 1024)) / elapsed.Seconds()
	}

	lastErr, _ := m.lastError.Load().(string)

	snap := Snapshot{
		Elapsed:        elapsed,
		TotalSent:      totalSent,
		TotalBytes:     totalBytes,
		TotalErrors:    totalErrors,
		LastError:      lastErr,
		AverageMsgRate: avgMsgRate,
		AverageMBRate:  avgMBRate,
	}

	m.calculateLatencyPercentiles(&snap)
	return snap
}

func (m *Metrics) calculateLatencyPercentiles(snap *Snapshot) {
	n := len(m.latencies)
	if n == 0 {
		return
	}

	sorted := make([]time.Duration, n)
	copy(sorted, m.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	snap.MinLatency = sorted[0]
	snap.MaxLatency = sorted[n-1]
	snap.P50Latency = sorted[n*50/100]
	snap.P90Latency = sorted[n*90/100]
	snap.P95Latency = sorted[n*95/100]
	snap.P99Latency = sorted[n*99/100]

	var sum time.Duration
	for _, lat := range sorted {
		sum += lat
	}
	snap.AvgLatency = sum / time.Duration(n)
}

// FormatSnapshot produces a concise one-line status string.
func (s Snapshot) FormatSnapshot() string {
	mins := int(s.Elapsed.Seconds()) / 60
	secs := int(s.Elapsed.Seconds()) % 60

	return fmt.Sprintf("[%02d:%02d] Sent: %d | Rate: %.0f msg/s (%.2f MB/s) | Latency: p50=%v p95=%v p99=%v | Errors: %d",
		mins, secs,
		s.TotalSent,
		s.CurrentMsgRate,
		s.CurrentMBRate,
		roundDuration(s.P50Latency),
		roundDuration(s.P95Latency),
		roundDuration(s.P99Latency),
		s.TotalErrors,
	)
}

// FormatSummary produces a detailed multiline summary.
func (s Snapshot) FormatSummary() string {
	var errDetails string
	if s.TotalErrors > 0 && s.LastError != "" {
		errDetails = fmt.Sprintf("\n  Last Error: %s", s.LastError)
	}

	return fmt.Sprintf(`
========================== TEST SUMMARY ==========================
  Duration:           %s
  Total Messages:     %d
  Total Data:         %.2f MB
  Total Errors:       %d%s

  Throughput:
    Avg Msg Rate:     %.0f msg/s
    Avg Data Rate:    %.2f MB/s

  Latency:
    Min:              %v
    Avg:              %v
    p50 (Median):     %v
    p90:              %v
    p95:              %v
    p99:              %v
    Max:              %v
==================================================================`,
		s.Elapsed.Round(time.Millisecond),
		s.TotalSent,
		float64(s.TotalBytes)/(1024*1024),
		s.TotalErrors,
		errDetails,
		s.AverageMsgRate,
		s.AverageMBRate,
		roundDuration(s.MinLatency),
		roundDuration(s.AvgLatency),
		roundDuration(s.P50Latency),
		roundDuration(s.P90Latency),
		roundDuration(s.P95Latency),
		roundDuration(s.P99Latency),
		roundDuration(s.MaxLatency),
	)
}

func roundDuration(d time.Duration) time.Duration {
	if d > time.Millisecond {
		return d.Round(100 * time.Microsecond)
	}
	return d.Round(time.Microsecond)
}
