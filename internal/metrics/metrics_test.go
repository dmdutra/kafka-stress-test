package metrics

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMetricsRecordAndSnapshot(t *testing.T) {
	m := NewMetrics(false)

	m.RecordSuccess(5*time.Millisecond, 100)
	m.RecordSuccess(15*time.Millisecond, 200)
	m.RecordSuccess(10*time.Millisecond, 300)
	m.RecordFailure(errors.New("connection reset"))

	snap := m.Tick()
	if snap.TotalSent != 3 {
		t.Errorf("expected 3 total sent, got %d", snap.TotalSent)
	}
	if snap.TotalBytes != 600 {
		t.Errorf("expected 600 bytes, got %d", snap.TotalBytes)
	}
	if snap.TotalErrors != 1 {
		t.Errorf("expected 1 error, got %d", snap.TotalErrors)
	}
	if snap.LastError != "connection reset" {
		t.Errorf("expected 'connection reset', got '%s'", snap.LastError)
	}

	if snap.MinLatency != 5*time.Millisecond {
		t.Errorf("expected min latency 5ms, got %v", snap.MinLatency)
	}
	if snap.MaxLatency != 15*time.Millisecond {
		t.Errorf("expected max latency 15ms, got %v", snap.MaxLatency)
	}

	summary := snap.FormatSummary()
	if !strings.Contains(summary, "TEST SUMMARY") {
		t.Errorf("expected summary header in output")
	}
	if !strings.Contains(summary, "connection reset") {
		t.Errorf("expected error in summary")
	}

	statusLine := snap.FormatSnapshot()
	if !strings.Contains(statusLine, "Sent: 3") {
		t.Errorf("expected status line to contain 'Sent: 3'")
	}
}
