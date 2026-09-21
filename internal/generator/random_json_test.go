package generator

import (
	"encoding/json"
	"testing"
)

func TestRandomJSONGenerator(t *testing.T) {
	gen := NewRandomJSONGenerator(0)

	for i := int64(0); i < 50; i++ {
		data, err := gen.Generate(i)
		if err != nil {
			t.Fatalf("unexpected error generating json: %v", err)
		}

		var event RandomEvent
		if err := json.Unmarshal(data, &event); err != nil {
			t.Fatalf("invalid json generated: %v; raw: %s", err, string(data))
		}

		if event.Sequence != i {
			t.Errorf("expected sequence %d, got %d", i, event.Sequence)
		}
		if event.ID == "" {
			t.Errorf("expected non-empty ID")
		}
		if event.Timestamp == 0 {
			t.Errorf("expected non-zero timestamp")
		}
		if event.EventType == "" {
			t.Errorf("expected non-empty event type")
		}
		if event.Amount < 0 {
			t.Errorf("expected non-negative amount")
		}
		if event.Metadata.Device == "" {
			t.Errorf("expected device metadata")
		}
	}
}

func TestRandomJSONGeneratorWithPadding(t *testing.T) {
	padding := 512
	gen := NewRandomJSONGenerator(padding)

	data, err := gen.Generate(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var event RandomEvent
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("invalid json: %v", err)
	}

	if len(event.Padding) != padding {
		t.Errorf("expected padding of length %d, got %d", padding, len(event.Padding))
	}
}
