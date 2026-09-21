package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid default config, got: %v", err)
	}

	if len(cfg.Brokers) != 1 || cfg.Brokers[0] != "localhost:9092" {
		t.Errorf("unexpected default brokers: %v", cfg.Brokers)
	}
	if cfg.Topic != "stress-test-topic" {
		t.Errorf("unexpected default topic: %v", cfg.Topic)
	}
	if cfg.PayloadMode != "random-json" {
		t.Errorf("unexpected default payload mode: %v", cfg.PayloadMode)
	}
}

func TestLoadFromFile(t *testing.T) {
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test-config.yaml")

	yamlContent := `
brokers:
  - "kafka1:9092"
  - "kafka2:9092"
topic: "test-orders"
total_messages: 5000
rate_limit: 1000
workers: 20
acks: "all"
compression: "zstd"
key_mode: "uuid"
payload_mode: "random-json"
report_interval: 2s
`
	if err := os.WriteFile(configFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	cfg, err := LoadFromFile(configFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validation failed: %v", err)
	}

	if len(cfg.Brokers) != 2 || cfg.Brokers[0] != "kafka1:9092" {
		t.Errorf("unexpected brokers: %v", cfg.Brokers)
	}
	if cfg.Topic != "test-orders" {
		t.Errorf("unexpected topic: %v", cfg.Topic)
	}
	if cfg.TotalMessages != 5000 {
		t.Errorf("unexpected total_messages: %v", cfg.TotalMessages)
	}
	if cfg.RateLimit != 1000 {
		t.Errorf("unexpected rate_limit: %v", cfg.RateLimit)
	}
	if cfg.Workers != 20 {
		t.Errorf("unexpected workers: %v", cfg.Workers)
	}
	if cfg.Acks != "all" {
		t.Errorf("unexpected acks: %v", cfg.Acks)
	}
	if cfg.Compression != "zstd" {
		t.Errorf("unexpected compression: %v", cfg.Compression)
	}
	if cfg.ReportInterval != 2*time.Second {
		t.Errorf("unexpected report_interval: %v", cfg.ReportInterval)
	}
}

func TestValidationErrors(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Brokers = []string{}
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected error for empty brokers")
	}

	cfg = DefaultConfig()
	cfg.Topic = ""
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected error for empty topic")
	}

	cfg = DefaultConfig()
	cfg.Acks = "invalid-ack"
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected error for invalid acks")
	}

	cfg = DefaultConfig()
	cfg.Compression = "invalid-comp"
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected error for invalid compression")
	}

	cfg = DefaultConfig()
	cfg.PayloadMode = "file"
	cfg.FilePath = "non-existent-file.json"
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected error for non-existent file")
	}
}
