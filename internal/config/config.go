package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration parameters for the Kafka stress test producer.
type Config struct {
	Brokers        []string      `yaml:"brokers"`
	Topic          string        `yaml:"topic"`
	TotalMessages  int64         `yaml:"total_messages"`  // 0 = unlimited
	Duration       time.Duration `yaml:"duration"`        // 0 = run until total messages or Ctrl+C
	RateLimit      int           `yaml:"rate_limit"`      // msgs/sec; 0 = unlimited
	Workers        int           `yaml:"workers"`         // number of concurrent worker goroutines
	Acks           string        `yaml:"acks"`            // "all", "1", "0"
	Compression    string        `yaml:"compression"`     // "none", "snappy", "gzip", "lz4", "zstd"
	KeyMode        string        `yaml:"key_mode"`        // "none", "uuid", "counter", "fixed"
	FixedKey       string        `yaml:"fixed_key"`       // used if KeyMode == "fixed"
	PayloadMode    string        `yaml:"payload_mode"`    // "random-json" or "file"
	FilePath       string        `yaml:"file_path"`       // path to payload file if PayloadMode == "file"
	RandomJsonSize int           `yaml:"random_json_size"`// extra padding size in bytes for random json
	ReportInterval time.Duration `yaml:"report_interval"` // reporting interval (default 1s)
	DryRun         bool          `yaml:"dry_run"`         // generate payloads without connecting to Kafka
	PrometheusPort int           `yaml:"prometheus_port"` // port for prometheus metrics (0 = disabled)
	PushgatewayURL string        `yaml:"pushgateway_url"` // Prometheus Pushgateway URL (e.g., http://localhost:9091)
	PushgatewayJob string        `yaml:"pushgateway_job"` // Job name for Pushgateway
	RemoteWriteURL string        `yaml:"remote_write_url"`// Prometheus Remote Write URL (e.g., http://localhost:9090/api/v1/write)

	// TLS / SSL settings
	TLSEnabled            bool   `yaml:"tls_enabled"`
	TLSCAFile             string `yaml:"tls_ca_file"`              // path to CA certificate (PEM)
	TLSCertFile           string `yaml:"tls_cert_file"`            // path to client certificate (PEM)
	TLSKeyFile            string `yaml:"tls_key_file"`             // path to client private key (PEM)
	TLSInsecureSkipVerify bool   `yaml:"tls_insecure_skip_verify"` // skip server certificate verification
	TLSServerName         string `yaml:"tls_server_name"`          // override SNI server name
}

// DefaultConfig returns a Config with sensible default settings.
func DefaultConfig() *Config {
	return &Config{
		Brokers:        []string{"localhost:9092"},
		Topic:          "stress-test-topic",
		TotalMessages:  0,
		Duration:       0,
		RateLimit:      0,
		Workers:        10,
		Acks:           "1",
		Compression:    "snappy",
		KeyMode:        "uuid",
		FixedKey:       "",
		PayloadMode:    "random-json",
		FilePath:       "",
		RandomJsonSize: 0,
		ReportInterval: 1 * time.Second,
		DryRun:         false,
		PrometheusPort: 0,
		PushgatewayJob: "kafka_stress_test",
	}
}

// LoadFromFile loads configuration from a YAML file.
func LoadFromFile(filePath string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse yaml config: %w", err)
	}

	return cfg, nil
}

// Validate checks whether the configuration is valid.
func (c *Config) Validate() error {
	if !c.DryRun && len(c.Brokers) == 0 {
		return fmt.Errorf("at least one broker address must be provided")
	}

	if !c.DryRun && strings.TrimSpace(c.Topic) == "" {
		return fmt.Errorf("topic name cannot be empty")
	}

	if c.Workers <= 0 {
		c.Workers = 1
	}

	c.Acks = strings.ToLower(strings.TrimSpace(c.Acks))
	switch c.Acks {
	case "all", "-1", "1", "0", "none":
		if c.Acks == "-1" {
			c.Acks = "all"
		}
		if c.Acks == "none" {
			c.Acks = "0"
		}
	default:
		return fmt.Errorf("invalid acks '%s': must be 'all', '1', or '0'", c.Acks)
	}

	c.Compression = strings.ToLower(strings.TrimSpace(c.Compression))
	switch c.Compression {
	case "none", "snappy", "gzip", "lz4", "zstd":
	default:
		return fmt.Errorf("invalid compression '%s': must be 'none', 'snappy', 'gzip', 'lz4', or 'zstd'", c.Compression)
	}

	c.KeyMode = strings.ToLower(strings.TrimSpace(c.KeyMode))
	switch c.KeyMode {
	case "none", "uuid", "counter", "fixed":
	default:
		return fmt.Errorf("invalid key_mode '%s': must be 'none', 'uuid', 'counter', or 'fixed'", c.KeyMode)
	}

	if c.KeyMode == "fixed" && c.FixedKey == "" {
		return fmt.Errorf("fixed_key must be specified when key_mode is 'fixed'")
	}

	c.PayloadMode = strings.ToLower(strings.TrimSpace(c.PayloadMode))
	switch c.PayloadMode {
	case "random-json", "file":
	default:
		return fmt.Errorf("invalid payload_mode '%s': must be 'random-json' or 'file'", c.PayloadMode)
	}

	if c.PayloadMode == "file" {
		if strings.TrimSpace(c.FilePath) == "" {
			return fmt.Errorf("file_path must be specified when payload_mode is 'file'")
		}
		if _, err := os.Stat(c.FilePath); err != nil {
			return fmt.Errorf("payload file '%s' not found or inaccessible: %w", c.FilePath, err)
		}
	}

	if c.ReportInterval <= 0 {
		c.ReportInterval = 1 * time.Second
	}

	// Auto-enable TLS if certificates or related options are provided
	if c.TLSCAFile != "" || c.TLSCertFile != "" || c.TLSKeyFile != "" || c.TLSInsecureSkipVerify || c.TLSServerName != "" {
		c.TLSEnabled = true
	}

	if c.TLSEnabled {
		if c.TLSCAFile != "" {
			if _, err := os.Stat(c.TLSCAFile); err != nil {
				return fmt.Errorf("tls_ca_file '%s' not found or inaccessible: %w", c.TLSCAFile, err)
			}
		}
		if c.TLSCertFile != "" || c.TLSKeyFile != "" {
			if c.TLSCertFile == "" || c.TLSKeyFile == "" {
				return fmt.Errorf("both tls_cert_file and tls_key_file must be specified for mutual TLS (mTLS)")
			}
			if _, err := os.Stat(c.TLSCertFile); err != nil {
				return fmt.Errorf("tls_cert_file '%s' not found or inaccessible: %w", c.TLSCertFile, err)
			}
			if _, err := os.Stat(c.TLSKeyFile); err != nil {
				return fmt.Errorf("tls_key_file '%s' not found or inaccessible: %w", c.TLSKeyFile, err)
			}
		}
	}

	return nil
}
