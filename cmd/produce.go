package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"kafka-stress-test/internal/config"
	"kafka-stress-test/internal/runner"
)

var (
	configFile     string
	brokersFlag    string
	topicFlag      string
	totalFlag      int64
	durationFlag   time.Duration
	rateFlag       int
	workersFlag    int
	payloadMode    string
	filePathFlag   string
	paddingSize    int
	acksFlag       string
	compFlag       string
	keyModeFlag    string
	fixedKeyFlag   string
	reportIntFlag  time.Duration
	dryRunFlag     bool
	tlsEnabledFlag bool
	tlsCAFlag      string
	tlsCertFlag    string
	tlsKeyFlag     string
	tlsInsecureFlag bool
	tlsServerNameFlag string
	promPortFlag   int
)

var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "Produce messages to Kafka to perform stress testing",
	Long: `Run a high-throughput producer load test against an Apache Kafka cluster.

Examples:
  # Stress test with random JSON payload (100,000 messages)
  kafka-stress-test produce --brokers localhost:9092 --topic test-topic -n 100000 -w 20

  # Stress test using a message file with dynamic templating
  kafka-stress-test produce -b localhost:9092 -t test-topic -m file -f ./samples/message_templated.json -n 50000

  # Target a specific throughput rate (5,000 msgs/sec for 30s)
  kafka-stress-test produce -b localhost:9092 -t test-topic -r 5000 -d 30s

  # Run using a YAML configuration file
  kafka-stress-test produce --config config.yaml

  # Dry run without sending to Kafka (benchmarks generation throughput)
  kafka-stress-test produce -m random-json -n 200000 -w 10 --dry-run
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		var cfg *config.Config

		if configFile != "" {
			var err error
			cfg, err = config.LoadFromFile(configFile)
			if err != nil {
				return fmt.Errorf("failed to load configuration from %s: %w", configFile, err)
			}
		} else {
			cfg = config.DefaultConfig()
		}

		// Override with explicit flags if they were set
		if cmd.Flags().Changed("brokers") {
			parts := strings.Split(brokersFlag, ",")
			cfg.Brokers = make([]string, 0, len(parts))
			for _, p := range parts {
				trimmed := strings.TrimSpace(p)
				if trimmed != "" {
					cfg.Brokers = append(cfg.Brokers, trimmed)
				}
			}
		}
		if cmd.Flags().Changed("topic") {
			cfg.Topic = topicFlag
		}
		if cmd.Flags().Changed("total") {
			cfg.TotalMessages = totalFlag
		}
		if cmd.Flags().Changed("duration") {
			cfg.Duration = durationFlag
		}
		if cmd.Flags().Changed("rate") {
			cfg.RateLimit = rateFlag
		}
		if cmd.Flags().Changed("workers") {
			cfg.Workers = workersFlag
		}
		if cmd.Flags().Changed("payload-mode") {
			cfg.PayloadMode = payloadMode
		}
		if cmd.Flags().Changed("file") {
			cfg.FilePath = filePathFlag
		}
		if cmd.Flags().Changed("padding-size") {
			cfg.RandomJsonSize = paddingSize
		}
		if cmd.Flags().Changed("acks") {
			cfg.Acks = acksFlag
		}
		if cmd.Flags().Changed("compression") {
			cfg.Compression = compFlag
		}
		if cmd.Flags().Changed("key-mode") {
			cfg.KeyMode = keyModeFlag
		}
		if cmd.Flags().Changed("fixed-key") {
			cfg.FixedKey = fixedKeyFlag
		}
		if cmd.Flags().Changed("report-interval") {
			cfg.ReportInterval = reportIntFlag
		}
		if cmd.Flags().Changed("dry-run") {
			cfg.DryRun = dryRunFlag
		}
		if cmd.Flags().Changed("prometheus-port") {
			cfg.PrometheusPort = promPortFlag
		}
		if cmd.Flags().Changed("tls") || cmd.Flags().Changed("ssl") {
			cfg.TLSEnabled = tlsEnabledFlag
		}
		if cmd.Flags().Changed("tls-ca") || cmd.Flags().Changed("ssl-ca") {
			cfg.TLSCAFile = tlsCAFlag
		}
		if cmd.Flags().Changed("tls-cert") || cmd.Flags().Changed("ssl-cert") {
			cfg.TLSCertFile = tlsCertFlag
		}
		if cmd.Flags().Changed("tls-key") || cmd.Flags().Changed("ssl-key") {
			cfg.TLSKeyFile = tlsKeyFlag
		}
		if cmd.Flags().Changed("tls-insecure") || cmd.Flags().Changed("ssl-insecure") {
			cfg.TLSInsecureSkipVerify = tlsInsecureFlag
		}
		if cmd.Flags().Changed("tls-server-name") {
			cfg.TLSServerName = tlsServerNameFlag
		}

		r, err := runner.NewRunner(cfg)
		if err != nil {
			return err
		}

		return r.Run(context.Background())
	},
}

func init() {
	produceCmd.Flags().StringVarP(&configFile, "config", "c", "", "Path to YAML configuration file")
	produceCmd.Flags().StringVarP(&brokersFlag, "brokers", "b", "localhost:9092", "Kafka brokers (comma-separated)")
	produceCmd.Flags().StringVarP(&topicFlag, "topic", "t", "stress-test-topic", "Target Kafka topic")
	produceCmd.Flags().Int64VarP(&totalFlag, "total", "n", 0, "Total number of messages to produce (0 = unlimited)")
	produceCmd.Flags().DurationVarP(&durationFlag, "duration", "d", 0, "Duration of the test (e.g. 30s, 5m; 0 = run until total or Ctrl+C)")
	produceCmd.Flags().IntVarP(&rateFlag, "rate", "r", 0, "Throughput limit in msgs/sec (0 = unlimited max speed)")
	produceCmd.Flags().IntVarP(&workersFlag, "workers", "w", 10, "Number of concurrent worker goroutines")
	produceCmd.Flags().StringVarP(&payloadMode, "payload-mode", "m", "random-json", "Payload mode: 'random-json' or 'file'")
	produceCmd.Flags().StringVarP(&filePathFlag, "file", "f", "", "Path to input payload file (required if payload-mode is 'file')")
	produceCmd.Flags().IntVar(&paddingSize, "padding-size", 0, "Extra padding size in bytes for random JSON payload")
	produceCmd.Flags().StringVarP(&acksFlag, "acks", "a", "1", "Producer acks: 'all', '1', or '0'")
	produceCmd.Flags().StringVarP(&compFlag, "compression", "z", "snappy", "Compression codec: 'none', 'snappy', 'gzip', 'lz4', 'zstd'")
	produceCmd.Flags().StringVarP(&keyModeFlag, "key-mode", "k", "uuid", "Key generation mode: 'none', 'uuid', 'counter', 'fixed'")
	produceCmd.Flags().StringVar(&fixedKeyFlag, "fixed-key", "", "Fixed key string (when key-mode is 'fixed')")
	produceCmd.Flags().DurationVar(&reportIntFlag, "report-interval", 1*time.Second, "Interval between live progress reports")
	produceCmd.Flags().BoolVar(&dryRunFlag, "dry-run", false, "Dry run mode (generates payloads and benchmarks without connecting to Kafka)")
	produceCmd.Flags().IntVar(&promPortFlag, "prometheus-port", 0, "Port to expose Prometheus metrics (0 = disabled)")

	// TLS / SSL flags
	produceCmd.Flags().BoolVar(&tlsEnabledFlag, "tls", false, "Enable TLS/SSL connection to Kafka brokers")
	produceCmd.Flags().BoolVar(&tlsEnabledFlag, "ssl", false, "Alias for --tls")
	produceCmd.Flags().StringVar(&tlsCAFlag, "tls-ca", "", "Path to CA certificate file (PEM) to verify broker")
	produceCmd.Flags().StringVar(&tlsCAFlag, "ssl-ca", "", "Alias for --tls-ca")
	produceCmd.Flags().StringVar(&tlsCertFlag, "tls-cert", "", "Path to client certificate file (PEM) for mTLS")
	produceCmd.Flags().StringVar(&tlsCertFlag, "ssl-cert", "", "Alias for --tls-cert")
	produceCmd.Flags().StringVar(&tlsKeyFlag, "tls-key", "", "Path to client private key file (PEM) for mTLS")
	produceCmd.Flags().StringVar(&tlsKeyFlag, "ssl-key", "", "Alias for --tls-key")
	produceCmd.Flags().BoolVar(&tlsInsecureFlag, "tls-insecure", false, "Skip broker certificate verification (insecure)")
	produceCmd.Flags().BoolVar(&tlsInsecureFlag, "ssl-insecure", false, "Alias for --tls-insecure")
	produceCmd.Flags().StringVar(&tlsServerNameFlag, "tls-server-name", "", "Override TLS server name (SNI)")
}
