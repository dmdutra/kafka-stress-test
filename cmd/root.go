package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "kafka-stress-test",
	Short: "High-performance Kafka stress testing tool",
	Long: `kafka-stress-test is a high-throughput, concurrent load testing utility
for Apache Kafka, supporting dynamic random JSON generation, file-based payloads
with templating, flexible rate limiting, and real-time latency/throughput statistics.`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(produceCmd)
}
