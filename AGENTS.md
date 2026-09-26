# AGENTS.md — Guidelines for AI Coding Agents

This document defines the architecture, design principles, conventions, and operational workflows for AI agents working on the **`kafka-stress-test`** codebase.

---

## 1. Project Overview & Philosophy

**`kafka-stress-test`** is a high-throughput, low-latency Apache Kafka stress testing and benchmarking CLI tool built in Go.

### Core Tenets
1. **Zero CGO Dependency**: Built strictly on pure Go using [`github.com/twmb/franz-go`](https://github.com/twmb/franz-go). Do not introduce `librdkafka` or CGO dependencies.
2. **Extreme Concurrency & Throughput**: Designed to produce 100k+ messages/second per single instance. Code in hot paths must prioritize zero or minimal memory allocations.
3. **Accurate Metrics**: True end-to-end latency tracking (broker ACK to client), percentiles (p50, p90, p95, p99), and throughput rates (msg/s and MB/s).
4. **Resilience & Safety**: Graceful shutdown on `SIGINT`/`SIGTERM`, buffer flushing before termination, backpressure management via client buffering.

---

## 2. Repository Layout

```
kafka-stress-test/
├── cmd/
│   ├── root.go             # Root Cobra CLI definition
│   └── produce.go          # 'produce' subcommand and CLI flag bindings
├── internal/
│   ├── config/             # Config struct, YAML parser, validation, and defaults
│   ├── generator/          # Payload generators (random-json, file, dynamic templates) and key strategies
│   ├── kafka/              # Franz-go client wrapper, TLS/SSL configuration, topic management
│   ├── metrics/            # Thread-safe metrics collector, reservoir sampling, snapshots, formatters
│   └── runner/             # Worker pool orchestrator, rate limiter, produce callbacks, lifecycle
├── samples/
│   ├── config.example.yaml # Fully annotated sample YAML configuration file
│   ├── message.json        # Static JSON sample payload
│   └── message_templated.json # Sample with dynamic template tokens ({{uuid}}, etc.)
├── docker-compose.yml       # Standalone KRaft Apache Kafka for local verification
├── main.go                 # Application entrypoint calling cmd.Execute()
├── go.mod / go.sum
└── README.md               # User-facing documentation
```

---

## 3. Essential Commands

### Build & Test
```bash
# Build the application binary
go build -o kafka-stress-test main.go

# Run all unit tests
go test -v ./...

# Run tests with race detection (critical for concurrent code)
go test -race ./...

# Tidy Go dependencies
go mod tidy
```

### Local Validation & Benchmarking
```bash
# Benchmark payload generator throughput without Kafka (dry-run mode)
./kafka-stress-test produce -m random-json -n 100000 -w 8 --dry-run

# Benchmark static file payload generator
./kafka-stress-test produce -m file -f ./samples/message.json -n 100000 -w 8 --dry-run

# Benchmark templated file payload generator
./kafka-stress-test produce -m file -f ./samples/message_templated.json -n 100000 -w 8 --dry-run

# Produce to a local Kafka broker
./kafka-stress-test produce --brokers localhost:9092 --topic stress-test-topic -n 1000 -w 4
```

---

## 4. Key Architectural Patterns & Rules

### 4.1. Kafka Client & Franz-go Specifics
- **Idempotency & Acks**:
  - In Kafka / `franz-go`, idempotency requires `acks=all`.
  - When configuring non-all acks (`acks=1` or `acks=0`), you **MUST** call `kgo.DisableIdempotentWrite()`. Failing to do so causes client initialization errors.
- **ACK Configuration**:
  - Always use `kgo.RequiredAcks(...)` with `kgo.AllISRAcks()`, `kgo.LeaderAck()`, or `kgo.NoAck()`.
- **Callback Concurrency**:
  - `franz-go` promises (`client.Produce(ctx, record, promise)`) execute serially within promise workers.
  - **NEVER** perform blocking I/O, heavy calculations, or call blocking client methods (like `Flush()`) inside the produce callback. Keep callbacks lightweight (e.g. record latency and message size to metrics).
- **Admin API**:
  - Use `github.com/twmb/franz-go/pkg/kadm` for topic creation and verification.
  - Check for `kerr.TopicAlreadyExists` to prevent race conditions during topic creation.

### 4.2. SSL / TLS Guidelines
- TLS options are wired through standard Go `crypto/tls` and `kgo.DialTLSConfig(tlsConfig)`.
- Support:
  - Standard TLS (`--tls` or `--ssl`)
  - Custom Root CA PEM file (`--tls-ca`)
  - Mutual TLS (mTLS) with client certificate and key (`--tls-cert`, `--tls-key`)
  - Insecure verification for development (`--tls-insecure`)
  - SNI override (`--tls-server-name`)
- Always validate that certificate and key files exist and are readable before attempting to dial.

### 4.3. High-Performance / Low-Allocation Conventions
- In `internal/generator/file.go`:
  - Static files are cached in memory at initialization and returned directly (`[]byte`), avoiding allocations.
  - Templated files are pre-parsed into static byte slices and token segments (`tokenSegment`), avoiding slow regex searches on each produce.
- In `internal/metrics/metrics.go`:
  - Use `sync/atomic` for monotonic counters (`sentMessages`, `sentBytes`, `failedMsgs`).
  - Reservoir sample size for latency recording is capped (e.g. `1,000,000` samples) to prevent unbounded memory growth during long-running tests.

### 4.4. Configuration & CLI Synchronization
When adding a new configuration parameter:
1. Add the field with `yaml` tag to `internal/config/config.go` (`Config` struct).
2. Set a sensible default in `DefaultConfig()`.
3. Add validation logic in `Config.Validate()`.
4. Add corresponding CLI flag in `cmd/produce.go` (`init()` function).
5. In `produceCmd.RunE`, check `cmd.Flags().Changed("<flag-name>")` to override YAML values with CLI arguments.
6. Document the parameter in `samples/config.example.yaml` and `README.md`.
7. Add unit test coverage in `internal/config/config_test.go`.

---

## 5. Coding & Documentation Standards

- **Language**: All source code, docstrings, variable names, git commit messages, and documentation **must be in English**.
- **Formatting**: Run `go fmt ./...` or `gofmt -s -w` on all modified files before finishing.
- **Git Branch**: Work strictly on the **`main`** branch (do not create or switch to `master`).
- **Git Hygiene**: Compiled binaries (`kafka-stress-test`, `kafka-stress*`), coverage reports, and logs are excluded in `.gitignore`. Do not commit binaries or temporary files.
- **Error Handling**: Always wrap errors with context: `fmt.Errorf("failed to <action>: %w", err)`.
- **Verification**: Always run `go test -v ./...` and `go build -o kafka-stress-test main.go` after making changes to verify compilation and test integrity.

---

## 6. Extending the Codebase (Roadmap)

When requested to implement upcoming features, follow these structural guidelines:

- **Consumer Stress Test**:
  - Implement under `cmd/consume.go` (`kafka-stress-test consume [flags]`).
  - Create consumer package in `internal/kafka/consumer.go` using `kgo.ConsumeTopics(...)` or group consumer options.
  - Collect read throughput, consumer group rebalance times, and fetch latency.
- **End-to-End Latency Tracking**:
  - Inject nanosecond timestamp headers in `ProducerRecord`.
  - Consumer reads timestamp header, calculates true broker-to-consumer latency distribution.
- **Schema Registry / Avro / Protobuf**:
  - Implement a dedicated payload generator under `internal/generator/avro.go` or `protobuf.go`.
  - Keep it optional so standard JSON/file generators remain lightweight and dependency-free.
