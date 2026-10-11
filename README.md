# Kafka Stress Test

High-performance stress testing and load generation tool for Apache Kafka, written in Go.

Powered by the pure Go [`franz-go`](https://github.com/twmb/franz-go) client (zero CGO and no `librdkafka` dependency required), delivering ultra-high throughput (over 100,000 messages/second per instance), strict end-to-end latency tracking, and native concurrency powered by Go goroutines.

---

## Features

- **Multiple Payload Modes**:
  - `random-json`: Generates realistic, complex JSON events at runtime (UUID v4 IDs, millisecond and ISO8601 timestamps, monotonic sequences, user IDs, amounts, currencies, statuses, and client device metadata).
  - `file`: Reads payload from a file on disk.
    - **Static files**: Zero-allocation with memory-cached buffers for maximum throughput.
    - **Dynamic templates**: Automatic token evaluation on every message: `{{uuid}}`, `{{timestamp}}`, `{{timestamp_iso}}`, `{{counter}}`, `{{random_int:min:max}}`, `{{random_str:len}}`.
- **Key Generation Strategies**:
  - `none`: No key (uniform round-robin partition distribution across brokers).
  - `uuid`: Random UUID keys for even partition spread with per-key partition ordering.
  - `counter`: Sequential numeric keys based on message counter.
  - `fixed`: Static custom key provided by the user.
- **Flexible Configuration**:
  - **YAML configuration file** support (`--config config.yaml`).
  - Command-line flags to override any configuration parameter at runtime.
- **Load & Concurrency Controls**:
  - Configurable concurrency (`--workers`).
  - Precise throughput rate limiter (`--rate` msgs/sec) backed by a smooth token bucket algorithm.
  - Stop by total message count (`--total`) or time duration (`--duration`).
- **Advanced Kafka Tuning**:
  - Acknowledgment modes: `acks=all`, `acks=1`, `acks=0`.
  - Batch compression: `snappy`, `zstd`, `gzip`, `lz4`, `none`.
  - Automatic destination topic verification and creation before testing.
- **SSL / TLS Security**:
  - Standard TLS encryption for secure clusters (`--tls` or `--ssl`).
  - Custom Root CA certificate validation (`--tls-ca`).
  - Mutual TLS (mTLS) client authentication with certificate and private key (`--tls-cert` and `--tls-key`).
  - Skip verification option for development with self-signed certs (`--tls-insecure`).
  - Custom SNI server name override (`--tls-server-name`).
- **Real-Time Metrics & Reporting**:
  - Live 1-second interval status ticker: sent count, throughput (msg/s and MB/s), latency percentiles (p50, p95, p99), and error count.
  - Comprehensive final test summary with complete latency breakdown (Min, Avg, p50, p90, p95, p99, Max) and error details.
- **Dry Run Mode**:
  - `--dry-run` flag to benchmark and validate payload generation speed locally without network or broker overhead.
- **Prometheus Metrics**:
  - Expose a `/metrics` HTTP endpoint (`--prometheus-port 8080`) to scrape internal metrics dynamically via Prometheus while tests are running.
  - Push metrics automatically to a Prometheus Pushgateway (`--pushgateway-url http://localhost:9091`).
  - Send metrics directly to Prometheus via native Remote Write protocol (`--remote-write-url http://localhost:9090/api/v1/write`).

---

## Project Structure

```
kafka-stress-test/
├── cmd/
│   ├── root.go                # Root CLI command (Cobra)
│   └── produce.go             # 'produce' subcommand and CLI flag bindings
├── internal/
│   ├── config/                # Configuration structs, defaults, and YAML parser
│   ├── generator/             # Payload generators (random-json, file, templates) and keys
│   ├── kafka/                 # franz-go Kafka client wrapper and admin topic management
│   ├── metrics/               # Metrics tracking, live snapshot formatting, and percentile calculation
│   └── runner/                # Worker pool orchestrator, rate limiter, and lifecycle manager
├── samples/
│   ├── config.example.yaml    # Annotated sample YAML configuration file
│   ├── message.json           # Sample static JSON payload
│   └── message_templated.json # Sample templated JSON payload with dynamic placeholders
├── docker-compose.yml          # Local KRaft Apache Kafka broker (optional for testing)
├── main.go                    # Application entrypoint
├── go.mod
└── README.md
```

---

## Build & Installation

Prerequisites: **Go 1.22+** installed.

```bash
# Clone and enter the repository
cd kafka-stress-test

# Build the binary
go build -o kafka-stress-test main.go
```

To run the unit test suite:

```bash
go test -v ./...
```

---

## Usage Examples

### 1. Produce Messages with Randomized JSON

Produce 50,000 messages with 10 concurrent workers:

```bash
./kafka-stress-test produce \
  --brokers localhost:9092 \
  --topic stress-test-topic \
  --payload-mode random-json \
  --total 50000 \
  --workers 10
```

Example JSON payload generated dynamically:
```json
{
  "id": "bbbeae88-82ee-4c8f-b351-480e587bd1a0",
  "timestamp": 1789989822085,
  "iso_time": "2026-09-21T11:23:42.085508274Z",
  "sequence": 2,
  "user_id": "usr_44577",
  "event_type": "order_created",
  "amount": 378.75,
  "currency": "EUR",
  "status": "PROCESSING",
  "metadata": {
    "device": "api",
    "client_ip": "192.168.38.183",
    "version": "1.0.0"
  }
}
```

---

### 2. Produce Messages from a Static File

Reads the exact payload from a file on disk:

```bash
./kafka-stress-test produce \
  --brokers localhost:9092 \
  --topic stress-test-topic \
  --payload-mode file \
  --file ./samples/message.json \
  --total 10000 \
  --workers 8
```

---

### 3. Produce Messages from a Templated File

Payload files can contain dynamic tokens evaluated for each message:

- `{{uuid}}`: Generates a new UUID v4
- `{{timestamp}}`: Current Unix epoch timestamp in milliseconds
- `{{timestamp_iso}}`: Current UTC timestamp in ISO 8601 (RFC3339) format
- `{{counter}}`: Monotonically increasing message counter (1, 2, 3...)
- `{{random_int:min:max}}`: Random integer between `min` and `max`
- `{{random_str:len}}`: Random alphanumeric string of length `len`

Example from `samples/message_templated.json`:
```json
{
  "event_id": "{{uuid}}",
  "sequence": {{counter}},
  "timestamp": {{timestamp}},
  "iso_time": "{{timestamp_iso}}",
  "source": "checkout_gateway",
  "action": "ORDER_PROCESSED",
  "account_id": "acc_{{random_int:10000:99999}}",
  "amount": {{random_int:10:5000}}.99,
  "currency": "USD",
  "tracking_code": "{{random_str:12}}"
}
```

Execution:
```bash
./kafka-stress-test produce \
  --brokers localhost:9092 \
  --topic stress-test-topic \
  --payload-mode file \
  --file ./samples/message_templated.json \
  --total 50000 \
  --workers 10
```

---

### 4. Rate Limiting (Throughput Control)

To simulate a steady throughput of **5,000 messages/sec** over **30 seconds**:

```bash
./kafka-stress-test produce \
  --brokers localhost:9092 \
  --topic stress-test-topic \
  --rate 5000 \
  --duration 30s \
  --workers 10
```

---

### 5. Using a YAML Configuration File

Create or customize a configuration file (see `samples/config.example.yaml`):

```yaml
brokers:
  - "localhost:9092"
topic: "stress-test-topic"
total_messages: 100000
rate_limit: 10000
workers: 20
acks: "1"
compression: "snappy"
key_mode: "uuid"
payload_mode: "random-json"
```

Run referencing the YAML file:
```bash
./kafka-stress-test produce --config config.yaml
```

> **Tip**: Any configuration parameter defined in the YAML file can be overridden via CLI flags, e.g.: `./kafka-stress-test produce --config config.yaml -w 30 --rate 0`

---

### 6. Connecting to Clusters with SSL / TLS

#### Standard TLS / SSL
```bash
./kafka-stress-test produce \
  --brokers kafka-broker.corp:9093 \
  --topic secure-topic \
  --tls \
  --total 50000
```

#### Custom CA & Self-Signed Certificates
```bash
# Verify broker against an internal enterprise CA:
./kafka-stress-test produce \
  --brokers kafka-broker.corp:9093 \
  --topic secure-topic \
  --tls-ca /path/to/ca.pem \
  --total 50000

# Or skip certificate verification (testing environments):
./kafka-stress-test produce \
  --brokers kafka-broker.corp:9093 \
  --topic secure-topic \
  --tls --tls-insecure \
  --total 50000
```

#### Mutual TLS (mTLS) Authentication
```bash
./kafka-stress-test produce \
  --brokers kafka-broker.corp:9093 \
  --topic secure-topic \
  --tls-ca /path/to/ca.pem \
  --tls-cert /path/to/client.pem \
  --tls-key /path/to/client.key \
  --total 50000
```

---

### 7. Dry Run Mode (Local Payload Generation Benchmark)

Benchmark the maximum generation speed on your machine without connecting to any Kafka broker:

```bash
./kafka-stress-test produce --payload-mode random-json --total 200000 --workers 10 --dry-run
```

---

### 8. Exposing Prometheus Metrics

You can integrate with Prometheus in three ways:

**Option A: Scrape via HTTP (/metrics)**
Start the producer and expose metrics dynamically on a specified HTTP port (e.g., `8080`).

```bash
./kafka-stress-test produce \
  --brokers localhost:9092 \
  --topic stress-test-topic \
  --total 500000 \
  --prometheus-port 8080
```
Then scrape metrics continuously (or view manually at `http://localhost:8080/metrics`).

**Option B: Push to Prometheus Pushgateway**
Push metrics automatically to a remote Pushgateway. This is ideal for short-lived benchmark runs.

```bash
./kafka-stress-test produce \
  --brokers localhost:9092 \
  --topic stress-test-topic \
  --total 500000 \
  --pushgateway-url "http://localhost:9091" \
  --pushgateway-job "my_custom_stress_test"
```

**Option C: Direct Push via Remote Write**
Send metrics directly to a Prometheus backend (or Thanos, VictoriaMetrics, Cortex) using the native Protobuf Remote Write protocol.

```bash
./kafka-stress-test produce \
  --brokers localhost:9092 \
  --topic stress-test-topic \
  --total 500000 \
  --remote-write-url "http://localhost:9090/api/v1/write"
```

Available metrics include `kafka_stress_messages_sent_total`, `kafka_stress_bytes_sent_total`, `kafka_stress_messages_failed_total`, and a latency histogram `kafka_stress_latency_seconds`.

---

## Metrics & Reports

During execution, a live status update is printed every second:
```text
[00:05] Sent: 50,000 | Rate: 10,000 msg/s (5.20 MB/s) | Latency: p50=1.2ms p95=3.5ms p99=8.1ms | Errors: 0
```

Upon completion, a detailed test summary is generated:
```text
========================== TEST SUMMARY ==========================
  Duration:           81ms
  Total Messages:     10000
  Total Data:         2.97 MB
  Total Errors:       0

  Throughput:
    Avg Msg Rate:     123728 msg/s
    Avg Data Rate:    36.79 MB/s

  Latency:
    Min:              15.3ms
    Avg:              32.1ms
    p50 (Median):     33.5ms
    p90:              41.5ms
    p95:              42.2ms
    p99:              43.8ms
    Max:              44.6ms
==================================================================
```

---

## Local Environment (Docker Compose)

If you need a local standalone Kafka broker and metrics observability stack for testing, a pre-configured `docker-compose.yml` (KRaft mode, no ZooKeeper needed) is included:

```bash
docker compose up -d
```
This starts:
- **Apache Kafka (KRaft)**: listening on `localhost:9092`
- **Prometheus**: listening on `http://localhost:9090` (with Remote Write receiver enabled)
- **Prometheus Pushgateway**: listening on `http://localhost:9091`
- **Grafana**: listening on `http://localhost:3000` (auto-configured with a default Kafka Stress Test dashboard)
