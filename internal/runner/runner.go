package runner

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/time/rate"
	"kafka-stress-test/internal/config"
	"kafka-stress-test/internal/generator"
	"kafka-stress-test/internal/kafka"
	"kafka-stress-test/internal/metrics"
)

// Runner coordinates workers, generators, producer, and metrics reporting.
type Runner struct {
	cfg        *config.Config
	keyGen     generator.KeyGenerator
	payloadGen generator.PayloadGenerator
	metrics    *metrics.Metrics
	producer   *kafka.Producer
}

// NewRunner initializes the stress test runner.
func NewRunner(cfg *config.Config) (*Runner, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	keyGen := generator.NewKeyGenerator(cfg.KeyMode, cfg.FixedKey)

	var payloadGen generator.PayloadGenerator
	switch cfg.PayloadMode {
	case "file":
		fg, err := generator.NewFileGenerator(cfg.FilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize file generator: %w", err)
		}
		payloadGen = fg
	case "random-json":
		payloadGen = generator.NewRandomJSONGenerator(cfg.RandomJsonSize)
	default:
		return nil, fmt.Errorf("unknown payload mode: %s", cfg.PayloadMode)
	}

	var prod *kafka.Producer
	if !cfg.DryRun {
		var err error
		prod, err = kafka.NewProducer(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka producer: %w", err)
		}
	}

	return &Runner{
		cfg:        cfg,
		keyGen:     keyGen,
		payloadGen: payloadGen,
		metrics:    metrics.NewMetrics(cfg.PrometheusPort > 0),
		producer:   prod,
	}, nil
}

// Run starts the stress test execution.
func (r *Runner) Run(parentCtx context.Context) error {
	ctx, cancel := signal.NotifyContext(parentCtx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if r.cfg.Duration > 0 {
		var durationCancel context.CancelFunc
		ctx, durationCancel = context.WithTimeout(ctx, r.cfg.Duration)
		defer durationCancel()
	}

	// Verify connectivity if not in dry-run mode
	if !r.cfg.DryRun && r.producer != nil {
		fmt.Printf("Connecting to Kafka brokers %v...\n", r.cfg.Brokers)
		pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
		if err := r.producer.Ping(pingCtx); err != nil {
			pingCancel()
			return fmt.Errorf("failed to connect to Kafka brokers: %w", err)
		}
		pingCancel()

		// Ensure destination topic exists
		topicCtx, topicCancel := context.WithTimeout(ctx, 5*time.Second)
		if err := r.producer.EnsureTopic(topicCtx, 3, 1); err != nil {
			fmt.Printf("Notice: topic creation check: %v\n", err)
		}
		topicCancel()

		fmt.Printf("Connected successfully to topic '%s'.\n", r.cfg.Topic)
	} else if r.cfg.DryRun {
		fmt.Println("[DRY RUN] Simulating message generation without Kafka brokers...")
	}

	if r.cfg.PrometheusPort > 0 {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			addr := fmt.Sprintf(":%d", r.cfg.PrometheusPort)
			fmt.Printf("Starting Prometheus metrics server on http://localhost%s/metrics\n", addr)
			server := &http.Server{Addr: addr, Handler: mux}
			go func() {
				<-ctx.Done()
				server.Close()
			}()
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Printf("Prometheus metrics server failed: %v\n", err)
			}
		}()
	}

	fmt.Printf("Starting stress test: Workers=%d, RateLimit=%d msg/s, PayloadMode=%s, Acks=%s, Compression=%s\n",
		r.cfg.Workers, r.cfg.RateLimit, r.cfg.PayloadMode, r.cfg.Acks, r.cfg.Compression)
	fmt.Println("------------------------------------------------------------------")

	// Rate limiter setup
	var limiter *rate.Limiter
	if r.cfg.RateLimit > 0 {
		burst := r.cfg.RateLimit / 10
		if burst < 10 {
			burst = 10
		}
		limiter = rate.NewLimiter(rate.Limit(r.cfg.RateLimit), burst)
	}

	// Reporter ticker
	reporterDone := make(chan struct{})
	go r.startReporter(ctx, reporterDone)

	var messageCounter atomic.Int64
	var workerWg sync.WaitGroup
	var inFlightWg sync.WaitGroup

	// Launch worker goroutines
	workerWg.Add(r.cfg.Workers)
	for w := 0; w < r.cfg.Workers; w++ {
		go func(workerID int) {
			defer workerWg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if r.cfg.TotalMessages > 0 {
					msgIdx := messageCounter.Add(1)
					if msgIdx > r.cfg.TotalMessages {
						return
					}
					r.sendMessage(ctx, msgIdx, limiter, &inFlightWg)
				} else {
					msgIdx := messageCounter.Add(1)
					r.sendMessage(ctx, msgIdx, limiter, &inFlightWg)
				}
			}
		}(w)
	}

	// Wait for workers to finish
	workerWg.Wait()

	// Flush Kafka buffers if applicable
	if !r.cfg.DryRun && r.producer != nil {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 15*time.Second)
		_ = r.producer.Flush(flushCtx)
		flushCancel()
	}

	// Wait for all remaining in-flight responses
	inFlightWg.Wait()

	// Stop reporter and metrics
	cancel()
	<-reporterDone
	r.metrics.Stop()

	if !r.cfg.DryRun && r.producer != nil {
		r.producer.Close()
	}

	// Print final summary
	fmt.Println(r.metrics.FinalSnapshot().FormatSummary())
	return nil
}

func (r *Runner) sendMessage(ctx context.Context, msgIdx int64, limiter *rate.Limiter, inFlightWg *sync.WaitGroup) {
	if limiter != nil {
		if err := limiter.Wait(ctx); err != nil {
			return
		}
	}

	key := r.keyGen.GenerateKey(msgIdx)
	payload, err := r.payloadGen.Generate(msgIdx)
	if err != nil {
		r.metrics.RecordFailure(fmt.Errorf("payload generation error: %w", err))
		return
	}

	if r.cfg.DryRun || r.producer == nil {
		// Dry run mode
		r.metrics.RecordSuccess(100*time.Microsecond, len(payload))
		return
	}

	inFlightWg.Add(1)
	start := time.Now()

	record := &kgo.Record{
		Key:   key,
		Value: payload,
	}

	r.producer.Produce(ctx, record, func(rec *kgo.Record, err error) {
		defer inFlightWg.Done()
		latency := time.Since(start)

		if err != nil {
			r.metrics.RecordFailure(err)
		} else {
			r.metrics.RecordSuccess(latency, len(rec.Value))
		}
	})
}

func (r *Runner) startReporter(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(r.cfg.ReportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snap := r.metrics.Tick()
			fmt.Println(snap.FormatSnapshot())
		}
	}
}
