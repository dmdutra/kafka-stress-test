package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"kafka-stress-test/internal/config"
)

// Producer wraps the franz-go Kafka client with optimized stress testing settings.
type Producer struct {
	client *kgo.Client
	topic  string
}

// NewProducer creates and connects a new Kafka producer.
func NewProducer(cfg *config.Config) (*Producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerBatchMaxBytes(1024 * 1024), // 1MB batch
		kgo.ProducerLinger(2 * time.Millisecond),
		kgo.MaxBufferedRecords(250000),
		kgo.RecordRetries(5),
	}

	// Configure ACKs
	switch cfg.Acks {
	case "all":
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case "0":
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite())
	case "1":
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	default:
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	}

	// Configure Compression
	switch cfg.Compression {
	case "none":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
	case "snappy":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
	case "gzip":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
	case "lz4":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
	case "zstd":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
	default:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &Producer{
		client: client,
		topic:  cfg.Topic,
	}, nil
}

// EnsureTopic creates the topic if it does not already exist.
func (p *Producer) EnsureTopic(ctx context.Context, partitions int32, replicationFactor int16) error {
	adm := kadm.NewClient(p.client)
	topics, err := adm.ListTopics(ctx, p.topic)
	if err == nil && topics.Has(p.topic) {
		return nil
	}

	if partitions <= 0 {
		partitions = 3
	}
	if replicationFactor <= 0 {
		replicationFactor = 1
	}

	resps, err := adm.CreateTopics(ctx, partitions, replicationFactor, nil, p.topic)
	if err != nil {
		return fmt.Errorf("failed to issue create topic request: %w", err)
	}
	for _, resp := range resps {
		if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("failed to create topic '%s': %w", resp.Topic, resp.Err)
		}
	}
	return nil
}

// Ping checks whether at least one broker is reachable.
func (p *Producer) Ping(ctx context.Context) error {
	return p.client.Ping(ctx)
}

// Produce sends a record asynchronously and invokes the callback promise on reply.
func (p *Producer) Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	if record.Topic == "" {
		record.Topic = p.topic
	}
	p.client.Produce(ctx, record, promise)
}

// Flush flushes all buffered records.
func (p *Producer) Flush(ctx context.Context) error {
	return p.client.Flush(ctx)
}

// Close gracefully closes the client.
func (p *Producer) Close() {
	p.client.Close()
}
