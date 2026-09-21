package generator

// PayloadGenerator defines an interface for producing Kafka record values.
type PayloadGenerator interface {
	// Generate generates a message payload for the given message index/counter.
	Generate(counter int64) ([]byte, error)
}

// KeyGenerator defines an interface for producing Kafka record keys.
type KeyGenerator interface {
	// GenerateKey produces a key for the given message index/counter.
	// Returning nil or empty slice means no key (round-robin partition distribution).
	GenerateKey(counter int64) []byte
}
