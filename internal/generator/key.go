package generator

import (
	"strconv"

	"github.com/google/uuid"
)

// NoneKeyGenerator produces nil keys for standard round-robin partition distribution.
type NoneKeyGenerator struct{}

func (g *NoneKeyGenerator) GenerateKey(counter int64) []byte {
	return nil
}

// UUIDKeyGenerator produces random UUID keys.
type UUIDKeyGenerator struct{}

func (g *UUIDKeyGenerator) GenerateKey(counter int64) []byte {
	return []byte(uuid.NewString())
}

// CounterKeyGenerator produces sequential counter keys.
type CounterKeyGenerator struct{}

func (g *CounterKeyGenerator) GenerateKey(counter int64) []byte {
	return []byte(strconv.FormatInt(counter, 10))
}

// FixedKeyGenerator produces a static fixed key.
type FixedKeyGenerator struct {
	key []byte
}

func NewFixedKeyGenerator(fixedKey string) *FixedKeyGenerator {
	return &FixedKeyGenerator{
		key: []byte(fixedKey),
	}
}

func (g *FixedKeyGenerator) GenerateKey(counter int64) []byte {
	return g.key
}

// NewKeyGenerator builds the appropriate KeyGenerator based on keyMode.
func NewKeyGenerator(keyMode string, fixedKey string) KeyGenerator {
	switch keyMode {
	case "uuid":
		return &UUIDKeyGenerator{}
	case "counter":
		return &CounterKeyGenerator{}
	case "fixed":
		return NewFixedKeyGenerator(fixedKey)
	default:
		return &NoneKeyGenerator{}
	}
}
