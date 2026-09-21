package generator

import (
	"testing"
)

func TestKeyGenerators(t *testing.T) {
	noneGen := NewKeyGenerator("none", "")
	if noneGen.GenerateKey(1) != nil {
		t.Errorf("expected nil key for none")
	}

	uuidGen := NewKeyGenerator("uuid", "")
	k1 := string(uuidGen.GenerateKey(1))
	k2 := string(uuidGen.GenerateKey(2))
	if k1 == "" || k2 == "" || k1 == k2 {
		t.Errorf("expected unique non-empty UUID keys, got k1=%s, k2=%s", k1, k2)
	}

	counterGen := NewKeyGenerator("counter", "")
	if string(counterGen.GenerateKey(42)) != "42" {
		t.Errorf("expected counter key '42', got '%s'", string(counterGen.GenerateKey(42)))
	}

	fixedGen := NewKeyGenerator("fixed", "partition-key-1")
	if string(fixedGen.GenerateKey(1)) != "partition-key-1" {
		t.Errorf("expected fixed key 'partition-key-1', got '%s'", string(fixedGen.GenerateKey(1)))
	}
}
