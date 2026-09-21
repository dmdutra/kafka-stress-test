package generator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileGeneratorStatic(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "static_message.json")
	content := `{"static_key": "static_value", "number": 42}`

	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	gen, err := NewFileGenerator(filePath)
	if err != nil {
		t.Fatalf("failed to create FileGenerator: %v", err)
	}

	if gen.isTemplated {
		t.Errorf("expected isTemplated to be false for static file")
	}

	res, err := gen.Generate(1)
	if err != nil {
		t.Fatalf("failed to generate: %v", err)
	}

	if string(res) != content {
		t.Errorf("expected '%s', got '%s'", content, string(res))
	}
}

func TestFileGeneratorTemplated(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "templated_message.json")
	content := `{"id": "{{uuid}}", "seq": {{counter}}, "ts": {{timestamp}}, "rand": {{random_int:10:20}}}`

	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	gen, err := NewFileGenerator(filePath)
	if err != nil {
		t.Fatalf("failed to create FileGenerator: %v", err)
	}

	if !gen.isTemplated {
		t.Errorf("expected isTemplated to be true")
	}

	res1, err := gen.Generate(10)
	if err != nil {
		t.Fatalf("failed to generate: %v", err)
	}

	res2, err := gen.Generate(11)
	if err != nil {
		t.Fatalf("failed to generate: %v", err)
	}

	// Should be valid JSON
	var obj1 map[string]interface{}
	if err := json.Unmarshal(res1, &obj1); err != nil {
		t.Fatalf("invalid json produced: %v, raw: %s", err, string(res1))
	}

	var obj2 map[string]interface{}
	if err := json.Unmarshal(res2, &obj2); err != nil {
		t.Fatalf("invalid json produced: %v, raw: %s", err, string(res2))
	}

	if obj1["seq"] != float64(10) {
		t.Errorf("expected seq 10, got %v", obj1["seq"])
	}
	if obj2["seq"] != float64(11) {
		t.Errorf("expected seq 11, got %v", obj2["seq"])
	}

	id1 := obj1["id"].(string)
	id2 := obj2["id"].(string)
	if id1 == id2 {
		t.Errorf("expected unique UUIDs, got identical: %s", id1)
	}

	if strings.Contains(string(res1), "{{") {
		t.Errorf("found unresolved template tags in result: %s", string(res1))
	}
}
