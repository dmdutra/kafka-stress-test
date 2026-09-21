package generator

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// TokenType represents the type of a template token.
type TokenType int

const (
	TokenStatic TokenType = iota
	TokenUUID
	TokenTimestampMillis
	TokenTimestampISO
	TokenCounter
	TokenRandomInt
	TokenRandomStr
)

type tokenSegment struct {
	tokenType TokenType
	static    []byte
	intMin    int
	intMax    int
	strLen    int
}

// FileGenerator loads a payload from a file and supports both static content
// and dynamic templating for high-throughput generation.
type FileGenerator struct {
	isTemplated   bool
	staticContent []byte
	segments      []tokenSegment
}

var tokenRegex = regexp.MustCompile(`\{\{([a-zA-Z0-9_:]+)\}\}`)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return string(b)
}

// NewFileGenerator creates a FileGenerator by reading and parsing the specified file.
func NewFileGenerator(filePath string) (*FileGenerator, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("unable to read payload file '%s': %w", filePath, err)
	}

	matches := tokenRegex.FindAllSubmatchIndex(content, -1)
	if len(matches) == 0 {
		return &FileGenerator{
			isTemplated:   false,
			staticContent: content,
		}, nil
	}

	segments := make([]tokenSegment, 0, len(matches)*2+1)
	lastIdx := 0

	for _, m := range matches {
		startTag := m[0]
		endTag := m[1]
		startExpr := m[2]
		endExpr := m[3]

		if startTag > lastIdx {
			segments = append(segments, tokenSegment{
				tokenType: TokenStatic,
				static:    content[lastIdx:startTag],
			})
		}

		expr := string(content[startExpr:endExpr])
		seg, err := parseExpression(expr)
		if err != nil {
			// If not recognized, treat as static literal
			segments = append(segments, tokenSegment{
				tokenType: TokenStatic,
				static:    content[startTag:endTag],
			})
		} else {
			segments = append(segments, seg)
		}

		lastIdx = endTag
	}

	if lastIdx < len(content) {
		segments = append(segments, tokenSegment{
			tokenType: TokenStatic,
			static:    content[lastIdx:],
		})
	}

	return &FileGenerator{
		isTemplated: true,
		segments:    segments,
	}, nil
}

func parseExpression(expr string) (tokenSegment, error) {
	parts := strings.Split(expr, ":")
	cmd := strings.ToLower(parts[0])

	switch cmd {
	case "uuid":
		return tokenSegment{tokenType: TokenUUID}, nil
	case "timestamp", "timestamp_millis":
		return tokenSegment{tokenType: TokenTimestampMillis}, nil
	case "timestamp_iso", "iso_time":
		return tokenSegment{tokenType: TokenTimestampISO}, nil
	case "counter", "seq", "sequence":
		return tokenSegment{tokenType: TokenCounter}, nil
	case "random_int":
		minVal := 0
		maxVal := 1000000
		if len(parts) >= 3 {
			if min, err := strconv.Atoi(parts[1]); err == nil {
				minVal = min
			}
			if max, err := strconv.Atoi(parts[2]); err == nil && max > minVal {
				maxVal = max
			}
		} else if len(parts) == 2 {
			if max, err := strconv.Atoi(parts[1]); err == nil && max > 0 {
				maxVal = max
			}
		}
		return tokenSegment{tokenType: TokenRandomInt, intMin: minVal, intMax: maxVal}, nil
	case "random_str", "random_string":
		length := 10
		if len(parts) >= 2 {
			if l, err := strconv.Atoi(parts[1]); err == nil && l > 0 {
				length = l
			}
		}
		return tokenSegment{tokenType: TokenRandomStr, strLen: length}, nil
	default:
		return tokenSegment{}, fmt.Errorf("unknown expression: %s", expr)
	}
}

// Generate returns the payload bytes.
func (g *FileGenerator) Generate(counter int64) ([]byte, error) {
	if !g.isTemplated {
		return g.staticContent, nil
	}

	var buf bytes.Buffer
	buf.Grow(len(g.staticContent) + 128)

	for _, seg := range g.segments {
		switch seg.tokenType {
		case TokenStatic:
			buf.Write(seg.static)
		case TokenUUID:
			buf.WriteString(uuid.NewString())
		case TokenTimestampMillis:
			buf.WriteString(strconv.FormatInt(time.Now().UnixMilli(), 10))
		case TokenTimestampISO:
			buf.WriteString(time.Now().UTC().Format(time.RFC3339Nano))
		case TokenCounter:
			buf.WriteString(strconv.FormatInt(counter, 10))
		case TokenRandomInt:
			delta := seg.intMax - seg.intMin
			val := seg.intMin
			if delta > 0 {
				val += rand.IntN(delta)
			}
			buf.WriteString(strconv.Itoa(val))
		case TokenRandomStr:
			buf.WriteString(randomString(seg.strLen))
		}
	}

	return buf.Bytes(), nil
}
