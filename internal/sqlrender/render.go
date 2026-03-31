package sqlrender

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/maksimilian/duckdb-orchestrator/internal/config"
)

func SanitizeSQL(input string) string {
	return strings.ReplaceAll(input, ";", "")
}

func ParseSQLFileRefs(filePath string) ([]string, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	re := regexp.MustCompile(`\{\{\s*ref\(['"]([^'"]+)['"]\)\s*\}\}`)
	matches := re.FindAllStringSubmatch(string(data), -1)

	var refs []string
	for _, match := range matches {
		if len(match) > 1 {
			refs = append(refs, match[1])
		}
	}
	return refs, nil
}

func ParseSQLFileSources(filePath string) ([]string, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	re := regexp.MustCompile(`\{\{\s*source\(['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\)\s*\}\}`)
	matches := re.FindAllStringSubmatch(string(data), -1)

	var sources []string
	for _, match := range matches {
		if len(match) > 2 {
			sources = append(sources, fmt.Sprintf("%s.%s", match[1], match[2]))
		}
	}
	return sources, nil
}

func HasIncremental(raw string) bool {
	return strings.Contains(raw, "is_incremental()")
}

func StripConfigBlocks(raw string) string {
	reConfig := regexp.MustCompile(`(?s)\{\{\s*config\s*\(.*?\)\s*\}\}`)
	return reConfig.ReplaceAllString(raw, "")
}

func ParseModelConfig(raw string) (config.ModelConfig, error) {
	cfg := config.ModelConfig{}
	reConfig := regexp.MustCompile(`(?s)\{\{\s*config\s*\((.*?)\)\s*\}\}`)
	match := reConfig.FindStringSubmatch(raw)
	if len(match) < 2 {
		return cfg, nil
	}

	assignments, err := splitConfigAssignments(match[1])
	if err != nil {
		return cfg, err
	}

	for _, assignment := range assignments {
		parts := strings.SplitN(assignment, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		value = trimQuoted(value)

		switch key {
		case "storage_location":
			cfg.StorageLocation = strings.ToLower(value)
		case "storage_option":
			cfg.StorageOption = value
		case "partition_column":
			cfg.PartitionColumn = value
		case "incremental_strategy":
			cfg.IncrementalStrategy = value
			if value != "" {
				cfg.Incremental = true
			}
		case "materialized":
			if strings.EqualFold(value, "incremental") {
				cfg.Incremental = true
			}
		case "incremental":
			cfg.Incremental = strings.EqualFold(value, "true")
		}
	}

	if HasIncremental(raw) {
		cfg.Incremental = true
	}

	return cfg, nil
}

func splitConfigAssignments(body string) ([]string, error) {
	var parts []string
	var current strings.Builder
	var quote rune
	bracketDepth := 0

	for _, r := range body {
		switch {
		case quote != 0:
			current.WriteRune(r)
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"':
			quote = r
			current.WriteRune(r)
		case r == '[':
			bracketDepth++
			current.WriteRune(r)
		case r == ']':
			if bracketDepth == 0 {
				return nil, fmt.Errorf("unbalanced brackets in config block")
			}
			bracketDepth--
			current.WriteRune(r)
		case r == ',' && bracketDepth == 0:
			part := strings.TrimSpace(current.String())
			if part != "" {
				parts = append(parts, part)
			}
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}

	if quote != 0 || bracketDepth != 0 {
		return nil, fmt.Errorf("unterminated config block")
	}

	if tail := strings.TrimSpace(current.String()); tail != "" {
		parts = append(parts, tail)
	}

	return parts, nil
}

func trimQuoted(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 {
		if (value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"') {
			return value[1 : len(value)-1]
		}
	}
	return value
}

func RenderTemplate(raw string, inc bool) string {
	raw = StripConfigBlocks(raw)
	boolStr := "FALSE"
	if inc {
		boolStr = "TRUE"
	}
	reIncr := regexp.MustCompile(`\{\{\s*is_incremental\(\)\s*\}\}`)
	raw = reIncr.ReplaceAllString(raw, boolStr)

	reIfElse := regexp.MustCompile(`(?s)\{% *if +is_incremental\(\) *%\}(.*?)\{% *else *%\}(.*?)\{% *endif *%\}`)
	for {
		loc := reIfElse.FindStringSubmatchIndex(raw)
		if loc == nil {
			break
		}
		tStart, tEnd := loc[2], loc[3] // true block
		fStart, fEnd := loc[4], loc[5] // false block
		replace := raw[fStart:fEnd]
		if inc {
			replace = raw[tStart:tEnd]
		}
		raw = raw[:loc[0]] + replace + raw[loc[1]:]
	}

	reIf := regexp.MustCompile(`(?s)\{% *if +is_incremental\(\) *%\}(.*?)\{% *endif *%\}`)
	for {
		loc := reIf.FindStringSubmatchIndex(raw)
		if loc == nil {
			break
		}
		blockStart, blockEnd := loc[2], loc[3]
		replace := ""
		if inc {
			replace = raw[blockStart:blockEnd]
		}
		raw = raw[:loc[0]] + replace + raw[loc[1]:]
	}
	return raw
}

func SubstituteRefs(sqlQuery string, refMapping map[string]string) string {
	for refName, parquetFile := range refMapping {
		pattern := fmt.Sprintf(`\{\{\s*ref\(['"]%s['"]\)\s*\}\}`, regexp.QuoteMeta(refName))
		re := regexp.MustCompile(pattern)
		replacement := fmt.Sprintf("read_parquet('%s')", parquetFile)
		sqlQuery = re.ReplaceAllString(sqlQuery, replacement)
	}
	return sqlQuery
}

func SubstituteSources(sqlQuery string, sourceMapping map[string]string) string {
	for sourceRef, location := range sourceMapping {
		parts := strings.SplitN(sourceRef, ".", 2)
		if len(parts) != 2 {
			continue
		}
		pattern := fmt.Sprintf(`\{\{\s*source\(['"]%s['"]\s*,\s*['"]%s['"]\)\s*\}\}`, regexp.QuoteMeta(parts[0]), regexp.QuoteMeta(parts[1]))
		re := regexp.MustCompile(pattern)
		replacement := fmt.Sprintf("read_parquet('%s')", location)
		sqlQuery = re.ReplaceAllString(sqlQuery, replacement)
	}
	return sqlQuery
}
