package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

func sanitizeSQL(input string) string {
	return strings.ReplaceAll(input, ";", "")
}

func parseSQLFileRefs(filePath string) ([]string, error) {
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

func renderTemplate(raw string, inc bool) string {
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

func substituteRefs(sqlQuery string, refMapping map[string]string) string {
	for refName, parquetFile := range refMapping {
		pattern := fmt.Sprintf(`\{\{\s*ref\(['"]%s['"]\)\s*\}\}`, regexp.QuoteMeta(refName))
		re := regexp.MustCompile(pattern)
		replacement := fmt.Sprintf("read_parquet('%s')", parquetFile)
		sqlQuery = re.ReplaceAllString(sqlQuery, replacement)
	}
	return sqlQuery
}
