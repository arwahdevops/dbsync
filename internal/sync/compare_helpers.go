// internal/sync/compare_helpers.go
package sync

import (
	"regexp"
	"strings"

	"go.uber.org/zap"
)

// --- Fungsi Helper Ekstraksi dan Normalisasi Tipe ---

func normalizeTypeName(typeName string) string {
	name := strings.ToLower(strings.TrimSpace(typeName))
	name = strings.ReplaceAll(name, " unsigned", "")
	name = strings.ReplaceAll(name, " zerofill", "")
	name = strings.TrimSpace(name)
	reMultiWordTypeWithNumericMod := regexp.MustCompile(`^([a-z\s]+[a-z])\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\)$`)
	if matches := reMultiWordTypeWithNumericMod.FindStringSubmatch(name); len(matches) > 1 {
		name = strings.TrimSpace(matches[1])
	} else {
		reSingleWordTypeWithNumericMod := regexp.MustCompile(`^([a-z]+)\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\)$`)
		if matches := reSingleWordTypeWithNumericMod.FindStringSubmatch(name); len(matches) > 1 {
			name = strings.TrimSpace(matches[1])
		}
	}
	reComplexStructure := regexp.MustCompile(`^([a-z][a-z\s]*[a-z]|[a-z])` + // G1: Tipe dasar
		`(\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?` + // G2: Modifier numerik opsional
		`(.*)$`) // G3: Sisa
	matchesComplex := reComplexStructure.FindStringSubmatch(name)
	if len(matchesComplex) > 1 {
		basePart := strings.TrimSpace(matchesComplex[1])
		numericModifierFound := len(matchesComplex) > 2 && matchesComplex[2] != ""
		remainingPart := ""
		if len(matchesComplex) > 3 {
			remainingPart = matchesComplex[3]
		}
		if numericModifierFound {
			name = strings.TrimSpace(basePart + " " + strings.TrimSpace(remainingPart))
		} else {
			name = strings.TrimSpace(basePart + remainingPart)
		}
	}
	name = regexp.MustCompile(`\s+`).ReplaceAllString(name, " ")
	name = strings.TrimSpace(name)
	aliases := map[string]string{
		"character varying":         "varchar", "double precision": "double", "boolean": "bool",
		"timestamp with time zone": "timestamptz", "timestamp without time zone": "timestamp",
		"time with time zone": "timetz", "time without time zone": "time", "integer": "int",
		"int4": "int", "int8": "bigint", "serial4": "serial", "serial8": "bigserial",
		"character": "char", "bit varying": "varbit",
	}
	if mapped, ok := aliases[name]; ok {
		name = mapped
	}
	return strings.TrimSpace(name)
}

func extractBaseTypeFromGenerated(fullType string, log *zap.Logger) string {
	trimmedFullType := strings.TrimSpace(fullType)
	originalInputForLog := trimmedFullType

	regexesForGeneratedClauses := []*regexp.Regexp{
		regexp.MustCompile(`(?i)(.*?)\s+GENERATED\s+BY\s+DEFAULT\s+AS\s+IDENTITY\s*\(.*\)`),
		regexp.MustCompile(`(?i)(.*?)\s+GENERATED\s+ALWAYS\s+AS\s+IDENTITY\s*\(.*\)`),
		regexp.MustCompile(`(?i)(.*?)\s+GENERATED\s+ALWAYS\s+AS\s*\(.*\)`),
		regexp.MustCompile(`(?i)(.*?)\s+AS\s*\(.*\)`),
		regexp.MustCompile(`(?i)(.*?)\s+GENERATED\s+BY\s+DEFAULT\s+AS\s+IDENTITY\s*$`),
		regexp.MustCompile(`(?i)(.*?)\s+GENERATED\s+ALWAYS\s+AS\s+IDENTITY\s*$`),
	}

	for _, re := range regexesForGeneratedClauses { // 'i' diganti '_'
		matches := re.FindStringSubmatch(trimmedFullType)
		if len(matches) > 1 {
			baseType := strings.TrimSpace(matches[1])
			if baseType != "" {
				upperBaseType := strings.ToUpper(baseType)
				if upperBaseType == "GENERATED" || upperBaseType == "AS" || upperBaseType == "IDENTITY" ||
					strings.HasPrefix(upperBaseType, "GENERATED ") ||
					strings.HasPrefix(upperBaseType, "AS ") {
					// log.Debug("Regex for generated clause matched, but extracted base type seems invalid.", zap.String("pattern", re.String()), zap.String("matched_base", baseType))
					continue
				}
				// log.Debug("Successfully extracted base type by matching a generated clause starter.", zap.String("pattern", re.String()), zap.String("original_trimmed_full_type", trimmedFullType), zap.String("extracted_base_type", baseType))
				return baseType
			}
		}
	}
	reSuffixKeywords := regexp.MustCompile(`(?i)^(.+?)\s+(VIRTUAL|STORED|PERSISTENT)\s*$`)
	matchesSuffix := reSuffixKeywords.FindStringSubmatch(trimmedFullType)
	if len(matchesSuffix) > 2 {
		baseType := strings.TrimSpace(matchesSuffix[1])
		if baseType != "" && len(baseType) > 1 && !isCommonSQLKeyword(strings.ToUpper(baseType)) {
			return baseType
		}
	}
	reSimpleSuffix := regexp.MustCompile(`(?i)^(.+?)\s+(GENERATED|IDENTITY)\s*$`)
	matchesSimpleSuffix := reSimpleSuffix.FindStringSubmatch(trimmedFullType)
	if len(matchesSimpleSuffix) > 2 {
		baseType := strings.TrimSpace(matchesSimpleSuffix[1])
		if baseType != "" && len(baseType) > 1 && !isCommonSQLKeyword(strings.ToUpper(baseType)) {
			return baseType
		}
	}
	log.Warn("Could not reliably extract base type from generated column definition. Falling back to using the full (trimmed) type string.",
		zap.String("trimmed_full_type", originalInputForLog))
	return originalInputForLog
}

func isCommonSQLKeyword(s string) bool {
	keywords := []string{"AS", "GENERATED", "IDENTITY", "BY", "DEFAULT", "ALWAYS", "STORED", "VIRTUAL", "PERSISTENT"}
	for _, kw := range keywords {
		if s == kw {
			return true
		}
	}
	return false
}

func isLargeTextOrBlob(normalizedTypeName string) bool {
	return strings.Contains(normalizedTypeName, "text") || strings.Contains(normalizedTypeName, "blob") ||
		normalizedTypeName == "clob" || normalizedTypeName == "bytea" ||
		normalizedTypeName == "json" || normalizedTypeName == "xml"
}

// --- Fungsi Helper Normalisasi Nilai Default ---

func normalizeDefaultValue(def string, dialect string) string {
	if def == "" { return "" }
	lower := strings.ToLower(strings.TrimSpace(def))

	stripOuterQuotes := func(s string) string {
		s = strings.TrimSpace(s)
		if len(s) >= 2 {
			firstChar, lastChar := s[0], s[len(s)-1]
			if (firstChar == '\'' && lastChar == '\'') || (firstChar == '"' && lastChar == '"') || (firstChar == '`' && lastChar == '`') {
				return s[1 : len(s)-1]
			}
		}
		return s
	}

	switch strings.ToLower(dialect) {
	case "mysql":
		if strings.Contains(lower, "on update current_timestamp") {
			parts := strings.Split(lower, "on update current_timestamp")
			lower = strings.TrimSpace(parts[0])
		}
		if strings.HasPrefix(lower, "b'") && strings.HasSuffix(lower, "'") && (lower == "b'0'" || lower == "b'1'") {
			lower = lower[2:3]
		}
	case "postgres":
		if strings.HasPrefix(lower, "nextval(") { return "nextval" }
		previousLower := ""
		for lower != previousLower {
			previousLower = lower
			reCast := regexp.MustCompile(`^(.*?)\s*::\s*[a-zA-Z_][a-zA-Z0-9_."\[\]<> ]*(?:\([^)]*\))?\s*$`)
			matches := reCast.FindStringSubmatch(lower)
			if len(matches) > 1 {
				potentialValue := strings.TrimSpace(matches[1])
				if len(potentialValue) >= 2 && potentialValue[0] == '(' && potentialValue[len(potentialValue)-1] == ')' {
					innerContent := potentialValue[1 : len(potentialValue)-1]
					if isBalanced(innerContent) {
						potentialValue = strings.TrimSpace(innerContent)
					}
				}
				lower = potentialValue
			}
		}
		lower = stripOuterQuotes(lower)
	case "sqlite":
		if strings.EqualFold(lower, "null") { return "null" }
	}

	switch lower {
	case "now()", "current_timestamp", "current_timestamp()", "getdate()", "sysdatetime()": return "current_timestamp"
	case "current_date", "current_date()": return "current_date"
	case "current_time", "current_time()": return "current_time"
	case "uuid()", "gen_random_uuid()", "newid()", "uuid_generate_v4()": return "uuid_function"
	}

	switch lower {
	case "true", "t", "yes", "y", "on", "1": return "1"
	case "false", "f", "no", "n", "off", "0": return "0"
	}

	lower = stripOuterQuotes(lower)
	if lower == "null" { return "null" }
	return lower
}

func isKnownDbFunction(normalizedDefaultValue string) bool {
	switch normalizedDefaultValue {
	case "current_timestamp", "current_date", "current_time", "nextval", "uuid_function":
		return true
	}
	return false
}

func isDefaultNullOrFunction(normalizedDefaultValue string) bool {
	if normalizedDefaultValue == "" || normalizedDefaultValue == "null" { return true }
	return isKnownDbFunction(normalizedDefaultValue)
}

// --- Fungsi Helper Normalisasi Definisi Constraint & Indeks ---

func normalizeCollation(coll, dialect string) string {
	c := strings.ToLower(strings.TrimSpace(coll))
	if c == "default" || c == "" { return "" }
	return c
}

func normalizeFKAction(action string) string {
	upper := strings.ToUpper(strings.TrimSpace(action))
	switch upper {
	case "SET DEFAULT", "SET NULL", "CASCADE", "RESTRICT": return upper
	case "", "NO ACTION": return "NO ACTION"
	}
	return upper
}

func isBalanced(s string) bool {
	balance := 0
	for _, char := range s {
		if char == '(' { balance++ } else if char == ')' { balance-- }
		if balance < 0 { return false }
	}
	return balance == 0
}

func normalizeCheckDefinition(def string) string {
	if def == "" { return "" }
	norm := regexp.MustCompile(`--.*`).ReplaceAllString(def, "")
	norm = regexp.MustCompile(`/\*.*?\*/`).ReplaceAllString(norm, "")
	norm = strings.ToLower(norm)
	norm = regexp.MustCompile(`\s+`).ReplaceAllString(norm, " ")
	norm = strings.TrimSpace(norm)
	for {
		if len(norm) < 2 || norm[0] != '(' || norm[len(norm)-1] != ')' { break }
		innerContent := norm[1 : len(norm)-1]
		if !isBalanced(innerContent) { break }
		norm = strings.TrimSpace(innerContent)
	}
	return norm
}

func normalizeIndexOrConstraintDef(rawDef string, defType string, dialect string, log *zap.Logger) string {
	if rawDef == "" { return "" }
	var normalizedDef string

	if defType == "CHECK" {
		normalizedDef = normalizeCheckDefinition(rawDef)
	} else if defType == "INDEX" {
		tempDef := strings.ToLower(rawDef)
		reIndexCore := regexp.MustCompile(`(?i)(?:create\s+(?:unique\s+)?index\s+(?:\S+)\s+on\s+\S+\s*)?(\(.*?\))(?:\s*using\s+\w+)?(?:\s*where\s+.*)?`)
		matchesCore := reIndexCore.FindStringSubmatch(tempDef)
		if len(matchesCore) > 1 {
			corePart := strings.TrimSpace(matchesCore[1])
			corePart = regexp.MustCompile(`\s*,\s*`).ReplaceAllString(corePart, ",")
			corePart = regexp.MustCompile(`\s+`).ReplaceAllString(corePart, " ")
			normalizedDef = corePart
		} else {
			normalizedDef = regexp.MustCompile(`\s+`).ReplaceAllString(strings.ToLower(strings.TrimSpace(rawDef))," ")
		}
	} else {
		normalizedDef = strings.ToLower(strings.TrimSpace(rawDef))
		normalizedDef = regexp.MustCompile(`\s+`).ReplaceAllString(normalizedDef, " ")
	}
	return normalizedDef
}

func normalizeGenerationExpression(expr, dialect string) string {
	if expr == "" { return "" }
	norm := strings.ToLower(strings.TrimSpace(expr))
	norm = regexp.MustCompile(`\s+`).ReplaceAllString(norm, " ")
	if strings.HasPrefix(norm, "(") && strings.HasSuffix(norm, ")") {
		inner := norm[1 : len(norm)-1]
		if isBalanced(inner) {
			norm = strings.TrimSpace(inner)
		}
	}
	return norm
}
