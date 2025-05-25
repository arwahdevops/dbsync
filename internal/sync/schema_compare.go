// internal/sync/schema_compare.go
package sync

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/apd/v3"
	"go.uber.org/zap"
)

// --- Fungsi Perbandingan Inti (Method dari SchemaSyncer) ---

func (s *SchemaSyncer) determineSourceTypeForComparison(src ColumnInfo, columnLogger *zap.Logger) string {
	if src.IsGenerated {
		baseSrcType := src.MappedType
		if baseSrcType == "" {
			columnLogger.Warn("Source generated column MappedType is empty, re-extracting base type.", zap.String("src_original_type", src.Type))
			baseSrcType = extractBaseTypeFromGenerated(src.Type, columnLogger)
		}
		columnLogger.Debug("Source column is generated. Using its base type for comparison.", zap.String("base_src_type_for_comp", baseSrcType))
		return baseSrcType
	}
	if src.MappedType != "" {
		return src.MappedType
	}
	columnLogger.Error("Source column MappedType is empty and not generated. Using original source type for comparison.", zap.String("src_original_type", src.Type))
	return src.Type
}

func (s *SchemaSyncer) getColumnModifications(src, dst ColumnInfo, log *zap.Logger) []string {
	diffs := []string{}
	columnLogger := log.With(zap.String("column", src.Name))
	srcTypeForComparison := s.determineSourceTypeForComparison(src, columnLogger)

	if !s.areTypesEquivalent(srcTypeForComparison, dst.Type, src, dst, columnLogger) {
		diffs = append(diffs, fmt.Sprintf("type (desired: %s, actual_dst: %s, src_original: %s)", src.MappedType, dst.Type, src.Type))
	}
	if src.IsNullable != dst.IsNullable {
		diffs = append(diffs, fmt.Sprintf("nullability (desired: %t, actual_dst: %t)", src.IsNullable, dst.IsNullable))
	}
	if !src.AutoIncrement && !src.IsGenerated && !dst.AutoIncrement && !dst.IsGenerated {
		srcDefValStr := ""
		if src.DefaultValue.Valid {
			srcDefValStr = src.DefaultValue.String
		}
		dstDefValStr := ""
		if dst.DefaultValue.Valid {
			dstDefValStr = dst.DefaultValue.String
		}
		if !s.areDefaultsEquivalent(srcDefValStr, dstDefValStr, srcTypeForComparison, columnLogger) {
			srcDefLog := "NULL"
			if src.DefaultValue.Valid {
				srcDefLog = fmt.Sprintf("'%s'", src.DefaultValue.String)
			}
			dstDefLog := "NULL"
			if dst.DefaultValue.Valid {
				dstDefLog = fmt.Sprintf("'%s'", dst.DefaultValue.String)
			}
			diffs = append(diffs, fmt.Sprintf("default (desired: %s, actual_dst: %s)", srcDefLog, dstDefLog))
		}
	}
	if src.AutoIncrement != dst.AutoIncrement {
		diffs = append(diffs, fmt.Sprintf("auto_increment (desired: %t, actual_dst: %t)", src.AutoIncrement, dst.AutoIncrement))
		columnLogger.Warn("AutoIncrement/Identity status difference detected.", zap.Bool("desired_auto_inc", src.AutoIncrement), zap.Bool("actual_dst_auto_inc", dst.AutoIncrement))
	}
	if src.IsGenerated != dst.IsGenerated {
		diffs = append(diffs, fmt.Sprintf("generated_status (desired: %t, actual_dst: %t)", src.IsGenerated, dst.IsGenerated))
		columnLogger.Warn("Generated column status difference detected.")
	} else if src.IsGenerated && dst.IsGenerated {
		normSrcGenExpr := normalizeGenerationExpression(src.GenerationExpression.String, s.srcDialect)
		normDstGenExpr := normalizeGenerationExpression(dst.GenerationExpression.String, s.dstDialect)
		if normSrcGenExpr != normDstGenExpr {
			diffs = append(diffs, fmt.Sprintf("generation_expression (desired_norm: '%s', actual_dst_norm: '%s')", normSrcGenExpr, normDstGenExpr))
			columnLogger.Warn("Generated column expressions differ (after normalization).",
				zap.String("src_gen_expr_raw", src.GenerationExpression.String), zap.String("dst_gen_expr_raw", dst.GenerationExpression.String))
		} else {
			columnLogger.Debug("Both columns are generated and their (normalized) expressions appear equivalent.")
		}
	}
	srcCollationStr := ""
	if src.Collation.Valid {
		srcCollationStr = src.Collation.String
	}
	dstCollationStr := ""
	if dst.Collation.Valid {
		dstCollationStr = dst.Collation.String
	}
	if isStringType(normalizeTypeName(srcTypeForComparison)) && isStringType(normalizeTypeName(dst.Type)) {
		if srcCollationStr != "" || dstCollationStr != "" {
			normSrcColl := normalizeCollation(srcCollationStr, s.srcDialect)
			normDstColl := normalizeCollation(dstCollationStr, s.dstDialect)
			if !strings.EqualFold(normSrcColl, normDstColl) {
				isCrossDialectDefaultMismatch := false
				if s.srcDialect != s.dstDialect && ((normSrcColl != "" && normDstColl == "") || (normSrcColl == "" && normDstColl != "")) {
					isCrossDialectDefaultMismatch = true
					columnLogger.Debug("Collation difference detected across dialects where one uses default.", zap.String("desired_collation", normSrcColl), zap.String("actual_dst_collation", normDstColl))
				}
				if !isCrossDialectDefaultMismatch || (normSrcColl != "" && normDstColl != "") {
					diffs = append(diffs, fmt.Sprintf("collation (desired: %s, actual_dst: %s)", srcCollationStr, dstCollationStr))
				}
			}
		}
	}
	srcCommentStr := ""
	if src.Comment.Valid {
		srcCommentStr = src.Comment.String
	}
	dstCommentStr := ""
	if dst.Comment.Valid {
		dstCommentStr = dst.Comment.String
	}
	if srcCommentStr != dstCommentStr {
		columnLogger.Debug("Column comment difference detected.",
			zap.String("desired_comment", srcCommentStr), zap.String("actual_dst_comment", dstCommentStr))
	}
	if len(diffs) > 0 {
		columnLogger.Info("Column differences identified.", zap.Strings("differences", diffs))
	}
	return diffs
}

func (s *SchemaSyncer) areTypesEquivalent(srcTypeForComp, dstRawType string, srcInfo, dstInfo ColumnInfo, log *zap.Logger) bool {
	log.Debug("Comparing types for equivalence",
		zap.String("src_type_for_comparison_as_target_base", srcTypeForComp),
		zap.String("dst_raw_type_from_db", dstRawType))
	normSrcCompTypeBase := normalizeTypeName(srcTypeForComp)
	normDstRawTypeBase := normalizeTypeName(dstRawType)
	log.Debug("Normalized base types for equivalence check",
		zap.String("norm_desired_target_base_type", normSrcCompTypeBase),
		zap.String("norm_actual_dst_base_type", normDstRawTypeBase))

	if normSrcCompTypeBase == normDstRawTypeBase {
		if isStringType(normSrcCompTypeBase) || isBinaryType(normSrcCompTypeBase) {
			desiredLen := int64(-1)
			if srcInfo.MappedType != "" {
				normDesiredFullMappedType := normalizeTypeName(srcInfo.MappedType)
				if isLargeTextOrBlob(normDesiredFullMappedType) {
					desiredLen = -2
				} else if srcInfo.Length.Valid {
					desiredLen = srcInfo.Length.Int64
				}
			} else if srcInfo.Length.Valid {
				desiredLen = srcInfo.Length.Int64
			}
			actualLen := int64(-1)
			if isLargeTextOrBlob(normDstRawTypeBase) {
				actualLen = -2
			} else if dstInfo.Length.Valid {
				actualLen = dstInfo.Length.Int64
			}
			isDesiredFixed := (strings.HasPrefix(normSrcCompTypeBase, "char") && !strings.HasPrefix(normSrcCompTypeBase, "varchar")) || (strings.HasPrefix(normSrcCompTypeBase, "binary") && !strings.HasPrefix(normSrcCompTypeBase, "varbinary"))
			isActualFixed := (strings.HasPrefix(normDstRawTypeBase, "char") && !strings.HasPrefix(normDstRawTypeBase, "varchar")) || (strings.HasPrefix(normDstRawTypeBase, "binary") && !strings.HasPrefix(normDstRawTypeBase, "varbinary"))
			if isDesiredFixed != isActualFixed && desiredLen == actualLen && desiredLen > 0 {
				log.Debug("Type fixed/variable nature mismatch with same length, considered different.")
				return false
			}
			if desiredLen != actualLen {
				log.Debug("Type length mismatch.", zap.Int64("desired_len", desiredLen), zap.Int64("actual_len", actualLen))
				return false
			}
		}
		if isPrecisionRelevant(normSrcCompTypeBase) {
			desiredPrec := int64(-1)
			if srcInfo.Precision.Valid {
				desiredPrec = srcInfo.Precision.Int64
			}
			actualPrec := int64(-1)
			if dstInfo.Precision.Valid {
				actualPrec = dstInfo.Precision.Int64
			}
			isTimeType := strings.Contains(normSrcCompTypeBase, "time") || strings.Contains(normSrcCompTypeBase, "timestamp") || strings.Contains(normSrcCompTypeBase, "datetime")
			if isTimeType {
				if !((desiredPrec <= 0 || !srcInfo.Precision.Valid) && (actualPrec <= 0 || !dstInfo.Precision.Valid)) && (desiredPrec != actualPrec) {
					log.Debug("Time type precision mismatch.")
					return false
				}
			} else {
				if srcInfo.Precision.Valid != dstInfo.Precision.Valid || (srcInfo.Precision.Valid && dstInfo.Precision.Valid && desiredPrec != actualPrec) {
					log.Debug("Decimal/Numeric type precision mismatch or one is not set.")
					return false
				}
			}
		}
		if isScaleRelevant(normSrcCompTypeBase) {
			desiredScale := int64(0)
			if srcInfo.Scale.Valid {
				desiredScale = srcInfo.Scale.Int64
			}
			actualScale := int64(0)
			if dstInfo.Scale.Valid {
				actualScale = dstInfo.Scale.Int64
			}
			if (srcInfo.Scale.Valid != dstInfo.Scale.Valid && !(desiredScale == 0 && actualScale == 0)) || (srcInfo.Scale.Valid && dstInfo.Scale.Valid && desiredScale != actualScale) {
				log.Debug("Decimal/Numeric type scale mismatch or one is explicitly set while other is default 0 and values differ.")
				return false
			}
		}
		log.Debug("Base types match and relevant attributes appear equivalent.")
		return true
	}
	if (s.srcDialect == "mysql" && (s.dstDialect == "postgres" || s.dstDialect == "sqlite")) || ((s.srcDialect == "postgres" || s.srcDialect == "sqlite") && s.dstDialect == "mysql") {
		isSrcEffectivelyBool := normSrcCompTypeBase == "bool"
		isDstEffectivelyBool := normDstRawTypeBase == "bool" || (normDstRawTypeBase == "tinyint" && dstInfo.Length.Valid && dstInfo.Length.Int64 == 1 && s.dstDialect == "mysql")
		if isSrcEffectivelyBool && isDstEffectivelyBool {
			log.Debug("Equivalent boolean types found across dialects.")
			return true
		}
	}
	if (normSrcCompTypeBase == "datetime" && normDstRawTypeBase == "timestamp") || (normSrcCompTypeBase == "timestamp" && normDstRawTypeBase == "datetime") {
		desiredPrec := int64(-1)
		if srcInfo.Precision.Valid {
			desiredPrec = srcInfo.Precision.Int64
		}
		actualPrec := int64(-1)
		if dstInfo.Precision.Valid {
			actualPrec = dstInfo.Precision.Int64
		}
		if ((desiredPrec <= 0 || !srcInfo.Precision.Valid) && (actualPrec <= 0 || !dstInfo.Precision.Valid)) || (desiredPrec == actualPrec) {
			log.Debug("Equivalent datetime/timestamp types with similar/default precision found.")
			return true
		}
		log.Debug("Types datetime/timestamp differ in precision.")
		return false
	}
	if (normSrcCompTypeBase == "decimal" && normDstRawTypeBase == "numeric") || (normSrcCompTypeBase == "numeric" && normDstRawTypeBase == "decimal") {
		desiredPrec := int64(-1)
		if srcInfo.Precision.Valid {
			desiredPrec = srcInfo.Precision.Int64
		}
		actualPrec := int64(-1)
		if dstInfo.Precision.Valid {
			actualPrec = dstInfo.Precision.Int64
		}
		desiredScale := int64(0)
		if srcInfo.Scale.Valid {
			desiredScale = srcInfo.Scale.Int64
		}
		actualScale := int64(0)
		if dstInfo.Scale.Valid {
			actualScale = dstInfo.Scale.Int64
		}
		precisionMatch := (srcInfo.Precision.Valid == dstInfo.Precision.Valid && desiredPrec == actualPrec) || (!srcInfo.Precision.Valid && !dstInfo.Precision.Valid)
		scaleMatch := desiredScale == actualScale
		if precisionMatch && scaleMatch {
			log.Debug("Equivalent decimal/numeric types with matching precision/scale found.")
			return true
		}
		log.Debug("Types decimal/numeric differ in precision/scale or explicit settings.")
		return false
	}
	log.Debug("Types are not equivalent after normalization and cross-dialect checks.")
	return false
}

func (s *SchemaSyncer) areDefaultsEquivalent(srcDefRaw, dstDefRaw, typeForDefaultComparison string, log *zap.Logger) bool {
	normSrcDef := normalizeDefaultValue(srcDefRaw, s.srcDialect)
	normDstDef := normalizeDefaultValue(dstDefRaw, s.dstDialect)
	log.Debug("Comparing default values", zap.String("src_def_raw", srcDefRaw), zap.String("dst_def_raw", dstDefRaw),
		zap.String("norm_src_def_for_comp", normSrcDef), zap.String("norm_dst_def_for_comp", normDstDef),
		zap.String("type_context_for_comparison (desired_target_type)", typeForDefaultComparison))
	normTypeForDefaultCtx := normalizeTypeName(typeForDefaultComparison)

	if normSrcDef == normDstDef {
		srcIsQuotedOriginal := (strings.HasPrefix(srcDefRaw, "'") && strings.HasSuffix(srcDefRaw, "'")) || (strings.HasPrefix(srcDefRaw, "\"") && strings.HasSuffix(srcDefRaw, "\""))
		dstIsQuotedOriginal := (strings.HasPrefix(dstDefRaw, "'") && strings.HasSuffix(dstDefRaw, "'")) || (strings.HasPrefix(dstDefRaw, "\"") && strings.HasSuffix(dstDefRaw, "\""))
		_, errSrcRawNumeric := strconv.ParseFloat(strings.TrimSpace(srcDefRaw), 64)
		_, errDstRawNumeric := strconv.ParseFloat(strings.TrimSpace(dstDefRaw), 64)
		srcIsPotentiallyRawNumeric := errSrcRawNumeric == nil && !srcIsQuotedOriginal
		dstIsPotentiallyRawNumeric := errDstRawNumeric == nil && !dstIsQuotedOriginal
		if isNumericType(normTypeForDefaultCtx) {
			if (srcIsPotentiallyRawNumeric && dstIsQuotedOriginal) || (dstIsPotentiallyRawNumeric && srcIsQuotedOriginal) {
				log.Debug("Normalized defaults are string-identical, BUT DDL representation (raw numeric vs. quoted) differs for a numeric type. Considered different.")
				return false
			}
		}
		log.Debug("Normalized defaults are string-identical and DDL representation consistent (or non-numeric type).")
		return true
	}
	if (normSrcDef == "null" && normDstDef == "") || (normSrcDef == "" && normDstDef == "null") {
		log.Debug("One default is 'null' (normalized) and other is empty (no default set). Considered equivalent.")
		return true
	}
	if isNumericType(normTypeForDefaultCtx) {
		if !isKnownDbFunction(normSrcDef) && !isKnownDbFunction(normDstDef) {
			if normTypeForDefaultCtx == "decimal" || normTypeForDefaultCtx == "numeric" {
				srcAPD, _, srcErrAPD := apd.NewFromString(normSrcDef)
				dstAPD, _, dstErrAPD := apd.NewFromString(normDstDef)
				if srcErrAPD == nil && dstErrAPD == nil {
					if srcAPD.Cmp(dstAPD) == 0 {
						log.Debug("Decimal/Numeric defaults (APD-parsed) are equivalent.")
						return true
					}
					log.Debug("Decimal/Numeric defaults (APD-parsed) differ.")
					return false
				}
				log.Debug("Could not parse one or both decimal/numeric defaults as APD. Falling back.")
			}
			srcNumVal, errSrcFloat := strconv.ParseFloat(normSrcDef, 64)
			dstNumVal, errDstFloat := strconv.ParseFloat(normDstDef, 64)
			if errSrcFloat == nil && errDstFloat == nil {
				if srcNumVal == dstNumVal {
					log.Debug("Numeric defaults (float-parsed) are equivalent.")
					return true
				}
				log.Debug("Numeric defaults (float-parsed) differ.")
			} else if (errSrcFloat == nil && errDstFloat != nil) || (errSrcFloat != nil && errDstFloat == nil) {
				log.Debug("One default is float-parsable, other is not (and not identical strings/functions). Considered different.")
				return false
			}
		}
	}
	log.Debug("Defaults are not equivalent after all checks.")
	return false
}

func (s *SchemaSyncer) needsColumnModification(src, dst ColumnInfo, log *zap.Logger) bool {
	return len(s.getColumnModifications(src, dst, log)) > 0
}

func (s *SchemaSyncer) needsIndexModification(src, dst IndexInfo, log *zap.Logger) bool {
	log = log.With(zap.String("index_name", src.Name))
	if src.IsUnique != dst.IsUnique {
		log.Debug("Index unique flag mismatch.", zap.Bool("desired_is_unique", src.IsUnique), zap.Bool("actual_dst_is_unique", dst.IsUnique))
		return true
	}
	if !reflect.DeepEqual(src.Columns, dst.Columns) {
		log.Debug("Index columns mismatch (order matters).", zap.Strings("desired_cols_ordered", src.Columns), zap.Strings("actual_dst_cols_ordered", dst.Columns))
		return true
	}
	if src.RawDef != "" || dst.RawDef != "" {
		normSrcRawDef := normalizeIndexOrConstraintDef(src.RawDef, "INDEX", s.srcDialect, log)
		normDstRawDef := normalizeIndexOrConstraintDef(dst.RawDef, "INDEX", s.dstDialect, log)
		if normSrcRawDef != normDstRawDef {
			log.Debug("Normalized index RawDef mismatch.", zap.String("desired_raw_def_norm", normSrcRawDef), zap.String("actual_dst_raw_def_norm", normDstRawDef))
			return true
		}
	}
	log.Debug("Index definitions appear equivalent.")
	return false
}

func (s *SchemaSyncer) needsConstraintModification(src, dst ConstraintInfo, log *zap.Logger) bool {
	log = log.With(zap.String("constraint_name", src.Name), zap.String("constraint_type", src.Type))
	if src.Type != dst.Type {
		log.Debug("Constraint type mismatch.", zap.String("desired_type", src.Type), zap.String("actual_dst_type", dst.Type))
		return true
	}
	if !reflect.DeepEqual(src.Columns, dst.Columns) {
		log.Debug("Constraint columns mismatch (order matters).", zap.Strings("desired_cols_ordered", src.Columns), zap.Strings("actual_dst_cols_ordered", dst.Columns))
		return true
	}
	switch src.Type {
	case "FOREIGN KEY":
		if src.ForeignTable != dst.ForeignTable {
			log.Debug("FK foreign table mismatch.", zap.String("desired_foreign_table", src.ForeignTable), zap.String("actual_dst_foreign_table", dst.ForeignTable))
			return true
		}
		if !reflect.DeepEqual(src.ForeignColumns, dst.ForeignColumns) {
			log.Debug("FK foreign columns mismatch (order matters).", zap.Strings("desired_foreign_cols_ordered", src.ForeignColumns), zap.Strings("actual_dst_foreign_cols_ordered", dst.ForeignColumns))
			return true
		}
		srcDelNorm := normalizeFKAction(src.OnDelete)
		dstDelNorm := normalizeFKAction(dst.OnDelete)
		if srcDelNorm != dstDelNorm {
			log.Debug("FK ON DELETE action mismatch.", zap.String("desired_on_delete", srcDelNorm), zap.String("actual_dst_on_delete", dstDelNorm))
			return true
		}
		srcUpdNorm := normalizeFKAction(src.OnUpdate)
		dstUpdNorm := normalizeFKAction(dst.OnUpdate)
		if srcUpdNorm != dstUpdNorm {
			log.Debug("FK ON UPDATE action mismatch.", zap.String("desired_on_update", srcUpdNorm), zap.String("actual_dst_on_update", dstUpdNorm))
			return true
		}
	case "CHECK":
		normSrcDef := normalizeCheckDefinition(src.Definition)
		normDstDef := normalizeCheckDefinition(dst.Definition)
		if normSrcDef != normDstDef {
			log.Debug("CHECK constraint definition mismatch after normalization.", zap.String("desired_check_def_norm", normSrcDef), zap.String("actual_dst_check_def_norm", normDstDef))
			return true
		}
	case "UNIQUE", "PRIMARY KEY":
		// Perbandingan RawDef untuk UNIQUE dan PRIMARY KEY dihapus karena ConstraintInfo tidak memiliki field RawDef.
		// Perbandingan berdasarkan Type dan Columns (urutan penting) sudah cukup untuk kasus umum.
		// Jika ada opsi spesifik dialek pada constraint ini yang perlu dibandingkan (misalnya, USING INDEX),
		// maka field RawDef perlu ditambahkan ke ConstraintInfo dan diisi oleh fetcher.
		break
	}
	log.Debug("Constraint definitions appear equivalent.")
	return false
}
