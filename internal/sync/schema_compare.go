// internal/sync/schema_compare.go
package sync

import (
	"reflect" // Masih dibutuhkan untuk perbandingan slice di index/constraint
	"sort"    // Masih dibutuhkan untuk sorting kolom di index/constraint
	// "fmt" // Mungkin tidak dibutuhkan lagi secara langsung
	// "regexp" // Mungkin tidak dibutuhkan lagi secara langsung
	// "strconv" // Mungkin tidak dibutuhkan lagi secara langsung
	// "strings" // Mungkin tidak dibutuhkan lagi secara langsung
	"go.uber.org/zap"
)

// needsColumnModification mengembalikan true jika ada perbedaan signifikan pada kolom.
// Ini memanggil helper s.getColumnModifications yang telah dipindahkan ke compare_columns.go
func (s *SchemaSyncer) needsColumnModification(src, dst ColumnInfo, log *zap.Logger) bool {
	return len(s.getColumnModifications(src, dst, log)) > 0
}

// needsIndexModification - Membandingkan definisi indeks (dasar)
// Untuk saat ini, kita biarkan implementasi ini di sini.
// Jika menjadi sangat kompleks, bisa dipindahkan ke compare_indexes.go
func (s *SchemaSyncer) needsIndexModification(src, dst IndexInfo, log *zap.Logger) bool {
	log = log.With(zap.String("index_name", src.Name))
	if src.IsUnique != dst.IsUnique {
		log.Debug("Index unique flag mismatch", zap.Bool("src_is_unique", src.IsUnique), zap.Bool("dst_is_unique", dst.IsUnique))
		return true
	}

	// Urutkan kolom sebelum membandingkan slice untuk konsistensi
	// Buat salinan agar tidak mengubah slice asli di IndexInfo
	srcColsSorted := make([]string, len(src.Columns)); copy(srcColsSorted, src.Columns); sort.Strings(srcColsSorted)
	dstColsSorted := make([]string, len(dst.Columns)); copy(dstColsSorted, dst.Columns); sort.Strings(dstColsSorted)

	if !reflect.DeepEqual(srcColsSorted, dstColsSorted) {
		log.Debug("Index columns mismatch", zap.Strings("src_cols", srcColsSorted), zap.Strings("dst_cols", dstColsSorted))
		return true
	}

	// Perbandingan RawDef (jika ada dan jika dialeknya sama, ini bisa berguna untuk PostgreSQL)
	// Namun, RawDef bisa sangat bervariasi karena whitespace atau urutan opsi.
	// Ini adalah perbandingan yang rapuh.
	// if s.srcDialect == s.dstDialect && src.RawDef != "" && dst.RawDef != "" {
	//    normSrcDef := normalizeSqlExpression(src.RawDef) // Perlu helper normalisasi
	//    normDstDef := normalizeSqlExpression(dst.RawDef)
	// 	if normSrcDef != normDstDef {
	// 		log.Debug("Index raw definition mismatch", zap.String("src_raw_def", src.RawDef), zap.String("dst_raw_def", dst.RawDef))
	// 		return true
	// 	}
	// }

	// TODO: Bandingkan tipe indeks (BTREE, HASH, dll.) jika info tersedia dan relevan.
	// Ini memerlukan pengambilan tipe indeks dari kedua database.
	log.Debug("Index definitions appear equivalent.")
	return false
}

// needsConstraintModification - Membandingkan definisi constraint (dasar)
// Untuk saat ini, kita biarkan implementasi ini di sini.
// Jika menjadi sangat kompleks, bisa dipindahkan ke compare_constraints.go
func (s *SchemaSyncer) needsConstraintModification(src, dst ConstraintInfo, log *zap.Logger) bool {
	log = log.With(zap.String("constraint_name", src.Name), zap.String("constraint_type", src.Type))

	if src.Type != dst.Type {
		log.Debug("Constraint type mismatch", zap.String("src_type", src.Type), zap.String("dst_type", dst.Type))
		return true
	}

	srcColsSorted := make([]string, len(src.Columns)); copy(srcColsSorted, src.Columns); sort.Strings(srcColsSorted)
	dstColsSorted := make([]string, len(dst.Columns)); copy(dstColsSorted, dst.Columns); sort.Strings(dstColsSorted)

	if !reflect.DeepEqual(srcColsSorted, dstColsSorted) {
		log.Debug("Constraint columns mismatch", zap.Strings("src_cols", srcColsSorted), zap.Strings("dst_cols", dstColsSorted))
		return true
	}

	switch src.Type {
	case "FOREIGN KEY":
		if src.ForeignTable != dst.ForeignTable {
			log.Debug("FK foreign table mismatch", zap.String("src_foreign_table", src.ForeignTable), zap.String("dst_foreign_table", dst.ForeignTable))
			return true
		}

		srcForeignColsSorted := make([]string, len(src.ForeignColumns)); copy(srcForeignColsSorted, src.ForeignColumns); sort.Strings(srcForeignColsSorted)
		dstForeignColsSorted := make([]string, len(dst.ForeignColumns)); copy(dstForeignColsSorted, dst.ForeignColumns); sort.Strings(dstForeignColsSorted)
		if !reflect.DeepEqual(srcForeignColsSorted, dstForeignColsSorted) {
			log.Debug("FK foreign columns mismatch", zap.Strings("src_foreign_cols", srcForeignColsSorted), zap.Strings("dst_foreign_cols", dstForeignColsSorted))
			return true
		}

		srcDelNorm := normalizeFKAction(src.OnDelete)
		dstDelNorm := normalizeFKAction(dst.OnDelete)
		if srcDelNorm != dstDelNorm {
			log.Debug("FK ON DELETE action mismatch", zap.String("src_on_delete", srcDelNorm), zap.String("dst_on_delete", dstDelNorm))
			return true
		}

		srcUpdNorm := normalizeFKAction(src.OnUpdate)
		dstUpdNorm := normalizeFKAction(dst.OnUpdate)
		if srcUpdNorm != dstUpdNorm {
			log.Debug("FK ON UPDATE action mismatch", zap.String("src_on_update", srcUpdNorm), zap.String("dst_on_update", dstUpdNorm))
			return true
		}
	case "CHECK":
		// Perbandingan definisi CHECK bisa rumit. Gunakan normalisasi.
		normSrcDef := normalizeCheckDefinition(src.Definition)
		normDstDef := normalizeCheckDefinition(dst.Definition)
		if normSrcDef != normDstDef {
			log.Debug("CHECK constraint definition mismatch", zap.String("src_check_def_norm", normSrcDef), zap.String("dst_check_def_norm", normDstDef), zap.String("src_check_def_raw", src.Definition), zap.String("dst_check_def_raw", dst.Definition))
			return true
		}
	case "UNIQUE":
		// Perbandingan kolom sudah cukup untuk UNIQUE constraint dasar.
		// Jika ada fitur tambahan untuk UNIQUE (misal, include columns di SQL Server), perlu ditambahkan.
		break
	case "PRIMARY KEY":
		// PK biasanya ditangani secara berbeda (tidak di-ALTER secara langsung untuk mengubah kolomnya)
		break
	}
	log.Debug("Constraint definitions appear equivalent.")
	return false
}

// Variabel s.rawSchemaInfo yang Anda sebutkan tidak ada di kode yang saya lihat sebelumnya.
// Jika ini adalah cache untuk informasi skema mentah, Anda perlu memastikan cara
// pengisian dan penggunaannya konsisten. Untuk saat ini, saya akan mengabaikannya
// dalam fungsi-fungsi ini kecuali jika Anda memberikan detail lebih lanjut.
// Contoh:
// type RawColumnInfo struct {
//    GenerationExpression sql.NullString
//    // ... other raw fields ...
// }
//
// // Dalam SchemaSyncer:
// rawSchemaInfo map[string]RawColumnInfo // map[columnName]RawColumnInfo
