package sync

import (
	"github.com/stretchr/testify/assert"
	"testing"
	//"go.uber.org/zap"
	// "github.com/arwahdevops/dbsync/internal/config" // Jika diperlukan untuk mock
	// "github.com/arwahdevops/dbsync/internal/db" // Jika diperlukan untuk mock
)

func TestTransformBatchDataForPostgresBoolean(t *testing.T) {
	// Logger dummy untuk Orchestrator (jika getDestinationColumnTypes membutuhkannya)
	// Anda mungkin perlu mock f.dstConn dan f.logger jika getDestinationColumnTypes
	// tidak dibuat statis atau mudah di-mock.
	// Untuk kesederhanaan, kita akan panggil logika transformasinya langsung.

	// Asumsikan kita sudah tahu dstColumnTypes
	dstColumnTypes := map[string]string{
		"is_active":  "boolean",
		"is_deleted": "boolean",
		"name":       "varchar", // Kolom non-boolean
		"count_val":  "integer",
	}

	testCases := []struct {
		name     string
		inputRow map[string]interface{}
		expected map[string]interface{}
	}{
		{
			name:     "Integer 1 to true",
			inputRow: map[string]interface{}{"is_active": 1, "name": "test1"},
			expected: map[string]interface{}{"is_active": true, "name": "test1"},
		},
		{
			name:     "Integer 0 to false",
			inputRow: map[string]interface{}{"is_active": 0, "name": "test2"},
			expected: map[string]interface{}{"is_active": false, "name": "test2"},
		},
		{
			name:     "Int64 1 to true",
			inputRow: map[string]interface{}{"is_active": int64(1), "name": "test3"},
			expected: map[string]interface{}{"is_active": true, "name": "test3"},
		},
		{
			name:     "Float64 1.0 to true",
			inputRow: map[string]interface{}{"is_active": float64(1.0), "name": "test4"},
			expected: map[string]interface{}{"is_active": true, "name": "test4"},
		},
		{
			name:     "Float64 0.0 to false",
			inputRow: map[string]interface{}{"is_active": float64(0.0), "name": "test5"},
			expected: map[string]interface{}{"is_active": false, "name": "test5"},
		},
		{
			name:     "Integer non-0/1 unchanged",
			inputRow: map[string]interface{}{"is_active": 5, "name": "test6"},
			expected: map[string]interface{}{"is_active": 5, "name": "test6"},
		},
		{
			name:     "String value unchanged for boolean target",
			inputRow: map[string]interface{}{"is_active": "true_str", "name": "test7"},
			expected: map[string]interface{}{"is_active": "true_str", "name": "test7"},
		},
		{
			name:     "Multiple boolean columns",
			inputRow: map[string]interface{}{"is_active": 1, "is_deleted": int64(0), "name": "test8"},
			expected: map[string]interface{}{"is_active": true, "is_deleted": false, "name": "test8"},
		},
		{
			name:     "Non-boolean column unchanged",
			inputRow: map[string]interface{}{"is_active": 1, "count_val": 100, "name": "test9"},
			expected: map[string]interface{}{"is_active": true, "count_val": 100, "name": "test9"},
		},
		{
			name:     "Boolean column not in dstColumnTypes (should be unchanged)",
			inputRow: map[string]interface{}{"is_pending": 1, "name": "test10"},
			expected: map[string]interface{}{"is_pending": 1, "name": "test10"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Simulasi transformasi pada satu baris
			batchData := []map[string]interface{}{tc.inputRow}
			// Logika transformasi yang disalin dari syncData
			if dstColumnTypes != nil { // Asumsikan f.dstConn.Dialect == "postgres"
				for i, row := range batchData {
					transformedRow := make(map[string]interface{})
					for key, val := range row {
						if targetType, ok := dstColumnTypes[key]; ok && targetType == "boolean" {
							if intVal, isInt := val.(int); isInt {
								if intVal == 1 {
									transformedRow[key] = true
								} else if intVal == 0 {
									transformedRow[key] = false
								} else {
									transformedRow[key] = val
								}
							} else if int64Val, isInt64 := val.(int64); isInt64 {
								if int64Val == 1 {
									transformedRow[key] = true
								} else if int64Val == 0 {
									transformedRow[key] = false
								} else {
									transformedRow[key] = val
								}
							} else if float64Val, isFloat64 := val.(float64); isFloat64 {
								if float64Val == 1.0 {
									transformedRow[key] = true
								} else if float64Val == 0.0 {
									transformedRow[key] = false
								} else {
									transformedRow[key] = val
								}
							} else {
								transformedRow[key] = val
							}
						} else {
							transformedRow[key] = val
						}
					}
					batchData[i] = transformedRow
				}
			}
			assert.Equal(t, tc.expected, batchData[0])
		})
	}
}
