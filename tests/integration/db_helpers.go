package integration

import (
	"context"
	"fmt"
	"io/ioutil" // Untuk Go < 1.16, atau os.ReadFile untuk Go >= 1.16
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	// Impor logger dbsync jika Anda ingin menggunakan GORM logger kustom di sini
	// dbsync_logger "github.com/arwahdevops/dbsync/internal/logger"
)

const (
	postgresImage = "postgres:13-alpine"
	mysqlImage    = "mysql:8.0"
)

// TestDBInstance menyimpan detail instance database untuk test.
type TestDBInstance struct {
	Container testcontainers.Container
	DSN       string
	Dialect   string
	DB        *gorm.DB
	Host      string
	Port      nat.Port
	Username  string
	Password  string
	DBName    string
}

// mustPortInt adalah helper untuk mengkonversi nat.Port ke int.
func mustPortInt(t *testing.T, port nat.Port) int {
	t.Helper()
	p, err := strconv.Atoi(port.Port())
	if err != nil {
		t.Fatalf("Failed to convert port %s to int: %v", port.Port(), err)
	}
	return p
}

// startPostgresContainer memulai kontainer PostgreSQL untuk test.
func startPostgresContainer(ctx context.Context, t *testing.T) *TestDBInstance {
	t.Helper()
	dbName := "testpgdb"
	dbUser := "testpguser"
	dbPassword := "testpgpass"

	req := testcontainers.ContainerRequest{
		Image:        postgresImage,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_DB":       dbName,
			"POSTGRES_USER":     dbUser,
			"POSTGRES_PASSWORD": dbPassword,
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("Failed to start postgres container: %s", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("Failed to get postgres container host: %s", err)
	}
	mappedPort, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("Failed to get mapped port for postgres: %s", err)
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable connect_timeout=10",
		host, mappedPort.Port(), dbUser, dbPassword, dbName)

	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		// Logger: dbsync_logger.NewGormLogger(true), // Aktifkan jika perlu
	})
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("Failed to connect to test postgres instance: %s", err)
	}

	t.Logf("PostgreSQL container started. Host: %s, Port: %s", host, mappedPort.Port())

	return &TestDBInstance{
		Container: container,
		DSN:       dsn,
		Dialect:   "postgres",
		DB:        gormDB,
		Host:      host,
		Port:      mappedPort,
		Username:  dbUser,
		Password:  dbPassword,
		DBName:    dbName,
	}
}

// startMySQLContainer memulai kontainer MySQL untuk test.
func startMySQLContainer(ctx context.Context, t *testing.T) *TestDBInstance {
	t.Helper()
	dbName := "testmysqldb"
	dbUser := "testmysqluser"
	dbPassword := "testmysqlpass"
	rootPassword := "MYSQL_R00T_P@$$W0RD!" // Ganti dengan password root yang kuat

	req := testcontainers.ContainerRequest{
		Image:        mysqlImage,
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_DATABASE":      dbName,
			"MYSQL_USER":          dbUser,
			"MYSQL_PASSWORD":      dbPassword,
			"MYSQL_ROOT_PASSWORD": rootPassword,
		},
		WaitingFor: wait.ForListeningPort("3306/tcp").
			WithStartupTimeout(120 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("Failed to start mysql container: %s", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("Failed to get mysql container host: %s", err)
	}
	mappedPort, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("Failed to get mapped port for mysql: %s", err)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=20s",
		dbUser, dbPassword, host, mappedPort.Port(), dbName)

	var gormDB *gorm.DB
	var gormErr error
	for i := 0; i < 10; i++ { // Retry loop for GORM connection
		gormDB, gormErr = gorm.Open(mysql.Open(dsn), &gorm.Config{
			// Logger: dbsync_logger.NewGormLogger(true), // Aktifkan jika perlu
		})
		if gormErr == nil {
			sqlDBForPing, dbErr := gormDB.DB()
			if dbErr == nil {
				ctxPing, cancelPing := context.WithTimeout(ctx, 5*time.Second)
				pingErr := sqlDBForPing.PingContext(ctxPing)
				cancelPing() // Selalu panggil cancel
				if pingErr == nil {
					break // Berhasil konek dan ping
				}
				gormErr = fmt.Errorf("ping failed: %w", pingErr)
			} else {
				gormErr = fmt.Errorf("failed to get underlying sql.DB: %w", dbErr)
			}
		}
		if i < 9 {
			t.Logf("MySQL connection attempt %d failed: %v. Retrying in 2s...", i+1, gormErr)
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done(): // Jika konteks utama dibatalkan saat menunggu
				_ = container.Terminate(ctx)
				t.Fatalf("Context cancelled while retrying MySQL connection: %v", ctx.Err())
			}
		}
	}
	if gormErr != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("Failed to connect to test mysql instance after retries: %s", gormErr)
	}

	t.Logf("MySQL container started. Host: %s, Port: %s", host, mappedPort.Port())

	return &TestDBInstance{
		Container: container,
		DSN:       dsn,
		Dialect:   "mysql",
		DB:        gormDB,
		Host:      host,
		Port:      mappedPort,
		Username:  dbUser,
		Password:  dbPassword,
		DBName:    dbName,
	}
}

// stopContainer menghentikan dan menghapus kontainer test.
func stopContainer(ctx context.Context, t *testing.T, instance *TestDBInstance) {
	t.Helper()
	if instance == nil {
		return
	}
	if instance.DB != nil {
		sqlDB, _ := instance.DB.DB()
		if sqlDB != nil {
			if err := sqlDB.Close(); err != nil {
				t.Logf("Warning: error closing GORM DB connection for %s: %v", instance.Dialect, err)
			}
		}
	}
	if instance.Container != nil {
		// Beri sedikit waktu untuk koneksi ditutup sebelum terminate
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done(): // Jangan block jika context sudah selesai
		}
		if err := instance.Container.Terminate(ctx); err != nil {
			t.Logf("Warning: failed to terminate container for %s: %s", instance.Dialect, err)
		} else {
			t.Logf("%s container terminated successfully.", instance.Dialect)
		}
	}
}

// truncateForLog adalah helper untuk mempersingkat string untuk logging.
func truncateForLog(s string, maxLength int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > maxLength {
		return s[:maxLength-3] + "..."
	}
	return s
}

// splitSQLStatements mencoba memisahkan string SQL menjadi statement individual.
func splitSQLStatements(sqlScript string) []string {
	var statements []string

	// Hapus komentar blok C-style /* ... */
	reCommentBlock := regexp.MustCompile(`/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/`)
	processedScript := reCommentBlock.ReplaceAllString(sqlScript, "")

	var currentStatement strings.Builder
	inSingleQuote := false
	inDoubleQuote := false // MySQL bisa menggunakan double quote untuk string, PG untuk identifier
	inDollarQuote := false
	var dollarQuoteTag string

	runes := []rune(processedScript)
	for i := 0; i < len(runes); i++ {
		char := runes[i]

		// Penanganan Dollar-Quoted Strings (PostgreSQL)
		if !inSingleQuote && !inDoubleQuote && char == '$' {
			// Cek apakah ini awal dari dollar quote tag
			match := regexp.MustCompile(`^\$([a-zA-Z_][a-zA-Z0-9_]*)?\$`).FindStringSubmatch(string(runes[i:]))
			if len(match) > 0 {
				if !inDollarQuote { // Memulai dollar quote baru
					inDollarQuote = true
					dollarQuoteTag = match[0]
					currentStatement.WriteString(dollarQuoteTag)
					i += len(dollarQuoteTag) - 1
					continue
				} else if string(runes[i:min(len(runes), i+len(dollarQuoteTag))]) == dollarQuoteTag { // Mengakhiri dollar quote
					inDollarQuote = false
					currentStatement.WriteString(dollarQuoteTag)
					i += len(dollarQuoteTag) - 1
					continue
				}
			}
		}
		if inDollarQuote {
			currentStatement.WriteRune(char)
			continue
		}

		// Penanganan Single Quotes
		if char == '\'' && (i == 0 || runes[i-1] != '\\') { // Abaikan escaped quote \'
			inSingleQuote = !inSingleQuote
		}

		// Penanganan Double Quotes (lebih relevan untuk identifier di PG atau string di MySQL mode ANSI_QUOTES)
		// Untuk MySQL, umumnya string pakai single quote.
		if char == '"' && (i == 0 || runes[i-1] != '\\') {
			// Anggap ini hanya relevan jika kita tidak di dalam single quote
			if !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			}
		}

		// Penanganan Komentar Satu Baris (-- atau #)
		if !inSingleQuote && !inDoubleQuote && !inDollarQuote {
			if char == '-' && i+1 < len(runes) && runes[i+1] == '-' {
				// Telan sisa baris
				for i < len(runes) && runes[i] != '\n' {
					i++
				}
				// Jika statement saat ini ada isinya dan tidak hanya spasi,
				// anggap itu statement selesai jika diakhiri titik koma sebelumnya, atau bagian dari statement.
				// Untuk keamanan, kita tambahkan newline agar split di bawahnya bekerja jika ada statement sebelum komentar.
				if currentStatement.Len() > 0 {
					currentStatement.WriteString("\n")
				}
				continue
			}
			if char == '#' { // Komentar MySQL
				for i < len(runes) && runes[i] != '\n' {
					i++
				}
				if currentStatement.Len() > 0 {
					currentStatement.WriteString("\n")
				}
				continue
			}
		}

		currentStatement.WriteRune(char)

		// Pemisahan Statement berdasarkan ';'
		if char == ';' && !inSingleQuote && !inDoubleQuote && !inDollarQuote {
			stmtStr := strings.TrimSpace(currentStatement.String())
			if stmtStr != "" {
				statements = append(statements, stmtStr)
			}
			currentStatement.Reset()
			inSingleQuote = false // Reset status quote setelah statement selesai
			inDoubleQuote = false
		}
	}

	// Tambahkan statement terakhir jika ada yang tersisa
	lastStmtStr := strings.TrimSpace(currentStatement.String())
	if lastStmtStr != "" {
		// Hapus titik koma di akhir jika ada (karena akan ditambahkan lagi)
		lastStmtStr = strings.TrimRight(lastStmtStr, ";")
		lastStmtStr = strings.TrimSpace(lastStmtStr)
		if lastStmtStr != "" {
			statements = append(statements, lastStmtStr+";") // Pastikan diakhiri ;
		}
	}

	// Hapus statement yang mungkin hanya ";"
	finalStatements := make([]string, 0, len(statements))
	for _, stmt := range statements {
		if strings.TrimSpace(stmt) != ";" && strings.TrimSpace(stmt) != "" {
			finalStatements = append(finalStatements, stmt)
		}
	}

	return finalStatements
}

// Helper min untuk dollar quote
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// executeSQLFile membaca dan mengeksekusi perintah SQL dari sebuah file.
func executeSQLFile(t *testing.T, db *gorm.DB, filePath string) {
	t.Helper()
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		t.Fatalf("Failed to get absolute path for SQL file %s: %s", filePath, err)
	}

	if _, errOsStat := os.Stat(absPath); os.IsNotExist(errOsStat) {
		t.Fatalf("SQL file does not exist at %s (resolved from %s)", absPath, filePath)
	}

	contentBytes, err := ioutil.ReadFile(absPath) // os.ReadFile untuk Go 1.16+
	if err != nil {
		t.Fatalf("Failed to read SQL file %s: %s", filePath, err)
	}

	statements := splitSQLStatements(string(contentBytes))

	statementCount := 0
	for i, stmtStr := range statements {
		trimmedStmt := strings.TrimSpace(stmtStr)
		if trimmedStmt == "" {
			continue
		}

		statementCount++
		t.Logf("Executing SQL statement #%d from %s: %s...", statementCount, filePath, truncateForLog(trimmedStmt, 70))

		if errDbExec := db.Exec(trimmedStmt).Error; errDbExec != nil {
			t.Fatalf("Failed to execute SQL statement #%d (index %d) from %s: %v\nFull Statement:\n%s\n", statementCount, i, filePath, errDbExec, trimmedStmt)
		}
	}

	if statementCount > 0 {
		t.Logf("Successfully executed %d SQL statements from file: %s", statementCount, filePath)
	} else {
		t.Logf("No executable SQL statements found in file (after processing comments and empty lines): %s", filePath)
	}
}
