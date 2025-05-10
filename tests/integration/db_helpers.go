package integration

import (
	"context"
	"fmt"
	"io/ioutil" // Untuk Go < 1.16, atau os.ReadFile untuk Go >= 1.16
	"os"        // Diperlukan jika executeSQLFile menggunakan os.ReadFile
	"path/filepath"
	"strconv" // Diperlukan untuk mustPortInt
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
	mysqlImage    = "mysql:8.0" // Atau versi lain yang Anda inginkan
)

// TestDBInstance menyimpan detail instance database untuk test.
type TestDBInstance struct {
	Container testcontainers.Container // Kontainer Docker (jika menggunakan TestContainers)
	DSN       string                   // Data Source Name untuk koneksi
	Dialect   string                   // Dialek database (mysql, postgres, sqlite)
	DB        *gorm.DB                 // Koneksi GORM ke database test
	Host      string                   // Host database
	Port      nat.Port                 // Port yang di-map oleh TestContainers (nat.Port)
	Username  string                   // Username database
	Password  string                   // Password database
	DBName    string                   // Nama database
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
			WithOccurrence(2). // Log ini terkadang muncul dua kali pada startup PG
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
		container.Terminate(ctx) // Coba hentikan kontainer jika gagal
		t.Fatalf("Failed to get postgres container host: %s", err)
	}
	mappedPort, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		container.Terminate(ctx)
		t.Fatalf("Failed to get mapped port for postgres: %s", err)
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable connect_timeout=10",
		host, mappedPort.Port(), dbUser, dbPassword, dbName)

	// Anda bisa menggunakan GORM logger kustom Anda di sini jika diperlukan
	// gormLogger := dbsync_logger.NewGormLogger(true) // Contoh
	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		// Logger: gormLogger,
	})
	if err != nil {
		container.Terminate(ctx)
		t.Fatalf("Failed to connect to test postgres instance: %s", err)
	}

	t.Logf("PostgreSQL container started. Host: %s, Port: %s, DSN: [REDACTED]", host, mappedPort.Port())

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
	// MySQL 8+ memerlukan root password, dan kita bisa buat user terpisah atau pakai root.
	// Untuk kesederhanaan test, kita bisa pakai root atau buat user baru.
	// Jika membuat user baru, pastikan user tersebut memiliki hak yang cukup.
	dbUser := "testmysqluser"
	dbPassword := "testmysqlpass"
	rootPassword := "mysqLRootPassw0rd!" // Gunakan password yang kuat

	req := testcontainers.ContainerRequest{
		Image:        mysqlImage,
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_DATABASE":       dbName,
			"MYSQL_USER":         dbUser,
			"MYSQL_PASSWORD":     dbPassword,
			"MYSQL_ROOT_PASSWORD": rootPassword,
		},
		// Untuk MySQL 8, log yang menandakan siap bisa "port: 3306  MySQL Community Server - GPL"
		// atau menunggu port terbuka.
		WaitingFor: wait.ForListeningPort("3306/tcp").
			WithStartupTimeout(120 * time.Second), // MySQL kadang butuh waktu lebih lama
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
		container.Terminate(ctx)
		t.Fatalf("Failed to get mysql container host: %s", err)
	}
	mappedPort, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		container.Terminate(ctx)
		t.Fatalf("Failed to get mapped port for mysql: %s", err)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=20s",
		dbUser, dbPassword, host, mappedPort.Port(), dbName)

	var gormDB *gorm.DB
	var gormErr error
	// MySQL kadang memerlukan sedikit waktu ekstra setelah port terbuka sebelum siap menerima koneksi aplikasi.
	// Lakukan beberapa percobaan koneksi.
	for i := 0; i < 10; i++ {
		// gormLogger := dbsync_logger.NewGormLogger(true) // Contoh
		gormDB, gormErr = gorm.Open(mysql.Open(dsn), &gorm.Config{
			// Logger: gormLogger,
		})
		if gormErr == nil {
			// Coba ping untuk memastikan koneksi benar-benar siap
			sqlDB, dbErr := gormDB.DB()
			if dbErr == nil {
				ctxPing, cancelPing := context.WithTimeout(ctx, 5*time.Second)
				defer cancelPing()
				if pingErr := sqlDB.PingContext(ctxPing); pingErr == nil {
					break // Koneksi dan ping berhasil
				} else {
					gormErr = fmt.Errorf("ping failed: %w", pingErr)
				}
			} else {
				gormErr = fmt.Errorf("failed to get underlying sql.DB: %w", dbErr)
			}
		}
		if i < 9 { // Jangan log di percobaan terakhir jika masih gagal
			t.Logf("MySQL connection attempt %d failed: %v. Retrying in 2s...", i+1, gormErr)
			time.Sleep(2 * time.Second)
		}
	}
	if gormErr != nil {
		container.Terminate(ctx)
		t.Fatalf("Failed to connect to test mysql instance after retries: %s. DSN: [REDACTED]", gormErr)
	}

	t.Logf("MySQL container started. Host: %s, Port: %s, DSN: [REDACTED]", host, mappedPort.Port())

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
			err := sqlDB.Close()
			if err != nil {
				t.Logf("Warning: error closing GORM DB connection for %s: %v", instance.Dialect, err)
			}
		}
	}
	if instance.Container != nil {
		// Beri sedikit waktu untuk koneksi ditutup sebelum terminate
		time.Sleep(100 * time.Millisecond)
		if err := instance.Container.Terminate(ctx); err != nil {
			// Log error tapi jangan gagalkan test hanya karena terminate gagal
			// (mungkin sudah berhenti atau ada isu sementara di Docker)
			t.Logf("Warning: failed to terminate container for %s: %s", instance.Dialect, err)
		} else {
			t.Logf("%s container terminated successfully.", instance.Dialect)
		}
	}
}

// executeSQLFile membaca dan mengeksekusi perintah SQL dari sebuah file.
func executeSQLFile(t *testing.T, db *gorm.DB, filePath string) {
	t.Helper()
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		t.Fatalf("Failed to get absolute path for SQL file %s: %s", filePath, err)
	}

	// Cek apakah file ada
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		t.Fatalf("SQL file does not exist at %s (resolved from %s)", absPath, filePath)
	}

	content, err := ioutil.ReadFile(absPath) // Gunakan os.ReadFile untuk Go 1.16+
	if err != nil {
		t.Fatalf("Failed to read SQL file %s (abs: %s): %s", filePath, absPath, err)
	}

	// GORM Exec umumnya bisa menangani multiple statements yang dipisah semicolon,
	// namun ini bisa bergantung pada driver database.
	// Jika ada masalah, Anda mungkin perlu memisahkan statement secara manual.
	if err := db.Exec(string(content)).Error; err != nil {
		t.Fatalf("Failed to execute SQL file %s: %s. Content: \n%s", filePath, err, string(content))
	}
	t.Logf("Successfully executed SQL file: %s", filePath)
}
