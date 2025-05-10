package integration // Pastikan ini adalah package yang sama

import (
	"context"
	"fmt"
	"io/ioutil" // atau os.ReadFile untuk Go >= 1.16
	"os"
	"path/filepath"
	"testing" // Masih diperlukan jika fungsi ini dipanggil dari test dan menggunakan t.Helper() atau t.Fatalf()
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/driver/postgres" // Impor driver
	"gorm.io/gorm"            // Impor GORM

    // Impor driver MySQL jika diperlukan
    // "gorm.io/driver/mysql"
)

const (
	postgresImage = "postgres:13-alpine"
	mysqlImage    = "mysql:8.0"
)

type TestDBInstance struct {
	Container testcontainers.Container
	DSN       string
	Dialect   string
	DB        *gorm.DB
	Host      string
	Port      nat.Port // Dari testcontainers
 PortN     string   // Port sebagai string (dari env var untuk simulasi)
	Username  string
	Password  string
	DBName    string
}

func startPostgresContainer(ctx context.Context, t *testing.T) *TestDBInstance {
	t.Helper()
	dbName := "testdb"
	dbUser := "testuser"
	dbPassword := "testpass"

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
		t.Fatalf("Failed to get postgres container host: %s", err)
	}
	mappedPort, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("Failed to get mapped port for postgres: %s", err)
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, mappedPort.Port(), dbUser, dbPassword, dbName)

	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		// Logger: logger.GetGormLogger(), // Sesuaikan
	})
	if err != nil {
		t.Fatalf("Failed to connect to test postgres: %s", err)
	}

	return &TestDBInstance{
		Container: container,
		DSN:       dsn,
		Dialect:   "postgres",
		DB:        gormDB,
		Host:      host,
		Port:      mappedPort, // Simpan objek nat.Port
		Username:  dbUser,
		Password:  dbPassword,
		DBName:    dbName,
	}
}
// Tambahkan startMySQLContainer di sini
// ...

func stopContainer(ctx context.Context, t *testing.T, instance *TestDBInstance) {
	t.Helper()
	if instance != nil && instance.Container != nil {
		sqlDB, _ := instance.DB.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
		if err := instance.Container.Terminate(ctx); err != nil {
			// Log error tapi jangan gagalkan test hanya karena terminate gagal (mungkin sudah berhenti)
			t.Logf("Warning: Failed to terminate container: %s", err)
		}
	} else if instance != nil && instance.DB != nil { // Jika DB simulasi tanpa kontainer
     sqlDB, _ := instance.DB.DB()
     if sqlDB != nil {
         sqlDB.Close()
     }
 }
}

func executeSQLFile(t *testing.T, db *gorm.DB, filePath string) {
	t.Helper()
	// Cek apakah file ada untuk path relatif yang lebih baik
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		t.Fatalf("Failed to get absolute path for SQL file %s: %s", filePath, err)
	}
	content, err := ioutil.ReadFile(absPath) // atau os.ReadFile untuk Go >= 1.16
	if err != nil {
		t.Fatalf("Failed to read SQL file %s (abs: %s): %s", filePath, absPath, err)
	}
	if err := db.Exec(string(content)).Error; err != nil {
		t.Fatalf("Failed to execute SQL file %s: %s", filePath, err)
	}
}
