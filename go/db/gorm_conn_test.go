package db_test

import (
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kdnetwork/code-snippet/go/db"
	"golang.org/x/mod/semver"
)

// Please fill in your own credentials here
// > mysql
var mysqlUser = ""
var mysqlPassword = ""
var mysqlHost = "" // host:port
var mysqlCertPool *x509.CertPool

// > postgresql
var pgUser = ""
var pgPassword = ""
var pgHost = "" // host:port
var pgSSLModeOption = ""

const testDB = "kdnet_code_snippet_gorm_conn_test_not_existed_db"

func init() {
	// init `mysqlCertPool` here
	// mysqlCertPool, _ = x509.SystemCertPool() //...
}

func TestSQLiteConn(t *testing.T) {
	t.Run("MemoryModeTest", func(t *testing.T) {
		// 1. AllowMemoryMode = false
		ctxNoMem := new(db.GormDBCtx).SetDBMode(db.DBModeSQLite)
		ctxNoMem.AllowMemoryMode = false
		err := ctxNoMem.ConnectToSQLite(":memory:")
		if err == nil || err.Error() != "memory mode not allowed" {
			t.Errorf("memory mode test failed: %v, AllowMemoryMode: %v", err, ctxNoMem.AllowMemoryMode)
		}

		// 2. AllowMemoryMode = true
		ctxMem := new(db.GormDBCtx).SetDBMode(db.DBModeSQLite)
		ctxMem.AllowMemoryMode = true
		if err := ctxMem.ConnectToSQLite(":memory:"); err != nil {
			t.Errorf("memory mode test failed: %v, AllowMemoryMode: %v", err, ctxNoMem.AllowMemoryMode)
		}
	})

	t.Run("InvalidPathTest", func(t *testing.T) {
		tempDir := os.TempDir()

		// 1. path is a directory
		dirPath := filepath.Join(tempDir, "test_dir_as_db")
		_ = os.MkdirAll(dirPath, 0755)
		defer os.RemoveAll(dirPath)

		ctxDir := new(db.GormDBCtx).SetDBPath(dirPath)
		err := ctxDir.Connect()
		if err == nil {
			t.Error("directory test failed: should return error")
		}

		// 2. parent directory does not exist
		nonExistentPath := filepath.Join(tempDir, "non_existent_sub_666", "test.db")
		ctxNonExistent := new(db.GormDBCtx).SetDBPath(nonExistentPath)

		err = ctxNonExistent.Connect()
		if err == nil {
			t.Error("parent directory test failed: should return error")
		}

		// FastDBCheck
		exists, _ := ctxNonExistent.FastDBCheck(nonExistentPath)
		if exists {
			t.Error("FastDBCheck should return false")
		}
	})

	t.Run("VersionCheckTest", func(t *testing.T) {
		dbFile := filepath.Join(os.TempDir(), "normal_test.db")
		defer os.Remove(dbFile)

		ctx := new(db.GormDBCtx).SetDBPath(dbFile)
		if err := ctx.Connect(); err != nil {
			t.Fatalf("Conn to db failed: %v", err)
		}

		// 1. FastDBCheck
		exists, err := ctx.FastDBCheck(dbFile)
		if err != nil {
			t.Errorf("FastDBCheck error: %v", err)
		}
		if !exists {
			t.Error("FastDBCheck should return true")
		}

		// 2. Version >= 3.51.1
		vStr := "v" + ctx.Version()

		t.Logf("Current SQLite version: %s", vStr)
		if vStr == "" {
			t.Fatal("empty version string")
		}

		targetV := "v3.51.1"

		if semver.Compare(vStr, targetV) < 0 {
			t.Errorf("Too low version: Current %s, Target >= %s", vStr, targetV)
		}
	})
}

func TestMySQLConn(t *testing.T) {
	t.Run("TimeoutCheck", func(t *testing.T) {
		timeout := time.Duration(0)
		ctx := new(db.GormDBCtx).SetDBMode(db.DBModeMySQL).SetDBAuth(mysqlUser, mysqlPassword, mysqlHost, "mysql", "").SetDialTimeout(&timeout).SetCertPool(mysqlCertPool)

		if err := ctx.Connect(); err != nil {
			if err.Error() != "database connection timeout" {
				t.Fatalf("Failed to connect to MySQL with timeout: %v", err)
			}
		}
	})

	t.Run("StandardConnectionAndCheck", func(t *testing.T) {
		ctx := new(db.GormDBCtx).SetDBMode(db.DBModeMySQL).SetDBAuth(mysqlUser, mysqlPassword, mysqlHost, "mysql", "").SetCertPool(mysqlCertPool)
		if err := ctx.Connect(); err != nil {
			t.Fatalf("Failed to connect to MySQL: %v", err)
		}

		// Check if specific database exists
		exists, err := ctx.FastDBCheck(testDB)
		if err != nil {
			t.Errorf("FastDBCheck error: %v", err)
		}
		t.Logf("Database '%s' exists: %v", testDB, exists)

		// Check version
		version := ctx.Version()
		if version == "" {
			t.Error("MySQL version string should not be empty")
		}
		t.Logf("MySQL Version: %s", version)
	})

	t.Run("ConnectToDefault", func(t *testing.T) {
		ctx := new(db.GormDBCtx).SetDBMode(db.DBModeMySQL).SetDBAuth(mysqlUser, mysqlPassword, mysqlHost, "", "").SetCertPool(mysqlCertPool)
		if err := ctx.ConnectToDefault(); err != nil {
			t.Fatalf("Failed to connect to default MySQL (no db): %v", err)
		}
	})

	t.Run("TLSWithManualCertPool", func(t *testing.T) {
		ctx := new(db.GormDBCtx).SetDBMode(db.DBModeMySQL).SetDBAuth(mysqlUser, mysqlPassword, mysqlHost, "mysql", "").SetCertPool(mysqlCertPool)

		if err := ctx.Connect(); err != nil {
			t.Skipf("Skipping TLS test as server might not support it: %v", err)
		}
	})
}

func TestPostgreSQLConn(t *testing.T) {
	t.Run("StandardConnectionAndCheck", func(t *testing.T) {
		ctx := new(db.GormDBCtx).SetDBMode(db.DBModePostgreSQL).SetDBAuth(pgUser, pgPassword, pgHost, "postgres", "disable")
		if err := ctx.Connect(); err != nil {
			t.Fatalf("Failed to connect to PostgreSQL: %v", err)
		}

		// Check if specific database exists
		exists, err := ctx.FastDBCheck(testDB)
		if err != nil {
			t.Errorf("FastDBCheck error: %v", err)
		}
		t.Logf("Database '%s' exists: %v", testDB, exists)

		// Check version
		version := ctx.Version()
		if version == "" {
			t.Error("PostgreSQL version string should not be empty")
		}
		t.Logf("PostgreSQL Version: %s", version)
	})

	t.Run("SSLModeConnection", func(t *testing.T) {
		// Testing with sslmode=prefer
		ctx := new(db.GormDBCtx).SetDBMode(db.DBModePostgreSQL).SetDBAuth(pgUser, pgPassword, pgHost, "postgres", pgSSLModeOption)
		if err := ctx.Connect(); err != nil {
			t.Errorf("PostgreSQL connection with SSLMode failed: %v", err)
		}
	})

	t.Run("ConnectToDefault", func(t *testing.T) {
		ctx := new(db.GormDBCtx).SetDBMode(db.DBModePostgreSQL).SetDBAuth(pgUser, pgPassword, pgHost, "", "disable")
		if err := ctx.ConnectToDefault(); err != nil {
			t.Fatalf("Failed to connect to default PostgreSQL (postgres db): %v", err)
		}
	})
}
