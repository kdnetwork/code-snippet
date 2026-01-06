package db

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"slices"
	"strings"

	"github.com/go-sql-driver/mysql"
	gorm_mysql_driver "gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	DBModeSQLite     = "sqlite"
	DBModeMySQL      = "mysql"
	DBModePostgreSQL = "postgresql"
)

type GormDBCtx struct {
	R *gorm.DB
	W *gorm.DB

	LogLevel      logger.LogLevel
	ServicePrefix string
	DBMode        string

	// *- sqlite only
	AllowMemMode bool
	WALMode      bool
}

func (ctx *GormDBCtx) ConnectToSQLite(path string) error {
	ctx.DBMode = DBModeSQLite

	allow, err := ctx.FastDBCheck(path)

	// memory mode
	if !allow && err == nil {
		slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "precheck", "err", "mem mode not allowed")
	}

	// write
	writeDBHandle, err := gorm.Open(sqlite.Open(path), &gorm.Config{
		Logger: logger.Default.LogMode(ctx.LogLevel),
	})
	if err != nil {
		slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "open", "conn_type", "w", "err", err)
		return err
	}
	connw, err := writeDBHandle.DB()
	connw.SetMaxOpenConns(1) // prevent "database is locked" error

	if err != nil {
		slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "edit", "conn_type", "w", "err", err)
		return err
	}

	//read
	readDBHandle, err := gorm.Open(sqlite.Open(path), &gorm.Config{
		Logger: logger.Default.LogMode(ctx.LogLevel),
	})
	if err != nil {
		slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "open", "conn_type", "r", "err", err)
		return err
	}

	// connr, err := readDBHandle.DB()
	// connr.SetMaxOpenConns(max(4, runtime.NumCPU()))
	//
	// if err != nil {
	// 	slog.Error(ctx.ServicePrefix, "edit r", err)
	// 	return err
	// }

	slog.Info(ctx.ServicePrefix, "dbmode", ctx.DBMode, "status", "connected")

	var magicSQLiteExecSQL = `PRAGMA busy_timeout = 5000;PRAGMA synchronous = NORMAL;PRAGMA cache_size = 100000;PRAGMA foreign_keys = true;PRAGMA temp_store = memory;`

	if ctx.WALMode {
		magicSQLiteExecSQL = `PRAGMA journal_mode = WAL;` + magicSQLiteExecSQL
	}

	if err := writeDBHandle.Exec(magicSQLiteExecSQL).Error; err != nil {
		slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "wal", "err", err)
		return err
	}

	ctx.R = readDBHandle
	ctx.W = writeDBHandle

	return nil
}

func (ctx *GormDBCtx) ConnectToMySQL(username string, password string, host string, dbname string, tls_option string) error {
	ctx.DBMode = DBModeMySQL

	dsn := mysql.NewConfig()
	dsn.User = username
	dsn.Passwd = password
	dsn.Net = "tcp"
	dsn.Addr = host
	dsn.DBName = dbname
	dsn.Params = map[string]string{
		"charset":   "utf8mb4",
		"parseTime": "True",
		"loc":       "Local",
	}

	if tls_option != "" {
		lowerTLSOption := strings.ToLower(tls_option)
		if slices.Contains([]string{"true", "false", "skip-verify", "preferred"}, lowerTLSOption) {
			dsn.Params["tls"] = lowerTLSOption
		} else {
			CACertPool := x509.NewCertPool()
			pem, err := os.ReadFile(tls_option)
			if err != nil {
				slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "read_cert", "err", err)
				return err
			}
			if ok := CACertPool.AppendCertsFromPEM(pem); !ok {
				slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "append_cert", "err", err)
				return errors.New("failed to append pem")
			}
			parsedURL, err := url.Parse("tcp://" + host)
			if err != nil {
				slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "read_host", "err", err)
				return err
			}

			mysql.RegisterTLSConfig("custom", &tls.Config{
				ServerName: parsedURL.Hostname(),
				RootCAs:    CACertPool,
			})
			dsn.Params["tls"] = "custom"
		}
	}

	sqlDB, _ := sql.Open("mysql", dsn.FormatDSN())

	dbHandle, err := gorm.Open(gorm_mysql_driver.New(gorm_mysql_driver.Config{
		Conn: sqlDB,
	}), &gorm.Config{Logger: logger.Default.LogMode(ctx.LogLevel)})

	if err != nil {
		slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "open", "err", err)
		return err
	}

	slog.Info(ctx.ServicePrefix, "dbmode", ctx.DBMode, "status", "connected")

	ctx.R = dbHandle
	ctx.W = dbHandle

	return nil
}

func (ctx *GormDBCtx) ConnectToPostgreSQL(username string, password string, host string, dbname string, tls_option string) error {
	ctx.DBMode = DBModePostgreSQL

	dsn := &url.URL{
		Scheme: "postgresql",
		Host:   host,
		Path:   "/" + dbname,
	}

	if username != "" {
		if password != "" {
			dsn.User = url.UserPassword(username, password)
		} else {
			dsn.User = url.User(username)
		}
	}

	q := dsn.Query()

	if tls_option != "" {
		lowerTLSOption := strings.ToLower(tls_option)
		if slices.Contains([]string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}, lowerTLSOption) {
			q.Set("sslmode", lowerTLSOption)
		} else {
			q.Set("sslmode", "verify-full")
			q.Set("sslrootcert", tls_option)
		}
	}

	dsn.RawQuery = q.Encode()

	dbHandle, err := gorm.Open(postgres.New(postgres.Config{
		DSN:                  dsn.String(),
		PreferSimpleProtocol: true, // disables implicit prepared statement usage
	}), &gorm.Config{Logger: logger.Default.LogMode(ctx.LogLevel)})

	if err != nil {
		slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "open", "err", err)
		return err
	}

	slog.Info(ctx.ServicePrefix, "dbmode", ctx.DBMode, "status", "connected")

	ctx.R = dbHandle
	ctx.W = dbHandle

	return nil
}

func (ctx *GormDBCtx) GetVersion() string {
	versionStruct := new(struct {
		Version string
	})

	switch ctx.DBMode {
	case DBModePostgreSQL:
		ctx.R.Raw("SELECT version();").Scan(versionStruct)
	case DBModeMySQL:
		ctx.R.Raw("SELECT @@version AS version;").Scan(versionStruct)
	case DBModeSQLite:
		// driver version
		ctx.R.Raw("SELECT sqlite_version() AS version;").Scan(versionStruct)
	}

	return versionStruct.Version
}

func (ctx *GormDBCtx) FastDBCheck(name string) (bool, error) {
	switch ctx.DBMode {
	case DBModePostgreSQL:
		var exists bool
		err := ctx.R.Raw("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = ?);", name).Scan(&exists).Error
		return exists, err
	case DBModeMySQL:
		var count int64
		err := ctx.R.Raw("SELECT COUNT(*) AS count FROM information_schema.schemata WHERE schema_name = ?;", name).Scan(&count).Error
		return count > 0, err
	case DBModeSQLite:
		if name == ":memory:" || strings.HasPrefix(name, "file::memory:") {
			return ctx.AllowMemMode, nil
		}

		db, err := sql.Open("sqlite3", "file:"+url.PathEscape(name)+"?mode=ro")

		if err != nil {
			return false, err
		}
		if err = db.Ping(); err != nil {
			_ = db.Close()
			return false, err
		}
		_ = db.Close()
		return true, nil
	}

	return false, errors.New("not supported db")
}
