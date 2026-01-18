package db

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

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
	// for mysql/postgresql: R == W
	R *gorm.DB
	W *gorm.DB

	LogLevel      logger.LogLevel
	ServicePrefix string
	DBMode        string

	// *- sqlite only
	AllowMemoryMode bool
	WALMode         bool

	// *- mysql only
	CertPool *x509.CertPool

	// auth
	dbPath    string
	dbName    string
	username  string
	password  string
	host      string
	tlsOption string

	// timeout
	dialTimeout        *time.Duration
	NumLeakedGoroutine atomic.Int64
}

// mysql, sqlite, postgresql
func (ctx *GormDBCtx) SetDBMode(mode string) *GormDBCtx {
	lowerMode := strings.ToLower(mode)
	if slices.Contains([]string{DBModeMySQL, DBModePostgreSQL, DBModeSQLite}, lowerMode) {
		ctx.DBMode = lowerMode
	}

	return ctx
}

// sqlite
func (ctx *GormDBCtx) SetDBPath(path string) *GormDBCtx {
	ctx.DBMode = DBModeSQLite
	ctx.dbPath = path

	return ctx
}

// mysql/postgresql
func (ctx *GormDBCtx) SetDBAuth(username, password, host, dbName, tlsOption string) *GormDBCtx {
	ctx.username = username
	ctx.password = password
	ctx.host = host
	ctx.dbName = dbName
	ctx.tlsOption = tlsOption

	return ctx
}

// mysql
func (ctx *GormDBCtx) SetCertPool(pool *x509.CertPool) *GormDBCtx {
	ctx.CertPool = pool

	return ctx
}

// mysql/postgresql
func (ctx *GormDBCtx) SetDialTimeout(timeout *time.Duration) *GormDBCtx {
	if timeout != nil && timeout.Seconds() >= 0 {
		ctx.dialTimeout = timeout
	}

	return ctx
}

func (ctx *GormDBCtx) Connect() error {
	switch ctx.DBMode {
	case DBModeSQLite:
		return ctx.ConnectToSQLite(ctx.dbPath)
	case DBModeMySQL:
		return ctx.ConnectToMySQL(ctx.username, ctx.password, ctx.host, ctx.dbName, ctx.tlsOption)
	case DBModePostgreSQL:
		return ctx.ConnectToPostgreSQL(ctx.username, ctx.password, ctx.host, ctx.dbName, ctx.tlsOption)
	}

	return errors.New("invalid db mode `" + ctx.DBMode + "`")
}

// sqlite -> :memory:
// mysql -> ""/<no_db>
// postgresql -> "postgres"
func (ctx *GormDBCtx) ConnectToDefault() error {
	switch ctx.DBMode {
	case DBModeSQLite:
		ctx.AllowMemoryMode = true
		return ctx.ConnectToSQLite(":memory:")
	case DBModeMySQL:
		return ctx.ConnectToMySQL(ctx.username, ctx.password, ctx.host, "", ctx.tlsOption)
	case DBModePostgreSQL:
		return ctx.ConnectToPostgreSQL(ctx.username, ctx.password, ctx.host, "postgres", ctx.tlsOption)
	}

	return errors.New("invalid db mode `" + ctx.DBMode + "`")
}

func (ctx *GormDBCtx) Close() error {
	closeDB := func(db *gorm.DB) error {
		if db == nil {
			return nil
		}
		sqlDB, err := db.DB()
		if err != nil {
			return err
		}
		if sqlDB != nil {
			return sqlDB.Close()
		}
		return nil
	}

	if err := closeDB(ctx.R); err != nil {
		return err
	}

	ctx.R = nil

	if ctx.W != ctx.R {
		if err := closeDB(ctx.W); err != nil {
			return err
		}
	}

	ctx.W = nil
	return nil
}

func (ctx *GormDBCtx) ConnectToSQLite(path string) error {
	ctx.DBMode = DBModeSQLite

	// memory mode
	if !ctx.AllowMemoryMode && (path == ":memory:" || strings.HasPrefix(path, "file::memory:")) {
		slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "precheck", "err", "memory mode not allowed")
		return errors.New("memory mode not allowed")
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
	if err != nil {
		slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "edit", "conn_type", "w", "err", err)
		return err
	}
	connw.SetMaxOpenConns(1) // prevent "database is locked" error

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

func (ctx *GormDBCtx) ConnectToMySQL(username string, password string, host string, dbname string, tlsOption string) error {
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

	if tlsOption != "" {
		lowerTLSOption := strings.ToLower(tlsOption)
		if slices.Contains([]string{"true", "false", "skip-verify", "preferred"}, lowerTLSOption) {
			dsn.Params["tls"] = lowerTLSOption
		} else {
			if ctx.CertPool == nil {
				ctx.CertPool = x509.NewCertPool()
			}

			pem, err := os.ReadFile(tlsOption)
			if err != nil {
				slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "read_cert", "err", err)
				return err
			}
			if ok := ctx.CertPool.AppendCertsFromPEM(pem); !ok {
				slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "append_cert", "err", err)
				return errors.New("failed to append pem")
			}
			parsedURL, err := url.Parse("tcp://" + host)
			if err != nil {
				slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "read_host", "err", err)
				return err
			}

			if err = mysql.RegisterTLSConfig("custom", &tls.Config{
				ServerName: parsedURL.Hostname(),
				RootCAs:    ctx.CertPool,
			}); err != nil {
				slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "register_tls_config_from_file", "err", err)
				return err
			}
			dsn.Params["tls"] = "custom"
		}
	} else if ctx.CertPool != nil {
		parsedURL, err := url.Parse("tcp://" + host)
		if err = mysql.RegisterTLSConfig("custom", &tls.Config{
			ServerName: parsedURL.Hostname(),
			RootCAs:    ctx.CertPool,
		}); err != nil {
			slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "register_tls_config_from_cert_pool", "err", err)
			return err
		}
		dsn.Params["tls"] = "custom"
	}

	var dbHandle *gorm.DB
	var err error

	if ctx.dialTimeout != nil {
		dsn.Timeout = *ctx.dialTimeout

		type result struct {
			db  *gorm.DB
			err error
		}
		resChan := make(chan result, 1)

		go func() {
			// unable to prevent leaking goroutines... when timeout
			// FYI-> https://github.com/0xERR0R/blocky/issues/1585
			// -> https://github.com/go-gorm/gorm/issues/6791
			// -> https://github.com/go-gorm/gorm/issues/5599
			ctx.NumLeakedGoroutine.Add(1)
			defer ctx.NumLeakedGoroutine.Add(-1)

			db, err := gorm.Open(gorm_mysql_driver.New(gorm_mysql_driver.Config{
				DSNConfig: dsn,
			}), &gorm.Config{Logger: logger.Default.LogMode(ctx.LogLevel)})
			resChan <- result{db, err}
		}()

		timeoutCtx, cancel := context.WithTimeout(context.Background(), *ctx.dialTimeout)
		defer cancel()

		select {
		case <-timeoutCtx.Done():
			err = errors.New("database connection timeout")
			slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "connect", "err", err)
			return err
		case res := <-resChan:
			if res.err != nil {
				return res.err
			}
			dbHandle = res.db
		}
	} else {
		dbHandle, err = gorm.Open(gorm_mysql_driver.New(gorm_mysql_driver.Config{
			DSNConfig: dsn,
		}), &gorm.Config{Logger: logger.Default.LogMode(ctx.LogLevel)})
	}

	if err != nil {
		slog.Error(ctx.ServicePrefix, "dbmode", ctx.DBMode, "method", "open", "err", err)
		return err
	}

	slog.Info(ctx.ServicePrefix, "dbmode", ctx.DBMode, "status", "connected")

	ctx.R = dbHandle
	ctx.W = dbHandle

	return nil
}

func (ctx *GormDBCtx) ConnectToPostgreSQL(username string, password string, host string, dbname string, tlsOption string) error {
	ctx.DBMode = DBModePostgreSQL

	if dbname == "" {
		dbname = "postgres"
	}

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

	if tlsOption != "" {
		lowerTLSOption := strings.ToLower(tlsOption)
		if slices.Contains([]string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}, lowerTLSOption) {
			q.Set("sslmode", lowerTLSOption)
		} else {
			q.Set("sslmode", "verify-full")
			q.Set("sslrootcert", tlsOption)
		}
	}

	if ctx.dialTimeout != nil {
		q.Set("connect_timeout", strconv.Itoa(int(ctx.dialTimeout.Seconds())))
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

func (ctx *GormDBCtx) Version() string {
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
func (ctx *GormDBCtx) GetVersion() string {
	return ctx.Version()

}

func (ctx *GormDBCtx) GetDB() string {
	return ctx.DBName()
}
func (ctx *GormDBCtx) DBName() string {
	return ctx.dbName
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
			if ctx.AllowMemoryMode {
				return true, nil
			} else {
				return false, errors.New("memory mode not allowed")
			}
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
