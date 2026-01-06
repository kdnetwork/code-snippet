package db

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
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
	if !ctx.AllowMemMode {
		if _, err := os.Stat(path); err != nil {
			slog.Error(ctx.ServicePrefix, "path", path+" is not exists", "err:", err)
			return err
		}
	}

	// write
	writeDBHandle, err := gorm.Open(sqlite.Open(path), &gorm.Config{
		Logger: logger.Default.LogMode(ctx.LogLevel),
	})
	if err != nil {
		slog.Error(ctx.ServicePrefix, "open w:", err)
		return err
	}
	connw, err := writeDBHandle.DB()
	connw.SetMaxOpenConns(1) // prevent "database is locked" error

	if err != nil {
		slog.Error(ctx.ServicePrefix, "open w:", err)
		return err
	}

	//read
	readDBHandle, err := gorm.Open(sqlite.Open(path), &gorm.Config{
		Logger: logger.Default.LogMode(ctx.LogLevel),
	})
	if err != nil {
		slog.Error(ctx.ServicePrefix, "open r:", err)
		return err
	}

	// connr, err := readDBHandle.DB()
	// connr.SetMaxOpenConns(max(4, runtime.NumCPU()))
	//
	// if err != nil {
	// 	slog.Error(ctx.ServicePrefix, "edit r:", err)
	// 	return err
	// }

	slog.Info(ctx.ServicePrefix, "status", "sqlite connected")

	var magicSQLiteExecSQL = `PRAGMA busy_timeout = 5000;PRAGMA synchronous = NORMAL;PRAGMA cache_size = 100000;PRAGMA foreign_keys = true;PRAGMA temp_store = memory;`

	if ctx.WALMode {
		magicSQLiteExecSQL = `PRAGMA journal_mode = WAL;` + magicSQLiteExecSQL
	}

	if err := writeDBHandle.Exec(magicSQLiteExecSQL).Error; err != nil {
		slog.Error(ctx.ServicePrefix, "wal", "set wal mode failed", "err:", err)
		return err
	}

	ctx.R = readDBHandle
	ctx.W = writeDBHandle
	ctx.DBMode = DBModeSQLite

	return nil
}

func (ctx *GormDBCtx) ConnectToMySQL(username string, password string, host string, dbname string, tls_option string) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", username, password, host, dbname)

	if tls_option != "" {
		lowerTLSOption := strings.ToLower(tls_option)
		if slices.Contains([]string{"true", "false", "skip-verify", "preferred"}, lowerTLSOption) {
			dsn += "&tls=" + lowerTLSOption
		} else {
			CACertPool := x509.NewCertPool()
			pem, err := os.ReadFile(tls_option)
			if err != nil {
				slog.Error(ctx.ServicePrefix, "cert", "failed to read the certificate", "err:", err)
				return err
			}
			if ok := CACertPool.AppendCertsFromPEM(pem); !ok {
				slog.Error(ctx.ServicePrefix, "cert", "failed to append pem")
				return errors.New("failed to append pem")
			}
			parsedURL, err := url.Parse("tcp://" + host)
			if err != nil {
				slog.Error(ctx.ServicePrefix, "host", "failed to parse host", "err:", err)
				return err
			}

			mysql.RegisterTLSConfig("custom", &tls.Config{
				ServerName: parsedURL.Hostname(),
				RootCAs:    CACertPool,
			})
			dsn += "&tls=custom"
		}
	}

	sqlDB, _ := sql.Open("mysql", dsn)

	dbHandle, err := gorm.Open(gorm_mysql_driver.New(gorm_mysql_driver.Config{
		Conn: sqlDB,
	}), &gorm.Config{Logger: logger.Default.LogMode(ctx.LogLevel)})

	if err != nil {
		slog.Error(ctx.ServicePrefix, "connect", "failed to connect postgresql", "err:", err)
		return err
	}

	slog.Info(ctx.ServicePrefix, "status", "mysql connected")

	ctx.R = dbHandle
	ctx.W = dbHandle
	ctx.DBMode = DBModeMySQL

	return nil
}

func (ctx *GormDBCtx) ConnectToPostgreSQL(username string, password string, host string, dbname string, tls_option string) error {
	dsn := fmt.Sprintf("postgresql://%s:%s@%s/%s", username, password, host, dbname)

	if tls_option != "" {
		lowerTLSOption := strings.ToLower(tls_option)
		if slices.Contains([]string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}, lowerTLSOption) {
			dsn += "?sslmode=" + lowerTLSOption
		} else {
			dsn += "?sslmode=verify-full&sslrootcert=" + tls_option
		}
	}

	dbHandle, err := gorm.Open(postgres.New(postgres.Config{
		DSN:                  dsn,
		PreferSimpleProtocol: true, // disables implicit prepared statement usage
	}), &gorm.Config{Logger: logger.Default.LogMode(ctx.LogLevel)})

	if err != nil {
		slog.Error(ctx.ServicePrefix, "connect", "failed to connect postgresql", "err:", err)
		return err
	}

	slog.Info(ctx.ServicePrefix, "status", "postgresql connected")

	ctx.R = dbHandle
	ctx.W = dbHandle
	ctx.DBMode = DBModePostgreSQL

	return nil
}
