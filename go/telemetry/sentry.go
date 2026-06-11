package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/getsentry/sentry-go"
	sentryslog "github.com/getsentry/sentry-go/slog"
	kdlog "github.com/kdnetwork/code-snippet/go/log"
	"github.com/kdnetwork/code-snippet/go/utils"
)

// InitSentryLogger MUST BE CALLED IN GOROUTINE
func InitSentryLogger(ctx context.Context, dsn, version string, client *http.Client) error {
	// env.SENTRY_DSN string option // official name
	if dsn == "" {
		dsn = utils.GetEnv("SENTRY_DSN", dsn)
		if dsn == "" {
			return errors.New("sentry dsn is empty")
		}
	}

	versionInfo := utils.VersionInfo()

	if version == "" {
		if versionInfo.Version != "(devel)" {
			version = versionInfo.Version
		} else {
			version = versionInfo.CommitHash
		}
	}

	if client == nil {
		client = utils.DefaultClient
	}

	err := sentry.Init(sentry.ClientOptions{
		Dsn:        dsn,
		Debug:      true,
		EnableLogs: true,
		Release:    version,
		Dist:       versionInfo.OS + "/" + versionInfo.Arch,

		HTTPClient: client,
	})
	if err != nil {
		slog.Error("sentry init error", "error", err.Error())
		return err
	}
	slog.Info("sentry init success")

	// Flush buffered events before the program terminates.
	// flush is no use here because ctx was canceled
	// defer sentry.Flush(2 * time.Second)
	// TODO Go 1.26+ includes slog.NewMultiHandler

	sentryLogLevels := []slog.Level{slog.LevelInfo, slog.LevelWarn, slog.LevelError, sentryslog.LevelFatal}
	if kdlog.SlogLevel.Level() == slog.LevelDebug {
		sentryLogLevels = append(sentryLogLevels, slog.LevelDebug)
	}

	handler := sentryslog.Option{
		LogLevel:  sentryLogLevels,
		AddSource: true,
	}.NewSentryHandler(ctx)

	slogBackup := slog.Default()
	defer slog.SetDefault(slogBackup)

	slog.SetDefault(kdlog.InitLoggerPresets(slog.New(handler)))
	<-ctx.Done()

	return err
}
