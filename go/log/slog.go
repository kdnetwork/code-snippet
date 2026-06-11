package log

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/kdnetwork/code-snippet/go/utils"
)

var SlogLevel slog.LevelVar

func InitDefaultLogger() {
	SlogLevel.Set(slog.LevelInfo) // slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level:     &SlogLevel,
		AddSource: true,
	})

	slog.SetDefault(InitLoggerPresets(slog.New(handler)))
}

func InitLoggerPresets(logger *slog.Logger) *slog.Logger {
	versionInfo := utils.VersionInfo()
	return logger.With(
		slog.Group("system",
			// slog.String("package", versionInfo.MainPath),
			// slog.String("version", versionInfo.Version),
			slog.String("os", versionInfo.OS),
			slog.String("arch", versionInfo.Arch),
			// slog.String("commit", versionInfo.CommitHash),
			// slog.String("commit_time", versionInfo.CommitTime),
			// slog.Bool("modified", versionInfo.Modified),
			// slog.Bool("cgo", versionInfo.CGO),
		),
	)
}

func Fatal(msg string, v ...any) {
	slog.Error(msg, v...)
	os.Exit(1)
}

func FmtFatal(v ...any) {
	fmt.Println(v...)
	os.Exit(1)
}
