package telemetry

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/kdnetwork/code-snippet/go/log"
	"github.com/kdnetwork/code-snippet/go/utils"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
)

func NewOTelHandler(ctx context.Context, logsEndpoint string, headers map[string]string, version string, client *http.Client) (slog.Handler, error) {
	versionInfo := utils.VersionInfo()
	if version == "" {
		version = versionInfo.Version
		if version == "(devel)" {
			version = versionInfo.CommitHash
		}
	}
	if client == nil {
		client = utils.DefaultClient
	}

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, err
	}

	logOptions := []otlploghttp.Option{
		otlploghttp.WithHTTPClient(client),
		otlploghttp.WithCompression(otlploghttp.GzipCompression),
	}

	if logsEndpoint != "" {
		logOptions = append(logOptions, otlploghttp.WithEndpointURL(logsEndpoint))
	}

	if len(headers) > 0 {
		logOptions = append(logOptions, otlploghttp.WithHeaders(headers))
	}

	logExporter, err := otlploghttp.New(ctx, logOptions...)
	if err != nil {
		slog.Error("otel log exporter init error", "error", err)
		return nil, err
	}

	logProvider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		sdklog.WithResource(res),
	)

	context.AfterFunc(ctx, func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := logProvider.Shutdown(shutdownCtx); err != nil {
			slog.Error("otel shutdown error", "error", err)
		}
	})

	slog.Info("otel init success")
	handler := otelslog.NewHandler(
		"",
		otelslog.WithLoggerProvider(logProvider),
		otelslog.WithSource(true),
		otelslog.WithVersion(version),
	)

	return otelSlogLevelHandler{Handler: handler, level: log.SlogLevel}, nil
}

type otelSlogLevelHandler struct {
	slog.Handler
	level slog.Leveler
}

func (h otelSlogLevelHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.level.Level() && h.Handler.Enabled(ctx, level)
}

func (h otelSlogLevelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return otelSlogLevelHandler{Handler: h.Handler.WithAttrs(attrs), level: h.level}
}

func (h otelSlogLevelHandler) WithGroup(name string) slog.Handler {
	return otelSlogLevelHandler{Handler: h.Handler.WithGroup(name), level: h.level}
}
