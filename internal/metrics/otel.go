package metrics

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
)

func SetupOtel(ctx context.Context) (func(context.Context) error, error) {
	var shutdownFuncs []func(context.Context) error

	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", "taskq"),
	))
	if err != nil {
		return nil, err
	}

	prop := newPropagator()
	otel.SetTextMapPropagator(prop)

	traceProvider, err := newTraceProvider(ctx)
	if err != nil {
		return nil, err
	}
	shutdownFuncs = append(shutdownFuncs, traceProvider.Shutdown)
	otel.SetTracerProvider(traceProvider)

	meterProvider, err := newMeterProvider(ctx, res)
	if err != nil {
		return nil, err
	}
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	loggerProvider, err := newLoggerProvider(ctx)
	if err != nil {
		return nil, err
	}
	shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
	global.SetLoggerProvider(loggerProvider)

	shutdownFunc := func(ctx context.Context) error {
		for _, f := range shutdownFuncs {
			if err := f(ctx); err != nil {
				return err
			}
		}
		return nil
	}

	return shutdownFunc, nil
}

func newMeterProvider(ctx context.Context, res *resource.Resource) (*metric.MeterProvider, error) {
	exporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithInsecure())
	if err != nil {
		return nil, err
	}
	meterProvider := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(exporter)),
		metric.WithResource(res),
	)
	return meterProvider, nil
}

func newLoggerProvider(ctx context.Context) (*log.LoggerProvider, error) {
	logExporter, err := otlploghttp.New(ctx, otlploghttp.WithInsecure())
	if err != nil {
		return nil, err
	}
	loggerProvider := log.NewLoggerProvider(log.WithProcessor(log.NewBatchProcessor(logExporter)))
	return loggerProvider, nil
}

func newTraceProvider(ctx context.Context) (*trace.TracerProvider, error) {
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithInsecure())
	if err != nil {
		return nil, err
	}
	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(exporter, trace.WithBatchTimeout(time.Second)),
	)
	return traceProvider, nil
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{})
}
