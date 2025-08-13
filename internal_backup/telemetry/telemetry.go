// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package telemetry

import (
	"context"
	"errors"
	"fmt"

	mexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"go.opentelemetry.io/contrib/propagators/autoprop"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

// setupOTelSDK bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func SetupOTel(ctx context.Context, versionString, telemetryOTLP string, telemetryGCP bool, telemetryServiceName string) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error

	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	// handleErr calls shutdown for cleanup and makes sure that all errors are returned.
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	// Configure Context Propagation to use the default W3C traceparent format.
	otel.SetTextMapPropagator(autoprop.NewTextMapPropagator())

	res, err := newResource(ctx, versionString, telemetryServiceName)
	if err != nil {
		errMsg := fmt.Errorf("unable to set up resource: %w", err)
		handleErr(errMsg)
		return
	}

	tracerProvider, err := newTracerProvider(ctx, res, telemetryOTLP, telemetryGCP)
	if err != nil {
		errMsg := fmt.Errorf("unable to set up trace provider: %w", err)
		handleErr(errMsg)
		return
	}
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	meterProvider, err := newMeterProvider(ctx, res, telemetryOTLP, telemetryGCP)
	if err != nil {
		errMsg := fmt.Errorf("unable to set up meter provider: %w", err)
		handleErr(errMsg)
		return
	}
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	return shutdown, nil
}

// newResource create default resources for telemetry data.
// Resource represents the entity producing telemetry.
func newResource(ctx context.Context, versionString string, telemetryServiceName string) (*resource.Resource, error) {
	// Ensure default SDK resources and the required service name are set.
	r, err := resource.New(
		ctx,
		resource.WithFromEnv(),      // Discover and provide attributes from OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME environment variables.
		resource.WithTelemetrySDK(), // Discover and provide information about the OTel SDK used.
		resource.WithOS(),           // Discover and provide OS information.
		resource.WithContainer(),    // Discover and provide container information.
		resource.WithHost(),         //Discover and provide host information.
		resource.WithSchemaURL(semconv.SchemaURL), // Set the schema url.
		resource.WithAttributes( // Add other custom resource attributes.
			semconv.ServiceName(telemetryServiceName),
			semconv.ServiceVersion(versionString),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("trace provider fail to set up resource: %w", err)
	}
	return r, nil
}

// newTracerProvider creates TracerProvider.
// TracerProvider is a factory for Tracers and is responsible for creating spans.
func newTracerProvider(ctx context.Context, r *resource.Resource, telemetryOTLP string, telemetryGCP bool) (*tracesdk.TracerProvider, error) {
	traceOpts := []tracesdk.TracerProviderOption{}
	if telemetryOTLP != "" {
		// otlptracehttp provides an OTLP span exporter using HTTP with protobuf payloads.
		// By default, the telemetry is sent to https://localhost:4318/v1/traces.
		otlpExporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(telemetryOTLP))
		if err != nil {
			return nil, err
		}
		traceOpts = append(traceOpts, tracesdk.WithBatcher(otlpExporter))
	}
	if telemetryGCP {
		gcpExporter, err := texporter.New()
		if err != nil {
			return nil, err
		}
		traceOpts = append(traceOpts, tracesdk.WithBatcher(gcpExporter))
	}
	traceOpts = append(traceOpts, tracesdk.WithResource(r))

	traceProvider := tracesdk.NewTracerProvider(traceOpts...)
	return traceProvider, nil
}

// newMeterProvider creates MeterProvider.
// MeterProvider is a factory for Meters, and is responsible for creating metrics.
func newMeterProvider(ctx context.Context, r *resource.Resource, telemetryOTLP string, telemetryGCP bool) (*metric.MeterProvider, error) {
	metricOpts := []metric.Option{}
	if telemetryOTLP != "" {
		// otlpmetrichttp provides an OTLP metrics exporter using HTTP with protobuf payloads.
		// By default, the telemetry is sent to https://localhost:4318/v1/metrics.
		otlpExporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpoint(telemetryOTLP))
		if err != nil {
			return nil, err
		}
		metricOpts = append(metricOpts, metric.WithReader(metric.NewPeriodicReader(otlpExporter)))
	}
	if telemetryGCP {
		gcpExporter, err := mexporter.New()
		if err != nil {
			return nil, err
		}
		metricOpts = append(metricOpts, metric.WithReader(metric.NewPeriodicReader(gcpExporter)))
	}

	meterProvider := metric.NewMeterProvider(metricOpts...)
	return meterProvider, nil
}
