// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http"
	_ "net/http/pprof"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	otelmetrics "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

var (
	// Metric variable declarations are global, so they can be accessed directly in the application code.
	ActivationsPerCall     otelmetrics.Int64Histogram
	DeactivationsPerCall   otelmetrics.Int64Histogram
	AssignmentsPerCall     otelmetrics.Int64Histogram
	TicketSize             otelmetrics.Float64Histogram
	RpcDuration            otelmetrics.Float64Histogram
	RpcErrors              otelmetrics.Int64Counter
	MmfTicketDeactivations otelmetrics.Int64Counter
	Matches                otelmetrics.Int64Counter
	AssignmentWatches      otelmetrics.Int64UpDownCounter
	metricsNamePrefix      = "om_core."
	otelLogger             = logrus.WithFields(logrus.Fields{
		"app":            "open_match",
		"component":      "core",
		"implementation": "otel",
	})
)

func initializeOtel() *otelmetrics.Meter {
	// Get background resource attributes.
	res, err := resource.New(
		context.Background(),
		// Use the GCP resource detector to detect information about the GCP platform
		resource.WithDetectors(gcp.NewDetector()),
		// Discover and provide attributes from OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME environment variables.
		resource.WithFromEnv(),
		// Discover and provide information about the OpenTelemetry SDK used.
		resource.WithTelemetrySDK(),
		// Open Match attributes
		resource.WithAttributes(
			semconv.ServiceNameKey.String("open_match"),
			semconv.ServiceVersionKey.String("2.0.0"),
		),
	)
	if errors.Is(err, resource.ErrPartialResource) || errors.Is(err, resource.ErrSchemaURLConflict) {
		otelLogger.Println(err) // Log non-fatal issues.
	} else if err != nil {
		otelLogger.Errorf("Failed to create open telemetry resource: %w", err)
	}

	// The exporter embeds a default OpenTelemetry Reader and
	// implements prometheus.Collector, allowing it to be used as
	// both a Reader and Collector. http://localhost:2224/metrics
	// TODO: config for otel collector
	exporter, err := prometheus.New()
	if err != nil {
		otelLogger.Fatal(err)
	}

	// Start the prometheus HTTP server and pass the exporter Collector to it
	go func() {
		otelLogger.Infof("serving metrics at localhost:2223/metrics")
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":2223", nil) //nolint:gosec // Ignoring G114: Use of net/http serve function that has no support for setting timeouts.
		if err != nil {
			fmt.Printf("error serving http: %v", err)
			return
		}
	}()

	// Otel meter and meterprovider init
	provider := metric.NewMeterProvider(metric.WithResource(res), metric.WithReader(exporter))
	meter := provider.Meter("open_match.core")

	return &meter
}

func registerMetrics(meterPointer *otelmetrics.Meter) {
	meter := *meterPointer
	var err error

	// Initialize all declared Metrics
	RpcDuration, err = meter.Float64Histogram(
		metricsNamePrefix+"rpc.duration",
		otelmetrics.WithDescription("RPC duration"),
		otelmetrics.WithUnit("ms"),
	)
	if err != nil {
		otelLogger.Fatal(err)
	}

	RpcErrors, err = meter.Int64Counter(
		metricsNamePrefix+"rpc.errors",
		otelmetrics.WithDescription("Total gRPC errors returned"),
	)
	if err != nil {
		otelLogger.Fatal(err)
	}

	ActivationsPerCall, err = meter.Int64Histogram(
		metricsNamePrefix+"ticket.activation.requests",
		otelmetrics.WithDescription("Total tickets set to active, so they appear in pools"),
	)
	if err != nil {
		otelLogger.Fatal(err)
	}

	// TODO: Distinguish between active deactivations (using the API endpoint) and
	// triggered deactivations (by matches returned from mmfs) using metric
	// attributes at the time of recording...?
	DeactivationsPerCall, err = meter.Int64Histogram(
		metricsNamePrefix+"ticket.deactivation.requests",
		otelmetrics.WithDescription("Total tickets set to deactive, so they do not appear in pools"),
	)
	if err != nil {
		otelLogger.Fatal(err)
	}

	AssignmentsPerCall, err = meter.Int64Histogram(
		metricsNamePrefix+"ticket.assignment",
		otelmetrics.WithDescription("Number of tickets assignments included in each CreateAssignments() call"),
	)
	if err != nil {
		otelLogger.Fatal(err)
	}

	AssignmentWatches, err = meter.Int64UpDownCounter(
		metricsNamePrefix+"ticket.assigment.watch",
		otelmetrics.WithDescription("Number of tickets assignments included in each CreateAssignments() call"),
	)
	if err != nil {
		otelLogger.Fatal(err)
	}

	Matches, err = meter.Int64Counter(
		metricsNamePrefix+"match.received",
		otelmetrics.WithDescription("Total matches received from MMFs"),
	)
	if err != nil {
		otelLogger.Fatal(err)
	}

}
