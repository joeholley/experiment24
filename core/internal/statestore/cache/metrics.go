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
package cache

import (
	"context"
	"log"
	_ "net/http"
	_ "net/http/pprof"

	otelmetrics "go.opentelemetry.io/otel/metric"
)

var (
	// Metric variable declarations are global to the module, so they can be
	// accessed directly in the module code.
	// Observable counters
	ActiveTickets   otelmetrics.Int64ObservableUpDownCounter
	InactiveTickets otelmetrics.Int64ObservableUpDownCounter
	Assignments     otelmetrics.Int64ObservableUpDownCounter

	TicketCount     int64
	InactiveCount   int64
	AssignmentCount int64

	ExpirationCycleDuration    otelmetrics.Float64Histogram
	AssignmentsExpiredPerCycle otelmetrics.Int64Histogram
	InactivesExpiredPerCycle   otelmetrics.Int64Histogram
	TicketsExpiredPerCycle     otelmetrics.Int64Histogram
	metricsNamePrefix          = "om_core.cache."
)

func RegisterMetrics(meterPointer *otelmetrics.Meter) {
	meter := *meterPointer
	var err error

	ActiveTickets, err = meter.Int64ObservableUpDownCounter(
		metricsNamePrefix+"tickets.active",
		otelmetrics.WithDescription("Active tickets that can appear in pools"),
	)
	if err != nil {
		log.Fatal(err)
	}

	InactiveTickets, err = meter.Int64ObservableUpDownCounter(
		metricsNamePrefix+"tickets.inactive",
		otelmetrics.WithDescription("Inactive tickets that won't appear in pools"),
	)
	if err != nil {
		log.Fatal(err)
	}

	Assignments, err = meter.Int64ObservableUpDownCounter(
		metricsNamePrefix+"assignments",
		otelmetrics.WithDescription("Assignments in Open Match cache"),
	)
	if err != nil {
		log.Fatal(err)
	}

	ExpirationCycleDuration, err = meter.Float64Histogram(
		metricsNamePrefix+"expiration.duration",
		otelmetrics.WithDescription("Duration of cache expiration logic"),
		otelmetrics.WithUnit("ms"),
	)

	AssignmentsExpiredPerCycle, err = meter.Int64Histogram(
		metricsNamePrefix+"assignment.expirations",
		otelmetrics.WithDescription("Number of assignments expired per cache expiration cycle"),
	)

	TicketsExpiredPerCycle, err = meter.Int64Histogram(
		metricsNamePrefix+"ticket.expirations",
		otelmetrics.WithDescription("Number of tickets expired per cache expiration cycle"),
	)

	InactivesExpiredPerCycle, err = meter.Int64Histogram(
		metricsNamePrefix+"ticket.inactive.expirations",
		otelmetrics.WithDescription("Number of inactive tickets expired per cache expiration cycle"),
	)

	// Each time metrics are sampled, get the latest counts of active/inactive
	// tickets and assignments.
	if _, err := meter.RegisterCallback(
		func(ctx context.Context, o otelmetrics.Observer) error {
			// TODO
			o.ObserveInt64(ActiveTickets, TicketCount-InactiveCount)
			o.ObserveInt64(InactiveTickets, InactiveCount)
			o.ObserveInt64(Assignments, AssignmentCount)
			return nil
		},
		ActiveTickets, InactiveTickets, Assignments,
	); err != nil {
		panic(err)
	}
}
