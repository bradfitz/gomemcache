// Copyright 2018 Google LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memcache

import (
	"context"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

// Metric Tags
var (
	KeyError, _  = tag.NewKey("error")
	KeyMethod, _ = tag.NewKey("method")
	KeyStatus, _ = tag.NewKey("status")
)

type latencyTrackingSpan struct {
	startTime  time.Time
	methodName string
	span       *trace.Span
}

func newLatencyTrackingSpan(ctx context.Context, methodName string, keys ...string) (context.Context, *latencyTrackingSpan) {
	lts := new(latencyTrackingSpan)
	ctx = lts.start(ctx, methodName, keys...)
	return ctx, lts
}

func (lts *latencyTrackingSpan) start(ctx context.Context, methodName string, keys ...string) context.Context {
	lts.startTime = time.Now()
	lts.methodName = methodName
	ctx, _ = tag.New(ctx, tag.Upsert(KeyMethod, lts.methodName))

	var measurements []stats.Measurement
	for _, key := range keys {
		measurements = append(measurements, mKeyLength.M(int64(len(key))))
	}
	if len(measurements) > 0 {
		stats.Record(ctx, measurements...)
	}
	ctx, lts.span = trace.StartSpan(ctx, methodName)

	return ctx
}

func (lts *latencyTrackingSpan) end(ctx context.Context, err error, valueLengths ...int64) {
	ctx, _ = tag.New(ctx, tag.Upsert(KeyMethod, lts.methodName))

	if err == nil {
		ctx, _ = tag.New(ctx, tag.Upsert(KeyStatus, "OK"))
	} else {
		msg := err.Error()
		ctx, _ = tag.New(ctx, tag.Upsert(KeyError, msg), tag.Upsert(KeyStatus, "ERROR"))
		lts.span.SetStatus(trace.Status{Code: errToStatusCode(err), Message: msg})
	}

	var measurements []stats.Measurement
	for _, valueLength := range valueLengths {
		measurements = append(measurements, mValueLength.M(valueLength))
	}
	latencyMs := float64(time.Since(lts.startTime).Nanoseconds()) / 1e6
	measurements = append(measurements, mLatencyMs.M(latencyMs))

	stats.Record(ctx, measurements...)
	lts.span.End()
}

func (lts *latencyTrackingSpan) recordMeasurements(ctx context.Context, measurements ...stats.Measurement) {
	stats.Record(ctx, measurements...)
}

func errToStatusCode(err error) int32 {
	switch err {
	default:
		return trace.StatusCodeUnknown

	case ErrCacheMiss:
		return trace.StatusCodeNotFound

	case ErrCASConflict:
		return trace.StatusCodeDataLoss

	case ErrNotStored:
		return trace.StatusCodeDataLoss

	case ErrServerError:
		return trace.StatusCodeInternal

	case ErrNoServers:
		return trace.StatusCodeUnavailable

	case ErrNoStats:
		return trace.StatusCodeUnavailable

	case ErrMalformedKey:
		return trace.StatusCodeInvalidArgument

	case ErrNotStored:
		return trace.StatusCodeDataLoss
	}
}

// Metrics and views below:
var (
	// Measures
	mKeyLength   = stats.Int64("key_length", "Measures the length of keys", "By")
	mValueLength = stats.Int64("value_length", "Measures the length of values", "By")
	mLatencyMs   = stats.Float64("latency", "Measures the latency of the various methods", "ms")

	// Views

	KeyLengthView = &view.View{
		Name:        "gomemcache/key_length",
		Description: "The distribution of the lengths of keys",
		Measure:     mKeyLength,
		TagKeys:     []tag.Key{KeyMethod},
		// The longest memcache key is 255
		Aggregation: view.Distribution(0, 20, 40, 80, 100, 120, 140, 160, 180, 200, 220, 240, 260),
	}
	KeyValueLengthView = &view.View{
		Name:        "gomemcache/value_length",
		Description: "The distribution of the lengths of values",
		Measure:     mValueLength,
		TagKeys:     []tag.Key{KeyMethod},
		Aggregation: view.Distribution(
			0, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288,
			1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456,
			536870912, 1073741824, 2147483648, 4294967296, 8589934592, 17179869184, 34359738368,
			68719476736, 137438953472, 274877906944, 549755813888, 1099511627776, 2199023255552,
			4398046511104, 8796093022208, 17592186044416, 35184372088832, 70368744177664, 140737488355328,
			281474976710656, 562949953421312, 1125899906842624, 2251799813685248, 4503599627370496,
			9007199254740992, 18014398509481984, 36028797018963968, 72057594037927936, 144115188075855872,
			288230376151711744, 576460752303423488, 1152921504606846976, 2305843009213693952, 4611686018427387904,
			9223372036854775808,
		),
	}
	KeyCallsView = &view.View{
		Name:        "gomemcache/calls",
		Description: "Number of the various method invocations",
		Measure:     mLatencyMs,
		TagKeys:     []tag.Key{KeyMethod, KeyError, KeyStatus},
		Aggregation: view.Count(),
	}
	KeyLatencyView = &view.View{
		Name:        "gomemcache/latency",
		Description: "The distribution of the latencies in milliseconds",
		Measure:     mLatencyMs,
		TagKeys:     []tag.Key{KeyMethod, KeyError, KeyStatus},
		Aggregation: view.Distribution(
			// [0ms, 0.001ms, 0.005ms, 0.01ms, 0.05ms, 0.1ms, 0.5ms, 1ms, 1.5ms, 2ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms, 200ms, 400ms, 600ms, 800ms, 1s, 1.5s, 2.5s, 5s, 10s, 20s, 40s, 100s, 200s, 500s]
			0.0, 0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.0015, 0.002, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.5, 2.5, 5.0, 10.0, 20.0, 40.0, 100.0, 200.0, 500.0),
	}

	AllViews = []*view.View{
		KeyLengthView,
		KeyValueLengthView,
		KeyCallsView,
		KeyLatencyView,
	}
)
