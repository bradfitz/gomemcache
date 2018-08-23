/*
Copyright 2018 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package memcache

import (
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const unitDimensionless = "1"

var (
	// Stats
	mCacheMisses     = stats.Int64("cache_misses", "Number of cache misses", unitDimensionless)
	mCacheHits       = stats.Int64("cache_hits", "Number of cache hits", unitDimensionless)
	mErrors          = stats.Int64("errors", "Number of errors", unitDimensionless)
	mIllegalKeys     = stats.Int64("illegal_key", "Number of illegal keys", unitDimensionless)
	mCASConflicts    = stats.Int64("cas_conflicts", "Number of CAS conflicts", unitDimensionless)
	mUnstoredResults = stats.Int64("unstored_results", "Number of unstored results", unitDimensionless)
	mDialErrors      = stats.Int64("dial_errors", "Number of dial errors", unitDimensionless)
	mDelta           = stats.Int64("delta", "The values of deltas used in increment or decrement operations", unitDimensionless)

	mCalls       = stats.Int64("calls", "The number of calls to Memcache, they are disambiguated by a method key", unitDimensionless)
	mKeyLength   = stats.Int64("key_length", "Measures the length of keys", "By")
	mValueLength = stats.Int64("key_length", "Measures the length of values", "By")
	mLatencyMs   = stats.Float64("latency", "Measures the latency of the various methods", "ms")

	// TagKeys
	keyMethod, _ = tag.NewKey("method")
	keyReason, _ = tag.NewKey("reason")
	keyType, _   = tag.NewKey("type")

	// Views
	AllViews = []*view.View{
		{
			Name:        "gomemcache/cache_hits",
			Description: "Number of cache hits",
			Measure:     mCacheHits,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/cache_misses",
			Description: "Number of cache misses",
			Measure:     mCacheMisses,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/errors",
			Description: "Number of errors",
			Measure:     mErrors,
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{keyMethod, keyReason, keyType},
		},
		{
			Name:        "gomemcache/illegal_keys",
			Description: "Number of illegal keys encountered",
			Measure:     mIllegalKeys,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/cas_conflicts",
			Description: "Number of CAS conflicts encountered",
			Measure:     mCASConflicts,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/unstored_results",
			Description: "Number of unstored results",
			Measure:     mUnstoredResults,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/dial_errors",
			Description: "Number of dial errors",
			Measure:     mDialErrors,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/key_length",
			Description: "The distribution of the lengths of keys",
			Measure:     mKeyLength,
			// The longest memcache key is 255
			Aggregation: view.Distribution(0, 20, 40, 80, 100, 120, 140, 160, 180, 200, 220, 240, 260),
		},
		{
			Name:        "gomemcache/value_length",
			Description: "The distribution of the lengths of values",
			Measure:     mValueLength,
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
		},

		{
			Name:        "gomemcache/calls",
			Description: "Number of the various method invocations",
			Measure:     mCalls,
			TagKeys:     []tag.Key{keyMethod},
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/latency",
			Description: "The distribution of the latencies in milliseconds",
			Measure:     mLatencyMs,
			TagKeys:     []tag.Key{keyMethod},
			Aggregation: view.Distribution(
				// [0ms, 0.001ms, 0.005ms, 0.01ms, 0.05ms, 0.1ms, 0.5ms, 1ms, 1.5ms, 2ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms, 200ms, 400ms, 600ms, 800ms, 1s, 1.5s, 2.5s, 5s, 10s, 20s, 40s, 100s, 200s, 500s]
				0.0, 0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.0015, 0.002, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.5, 2.5, 5.0, 10.0, 20.0, 40.0, 100.0, 200.0, 500.0),
		},
		{
			Name:        "gomemcache/delta",
			Description: "The distribution of deltas used",
			Measure:     mDelta,
			Aggregation: view.Distribution(0, 25, 50, 100, 200, 250, 400, 500, 800, 1000, 1400, 1600, 3200, 5000, 10000, 100000, 1000000),
		},
	}
)

func sinceInMs(startTime time.Time) float64 {
	return float64(time.Since(startTime).Nanoseconds()) / 1e6
}
