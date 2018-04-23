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
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
)

const unitDimensionless = "1"

var (
	// Stats
	mCacheMisses     = stats.Int64("cache_misses", "Number of cache misses", unitDimensionless)
	mCacheHits       = stats.Int64("cache_hits", "Number of cache hits", unitDimensionless)
	mClientErrors    = stats.Int64("client_errors", "Number of general client errors", unitDimensionless)
	mParseErrors     = stats.Int64("parse_errors", "Number of errors encountered while parsing", unitDimensionless)
	mIllegalKeys     = stats.Int64("illegal_key", "Number of illegal keys", unitDimensionless)
	mCASConflicts    = stats.Int64("cas_conflicts", "Number of CAS conflicts", unitDimensionless)
	mUnstoredResults = stats.Int64("unstored_results", "Number of unstored results", unitDimensionless)
	mDialErrors      = stats.Int64("dial_errors", "Number of dial errors", unitDimensionless)

	mFlushAll       = stats.Int64("flushall", "Number of FlushAll invocations", unitDimensionless)
	mGet            = stats.Int64("get", "Number of Get invocations", unitDimensionless)
	mTouch          = stats.Int64("touch", "Number of Touch invocations", unitDimensionless)
	mKeyLength      = stats.Int64("key_length", "Measures the length of keys", unitDimensionless)
	mValueLength    = stats.Int64("key_length", "Measures the length of values", unitDimensionless)
	mSet            = stats.Int64("set", "Number of Set invocations", unitDimensionless)
	mAdd            = stats.Int64("add", "Number of Add invocations", unitDimensionless)
	mDelete         = stats.Int64("delete", "Number of Delete invocations", unitDimensionless)
	mDeleteAll      = stats.Int64("deleteall", "Number of DeleteAll invocations", unitDimensionless)
	mDelta          = stats.Int64("delta", "Number of Delta invocations", unitDimensionless)
	mCompareAndSwap = stats.Int64("delta", "Number of Delta invocations", unitDimensionless)
	mReplace        = stats.Int64("replace ", "Number of Replace invocations", unitDimensionless)
	mDecrement      = stats.Int64("decrement", "Number of Decrement invocations", unitDimensionless)
	mIncrement      = stats.Int64("increment", "Number of Increment invocations", unitDimensionless)

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
			Name:        "gomemcache/client_errors",
			Description: "Number of general client errors",
			Measure:     mClientErrors,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/parse_errors",
			Description: "Number of errors encountered while parsing",
			Measure:     mParseErrors,
			Aggregation: view.Count(),
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
			Name:        "gomemcache/flushall",
			Description: "Number of FlushAll invocations",
			Measure:     mFlushAll,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/get",
			Description: "Number of Get invocations",
			Measure:     mGet,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/touch",
			Description: "Number of Touch invocations",
			Measure:     mTouch,
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
			Name:        "gomemcache/set",
			Description: "Number of Set invocations",
			Measure:     mSet,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/add",
			Description: "Number of Add invocations",
			Measure:     mSet,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/delete",
			Description: "Number of Delete invocations",
			Measure:     mDelete,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/replace",
			Description: "Number of Replace invocations",
			Measure:     mReplace,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/cas",
			Description: "Number of CompareAndSwap invocations",
			Measure:     mCompareAndSwap,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/delta",
			Description: "The distribution of deltas used",
			Measure:     mDelta,
			Aggregation: view.Distribution(0, 25, 50, 100, 200, 250, 400, 500, 800, 1000, 1400, 1600, 3200, 5000, 10000, 100000, 1000000),
		},
		{
			Name:        "gomemcache/deleteall",
			Description: "Number of DeleteAll invocations",
			Measure:     mDeleteAll,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/decrement",
			Description: "Number of Decrement invocations",
			Measure:     mDecrement,
			Aggregation: view.Count(),
		},
		{
			Name:        "gomemcache/inccrement",
			Description: "Number of Increment invocations",
			Measure:     mIncrement,
			Aggregation: view.Count(),
		},
	}
)
