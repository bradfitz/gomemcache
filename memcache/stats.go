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
	"log"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
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

	// Views
	AllViews = []*view.View{
		{
			Name:        "gomemcache/cache_hits",
			Description: "Number of cache hits",
			Measure:     mCacheHits,
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{mustKey("cache_hits")},
		},
		{
			Name:        "gomemcache/cache_misses",
			Description: "Number of cache misses",
			Measure:     mCacheMisses,
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{mustKey("cache_misses")},
		},
		{
			Name:        "gomemcache/client_errors",
			Description: "Number of general client errors",
			Measure:     mClientErrors,
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{mustKey("client_errors"), mustKey("miscellaneous_errors")},
		},
		{
			Name:        "gomemcache/parse_errors",
			Description: "Number of errors encountered while parsing",
			Measure:     mParseErrors,
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{mustKey("parse_errors"), mustKey("miscellaneous_errors")},
		},
		{
			Name:        "gomemcache/illegal_keys",
			Description: "Number of illegal keys encountered",
			Measure:     mIllegalKeys,
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{mustKey("illegal_keys"), mustKey("miscellaneous_errors")},
		},
		{
			Name:        "gomemcache/cas_conflicts",
			Description: "Number of CAS conflicts encountered",
			Measure:     mCASConflicts,
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{mustKey("cas_conflicts")},
		},
		{
			Name:        "gomemcache/unstored_results",
			Description: "Number of unstored results",
			Measure:     mUnstoredResults,
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{mustKey("unstored_results")},
		},
		{
			Name:        "gomemcache/dial_errors",
			Description: "Number of dial errors",
			Measure:     mDialErrors,
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{mustKey("dial_errors")},
		},
	}
)

func mustKey(strKey string) tag.Key {
	key, err := tag.NewKey(strKey)
	if err != nil {
		log.Fatalf("Failed to create key %q error: %v", strKey, err)
	}
	return key
}
