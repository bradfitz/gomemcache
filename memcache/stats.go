package memcache

import (
	"strings"
)

const (
	statsCommandKeyIndex = 1
	statsCommandValIndex = 2
)

// Stats defines a line of statistics for a memcached instance
type Stats map[string]string

// parse parses the current line assuming the format `STATS <key> <value>` and stores in the current stats
func (s Stats) parse(line string) error {
	trimmedLine := strings.TrimSpace(line)
	statEl := strings.Split(trimmedLine, " ")
	if len(statEl) != 3 {
		return ErrNoStats
	}

	key := statEl[statsCommandKeyIndex]
	val := statEl[statsCommandValIndex]

	s[key] = val
	return nil
}
