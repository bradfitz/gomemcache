// Package is a golang port of https://github.com/RJ/ketama/tree/master/libketama
package ketama

import (
	"bufio"
	"crypto/md5"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)

type MCS struct {
	point int
	ip    string
}

type serverInfo struct {
	address string
	weight  int
}

type Continuum struct {
	numPoints int
	servers   []MCS
	modTime   time.Time
}

func New() *Continuum {
	return &Continuum{
		modTime: time.Now(),
	}
}

func (c *Continuum) Roll(filename string) error {
	var (
		modtime time.Time
		err     error
	)
	if modtime, err = c.fileModTime(filename); err != nil {
		return err
	}
	if !modtime.IsZero() {
		c.readServerDefinitions(filename)
	}
	return nil
}

// Generates the continuum of servers (each server as many points on a circle).
func (c *Continuum) CreateFromFile(filename string) error {
	slist, _, err := c.readServerDefinitions(filename)
	if err != nil {
		return err
	}
	return c.Create(slist)
}

// Generates the continuum of servers (each server as many points on a circle).
func (c *Continuum) Create(slist []*serverInfo) error {
	var (
		cont = 0
	)
	numServers := len(slist)
	if numServers < 1 {
		return errors.New("no valid server definitions in given")
	}
	log.Printf("Server definitions read: %d servers\n", numServers)
	c.servers = make([]MCS, 0, numServers*160)
	totalWeight := 0
	for _, server := range slist {
		if server.weight == 0 {
			server.weight = 1
		}
		totalWeight += server.weight
	}
	for _, server := range slist {
		pct := float32(server.weight) / float32(totalWeight)
		ks := int(float32(float64(pct) * 40.0 * float64(numServers)))
		for k := 0; k < ks; k++ {
			/* 40 hashes, 4 numbers per hash = 160 points per bucket */
			md5 := c.md5Digest(server.address)
			for h := 0; h < 4; h++ {
				point := md5[3+h*4]<<24 | md5[3+h*4]<<16 | md5[3+h*4]<<8 | md5[3+h*4]
				c.servers = append(c.servers, MCS{
					point: int(point),
					ip:    server.address,
				})
				cont++
			}
		}
	}
	sort.Slice(c.servers[:], func(i, j int) bool {
		return c.servers[i].point < c.servers[j].point
	})
	c.numPoints = cont
	return nil
}

func (c *Continuum) readServerDefinitions(filename string) ([]*serverInfo, int, error) {
	var (
		lineno     int
		numServers int
		servers    []*serverInfo
		serverInfo *serverInfo
	)
	if file, err := os.Open(filename); err == nil {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			lineno++
			if serverInfo, err = c.readServerLine(scanner.Text()); err != nil {
				return nil, numServers, fmt.Errorf("error reading server line: %s", err)
			}
			numServers++
			servers = append(servers, serverInfo)
		}
	} else {
		return nil, numServers, fmt.Errorf("file does not exist: %s", filename)
	}

	return servers, numServers, nil
}

func (c *Continuum) readServerLine(line string) (*serverInfo, error) {
	var (
		delim = "\t"
	)

	splitLine := strings.Split(line, delim)
	if splitLine[0] == "" {
		return nil, errors.New("server line could not be read.")
	}
	return &serverInfo{
		address: splitLine[0],
	}, nil
}

func (c *Continuum) fileModTime(filename string) (time.Time, error) {
	var (
		fileInfo os.FileInfo
		err      error
	)
	if fileInfo, err = os.Stat(filename); err != nil {
		return time.Time{}, fmt.Errorf("modtime could not be retrieved: %s", err)
	}
	return fileInfo.ModTime(), nil

}

func (c *Continuum) GetServer(key string) MCS {
	var (
		lowp    = 0
		midp    int
		midval  int
		midval1 int
	)
	h := c.Hashi(key)
	highp := c.numPoints
	for range c.servers {
		midp = (lowp + highp) / 2

		if midp == c.numPoints {
			return c.servers[0]
		}

		midval = c.servers[midp].point
		if midp == 0 {
			midval1 = 0
		} else {
			midval1 = c.servers[midp-1].point
		}

		if h <= midval && h > midval1 {
			return c.servers[midp]
		}

		if midval < h {
			lowp = midp + 1
		} else {
			lowp = midp - 1
		}

		if lowp > highp {
			return c.servers[0]
		}
	}
	return MCS{}
}

func (c *Continuum) Print() {}

func (c *Continuum) Compare(pointA *MCS, pointB *MCS) {}

func (c *Continuum) md5Digest(inString string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(inString)))
}

func (c *Continuum) Hashi(inString string) int {
	md5 := c.md5Digest(inString)
	return int(md5[3]<<24 | md5[2]<<16 | md5[1]<<8 | md5[0])
}
