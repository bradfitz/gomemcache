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
}

type Continuum struct {
	numPoints int
	servers   []*MCS
	modTime   time.Time
}

func New() *Continuum {
	return &Continuum{
		modTime: time.Now(),
	}
}

func (c *Continuum) Roll(filename string) error {
	if modtime, err := c.fileModTime(filename); err != nil {
		return err
	}
	if !modtime.IsZero() {
		c.readerServerDefinitions(filename)
	}

}

// Generates the continuum of servers (each server as many points on a circle).
func (c *Continuum) Create(key string, filename string) error {
	var cont = 0
	if slist, numServers, err := c.readServerDefinitions(filename); err != nil {
		return err
	}
	if numServers < 1 {
		return errors.New("no valid server definitions in file", filename)
	}
	log.Printf("Server definitions read: %d servers\n", numServers)
	for i, server := range slist {
		md5 := c.md5Digest(server)
		for h := 0; h < 4; h++ {
			point := md5[3+h*4]<<24 | md5[3+h*4]<<16 | md5[3+h*4]<<8 | md5[3+h*4]
			fmt.Println(test)
			c.servers = append(c.servers, &MCS{
				point: point,
				ip:    server,
			})
			cont++
		}
	}
	sort.Slice(c.servers[:], func(i, j int) bool {
		return c.servers[i].point < c.servers[j].point
	})
}

func (c *Continuum) readServerDefinitions(filename string) ([]*serverInfo, *int, error) {
	var (
		lineno     int
		numServers int
		line       string
		servers    []*serverInfo
	)
	if file, err := os.Open(filename); err == nil {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			lineno++
			if serverInfo, err := c.readServerLine(scanner.Text()); err != nil {
				return nil, numServers, errors.New("error reading server line: ", err)
			}
			numServers++
			servers = append(servers, serverInfo)
		}
	} else {
		return nil, numServers, errors.New("file does not exist: ", filename)
	}

	return servers, numServers, nil
}

func (c *Continuum) readServerLine(line string) (*serverInfo, error) {
	var (
		delim      = "\t"
		serverInfo server
	)

	splitLine := strings.Split(line, delim)
	if splitLine == "" {
		return nil, errors.New("server line could not be read.")
	}
	return &serverInfo{
		address: splitLine[0],
	}, nil
}

func (c *Continuum) fileModTime(filename) (*time.Time, error) {
	if fileInfo, err := os.Stat(filename); err != nil {
		return nil, errors.New("modtime could not be retrieved:", err)
	}
	return fileInfo.ModTime(), nil

}

func (c *Continuum) GetServer(key string) {}

func (c *Continuum) Print() {}

func (c *Continuum) Compare(pointA *point, pointB *point) {}

func (c *Continuum) md5Digest(inString string) string {
	m := md5.New([]byte(inString))
	return fmt.Sprintf("%x", m.Sum(in))
}

func (c *Continuum) Hashi(inString string) string {}
