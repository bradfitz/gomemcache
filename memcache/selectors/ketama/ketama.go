// Package is a golang port of https://github.com/RJ/ketama/tree/master/libketama
package ketama

import (
	"bufio"
	"errors"
	"os"
	"strings"
	"time"
)

type point struct {
	point int
	ip    string
}

type serverInfo struct {
	address string
}

type Continuum struct {
	numPoints int
	points    []*point
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

func (c *Continuum) Create(key string, filename string) {}

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

func (c *Continuum) Hashi(inString string) {}
