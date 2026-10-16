package migrate

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc"
)

// Config holds migrate.volume runtime options.
type Config struct {
	Dir           string
	Collection    string
	VolumeIDs     []uint32 // empty = all volumes for collection
	Filer         string   // e.g. http://filer:8888
	PathTemplate  string
	StateDir      string
	Decompress    bool
	DryRun        bool
	VerifyMode    VerifyLevel
	Retries       int
	Limit         int // 0 = unlimited successful migrations
	Workers       int // concurrent uploads within a volume
	Newer         time.Time
	HasNewer      bool
	TimeZone      *time.Location
	CollectionDst string
	Replication   string
	TTL           string
	Disk          string
	ProgressEvery int // flush volume cursor every N committed idx entries
	ForceConfig   bool
	// Mode is "filer" (HTTP filer, new fid) or "blob" (master+volume, keep source fid).
	Mode     string
	Masters  string // comma-separated host:port, required for blob
	GrpcDial grpc.DialOption
}

func (c *Config) IsBlob() bool {
	return strings.EqualFold(c.Mode, "blob")
}

func (c *Config) Normalize() error {
	if c.Mode == "" {
		c.Mode = "filer"
	}
	c.Mode = strings.ToLower(c.Mode)
	if c.Mode != "filer" && c.Mode != "blob" {
		return fmt.Errorf("mode must be filer or blob, got %q", c.Mode)
	}
	if c.Dir == "" {
		return fmt.Errorf("dir is required")
	}
	if c.StateDir == "" {
		return fmt.Errorf("stateDir is required")
	}
	if c.IsBlob() {
		masters := splitCSV(c.Masters)
		if len(masters) == 0 {
			return fmt.Errorf("blob mode requires -masters=host:9333[,host:9334]")
		}
		for i, m := range masters {
			masters[i] = strings.TrimPrefix(strings.TrimPrefix(m, "http://"), "https://")
			if !strings.Contains(masters[i], ":") {
				return fmt.Errorf("master address %q must be host:port", m)
			}
		}
		c.Masters = strings.Join(masters, ",")
	} else {
		if c.Filer == "" {
			return fmt.Errorf("filer is required")
		}
		if c.PathTemplate == "" {
			return fmt.Errorf("path template is required")
		}
		if !strings.HasPrefix(c.PathTemplate, "/") {
			return fmt.Errorf("path template must be absolute, got %q", c.PathTemplate)
		}
		u, err := url.Parse(strings.Split(c.Filer, ",")[0])
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("filer must be a URL like http://host:8888, got %q", c.Filer)
		}
		c.Filer = strings.TrimRight(c.Filer, "/")
		if _, err := NewPathTemplate(c.PathTemplate); err != nil {
			return err
		}
	}
	if c.Retries < 0 {
		c.Retries = 0
	}
	if c.ProgressEvery <= 0 {
		c.ProgressEvery = 100
	}
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.TimeZone == nil {
		c.TimeZone = time.Local
	}
	if c.Replication == "" && c.IsBlob() {
		c.Replication = "000"
	}
	return nil
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// FilerBases returns one or more filer HTTP bases (comma-separated input).
func (c *Config) FilerBases() []string {
	parts := strings.Split(c.Filer, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(strings.TrimRight(p, "/"))
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
