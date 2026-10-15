package migrate

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Config holds migrate.volume runtime options (P0).
type Config struct {
	Dir            string
	Collection     string
	VolumeIDs      []uint32 // empty = all volumes for collection
	Filer          string   // e.g. http://filer:8888
	PathTemplate   string
	StateDir       string
	Decompress     bool
	DryRun         bool
	VerifyMeta     bool // L1: exists + size after upload
	Retries        int
	Limit          int // 0 = unlimited successful migrations
	Newer          time.Time
	HasNewer       bool
	TimeZone       *time.Location
	CollectionDst  string
	Replication    string
	TTL            string
	Disk           string
	ProgressEvery  int // flush volume cursor every N processed idx entries
	ForceConfig    bool
}

func (c *Config) Normalize() error {
	if c.Dir == "" {
		return fmt.Errorf("dir is required")
	}
	if c.Filer == "" {
		return fmt.Errorf("filer is required")
	}
	if c.PathTemplate == "" {
		return fmt.Errorf("path template is required")
	}
	if !strings.HasPrefix(c.PathTemplate, "/") {
		return fmt.Errorf("path template must be absolute, got %q", c.PathTemplate)
	}
	if c.StateDir == "" {
		return fmt.Errorf("stateDir is required")
	}
	u, err := url.Parse(c.Filer)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("filer must be a URL like http://host:8888, got %q", c.Filer)
	}
	c.Filer = strings.TrimRight(c.Filer, "/")
	if c.Retries < 0 {
		c.Retries = 0
	}
	if c.ProgressEvery <= 0 {
		c.ProgressEvery = 100
	}
	if c.TimeZone == nil {
		c.TimeZone = time.Local
	}
	if _, err := NewPathTemplate(c.PathTemplate); err != nil {
		return err
	}
	return nil
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
