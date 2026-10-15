package migrate

import (
	"fmt"
	"hash/fnv"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	tokenRe   = regexp.MustCompile(`\{([a-zA-Z0-9_]+)\}`)
	hashNRe   = regexp.MustCompile(`^hash(\d+)$`)
	knownVars = map[string]bool{
		"fid": true, "volume_id": true, "collection": true,
		"yyyy": true, "yyyyMM": true, "yyyyMMdd": true, "HH": true, "mm": true,
		"hash12": true,
	}
)

// PathContext feeds path template rendering.
type PathContext struct {
	Fid          string // raw "3,abc..."
	FidPathSafe  string // "3_abc..."
	VolumeID     uint32
	Collection   string
	Modified     time.Time
	TimeZone     *time.Location
}

// PathTemplate renders destination paths.
type PathTemplate struct {
	raw string
}

func NewPathTemplate(tmpl string) (*PathTemplate, error) {
	if tmpl == "" {
		return nil, fmt.Errorf("empty path template")
	}
	if !strings.HasPrefix(tmpl, "/") {
		return nil, fmt.Errorf("path template must start with /")
	}
	matches := tokenRe.FindAllStringSubmatch(tmpl, -1)
	for _, m := range matches {
		name := m[1]
		if knownVars[name] {
			continue
		}
		if hashNRe.MatchString(name) {
			continue
		}
		return nil, fmt.Errorf("unknown path variable {%s}", name)
	}
	if !strings.Contains(tmpl, "{fid}") {
		return nil, fmt.Errorf("path template must include {fid}")
	}
	return &PathTemplate{raw: tmpl}, nil
}

func FidPathSafe(fid string) string {
	return strings.ReplaceAll(fid, ",", "_")
}

func hashMod(s string, mod uint64) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	if mod == 0 {
		return 0
	}
	return h.Sum64() % mod
}

func (p *PathTemplate) Render(ctx PathContext) (string, error) {
	tz := ctx.TimeZone
	if tz == nil {
		tz = time.Local
	}
	t := ctx.Modified
	if t.IsZero() {
		t = time.Now().In(tz)
	} else {
		t = t.In(tz)
	}
	fidSafe := ctx.FidPathSafe
	if fidSafe == "" {
		fidSafe = FidPathSafe(ctx.Fid)
	}

	out := tokenRe.ReplaceAllStringFunc(p.raw, func(tok string) string {
		name := tok[1 : len(tok)-1]
		switch name {
		case "fid":
			return fidSafe
		case "volume_id":
			return strconv.FormatUint(uint64(ctx.VolumeID), 10)
		case "collection":
			return ctx.Collection
		case "yyyy":
			return t.Format("2006")
		case "yyyyMM":
			return t.Format("200601")
		case "yyyyMMdd":
			return t.Format("20060102")
		case "HH":
			return t.Format("15")
		case "mm":
			return t.Format("04")
		case "hash12":
			return strconv.FormatUint(hashMod(ctx.Fid, 12), 10)
		default:
			if m := hashNRe.FindStringSubmatch(name); m != nil {
				n, _ := strconv.ParseUint(m[1], 10, 64)
				return strconv.FormatUint(hashMod(ctx.Fid, n), 10)
			}
			return tok
		}
	})

	clean := path.Clean(out)
	if clean != out && strings.Contains(out, "..") {
		return "", fmt.Errorf("path escapes after clean: %s", out)
	}
	if !strings.HasPrefix(clean, "/") {
		return "", fmt.Errorf("rendered path is not absolute: %s", clean)
	}
	if path.Base(clean) != fidSafe {
		return "", fmt.Errorf("path basename %q must equal fid %q", path.Base(clean), fidSafe)
	}
	return clean, nil
}
