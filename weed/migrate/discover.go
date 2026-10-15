package migrate

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// VolumeRef identifies one volume data/index pair under a directory.
type VolumeRef struct {
	Dir        string
	Collection string
	VolumeID   needle.VolumeId
	BasePath   string // without extension
}

func (v VolumeRef) DatPath() string { return v.BasePath + ".dat" }
func (v VolumeRef) IdxPath() string { return v.BasePath + ".idx" }

// DiscoverVolumes lists volumes that have both .dat and .idx.
func DiscoverVolumes(dir, collection string, allowlist []uint32) ([]VolumeRef, []string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("read dir %s: %w", dir, err)
	}

	allow := map[uint32]bool{}
	for _, id := range allowlist {
		allow[id] = true
	}

	var pattern *regexp.Regexp
	if collection == "" {
		pattern = regexp.MustCompile(`^(\d+)\.dat$`)
	} else {
		pattern = regexp.MustCompile("^" + regexp.QuoteMeta(collection) + `_(\d+)\.dat$`)
	}

	var refs []VolumeRef
	var skipped []string
	seen := map[uint32]bool{}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := pattern.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		id64, err := strconv.ParseUint(m[1], 10, 32)
		if err != nil {
			skipped = append(skipped, e.Name()+": bad volume id")
			continue
		}
		vid := uint32(id64)
		if len(allow) > 0 && !allow[vid] {
			continue
		}
		if seen[vid] {
			continue
		}
		seen[vid] = true

		base := storage.VolumeFileName(dir, collection, int(vid))
		ref := VolumeRef{
			Dir:        dir,
			Collection: collection,
			VolumeID:   needle.VolumeId(vid),
			BasePath:   base,
		}
		if _, err := os.Stat(ref.DatPath()); err != nil {
			skipped = append(skipped, fmt.Sprintf("%s: missing .dat", filepath.Base(base)))
			continue
		}
		if _, err := os.Stat(ref.IdxPath()); err != nil {
			skipped = append(skipped, fmt.Sprintf("%s: missing .idx", filepath.Base(base)))
			continue
		}
		refs = append(refs, ref)
	}

	sort.Slice(refs, func(i, j int) bool { return refs[i].VolumeID < refs[j].VolumeID })
	return refs, skipped, nil
}
