package migrate

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// SyncPoint is the per-volume resume index.
// CommittedUnix means every live needle with LastModified <= that unix second is synced.
// PartialUnix/PartialFIDs record an in-progress second newer than CommittedUnix.
// Untimed holds fids whose needles have no LastModified.
type SyncPoint struct {
	CommittedUnix int64
	PartialUnix   int64
	PartialFIDs   map[string]struct{}
	Untimed       map[string]struct{}
}

// LiveNeedle is the time index of one live file, without its body.
type LiveNeedle struct {
	Key     string
	Unix    int64
	HasTime bool
}

func (p SyncPoint) needs(n LiveNeedle) bool {
	if !n.HasTime {
		_, ok := p.Untimed[n.Key]
		return !ok
	}
	if n.Unix <= p.CommittedUnix {
		return false
	}
	if p.PartialUnix > p.CommittedUnix && n.Unix == p.PartialUnix {
		_, ok := p.PartialFIDs[n.Key]
		return !ok
	}
	return true
}

// Pending returns live needles that are not covered by the resume point.
func (p SyncPoint) Pending(live []LiveNeedle) []LiveNeedle {
	var out []LiveNeedle
	for _, n := range live {
		if p.needs(n) {
			out = append(out, n)
		}
	}
	return out
}

// Advance moves the resume point forward through seconds that fully succeeded.
// ok keys are fids synced in this pass. Seconds are only committed in time order;
// a failure in an earlier second holds the watermark even if a later second succeeded.
func (p SyncPoint) Advance(live []LiveNeedle, ok map[string]struct{}) SyncPoint {
	next := SyncPoint{
		CommittedUnix: p.CommittedUnix,
		PartialUnix:   p.PartialUnix,
		PartialFIDs:   cloneSet(p.PartialFIDs),
		Untimed:       cloneSet(p.Untimed),
	}
	for _, n := range live {
		if n.HasTime {
			continue
		}
		if _, done := ok[n.Key]; done {
			next.Untimed[n.Key] = struct{}{}
		}
	}

	seconds := map[int64][]string{}
	for _, n := range live {
		if !n.HasTime || n.Unix <= p.CommittedUnix {
			continue
		}
		seconds[n.Unix] = append(seconds[n.Unix], n.Key)
	}
	var times []int64
	for t := range seconds {
		times = append(times, t)
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })

	satisfied := func(unix int64, key string) bool {
		if p.PartialUnix == unix {
			if _, ok := p.PartialFIDs[key]; ok {
				return true
			}
		}
		_, done := ok[key]
		return done
	}

	next.PartialUnix = 0
	next.PartialFIDs = map[string]struct{}{}
	for _, unix := range times {
		keys := seconds[unix]
		all := true
		var done []string
		for _, key := range keys {
			if satisfied(unix, key) {
				done = append(done, key)
			} else {
				all = false
			}
		}
		if !all {
			if len(done) > 0 {
				next.PartialUnix = unix
				for _, key := range done {
					next.PartialFIDs[key] = struct{}{}
				}
			}
			break
		}
		next.CommittedUnix = unix
	}
	return next
}

// Lag returns how far the oldest unsynced timed needle is behind now.
// Untimed needles do not contribute. Zero means no timed needle is waiting.
func (p SyncPoint) Lag(live []LiveNeedle, now time.Time) time.Duration {
	var oldest int64
	for _, n := range p.Pending(live) {
		if !n.HasTime {
			continue
		}
		if oldest == 0 || n.Unix < oldest {
			oldest = n.Unix
		}
	}
	if oldest == 0 {
		return 0
	}
	d := now.Sub(time.Unix(oldest, 0))
	if d < 0 {
		return 0
	}
	return d
}

func cloneSet(in map[string]struct{}) map[string]struct{} {
	out := map[string]struct{}{}
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}

// VolumeSyncState is one volume's on-disk sync record.
type VolumeSyncState struct {
	VolumeID        uint32   `json:"volume_id"`
	Status          string   `json:"status"` // idle|running|error
	Files           int64    `json:"files"`
	Synced          int64    `json:"synced"`
	Failed          int64    `json:"failed"`
	Bytes           int64    `json:"bytes"`
	CommittedUnix   int64    `json:"committed_unix"`
	PartialUnix     int64    `json:"partial_unix,omitempty"`
	PartialFIDs     []string `json:"partial_fids,omitempty"`
	UntimedFIDs     []string `json:"untimed_fids,omitempty"`
	LagSeconds      int64    `json:"lag_seconds"`
	SourceMtimeNano int64    `json:"source_mtime_nano"`
	LastFID         string   `json:"last_fid,omitempty"`
	LastError       string   `json:"last_error,omitempty"`
	PassUploaded    int64    `json:"pass_uploaded"`
	PassBytes       int64    `json:"pass_bytes"`
	PassDurationMs  int64    `json:"pass_duration_ms"`
	Deleted         int64    `json:"deleted"`
	UpdatedAt       string   `json:"updated_at"`
}

func (s VolumeSyncState) Point() SyncPoint {
	p := SyncPoint{
		CommittedUnix: s.CommittedUnix,
		PartialUnix:   s.PartialUnix,
		PartialFIDs:   map[string]struct{}{},
		Untimed:       map[string]struct{}{},
	}
	for _, fid := range s.PartialFIDs {
		p.PartialFIDs[fid] = struct{}{}
	}
	for _, fid := range s.UntimedFIDs {
		p.Untimed[fid] = struct{}{}
	}
	return p
}

func (s *VolumeSyncState) SetPoint(p SyncPoint) {
	s.CommittedUnix = p.CommittedUnix
	s.PartialUnix = p.PartialUnix
	s.PartialFIDs = setKeys(p.PartialFIDs)
	s.UntimedFIDs = setKeys(p.Untimed)
}

func setKeys(m map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func syncVolumePath(stateDir string, vid uint32) string {
	return filepath.Join(stateDir, "volumes", fmt.Sprintf("%d.json", vid))
}

func LoadVolumeSync(stateDir string, vid uint32) (VolumeSyncState, error) {
	data, err := os.ReadFile(syncVolumePath(stateDir, vid))
	if err != nil {
		if os.IsNotExist(err) {
			return VolumeSyncState{VolumeID: vid, Status: "idle"}, nil
		}
		return VolumeSyncState{}, err
	}
	var st VolumeSyncState
	if err := json.Unmarshal(data, &st); err != nil {
		return VolumeSyncState{}, fmt.Errorf("parse %s: %w", syncVolumePath(stateDir, vid), err)
	}
	st.VolumeID = vid
	return st, nil
}

func SaveVolumeSync(stateDir string, st VolumeSyncState) error {
	if err := os.MkdirAll(filepath.Join(stateDir, "volumes"), 0755); err != nil {
		return err
	}
	st.UpdatedAt = time.Now().Format(time.RFC3339)
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	path := syncVolumePath(stateDir, st.VolumeID)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func appendSyncLog(stateDir, line string) error {
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(stateDir, "sync.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintln(f, line)
	return err
}

func deletedFIDPath(stateDir string, vid uint32) string {
	return filepath.Join(stateDir, "deleted", fmt.Sprintf("%d.log", vid))
}

// LoadDeletedFIDs returns fids already deleted on the target for this volume.
func LoadDeletedFIDs(stateDir string, vid uint32) (map[string]struct{}, error) {
	f, err := os.Open(deletedFIDPath(stateDir, vid))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]struct{}{}, nil
		}
		return nil, err
	}
	defer f.Close()
	out := map[string]struct{}{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" {
			out[line] = struct{}{}
		}
	}
	return out, sc.Err()
}

// MarkDeletedFID records one fid whose target delete has been applied or skipped.
func MarkDeletedFID(stateDir string, vid uint32, fid string) error {
	if err := os.MkdirAll(filepath.Join(stateDir, "deleted"), 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(deletedFIDPath(stateDir, vid), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintln(f, fid)
	return err
}
