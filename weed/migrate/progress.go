package migrate

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const jobVersion = 1

type JobMeta struct {
	Version   int               `json:"version"`
	CreatedAt string            `json:"created_at"`
	UpdatedAt string            `json:"updated_at"`
	Config    map[string]string `json:"config"`
	Stats     JobStats          `json:"stats"`
}

type JobStats struct {
	Success int64 `json:"success"`
	Skip    int64 `json:"skip"`
	Fail    int64 `json:"fail"`
	Bytes   int64 `json:"bytes"`
}

type VolumeProgress struct {
	VolumeID   uint32 `json:"volume_id"`
	Status     string `json:"status"` // pending|running|done
	IdxCursor  uint64 `json:"idx_cursor"`
	IdxEntries uint64 `json:"idx_entries"`
	Uploaded   int64  `json:"uploaded"`
	Skipped    int64  `json:"skipped"`
	Failed     int64  `json:"failed"`
	Bytes      int64  `json:"bytes"`
	LastFID    string `json:"last_fid,omitempty"`
	UpdatedAt  string `json:"updated_at"`
}

type FailRecord struct {
	TS    string `json:"ts"`
	VID   uint32 `json:"vid"`
	FID   string `json:"fid"`
	Path  string `json:"path"`
	Stage string `json:"stage"`
	Err   string `json:"err"`
	Retry int    `json:"retries"`
}

// ProgressStore persists migration checkpoints under stateDir.
type ProgressStore struct {
	dir string
	mu  sync.Mutex

	job      JobMeta
	success  map[uint32]map[string]struct{}
	successF map[uint32]*os.File
}

func OpenProgressStore(stateDir string) (*ProgressStore, error) {
	if err := os.MkdirAll(filepath.Join(stateDir, "volumes"), 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(stateDir, "success"), 0755); err != nil {
		return nil, err
	}
	ps := &ProgressStore{
		dir:      stateDir,
		success:  map[uint32]map[string]struct{}{},
		successF: map[uint32]*os.File{},
	}
	return ps, nil
}

func (ps *ProgressStore) Close() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	var first error
	for vid, f := range ps.successF {
		if err := f.Close(); err != nil && first == nil {
			first = err
		}
		delete(ps.successF, vid)
	}
	return first
}

func (ps *ProgressStore) jobPath() string { return filepath.Join(ps.dir, "job.json") }
func (ps *ProgressStore) volumePath(vid uint32) string {
	return filepath.Join(ps.dir, "volumes", fmt.Sprintf("%d.json", vid))
}
func (ps *ProgressStore) successPath(vid uint32) string {
	return filepath.Join(ps.dir, "success", fmt.Sprintf("%d.log", vid))
}
func (ps *ProgressStore) failedPath() string { return filepath.Join(ps.dir, "failed.ndjson") }
func (ps *ProgressStore) metricsPath() string {
	return filepath.Join(ps.dir, "metrics.json")
}
func (ps *ProgressStore) skippedPath() string {
	return filepath.Join(ps.dir, "SKIPPED_VOLUMES.log")
}

func configFingerprint(cfg Config) map[string]string {
	mode := cfg.Mode
	if mode == "" {
		mode = "filer"
	}
	return map[string]string{
		"dir":           cfg.Dir,
		"collection":    cfg.Collection,
		"filer":         cfg.Filer,
		"path_template": cfg.PathTemplate,
		"mode":          mode,
		"masters":       cfg.Masters,
	}
}

func (ps *ProgressStore) LoadOrInit(cfg Config) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	fp := configFingerprint(cfg)
	now := time.Now().Format(time.RFC3339)

	data, err := os.ReadFile(ps.jobPath())
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		ps.job = JobMeta{
			Version:   jobVersion,
			CreatedAt: now,
			UpdatedAt: now,
			Config:    fp,
		}
		return ps.writeJSONLocked(ps.jobPath(), ps.job)
	}

	if err := json.Unmarshal(data, &ps.job); err != nil {
		return fmt.Errorf("parse job.json: %w", err)
	}
	if !cfg.ForceConfig {
		keys := []string{"dir", "collection"}
		if fp["mode"] == "blob" || ps.job.Config["mode"] == "blob" {
			keys = append(keys, "mode", "masters")
		} else {
			keys = append(keys, "filer", "path_template")
		}
		for _, k := range keys {
			stored := ps.job.Config[k]
			if k == "mode" && stored == "" {
				stored = "filer"
			}
			if stored != fp[k] {
				return fmt.Errorf("stateDir config mismatch on %s: stored=%q current=%q (use -force to override)",
					k, stored, fp[k])
			}
		}
	} else {
		ps.job.Config = fp
	}
	ps.job.UpdatedAt = now
	return ps.writeJSONLocked(ps.jobPath(), ps.job)
}

func (ps *ProgressStore) AddStats(delta JobStats) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.job.Stats.Success += delta.Success
	ps.job.Stats.Skip += delta.Skip
	ps.job.Stats.Fail += delta.Fail
	ps.job.Stats.Bytes += delta.Bytes
	ps.job.UpdatedAt = time.Now().Format(time.RFC3339)
	return ps.writeJSONLocked(ps.jobPath(), ps.job)
}

func (ps *ProgressStore) GetVolume(vid uint32) (VolumeProgress, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	data, err := os.ReadFile(ps.volumePath(vid))
	if err != nil {
		if os.IsNotExist(err) {
			return VolumeProgress{VolumeID: vid, Status: "pending"}, nil
		}
		return VolumeProgress{}, err
	}
	var vp VolumeProgress
	if err := json.Unmarshal(data, &vp); err != nil {
		return VolumeProgress{}, err
	}
	return vp, nil
}

func (ps *ProgressStore) SaveVolume(vp VolumeProgress) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	vp.UpdatedAt = time.Now().Format(time.RFC3339)
	return ps.writeJSONLocked(ps.volumePath(vp.VolumeID), vp)
}

func (ps *ProgressStore) ensureSuccessLoaded(vid uint32) error {
	if _, ok := ps.success[vid]; ok {
		return nil
	}
	set := map[string]struct{}{}
	path := ps.successPath(vid)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" {
			set[line] = struct{}{}
		}
	}
	if err := sc.Err(); err != nil {
		_ = f.Close()
		return err
	}
	// seek to end for append
	if _, err := f.Seek(0, 2); err != nil {
		_ = f.Close()
		return err
	}
	ps.success[vid] = set
	ps.successF[vid] = f
	return nil
}

func (ps *ProgressStore) HasSuccess(vid uint32, fidSafe string) (bool, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := ps.ensureSuccessLoaded(vid); err != nil {
		return false, err
	}
	_, ok := ps.success[vid][fidSafe]
	return ok, nil
}

func (ps *ProgressStore) MarkSuccess(vid uint32, fidSafe string) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := ps.ensureSuccessLoaded(vid); err != nil {
		return err
	}
	if _, ok := ps.success[vid][fidSafe]; ok {
		return nil
	}
	if _, err := ps.successF[vid].WriteString(fidSafe + "\n"); err != nil {
		return err
	}
	ps.success[vid][fidSafe] = struct{}{}
	return nil
}

// LoadSuccessFIDs reads success/{vid}.log without opening a write handle.
func LoadSuccessFIDs(stateDir string, vid uint32) (map[string]struct{}, error) {
	path := filepath.Join(stateDir, "success", fmt.Sprintf("%d.log", vid))
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]struct{}{}, nil
		}
		return nil, err
	}
	defer f.Close()
	set := map[string]struct{}{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" {
			set[line] = struct{}{}
		}
	}
	return set, sc.Err()
}

func (ps *ProgressStore) SuccessCount(vid uint32) (int64, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := ps.ensureSuccessLoaded(vid); err != nil {
		return 0, err
	}
	return int64(len(ps.success[vid])), nil
}

func (ps *ProgressStore) MarkFailed(rec FailRecord) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if rec.TS == "" {
		rec.TS = time.Now().Format(time.RFC3339)
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(ps.failedPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(b, '\n'))
	return err
}

func (ps *ProgressStore) WriteSkipped(lines []string) error {
	if len(lines) == 0 {
		return nil
	}
	f, err := os.OpenFile(ps.skippedPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, line := range lines {
		if _, err := fmt.Fprintf(f, "%s\t%s\n", time.Now().Format(time.RFC3339), line); err != nil {
			return err
		}
	}
	return nil
}

func (ps *ProgressStore) WriteMetrics(m MetricsSnapshot) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	m.UpdatedAt = time.Now().Format(time.RFC3339)
	return ps.writeJSONLocked(ps.metricsPath(), m)
}

func (ps *ProgressStore) writeJSONLocked(path string, v interface{}) error {
	tmp := path + ".tmp"
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// MetricsSnapshot is flushed to metrics.json for monitoring.
type MetricsSnapshot struct {
	UpdatedAt      string   `json:"updated_at"`
	Phase          string   `json:"phase"`
	VolumesTotal   int      `json:"volumes_total"`
	VolumesDone    int      `json:"volumes_done"`
	VolumesRunning []uint32 `json:"volumes_running"`
	FilesScanned   int64    `json:"files_scanned"`
	FilesUploaded  int64    `json:"files_uploaded"`
	FilesSkipped   int64    `json:"files_skipped"`
	FilesFailed    int64    `json:"files_failed"`
	FilesVerified  int64    `json:"files_verified"`
	BytesUploaded  int64    `json:"bytes_uploaded"`
	BytesPerSec    float64  `json:"bytes_per_sec"`
	FilesPerSec    float64  `json:"files_per_sec"`
}

// StatusReport is produced by migrate.volume.status.
type StatusReport struct {
	StateDir          string            `json:"state_dir"`
	Config            map[string]string `json:"config,omitempty"`
	VolumesTotal      int               `json:"volumes_total"`
	VolumesDone       int               `json:"volumes_done"`
	VolumesRunning    int               `json:"volumes_running"`
	VolumesPending    int               `json:"volumes_pending"`
	FIDsMigratedTotal int64             `json:"fids_migrated_total"`
	Failed            int64             `json:"failed"`
	Volumes           []VolumeStatusRow `json:"volumes"`
}

type VolumeStatusRow struct {
	VolumeID     uint32 `json:"volume_id"`
	Status       string `json:"status"`
	FIDsMigrated int64  `json:"fids_migrated"`
	IdxCursor    uint64 `json:"idx_cursor"`
	IdxEntries   uint64 `json:"idx_entries"`
}

// ReadStatus builds a status report from stateDir (read-only).
func ReadStatus(stateDir string) (*StatusReport, error) {
	jobPath := filepath.Join(stateDir, "job.json")
	jobData, err := os.ReadFile(jobPath)
	if err != nil {
		return nil, fmt.Errorf("read job.json: %w", err)
	}
	var job JobMeta
	if err := json.Unmarshal(jobData, &job); err != nil {
		return nil, err
	}

	volDir := filepath.Join(stateDir, "volumes")
	entries, err := os.ReadDir(volDir)
	if err != nil {
		return nil, fmt.Errorf("read volumes/: %w", err)
	}

	rep := &StatusReport{
		StateDir: stateDir,
		Config:   job.Config,
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(volDir, e.Name()))
		if err != nil {
			return nil, err
		}
		var vp VolumeProgress
		if err := json.Unmarshal(data, &vp); err != nil {
			return nil, err
		}
		fidCount := countLines(filepath.Join(stateDir, "success", fmt.Sprintf("%d.log", vp.VolumeID)))
		row := VolumeStatusRow{
			VolumeID:     vp.VolumeID,
			Status:       vp.Status,
			FIDsMigrated: fidCount,
			IdxCursor:    vp.IdxCursor,
			IdxEntries:   vp.IdxEntries,
		}
		rep.Volumes = append(rep.Volumes, row)
		rep.VolumesTotal++
		rep.FIDsMigratedTotal += fidCount
		switch vp.Status {
		case "done":
			rep.VolumesDone++
		case "running":
			rep.VolumesRunning++
		default:
			rep.VolumesPending++
		}
	}

	rep.Failed = countLines(filepath.Join(stateDir, "failed.ndjson"))
	return rep, nil
}

func countLines(path string) int64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	var n int64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) != "" {
			n++
		}
	}
	return n
}

func FormatStatusText(rep *StatusReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "StateDir: %s\n", rep.StateDir)
	if rep.Config != nil {
		mode := rep.Config["mode"]
		if mode == "" {
			mode = "filer"
		}
		fmt.Fprintf(&b, "Mode: %s\n", mode)
		fmt.Fprintf(&b, "Collection: %s\n", rep.Config["collection"])
		if mode == "blob" {
			fmt.Fprintf(&b, "Masters: %s\n", rep.Config["masters"])
		} else {
			fmt.Fprintf(&b, "Filer: %s\n", rep.Config["filer"])
			fmt.Fprintf(&b, "Path: %s\n", rep.Config["path_template"])
		}
	}
	fmt.Fprintf(&b, "\nVolumes: %d done / %d running / %d pending  (total %d)\n",
		rep.VolumesDone, rep.VolumesRunning, rep.VolumesPending, rep.VolumesTotal)
	fmt.Fprintf(&b, "FIDs migrated (success): %d\n", rep.FIDsMigratedTotal)
	fmt.Fprintf(&b, "Failed records: %d\n", rep.Failed)
	fmt.Fprintf(&b, "\nPer volume:\n")
	for _, v := range rep.Volumes {
		fmt.Fprintf(&b, "  vid=%-6d status=%-8s fids=%-8d idx_cursor=%d/%d\n",
			v.VolumeID, v.Status, v.FIDsMigrated, v.IdxCursor, v.IdxEntries)
	}
	return b.String()
}
