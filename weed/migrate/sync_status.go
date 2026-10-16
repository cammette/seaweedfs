package migrate

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// SyncStatusReport is the monitor view of a sync state directory.
type SyncStatusReport struct {
	StateDir      string            `json:"state_dir"`
	Volumes       int               `json:"volumes"`
	Running       int               `json:"running"`
	Idle          int               `json:"idle"`
	Error         int               `json:"error"`
	Files         int64             `json:"files"`
	Synced        int64             `json:"synced"`
	Failed        int64             `json:"failed"`
	MaxLagSeconds int64             `json:"max_lag_seconds"`
	VolumeRows    []VolumeSyncState `json:"volume_rows"`
}

// ReadSyncStatus loads per-volume records written by sync.volume.
func ReadSyncStatus(stateDir string) (*SyncStatusReport, error) {
	dir := filepath.Join(stateDir, "volumes")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return &SyncStatusReport{StateDir: stateDir}, nil
		}
		return nil, err
	}
	rep := &SyncStatusReport{StateDir: stateDir}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		var st VolumeSyncState
		if err := json.Unmarshal(data, &st); err != nil {
			return nil, fmt.Errorf("parse %s: %w", e.Name(), err)
		}
		rep.VolumeRows = append(rep.VolumeRows, st)
	}
	sort.Slice(rep.VolumeRows, func(i, j int) bool { return rep.VolumeRows[i].VolumeID < rep.VolumeRows[j].VolumeID })
	for _, st := range rep.VolumeRows {
		rep.Volumes++
		rep.Files += st.Files
		rep.Synced += st.Synced
		rep.Failed += st.Failed
		if st.LagSeconds > rep.MaxLagSeconds {
			rep.MaxLagSeconds = st.LagSeconds
		}
		switch st.Status {
		case "running":
			rep.Running++
		case "error":
			rep.Error++
		default:
			rep.Idle++
		}
	}
	return rep, nil
}

func FormatSyncStatus(rep *SyncStatusReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "StateDir: %s\n", rep.StateDir)
	fmt.Fprintf(&b, "Volumes: %d  running=%d idle=%d error=%d\n", rep.Volumes, rep.Running, rep.Idle, rep.Error)
	fmt.Fprintf(&b, "Files: %d  synced=%d  failed=%d  max_lag=%ds\n", rep.Files, rep.Synced, rep.Failed, rep.MaxLagSeconds)
	fmt.Fprintf(&b, "\nPer volume:\n")
	for _, st := range rep.VolumeRows {
		wm := "-"
		if st.CommittedUnix > 0 {
			wm = time.Unix(st.CommittedUnix, 0).Format(time.RFC3339)
		}
		fmt.Fprintf(&b, "  vid=%-6d status=%-7s files=%-8d synced=%-8d failed=%-6d lag=%ds watermark=%s last=%s\n",
			st.VolumeID, st.Status, st.Files, st.Synced, st.Failed, st.LagSeconds, wm, st.LastFID)
		if st.LastError != "" {
			fmt.Fprintf(&b, "    error: %s\n", st.LastError)
		}
	}
	return b.String()
}
