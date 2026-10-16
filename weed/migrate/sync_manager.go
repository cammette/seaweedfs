package migrate

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// SyncConfig is the resident blob sync process.
type SyncConfig struct {
	Dir           string
	Collection    string
	VolumeIDs     []uint32
	StateDir      string
	Masters       string
	CollectionDst string
	Replication   string
	TTL           string
	Disk          string
	Interval      time.Duration
	Decompress    bool
	VerifyMode    VerifyLevel
	Retries       int
	Center        string
	CenterDst     string
	GrpcDial      grpc.DialOption
}

func (c *SyncConfig) Normalize() error {
	if c.Dir == "" {
		return fmt.Errorf("dir is required")
	}
	if c.StateDir == "" {
		return fmt.Errorf("stateDir is required")
	}
	if c.Masters == "" {
		return fmt.Errorf("masters is required")
	}
	base := Config{
		Dir:           c.Dir,
		Collection:    c.Collection,
		StateDir:      c.StateDir,
		Mode:          "blob",
		Masters:       c.Masters,
		CollectionDst: c.CollectionDst,
		Replication:   c.Replication,
		TTL:           c.TTL,
		Disk:          c.Disk,
		VerifyMode:    c.VerifyMode,
		Decompress:    c.Decompress,
		Retries:       c.Retries,
		GrpcDial:      c.GrpcDial,
	}
	if err := base.Normalize(); err != nil {
		return err
	}
	c.Masters = base.Masters
	c.Replication = base.Replication
	if c.Interval <= 0 {
		c.Interval = 10 * time.Second
	}
	if c.Retries <= 0 {
		c.Retries = 3
	}
	if err := validCenterID(c.Center); err != nil {
		return fmt.Errorf("center: %w", err)
	}
	if err := validCenterID(c.CenterDst); err != nil {
		return fmt.Errorf("centerDst: %w", err)
	}
	if c.VerifyMode == VerifyNone && base.VerifyMode != VerifyNone {
		c.VerifyMode = base.VerifyMode
	}
	return nil
}

// RunSync scans the volume directory until ctx is cancelled.
// Each volume gets at most one goroutine. A new pass starts when the volume
// file mtime is newer than the last successful pass, or the last pass had failures.
func RunSync(ctx context.Context, cfg SyncConfig) error {
	if err := cfg.Normalize(); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.StateDir, 0755); err != nil {
		return err
	}
	dst := blobDest{NewBlobClient(Config{
		Masters:       cfg.Masters,
		GrpcDial:      cfg.GrpcDial,
		Collection:    cfg.Collection,
		CollectionDst: cfg.CollectionDst,
		Replication:   cfg.Replication,
		TTL:           cfg.TTL,
		Disk:          cfg.Disk,
	})}

	var mu sync.Mutex
	running := map[uint32]struct{}{}
	var wg sync.WaitGroup

	scan := func() {
		refs, skipped, err := DiscoverVolumes(cfg.Dir, cfg.Collection, cfg.VolumeIDs)
		if err != nil {
			glog.Errorf("sync scan %s: %v", cfg.Dir, err)
			return
		}
		for _, s := range skipped {
			glog.V(1).Infof("sync skip %s", s)
		}
		for _, ref := range refs {
			select {
			case <-ctx.Done():
				return
			default:
			}
			vid := uint32(ref.VolumeID)
			mtime, err := volumeSourceMTime(ref)
			if err != nil {
				glog.V(1).Infof("sync stat vid=%d: %v", vid, err)
				continue
			}
			st, err := LoadVolumeSync(cfg.StateDir, vid)
			if err != nil {
				glog.Errorf("sync load vid=%d: %v", vid, err)
				continue
			}
			if !needsVolumePass(st, mtime) {
				continue
			}
			mu.Lock()
			if _, busy := running[vid]; busy {
				mu.Unlock()
				continue
			}
			running[vid] = struct{}{}
			mu.Unlock()

			wg.Add(1)
			go func(ref VolumeRef, vid uint32) {
				defer wg.Done()
				defer func() {
					mu.Lock()
					delete(running, vid)
					mu.Unlock()
				}()
				fmt.Fprintf(os.Stderr, "sync start volume %d\n", vid)
				for {
					if ctx.Err() != nil {
						return
					}
					before, statErr := volumeSourceMTime(ref)
					if statErr != nil {
						glog.Errorf("sync stat vid=%d: %v", vid, statErr)
						return
					}
					st, passErr := syncVolumePass(ctx, cfg, ref, dst)
					if ctx.Err() != nil {
						return
					}
					after, statErr := volumeSourceMTime(ref)
					if statErr != nil {
						glog.Errorf("sync stat vid=%d: %v", vid, statErr)
						return
					}
					if passErr != nil {
						fmt.Fprintf(os.Stderr, "sync volume %d error: %v\n", vid, passErr)
						return
					}
					if st.Failed > 0 {
						fmt.Fprintf(os.Stderr, "sync volume %d failed=%d lag=%ds\n", vid, st.Failed, st.LagSeconds)
						return
					}
					if err := markSourceCaughtUp(cfg.StateDir, vid, before); err != nil {
						glog.Errorf("sync save vid=%d: %v", vid, err)
						return
					}
					if !after.After(before) {
						fmt.Fprintf(os.Stderr, "sync volume %d idle files=%d synced=%d lag=%ds\n",
							vid, st.Files, st.Synced, st.LagSeconds)
						return
					}
					fmt.Fprintf(os.Stderr, "sync volume %d source changed, running another pass\n", vid)
				}
			}(ref, vid)
		}
	}

	scan()
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return nil
		case <-ticker.C:
			scan()
		}
	}
}

func needsVolumePass(st VolumeSyncState, mtime time.Time) bool {
	if st.Failed > 0 || st.Status == "error" || st.Status == "running" {
		return true
	}
	return mtime.UnixNano() > st.SourceMtimeNano
}

func markSourceCaughtUp(stateDir string, vid uint32, mtime time.Time) error {
	st, err := LoadVolumeSync(stateDir, vid)
	if err != nil {
		return err
	}
	if mtime.UnixNano() > st.SourceMtimeNano {
		st.SourceMtimeNano = mtime.UnixNano()
	}
	st.Status = "idle"
	return SaveVolumeSync(stateDir, st)
}
