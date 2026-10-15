package migrate

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// Run executes a volume→filer migration job (P0: serial volumes).
func Run(cfg Config) error {
	if err := cfg.Normalize(); err != nil {
		return err
	}

	tmpl, err := NewPathTemplate(cfg.PathTemplate)
	if err != nil {
		return err
	}

	store, err := OpenProgressStore(cfg.StateDir)
	if err != nil {
		return err
	}
	defer store.Close()

	if err := store.LoadOrInit(cfg); err != nil {
		return err
	}

	refs, skipped, err := DiscoverVolumes(cfg.Dir, cfg.Collection, cfg.VolumeIDs)
	if err != nil {
		return err
	}
	if err := store.WriteSkipped(skipped); err != nil {
		glog.Warningf("write skipped volumes: %v", err)
	}
	for _, s := range skipped {
		fmt.Fprintf(os.Stderr, "skip volume: %s\n", s)
	}
	if len(refs) == 0 {
		return fmt.Errorf("no volumes found under %s collection=%q", cfg.Dir, cfg.Collection)
	}

	filer := NewFilerHTTP(cfg.FilerBases(), cfg.CollectionDst, cfg.Replication, cfg.TTL, cfg.Disk)

	var (
		uploaded atomic.Int64
		skippedN atomic.Int64
		failed   atomic.Int64
		bytesUp  atomic.Int64
		scanned  atomic.Int64
		verified atomic.Int64
	)
	start := time.Now()
	doneVolumes := 0

	flushMetrics := func(phase string, running []uint32) {
		elapsed := time.Since(start).Seconds()
		if elapsed < 0.001 {
			elapsed = 0.001
		}
		_ = store.WriteMetrics(MetricsSnapshot{
			Phase:          phase,
			VolumesTotal:   len(refs),
			VolumesDone:    doneVolumes,
			VolumesRunning: running,
			FilesScanned:   scanned.Load(),
			FilesUploaded:  uploaded.Load(),
			FilesSkipped:   skippedN.Load(),
			FilesFailed:    failed.Load(),
			FilesVerified:  verified.Load(),
			BytesUploaded:  bytesUp.Load(),
			BytesPerSec:    float64(bytesUp.Load()) / elapsed,
			FilesPerSec:    float64(uploaded.Load()) / elapsed,
		})
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	stopPrint := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				flushMetrics("migrate", nil)
				fmt.Fprintf(os.Stderr, "progress uploaded=%d skipped=%d failed=%d bytes=%d Bps=%.0f\n",
					uploaded.Load(), skippedN.Load(), failed.Load(), bytesUp.Load(),
					float64(bytesUp.Load())/time.Since(start).Seconds())
			case <-stopPrint:
				return
			}
		}
	}()
	defer close(stopPrint)

	var migratedLimit int
	for _, ref := range refs {
		vp, err := store.GetVolume(uint32(ref.VolumeID))
		if err != nil {
			return err
		}
		if vp.Status == "done" {
			doneVolumes++
			continue
		}
		if cfg.Limit > 0 && migratedLimit >= cfg.Limit {
			break
		}

		entries, err := IdxEntryCount(ref.IdxPath())
		if err != nil {
			return err
		}
		vp.VolumeID = uint32(ref.VolumeID)
		vp.Status = "running"
		vp.IdxEntries = entries
		if err := store.SaveVolume(vp); err != nil {
			return err
		}
		flushMetrics("migrate", []uint32{uint32(ref.VolumeID)})
		fmt.Fprintf(os.Stderr, "migrating volume %d (%d idx entries, cursor=%d)\n",
			ref.VolumeID, entries, vp.IdxCursor)

		processedSinceFlush := 0
		err = ScanVolume(ref, vp.IdxCursor, cfg.Newer, cfg.HasNewer, cfg.Decompress,
			func(idxPos uint64, rec *NeedleRecord) error {
				scanned.Add(1)
				vp.IdxCursor = idxPos + 1
				processedSinceFlush++

				defer func() {
					if processedSinceFlush >= cfg.ProgressEvery {
						_ = store.SaveVolume(vp)
						processedSinceFlush = 0
					}
				}()

				if rec == nil {
					return nil
				}
				if cfg.Limit > 0 && migratedLimit >= cfg.Limit {
					return ioEOF
				}

				ok, err := store.HasSuccess(uint32(ref.VolumeID), rec.FidPathSafe)
				if err != nil {
					return err
				}
				if ok {
					skippedN.Add(1)
					vp.Skipped++
					return nil
				}

				dest, err := tmpl.Render(PathContext{
					Fid:         rec.Fid,
					FidPathSafe: rec.FidPathSafe,
					VolumeID:    uint32(ref.VolumeID),
					Collection:  cfg.Collection,
					Modified:    rec.Modified,
					TimeZone:    cfg.TimeZone,
				})
				if err != nil {
					failed.Add(1)
					vp.Failed++
					_ = store.MarkFailed(FailRecord{
						VID: uint32(ref.VolumeID), FID: rec.FidPathSafe, Path: "", Stage: "path", Err: err.Error(),
					})
					return nil
				}

				if cfg.DryRun {
					fmt.Printf("DRY-RUN vid=%d fid=%s -> %s size=%d\n", ref.VolumeID, rec.FidPathSafe, dest, rec.DataSize)
					if err := store.MarkSuccess(uint32(ref.VolumeID), rec.FidPathSafe); err != nil {
						return err
					}
					uploaded.Add(1)
					migratedLimit++
					vp.Uploaded++
					vp.LastFID = rec.FidPathSafe
					return nil
				}

				ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout(rec.DataSize))
				exists, remoteSize, err := filer.Exists(ctx, dest)
				cancel()
				if err != nil {
					glog.V(1).Infof("exists check %s: %v", dest, err)
				} else if exists {
					if !cfg.VerifyMeta || remoteSize < 0 || remoteSize == rec.DataSize {
						if err := store.MarkSuccess(uint32(ref.VolumeID), rec.FidPathSafe); err != nil {
							return err
						}
						skippedN.Add(1)
						verified.Add(1)
						vp.Skipped++
						vp.LastFID = rec.FidPathSafe
						return nil
					}
					// size mismatch: re-upload
				}

				ctx, cancel = context.WithTimeout(context.Background(), uploadTimeout(rec.DataSize))
				err = filer.UploadWithRetry(ctx, dest, rec.Data, rec.Mime, cfg.Retries)
				cancel()
				if err != nil {
					failed.Add(1)
					vp.Failed++
					_ = store.MarkFailed(FailRecord{
						VID: uint32(ref.VolumeID), FID: rec.FidPathSafe, Path: dest, Stage: "upload",
						Err: err.Error(), Retry: cfg.Retries,
					})
					return nil
				}

				if cfg.VerifyMeta {
					ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
					okExists, gotSize, verr := filer.Exists(ctx, dest)
					cancel()
					if verr != nil || !okExists || (gotSize >= 0 && gotSize != rec.DataSize) {
						msg := fmt.Sprintf("verify meta failed exists=%v size=%d want=%d err=%v", okExists, gotSize, rec.DataSize, verr)
						failed.Add(1)
						vp.Failed++
						_ = store.MarkFailed(FailRecord{
							VID: uint32(ref.VolumeID), FID: rec.FidPathSafe, Path: dest, Stage: "verify", Err: msg,
						})
						return nil
					}
					verified.Add(1)
				}

				if err := store.MarkSuccess(uint32(ref.VolumeID), rec.FidPathSafe); err != nil {
					return err
				}
				uploaded.Add(1)
				bytesUp.Add(rec.DataSize)
				migratedLimit++
				vp.Uploaded++
				vp.Bytes += rec.DataSize
				vp.LastFID = rec.FidPathSafe
				glog.V(1).Infof("migrated %s -> %s (%d bytes)", rec.FidPathSafe, dest, rec.DataSize)
				return nil
			})

		if err != nil && err != ioEOF {
			_ = store.SaveVolume(vp)
			return fmt.Errorf("scan volume %d: %w", ref.VolumeID, err)
		}

		if cfg.Limit > 0 && migratedLimit >= cfg.Limit {
			_ = store.SaveVolume(vp)
			fmt.Fprintf(os.Stderr, "reached -limit=%d, stopping\n", cfg.Limit)
			break
		}

		vp.Status = "done"
		if err := store.SaveVolume(vp); err != nil {
			return err
		}
		doneVolumes++
		fmt.Fprintf(os.Stderr, "volume %d done uploaded=%d skipped=%d failed=%d\n",
			ref.VolumeID, vp.Uploaded, vp.Skipped, vp.Failed)
	}

	_ = store.AddStats(JobStats{
		Success: uploaded.Load(),
		Skip:    skippedN.Load(),
		Fail:    failed.Load(),
		Bytes:   bytesUp.Load(),
	})
	flushMetrics("done", nil)

	fmt.Fprintf(os.Stderr, "finished volumes_done=%d/%d uploaded=%d skipped=%d failed=%d bytes=%d\n",
		doneVolumes, len(refs), uploaded.Load(), skippedN.Load(), failed.Load(), bytesUp.Load())

	if failed.Load() > 0 {
		return fmt.Errorf("completed with %d failures (see %s/failed.ndjson)", failed.Load(), cfg.StateDir)
	}
	return nil
}

var ioEOF = fmt.Errorf("migrate limit reached")

func uploadTimeout(size int64) time.Duration {
	// 60s base + ~1s per MB
	d := 60*time.Second + time.Duration(size/1_000_000)*time.Second
	if d < 60*time.Second {
		d = 60 * time.Second
	}
	if d > 30*time.Minute {
		d = 30 * time.Minute
	}
	return d
}
