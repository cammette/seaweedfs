package migrate

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

type migrateJob struct {
	idxPos uint64
	rec    *NeedleRecord // nil = idx-only (advance cursor, no upload)
}

// Run executes a volume→filer migration job.
func Run(cfg Config) error {
	if err := cfg.Normalize(); err != nil {
		return err
	}
	if cfg.VerifyMode == VerifyCRC && !cfg.IsBlob() {
		return fmt.Errorf("verifyMode=crc compares needle checksums and requires -mode=blob")
	}

	var (
		tmpl *PathTemplate
		err  error
	)
	if !cfg.IsBlob() {
		tmpl, err = NewPathTemplate(cfg.PathTemplate)
		if err != nil {
			return err
		}
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

	var dst destIO
	if cfg.IsBlob() {
		dst = blobDest{NewBlobClient(cfg)}
	} else {
		dst = filerDest{NewFilerHTTP(cfg.FilerBases(), cfg.CollectionDst, cfg.Replication, cfg.TTL, cfg.Disk)}
	}

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
	var migratedLimit atomic.Int64

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

	for _, ref := range refs {
		vp, err := store.GetVolume(uint32(ref.VolumeID))
		if err != nil {
			return err
		}
		if vp.Status == "done" {
			doneVolumes++
			continue
		}
		if cfg.Limit > 0 && migratedLimit.Load() >= int64(cfg.Limit) {
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
		fmt.Fprintf(os.Stderr, "migrating volume %d (%d idx entries, cursor=%d, workers=%d, verify=%s)\n",
			ref.VolumeID, entries, vp.IdxCursor, cfg.Workers, cfg.VerifyMode)

		if err := migrateOneVolume(cfg, store, dst, tmpl, ref, &vp,
			&uploaded, &skippedN, &failed, &bytesUp, &scanned, &verified, &migratedLimit); err != nil {
			_ = store.SaveVolume(vp)
			return err
		}

		if cfg.Limit > 0 && migratedLimit.Load() >= int64(cfg.Limit) {
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

func migrateOneVolume(
	cfg Config,
	store *ProgressStore,
	dst destIO,
	tmpl *PathTemplate,
	ref VolumeRef,
	vp *VolumeProgress,
	uploaded, skippedN, failed, bytesUp, scanned, verified, migratedLimit *atomic.Int64,
) error {
	if err := dst.PrepareVolume(context.Background(), uint32(ref.VolumeID)); err != nil {
		return fmt.Errorf("prepare volume %d: %w", ref.VolumeID, err)
	}
	jobs := make(chan migrateJob, cfg.Workers*2)
	doneIdx := make(chan uint64, cfg.Workers*2)

	var (
		wg       sync.WaitGroup
		vpMu     sync.Mutex
		scanErr  error
		stopScan atomic.Bool
	)

	// Cursor committer: advance IdxCursor only over a contiguous completed prefix.
	commitDone := make(chan struct{})
	go func() {
		defer close(commitDone)
		next := vp.IdxCursor
		completed := map[uint64]struct{}{}
		sinceFlush := 0
		for idxPos := range doneIdx {
			completed[idxPos] = struct{}{}
			for {
				if _, ok := completed[next]; !ok {
					break
				}
				delete(completed, next)
				next++
				sinceFlush++
				vpMu.Lock()
				vp.IdxCursor = next
				vpMu.Unlock()
				if sinceFlush >= cfg.ProgressEvery {
					vpMu.Lock()
					_ = store.SaveVolume(*vp)
					vpMu.Unlock()
					sinceFlush = 0
				}
			}
		}
		vpMu.Lock()
		vp.IdxCursor = next
		_ = store.SaveVolume(*vp)
		vpMu.Unlock()
	}()

	process := func(job migrateJob) {
		defer func() { doneIdx <- job.idxPos }()
		scanned.Add(1)
		if job.rec == nil {
			return
		}
		if cfg.Limit > 0 && migratedLimit.Load() >= int64(cfg.Limit) {
			stopScan.Store(true)
			return
		}

		vid := uint32(ref.VolumeID)
		ok, err := store.HasSuccess(vid, job.rec.FidPathSafe)
		if err != nil {
			failed.Add(1)
			_ = store.MarkFailed(FailRecord{VID: vid, FID: job.rec.FidPathSafe, Stage: "state", Err: err.Error()})
			return
		}
		if ok {
			skippedN.Add(1)
			vpMu.Lock()
			vp.Skipped++
			vpMu.Unlock()
			return
		}

		var dest string
		if cfg.IsBlob() {
			dest = job.rec.Fid
		} else {
			var err error
			dest, err = tmpl.Render(PathContext{
				Fid:         job.rec.Fid,
				FidPathSafe: job.rec.FidPathSafe,
				VolumeID:    vid,
				Collection:  cfg.Collection,
				Modified:    job.rec.Modified,
				TimeZone:    cfg.TimeZone,
			})
			if err != nil {
				failed.Add(1)
				vpMu.Lock()
				vp.Failed++
				vpMu.Unlock()
				_ = store.MarkFailed(FailRecord{VID: vid, FID: job.rec.FidPathSafe, Stage: "path", Err: err.Error()})
				return
			}
		}

		if cfg.DryRun {
			fmt.Printf("DRY-RUN vid=%d fid=%s -> %s size=%d\n", ref.VolumeID, job.rec.FidPathSafe, dest, job.rec.DataSize)
			if err := store.MarkSuccess(vid, job.rec.FidPathSafe); err != nil {
				failed.Add(1)
				return
			}
			uploaded.Add(1)
			migratedLimit.Add(1)
			vpMu.Lock()
			vp.Uploaded++
			vp.LastFID = job.rec.FidPathSafe
			vpMu.Unlock()
			return
		}

		dig := Digest{}
		if cfg.VerifyMode == VerifyMD5 {
			dig = DigestOf(job.rec.Data)
		} else {
			dig = Digest{Size: job.rec.DataSize}
		}

		ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout(job.rec.DataSize))
		var exists bool
		var remoteSize int64
		var remoteETag string
		if cfg.VerifyMode == VerifyCRC {
			exists, remoteSize, remoteETag, err = dst.Head(ctx, dest)
		} else {
			exists, remoteSize, err = dst.Stat(ctx, dest)
		}
		cancel()
		if err != nil {
			glog.V(1).Infof("exists check %s: %v", dest, err)
		} else if exists {
			skipOK := true
			if cfg.VerifyMode.AtLeast(VerifySize) && remoteSize >= 0 && remoteSize != dig.Size {
				skipOK = false
			}
			if skipOK && cfg.VerifyMode == VerifyCRC {
				want := NeedleCRCHex(job.rec.Checksum)
				if remoteETag == "" || remoteETag != want {
					skipOK = false
				}
			}
			if skipOK && cfg.VerifyMode == VerifyMD5 {
				if !dig.Has {
					dig = DigestOf(job.rec.Data)
				}
				ctx, cancel = context.WithTimeout(context.Background(), uploadTimeout(job.rec.DataSize))
				body, derr := dst.Get(ctx, dest)
				cancel()
				if derr != nil || DigestOf(body).MD5 != dig.MD5 {
					skipOK = false
				}
			}
			if skipOK {
				if err := store.MarkSuccess(vid, job.rec.FidPathSafe); err != nil {
					failed.Add(1)
					return
				}
				skippedN.Add(1)
				verified.Add(1)
				vpMu.Lock()
				vp.Skipped++
				vp.LastFID = job.rec.FidPathSafe
				vpMu.Unlock()
				return
			}
		}

		if cfg.VerifyMode == VerifyMD5 && !dig.Has {
			dig = DigestOf(job.rec.Data)
		}

		ctx, cancel = context.WithTimeout(context.Background(), uploadTimeout(job.rec.DataSize))
		err = dst.Put(ctx, dest, job.rec.Name, job.rec.Mime, job.rec.Data, dig, cfg.Retries)
		cancel()
		if err != nil {
			failed.Add(1)
			vpMu.Lock()
			vp.Failed++
			vpMu.Unlock()
			_ = store.MarkFailed(FailRecord{
				VID: vid, FID: job.rec.FidPathSafe, Path: dest, Stage: "upload",
				Err: err.Error(), Retry: cfg.Retries,
			})
			return
		}

		if cfg.VerifyMode.AtLeast(VerifyExists) {
			ctx, cancel = context.WithTimeout(context.Background(), uploadTimeout(job.rec.DataSize))
			var okExists bool
			var gotSize int64
			var gotETag string
			var verr error
			if cfg.VerifyMode == VerifyCRC {
				okExists, gotSize, gotETag, verr = dst.Head(ctx, dest)
			} else {
				okExists, gotSize, verr = dst.Stat(ctx, dest)
			}
			cancel()
			if verr != nil || !okExists {
				msg := fmt.Sprintf("verify exists failed exists=%v err=%v", okExists, verr)
				failed.Add(1)
				vpMu.Lock()
				vp.Failed++
				vpMu.Unlock()
				_ = store.MarkFailed(FailRecord{VID: vid, FID: job.rec.FidPathSafe, Path: dest, Stage: "verify", Err: msg})
				return
			}
			if cfg.VerifyMode.AtLeast(VerifySize) && gotSize >= 0 && gotSize != dig.Size {
				msg := fmt.Sprintf("verify size failed size=%d want=%d", gotSize, dig.Size)
				failed.Add(1)
				vpMu.Lock()
				vp.Failed++
				vpMu.Unlock()
				_ = store.MarkFailed(FailRecord{VID: vid, FID: job.rec.FidPathSafe, Path: dest, Stage: "verify", Err: msg})
				return
			}
			if cfg.VerifyMode == VerifyCRC {
				want := NeedleCRCHex(job.rec.Checksum)
				if gotETag == "" || gotETag != want {
					msg := FormatCRCMismatch(want, gotETag)
					failed.Add(1)
					vpMu.Lock()
					vp.Failed++
					vpMu.Unlock()
					_ = store.MarkFailed(FailRecord{VID: vid, FID: job.rec.FidPathSafe, Path: dest, Stage: "verify", Err: msg})
					return
				}
			}
			if cfg.VerifyMode == VerifyMD5 {
				ctx, cancel = context.WithTimeout(context.Background(), uploadTimeout(job.rec.DataSize))
				body, derr := dst.Get(ctx, dest)
				cancel()
				if derr != nil {
					failed.Add(1)
					vpMu.Lock()
					vp.Failed++
					vpMu.Unlock()
					_ = store.MarkFailed(FailRecord{VID: vid, FID: job.rec.FidPathSafe, Path: dest, Stage: "verify", Err: derr.Error()})
					return
				}
				dst := DigestOf(body)
				if dst.MD5 != dig.MD5 {
					failed.Add(1)
					vpMu.Lock()
					vp.Failed++
					vpMu.Unlock()
					_ = store.MarkFailed(FailRecord{
						VID: vid, FID: job.rec.FidPathSafe, Path: dest, Stage: "verify",
						Err: FormatMD5Mismatch(dig, dst),
					})
					return
				}
			}
			verified.Add(1)
		}

		if err := store.MarkSuccess(vid, job.rec.FidPathSafe); err != nil {
			failed.Add(1)
			return
		}
		uploaded.Add(1)
		bytesUp.Add(job.rec.DataSize)
		migratedLimit.Add(1)
		vpMu.Lock()
		vp.Uploaded++
		vp.Bytes += job.rec.DataSize
		vp.LastFID = job.rec.FidPathSafe
		vpMu.Unlock()
		glog.V(1).Infof("migrated %s -> %s (%d bytes)", job.rec.FidPathSafe, dest, job.rec.DataSize)
	}

	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				process(job)
			}
		}()
	}

	err := ScanVolume(ref, vp.IdxCursor, cfg.Newer, cfg.HasNewer, cfg.Decompress,
		func(idxPos uint64, rec *NeedleRecord) error {
			if stopScan.Load() {
				return ioEOF
			}
			if cfg.Limit > 0 && migratedLimit.Load() >= int64(cfg.Limit) {
				stopScan.Store(true)
				return ioEOF
			}
			var jobRec *NeedleRecord
			if rec != nil {
				cp := *rec
				cp.Data = append([]byte(nil), rec.Data...)
				jobRec = &cp
			}
			jobs <- migrateJob{idxPos: idxPos, rec: jobRec}
			return nil
		})
	if err != nil && err != ioEOF {
		scanErr = err
	}
	close(jobs)
	wg.Wait()
	close(doneIdx)
	<-commitDone

	if scanErr != nil {
		return fmt.Errorf("scan volume %d: %w", ref.VolumeID, scanErr)
	}
	return nil
}

var ioEOF = fmt.Errorf("migrate limit reached")

func uploadTimeout(size int64) time.Duration {
	d := 60*time.Second + time.Duration(size/1_000_000)*time.Second
	if d < 60*time.Second {
		d = 60 * time.Second
	}
	if d > 30*time.Minute {
		d = 30 * time.Minute
	}
	return d
}
