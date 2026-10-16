package migrate

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

type syncItem struct {
	meta LiveNeedle
	rec  *NeedleRecord
}

// syncVolumePass copies live needles newer than the volume watermark to the target cluster.
// It returns the number of needle failures in this pass. A non-nil error means the pass
// could not finish (scan or volume prepare), and the source mtime must not be committed.
func syncVolumePass(ctx context.Context, cfg SyncConfig, ref VolumeRef, dst destIO) (VolumeSyncState, error) {
	started := time.Now()
	st, err := LoadVolumeSync(cfg.StateDir, uint32(ref.VolumeID))
	if err != nil {
		return st, err
	}
	st.Status = "running"
	st.LastError = ""
	st.PassUploaded = 0
	st.PassBytes = 0
	if err := SaveVolumeSync(cfg.StateDir, st); err != nil {
		return st, err
	}

	if err := dst.PrepareVolume(ctx, uint32(ref.VolumeID)); err != nil {
		st.Status = "error"
		st.LastError = err.Error()
		_ = SaveVolumeSync(cfg.StateDir, st)
		return st, err
	}

	point := st.Point()
	var live []LiveNeedle
	var pending []syncItem
	err = ScanVolume(ref, 0, time.Time{}, false, cfg.Decompress, func(_ uint64, rec *NeedleRecord) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if rec == nil {
			return nil
		}
		meta := LiveNeedle{Key: rec.FidPathSafe, Unix: rec.Modified.Unix(), HasTime: rec.HasModified}
		live = append(live, meta)
		if !point.needs(meta) {
			return nil
		}
		cp := *rec
		cp.Data = append([]byte(nil), rec.Data...)
		pending = append(pending, syncItem{meta: meta, rec: &cp})
		return nil
	})
	if err != nil {
		st.Status = "error"
		st.LastError = err.Error()
		_ = SaveVolumeSync(cfg.StateDir, st)
		return st, err
	}

	sort.Slice(pending, func(i, j int) bool {
		a, b := pending[i].meta, pending[j].meta
		if a.HasTime != b.HasTime {
			return a.HasTime
		}
		if a.Unix != b.Unix {
			return a.Unix < b.Unix
		}
		return a.Key < b.Key
	})

	ok := map[string]struct{}{}
	var failed int64
	var lastFID string
	var lastErr string
	for _, item := range pending {
		select {
		case <-ctx.Done():
			st.Status = "idle"
			st.LastError = ctx.Err().Error()
			st = fillSyncStats(st, point, live, failed, started)
			_ = SaveVolumeSync(cfg.StateDir, st)
			return st, ctx.Err()
		default:
		}
		if SkipForeignCenter(item.rec.Name, cfg.CenterDst) {
			ok[item.meta.Key] = struct{}{}
			continue
		}
		item.rec.Name = StampCenter(item.rec.Name, cfg.Center)
		if err := syncOneNeedle(ctx, cfg, dst, item.rec); err != nil {
			failed++
			lastErr = err.Error()
			glog.V(1).Infof("sync vid=%d fid=%s: %v", ref.VolumeID, item.rec.Fid, err)
			_ = appendSyncLog(cfg.StateDir, fmt.Sprintf("%s vid=%d fid=%s error=%s",
				time.Now().Format(time.RFC3339), ref.VolumeID, item.rec.Fid, err.Error()))
			continue
		}
		ok[item.meta.Key] = struct{}{}
		lastFID = item.rec.Fid
		st.PassUploaded++
		st.PassBytes += item.rec.DataSize
		st.Bytes += item.rec.DataSize
	}

	deletedN, delFailed, delErr := syncDeletes(ctx, cfg, ref, dst)
	failed += delFailed
	if delErr != "" {
		lastErr = delErr
	}
	st.Deleted = deletedN

	point = point.Advance(live, ok)
	st.SetPoint(point)
	st.LastFID = lastFID
	st.LastError = lastErr
	if failed > 0 {
		st.Status = "error"
	} else {
		st.Status = "idle"
	}
	st = fillSyncStats(st, point, live, failed, started)
	if err := SaveVolumeSync(cfg.StateDir, st); err != nil {
		return st, err
	}
	_ = appendSyncLog(cfg.StateDir, fmt.Sprintf("%s vid=%d files=%d synced=%d deleted=%d failed=%d lag=%ds watermark=%d uploaded=%d",
		st.UpdatedAt, st.VolumeID, st.Files, st.Synced, st.Deleted, st.Failed, st.LagSeconds, st.CommittedUnix, st.PassUploaded))
	return st, nil
}

func syncDeletes(ctx context.Context, cfg SyncConfig, ref VolumeRef, dst destIO) (deleted, failed int64, lastErr string) {
	done, err := LoadDeletedFIDs(cfg.StateDir, uint32(ref.VolumeID))
	if err != nil {
		return 0, 1, err.Error()
	}
	list, err := ScanDeleted(ref)
	if err != nil {
		return 0, 1, err.Error()
	}
	for _, item := range list {
		select {
		case <-ctx.Done():
			return deleted, failed + 1, ctx.Err().Error()
		default:
		}
		if _, ok := done[item.FidPathSafe]; ok {
			continue
		}
		if SkipForeignCenter(item.Name, cfg.CenterDst) {
			if err := MarkDeletedFID(cfg.StateDir, uint32(ref.VolumeID), item.FidPathSafe); err != nil {
				failed++
				lastErr = err.Error()
				continue
			}
			done[item.FidPathSafe] = struct{}{}
			continue
		}
		dctx, cancel := context.WithTimeout(ctx, uploadTimeout(0))
		err := dst.Delete(dctx, item.Fid)
		cancel()
		if err != nil {
			failed++
			lastErr = err.Error()
			glog.V(1).Infof("sync delete vid=%d fid=%s: %v", ref.VolumeID, item.Fid, err)
			_ = appendSyncLog(cfg.StateDir, fmt.Sprintf("%s vid=%d fid=%s delete error=%s",
				time.Now().Format(time.RFC3339), ref.VolumeID, item.Fid, err.Error()))
			continue
		}
		if err := MarkDeletedFID(cfg.StateDir, uint32(ref.VolumeID), item.FidPathSafe); err != nil {
			failed++
			lastErr = err.Error()
			continue
		}
		done[item.FidPathSafe] = struct{}{}
		deleted++
	}
	return deleted, failed, lastErr
}

func fillSyncStats(st VolumeSyncState, point SyncPoint, live []LiveNeedle, failed int64, started time.Time) VolumeSyncState {
	pending := point.Pending(live)
	st.Files = int64(len(live))
	st.Synced = st.Files - int64(len(pending))
	st.Failed = failed
	st.LagSeconds = int64(point.Lag(live, time.Now()) / time.Second)
	st.PassDurationMs = time.Since(started).Milliseconds()
	return st
}

func syncOneNeedle(ctx context.Context, cfg SyncConfig, dst destIO, rec *NeedleRecord) error {
	dig := Digest{Size: rec.DataSize}
	if cfg.VerifyMode == VerifyMD5 {
		dig = DigestOf(rec.Data)
	}
	timeout := uploadTimeout(rec.DataSize)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if cfg.VerifyMode.AtLeast(VerifyExists) {
		exists, size, etag, err := dst.Head(ctx, rec.Fid)
		if err != nil {
			return err
		}
		if exists && syncNeedleMatches(ctx, cfg, dst, rec, dig, size, etag) {
			return nil
		}
	}

	if err := dst.Put(ctx, rec.Fid, rec.Name, rec.Mime, rec.Data, dig, cfg.Retries); err != nil {
		return err
	}
	if cfg.VerifyMode == VerifyNone {
		return nil
	}
	exists, size, etag, err := dst.Head(ctx, rec.Fid)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("verify exists failed")
	}
	if cfg.VerifyMode.AtLeast(VerifySize) && size >= 0 && size != rec.DataSize {
		return fmt.Errorf("verify size %d want %d", size, rec.DataSize)
	}
	if cfg.VerifyMode == VerifyCRC {
		want := NeedleCRCHex(rec.Checksum)
		if etag == "" || etag != want {
			return fmt.Errorf("%s", FormatCRCMismatch(want, etag))
		}
	}
	if cfg.VerifyMode == VerifyMD5 {
		body, err := dst.Get(ctx, rec.Fid)
		if err != nil {
			return err
		}
		got := DigestOf(body)
		if got.MD5 != dig.MD5 {
			return fmt.Errorf("%s", FormatMD5Mismatch(dig, got))
		}
	}
	return nil
}

func syncNeedleMatches(ctx context.Context, cfg SyncConfig, dst destIO, rec *NeedleRecord, dig Digest, size int64, etag string) bool {
	if cfg.VerifyMode.AtLeast(VerifySize) && size >= 0 && size != rec.DataSize {
		return false
	}
	switch cfg.VerifyMode {
	case VerifyCRC:
		return etag != "" && etag == NeedleCRCHex(rec.Checksum)
	case VerifyMD5:
		body, err := dst.Get(ctx, rec.Fid)
		if err != nil {
			return false
		}
		return DigestOf(body).MD5 == dig.MD5
	default:
		return true
	}
}

func volumeSourceMTime(ref VolumeRef) (time.Time, error) {
	dat, err := os.Stat(ref.DatPath())
	if err != nil {
		return time.Time{}, err
	}
	idx, err := os.Stat(ref.IdxPath())
	if err != nil {
		return time.Time{}, err
	}
	if idx.ModTime().After(dat.ModTime()) {
		return idx.ModTime(), nil
	}
	return dat.ModTime(), nil
}
