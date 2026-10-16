package migrate

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// NeedleRecord is one live file found in a volume.
type NeedleRecord struct {
	Fid         string
	FidPathSafe string
	VolumeID    needle.VolumeId
	NeedleID    types.NeedleId
	Offset      int64
	DataSize    int64
	Modified    time.Time
	HasModified bool
	Name        string
	Mime        string
	Compressed  bool
	Data        []byte // payload to upload (optionally decompressed)
	Checksum    uint32 // CRC32-Castagnoli of Data; matches volume needle ETag
}

// IdxEntryCount returns number of index rows in .idx.
func IdxEntryCount(idxPath string) (uint64, error) {
	st, err := os.Stat(idxPath)
	if err != nil {
		return 0, err
	}
	return uint64(st.Size()) / uint64(types.NeedleMapEntrySize), nil
}

// VisitFunc is invoked for every idx entry from fromIdxEntry.
// rec is nil when the idx row is not a live needle to migrate (tombstone/outdated/filtered).
// idxEntryIndex is the 0-based index of the idx row that was just processed.
type VisitFunc func(idxEntryIndex uint64, rec *NeedleRecord) error

// ScanVolume walks .idx from fromIdxEntry and reads live needles from .dat.
func ScanVolume(ref VolumeRef, fromIdxEntry uint64, newerThan time.Time, hasNewer, decompress bool, visit VisitFunc) error {
	needleMap := needle_map.NewMemDb()
	if needleMap == nil {
		return fmt.Errorf("failed to open in-memory needle map")
	}
	defer needleMap.Close()

	if err := needleMap.LoadFromIdx(ref.IdxPath()); err != nil {
		return fmt.Errorf("load idx %s: %w", ref.IdxPath(), err)
	}

	datFile, err := backend.OpenVolumeFile(ref.DatPath(), os.O_RDONLY)
	if err != nil {
		return fmt.Errorf("open dat %s: %w", ref.DatPath(), err)
	}
	defer datFile.Close()
	datBackend := backend.NewDiskFile(datFile)

	sb, err := super_block.ReadSuperBlock(datBackend)
	if err != nil {
		return fmt.Errorf("read superblock %s: %w", ref.DatPath(), err)
	}
	version := sb.Version

	idxFile, err := os.Open(ref.IdxPath())
	if err != nil {
		return fmt.Errorf("open idx %s: %w", ref.IdxPath(), err)
	}
	defer idxFile.Close()

	var entryIndex uint64
	err = idx.WalkIndexFile(idxFile, fromIdxEntry, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		idxPos := fromIdxEntry + entryIndex
		entryIndex++

		if offset.IsZero() || !size.IsValid() {
			return visit(idxPos, nil)
		}

		nv, ok := needleMap.Get(key)
		if !ok || !nv.Size.IsValid() || nv.Offset.ToActualOffset() != offset.ToActualOffset() {
			return visit(idxPos, nil)
		}

		actualOffset := offset.ToActualOffset()
		n, _, bodyLen, err := needle.ReadNeedleHeader(datBackend, version, actualOffset)
		if err != nil {
			return fmt.Errorf("read needle header vid=%d offset=%d: %w", ref.VolumeID, actualOffset, err)
		}
		if _, err := n.ReadNeedleBody(datBackend, version, actualOffset+int64(types.NeedleHeaderSize), bodyLen); err != nil {
			return fmt.Errorf("read needle body vid=%d id=%s: %w", ref.VolumeID, key, err)
		}

		fid := needle.NewFileIdFromNeedle(ref.VolumeID, n).String()
		rec := &NeedleRecord{
			Fid:         fid,
			FidPathSafe: FidPathSafe(fid),
			VolumeID:    ref.VolumeID,
			NeedleID:    n.Id,
			Offset:      actualOffset,
			Name:        string(n.Name),
			Mime:        string(n.Mime),
			Compressed:  n.IsCompressed(),
			Data:        n.Data,
		}
		if n.HasLastModifiedDate() {
			rec.HasModified = true
			rec.Modified = time.Unix(int64(n.LastModified), 0)
		}
		if hasNewer && (!rec.HasModified || !rec.Modified.After(newerThan)) {
			return visit(idxPos, nil)
		}

		if decompress && n.IsCompressed() && util.IsGzippedContent(n.Data) {
			uncompressed, derr := util.DecompressData(n.Data)
			if derr != nil {
				return fmt.Errorf("decompress fid=%s: %w", fid, derr)
			}
			rec.Data = uncompressed
			rec.Compressed = false
		}
		rec.DataSize = int64(len(rec.Data))
		// CRC of the bytes that will be written. Destination needle ETag is this value.
		// When the payload is not decompressed it equals the source needle checksum.
		rec.Checksum = uint32(needle.NewCRC(rec.Data))
		return visit(idxPos, rec)
	})
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}
