package migrate

import (
	"fmt"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// DeletedNeedle is a live-map tombstone: the fid to delete on the target, plus the
// last known file name so center filtering can see "a::name".
type DeletedNeedle struct {
	Fid         string
	FidPathSafe string
	Name        string
}

// ScanDeleted lists needles whose latest idx entry is a tombstone.
func ScanDeleted(ref VolumeRef) ([]DeletedNeedle, error) {
	needleMap := needle_map.NewMemDb()
	if needleMap == nil {
		return nil, fmt.Errorf("failed to open in-memory needle map")
	}
	defer needleMap.Close()
	if err := needleMap.LoadFromIdx(ref.IdxPath()); err != nil {
		return nil, fmt.Errorf("load idx %s: %w", ref.IdxPath(), err)
	}

	datFile, err := backend.OpenVolumeFile(ref.DatPath(), os.O_RDONLY)
	if err != nil {
		return nil, fmt.Errorf("open dat %s: %w", ref.DatPath(), err)
	}
	defer datFile.Close()
	datBackend := backend.NewDiskFile(datFile)
	sb, err := super_block.ReadSuperBlock(datBackend)
	if err != nil {
		return nil, fmt.Errorf("read superblock %s: %w", ref.DatPath(), err)
	}
	version := sb.Version

	idxFile, err := os.Open(ref.IdxPath())
	if err != nil {
		return nil, err
	}
	defer idxFile.Close()

	prevOff := map[types.NeedleId]int64{}
	var out []DeletedNeedle
	err = idx.WalkIndexFile(idxFile, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		if !size.IsDeleted() && !offset.IsZero() && size.IsValid() {
			prevOff[key] = offset.ToActualOffset()
			return nil
		}
		if !size.IsDeleted() {
			return nil
		}
		nv, ok := needleMap.Get(key)
		if !ok || !nv.Size.IsDeleted() || nv.Offset.ToActualOffset() != offset.ToActualOffset() {
			return nil
		}
		if offset.IsZero() {
			return nil
		}
		n, _, bodyLen, err := needle.ReadNeedleHeader(datBackend, version, offset.ToActualOffset())
		if err != nil {
			return fmt.Errorf("read deleted needle header vid=%d id=%s: %w", ref.VolumeID, key, err)
		}
		name := ""
		if bodyLen > 0 {
			if _, err := n.ReadNeedleBody(datBackend, version, offset.ToActualOffset()+int64(types.NeedleHeaderSize), bodyLen); err == nil {
				name = string(n.Name)
			}
		}
		if name == "" {
			if off, ok := prevOff[key]; ok {
				prev, _, prevLen, err := needle.ReadNeedleHeader(datBackend, version, off)
				if err == nil && prevLen > 0 {
					if _, err := prev.ReadNeedleBody(datBackend, version, off+int64(types.NeedleHeaderSize), prevLen); err == nil {
						name = string(prev.Name)
					}
				}
			}
		}
		fid := needle.NewFileIdFromNeedle(ref.VolumeID, n).String()
		out = append(out, DeletedNeedle{Fid: fid, FidPathSafe: FidPathSafe(fid), Name: name})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
