package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/migrate"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	cmdSyncVolume.Run = runSyncVolume
}

var cmdSyncVolume = &Command{
	UsageLine: "sync.volume -dir=/data -collection=c -masters=host:9333 -stateDir=./sync-state",
	Short:     "continuously sync volume needles to a target cluster, keeping source fids",
	Long: `Resident blob sync. Scans a local volume directory and copies live needles to another cluster.

  A manager loop discovers volumes. When a volume .dat/.idx modification time is newer
  than the last successful pass, or the last pass failed, it starts one goroutine for
  that volume. The goroutine reads needles, syncs those newer than the saved time
  watermark, and writes progress under -stateDir.

  Resume index is the needle LastModified second, not the .idx offset. A second is
  committed only after every live needle in that second has succeeded.

  -center stamps filenames as "<center>::<name>". Needles whose name is already
  "<centerDst>::..." are not copied or deleted on the target.
  Tombstones are deleted on the target volume; HTTP 404 is ignored.

  Monitor: weed sync.volume.status -stateDir=...

`,
}

var (
	svDir           = cmdSyncVolume.Flag.String("dir", ".", "volume data directory")
	svCollection    = cmdSyncVolume.Flag.String("collection", "", "collection name; empty matches unprefixed volumes")
	svVolumeIds     = cmdSyncVolume.Flag.String("volumeIds", "", "comma-separated volume ids; empty = all")
	svMasters       = cmdSyncVolume.Flag.String("masters", "", "target master addresses host:port, comma-separated")
	svStateDir      = cmdSyncVolume.Flag.String("stateDir", "", "local directory for sync progress and sync.log")
	svCollectionDst = cmdSyncVolume.Flag.String("collectionDst", "", "collection used when creating a missing target volume")
	svReplication   = cmdSyncVolume.Flag.String("replication", "", "replication used when creating a missing target volume")
	svTTL           = cmdSyncVolume.Flag.String("ttl", "", "ttl used when creating a missing target volume")
	svDisk          = cmdSyncVolume.Flag.String("disk", "", "disk type used when creating a missing target volume")
	svInterval      = cmdSyncVolume.Flag.Duration("interval", 10*time.Second, "how often to scan for new or changed volumes")
	svDecompress    = cmdSyncVolume.Flag.Bool("decompress", true, "decompress gzip needles before upload")
	svVerifyMode    = cmdSyncVolume.Flag.String("verifyMode", "", "none|exists|size|crc|md5; empty = crc")
	svRetries       = cmdSyncVolume.Flag.Int("retries", 3, "upload retries")
	svCenter        = cmdSyncVolume.Flag.String("center", "", "source cluster id, written as id::filename")
	svCenterDst     = cmdSyncVolume.Flag.String("centerDst", "", "target cluster id; id::filename from this id is not replicated")
)

func runSyncVolume(cmd *Command, args []string) bool {
	util.LoadSecurityConfiguration()
	*svDir = util.ResolvePath(*svDir)
	*svStateDir = util.ResolvePath(*svStateDir)

	level, err := migrate.ParseVerifyLevel(*svVerifyMode)
	if err != nil {
		fmt.Printf("invalid -verifyMode: %v\n", err)
		return false
	}
	if strings.TrimSpace(*svVerifyMode) == "" {
		level = migrate.VerifyCRC
	}

	cfg := migrate.SyncConfig{
		Dir:           *svDir,
		Collection:    *svCollection,
		StateDir:      *svStateDir,
		Masters:       *svMasters,
		CollectionDst: *svCollectionDst,
		Replication:   *svReplication,
		TTL:           *svTTL,
		Disk:          *svDisk,
		Interval:      *svInterval,
		Decompress:    *svDecompress,
		VerifyMode:    level,
		Retries:       *svRetries,
		Center:        *svCenter,
		CenterDst:     *svCenterDst,
		GrpcDial:      security.LoadClientTLS(util.GetViper(), "grpc.client"),
	}
	if *svVolumeIds != "" {
		for _, p := range strings.Split(*svVolumeIds, ",") {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			id, err := strconv.ParseUint(p, 10, 32)
			if err != nil {
				fmt.Printf("invalid volumeId %q: %v\n", p, err)
				return false
			}
			cfg.VolumeIDs = append(cfg.VolumeIDs, uint32(id))
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Fprintf(os.Stderr, "sync.volume watching %s collection=%q masters=%s interval=%s\n",
		cfg.Dir, cfg.Collection, cfg.Masters, cfg.Interval)
	if err := migrate.RunSync(ctx, cfg); err != nil {
		fmt.Printf("sync.volume: %v\n", err)
		return false
	}
	fmt.Fprintln(os.Stderr, "sync.volume stopped")
	return true
}
