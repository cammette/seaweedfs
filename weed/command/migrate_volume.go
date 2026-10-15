package command

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/migrate"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	cmdMigrateVolume.Run = runMigrateVolume
}

var cmdMigrateVolume = &Command{
	UsageLine: "migrate.volume -dir=/data -collection=c -filer=http://filer:8888 -path='/collector/{yyyy}/{yyyyMMdd}/{volume_id}/{hash12}/{fid}' -stateDir=./migrate-state",
	Short:     "migrate volume files to a remote SeaweedFS filer via HTTP",
	Long: `Migrate live needles from local volume .dat/.idx files to a target Filer over HTTP.

  Reads the volume directory on a volume server, resumes from local stateDir,
  uploads each file to a path rendered from -path template (filename is source fid),
  and optionally verifies existence and Content-Length (meta).

  Progress can be inspected with: weed migrate.volume.status -stateDir=...

`,
}

var (
	mvDir           = cmdMigrateVolume.Flag.String("dir", ".", "volume data directory")
	mvCollection    = cmdMigrateVolume.Flag.String("collection", "", "collection name; empty matches unprefixed volumes")
	mvVolumeIds     = cmdMigrateVolume.Flag.String("volumeIds", "", "comma-separated volume ids; empty = all")
	mvFiler         = cmdMigrateVolume.Flag.String("filer", "", "target filer HTTP URL, e.g. http://filer:8888")
	mvPath          = cmdMigrateVolume.Flag.String("path", "/collector/{yyyy}/{yyyyMMdd}/{volume_id}/{hash12}/{fid}", "destination path template; must include {fid}")
	mvStateDir      = cmdMigrateVolume.Flag.String("stateDir", "", "local directory for migration progress")
	mvDecompress    = cmdMigrateVolume.Flag.Bool("decompress", true, "decompress gzip needles before upload")
	mvDryRun        = cmdMigrateVolume.Flag.Bool("dryRun", false, "print destinations without uploading")
	mvVerifyMeta    = cmdMigrateVolume.Flag.Bool("verifyMeta", true, "after upload, verify target exists and size matches")
	mvRetries       = cmdMigrateVolume.Flag.Int("retries", 3, "upload retries on failure")
	mvLimit         = cmdMigrateVolume.Flag.Int("limit", 0, "stop after N successful migrations (0=unlimited)")
	mvNewer         = cmdMigrateVolume.Flag.String("newer", "", "only migrate LastModified after this time (2006-01-02T15:04:05)")
	mvTimeZone      = cmdMigrateVolume.Flag.String("timeZone", "Local", "timezone for path date vars: Local or UTC")
	mvCollectionDst = cmdMigrateVolume.Flag.String("collectionDst", "", "optional target collection query param")
	mvReplication   = cmdMigrateVolume.Flag.String("replication", "", "optional replication query param")
	mvTTL           = cmdMigrateVolume.Flag.String("ttl", "", "optional ttl query param")
	mvDisk          = cmdMigrateVolume.Flag.String("disk", "", "optional disk query param")
	mvForce         = cmdMigrateVolume.Flag.Bool("force", false, "allow continuing when stateDir config fingerprint differs")
)

func runMigrateVolume(cmd *Command, args []string) bool {
	util.LoadSecurityConfiguration()

	*mvDir = util.ResolvePath(*mvDir)
	*mvStateDir = util.ResolvePath(*mvStateDir)

	cfg := migrate.Config{
		Dir:           *mvDir,
		Collection:    *mvCollection,
		Filer:         *mvFiler,
		PathTemplate:  *mvPath,
		StateDir:      *mvStateDir,
		Decompress:    *mvDecompress,
		DryRun:        *mvDryRun,
		VerifyMeta:    *mvVerifyMeta,
		Retries:       *mvRetries,
		Limit:         *mvLimit,
		CollectionDst: *mvCollectionDst,
		Replication:   *mvReplication,
		TTL:           *mvTTL,
		Disk:          *mvDisk,
		ForceConfig:   *mvForce,
	}

	if *mvVolumeIds != "" {
		for _, p := range strings.Split(*mvVolumeIds, ",") {
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

	switch strings.ToUpper(*mvTimeZone) {
	case "UTC":
		cfg.TimeZone = time.UTC
	default:
		cfg.TimeZone = time.Local
	}

	if *mvNewer != "" {
		t, err := time.ParseInLocation("2006-01-02T15:04:05", *mvNewer, cfg.TimeZone)
		if err != nil {
			fmt.Printf("invalid -newer: %v\n", err)
			return false
		}
		cfg.Newer = t
		cfg.HasNewer = true
	}

	if err := migrate.Run(cfg); err != nil {
		fmt.Printf("migrate.volume: %v\n", err)
		return false
	}
	return true
}
