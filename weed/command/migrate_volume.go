package command

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/migrate"
	"github.com/seaweedfs/seaweedfs/weed/security"
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
  uploads each file to a path rendered from -path template (filename is source fid).

  -verifyMode controls post-upload checks: none | exists | size(meta) | crc | md5.
  crc (blob default when -verifyMode is empty) compares the needle CRC32 via the volume ETag.

  -mode=filer writes through the filer (a new fid is assigned).
  -mode=blob talks only to target masters and volume servers and keeps the source fid.
  Missing volumes are created with that volume id, then the needle is POSTed to the volume.

  Progress: weed migrate.volume.status -stateDir=...
  Offline compare: weed migrate.volume.verify ...

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
	mvVerifyMode    = cmdMigrateVolume.Flag.String("verifyMode", "", "post-upload verify: none|exists|size|crc|md5; empty = size (filer) or crc (blob)")
	mvVerifyMeta    = cmdMigrateVolume.Flag.Bool("verifyMeta", true, "deprecated: if -verifyMode unset semantics, true=>size false=>none")
	mvRetries       = cmdMigrateVolume.Flag.Int("retries", 3, "upload retries on failure")
	mvLimit         = cmdMigrateVolume.Flag.Int("limit", 0, "stop after N successful migrations (0=unlimited)")
	mvWorkers       = cmdMigrateVolume.Flag.Int("workers", 4, "concurrent uploads within a volume")
	mvNewer         = cmdMigrateVolume.Flag.String("newer", "", "only migrate LastModified after this time (2006-01-02T15:04:05)")
	mvTimeZone      = cmdMigrateVolume.Flag.String("timeZone", "Local", "timezone for path date vars: Local or UTC")
	mvCollectionDst = cmdMigrateVolume.Flag.String("collectionDst", "", "optional target collection query param")
	mvReplication   = cmdMigrateVolume.Flag.String("replication", "", "optional replication query param")
	mvTTL           = cmdMigrateVolume.Flag.String("ttl", "", "optional ttl query param")
	mvDisk          = cmdMigrateVolume.Flag.String("disk", "", "optional disk query param")
	mvForce         = cmdMigrateVolume.Flag.Bool("force", false, "allow continuing when stateDir config fingerprint differs")
	mvMode          = cmdMigrateVolume.Flag.String("mode", "filer", "filer: write via filer HTTP (new fid); blob: master+volume, keep source fid")
	mvMasters       = cmdMigrateVolume.Flag.String("masters", "", "target master addresses host:port, comma-separated; required for -mode=blob")
)

func runMigrateVolume(cmd *Command, args []string) bool {
	util.LoadSecurityConfiguration()

	*mvDir = util.ResolvePath(*mvDir)
	*mvStateDir = util.ResolvePath(*mvStateDir)

	level, err := migrate.ParseVerifyLevel(*mvVerifyMode)
	if err != nil {
		fmt.Printf("invalid -verifyMode: %v\n", err)
		return false
	}
	// Empty -verifyMode: filer checks size, blob checks the needle CRC (volume ETag).
	if strings.TrimSpace(*mvVerifyMode) == "" && strings.EqualFold(*mvMode, "blob") {
		level = migrate.VerifyCRC
	}
	// Honor deprecated -verifyMeta only when user left verifyMode at the filer default and set verifyMeta=false.
	if !*mvVerifyMeta && (strings.TrimSpace(*mvVerifyMode) == "" || *mvVerifyMode == "size") && !strings.EqualFold(*mvMode, "blob") {
		level = migrate.VerifyNone
	}

	cfg := migrate.Config{
		Dir:           *mvDir,
		Collection:    *mvCollection,
		Filer:         *mvFiler,
		PathTemplate:  *mvPath,
		StateDir:      *mvStateDir,
		Decompress:    *mvDecompress,
		DryRun:        *mvDryRun,
		VerifyMode:    level,
		Retries:       *mvRetries,
		Limit:         *mvLimit,
		Workers:       *mvWorkers,
		CollectionDst: *mvCollectionDst,
		Replication:   *mvReplication,
		TTL:           *mvTTL,
		Disk:          *mvDisk,
		ForceConfig:   *mvForce,
		Mode:          *mvMode,
		Masters:       *mvMasters,
		GrpcDial:      security.LoadClientTLS(util.GetViper(), "grpc.client"),
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
