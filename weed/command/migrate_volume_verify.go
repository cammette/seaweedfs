package command

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/migrate"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	cmdMigrateVolumeVerify.Run = runMigrateVolumeVerify
}

var cmdMigrateVolumeVerify = &Command{
	UsageLine: "migrate.volume.verify -dir=/data -collection=c -filer=http://filer:8888 -path='/collector/{yyyy}/{yyyyMMdd}/{volume_id}/{hash12}/{fid}' -level=md5",
	Short:     "compare local volume needles against a remote filer or blob volume",
	Long: `Compare files from local volume .dat/.idx with a target.

  -mode=filer (default) compares HTTP objects on a Filer path template.
  -mode=blob compares the same fid on target volume servers (-masters required).

  Levels:
    exists  - destination exists
    size    - exists and length matches
    crc     - blob only: needle CRC32 matches the volume ETag (HEAD, no body)
    md5     - content MD5 matches (downloads the object)

  Optional -onlySuccess limits comparison to fids recorded in stateDir success logs.

`,
}

var (
	mvvDir         = cmdMigrateVolumeVerify.Flag.String("dir", ".", "volume data directory")
	mvvCollection  = cmdMigrateVolumeVerify.Flag.String("collection", "", "collection name")
	mvvVolumeIds   = cmdMigrateVolumeVerify.Flag.String("volumeIds", "", "comma-separated volume ids; empty = all")
	mvvFiler       = cmdMigrateVolumeVerify.Flag.String("filer", "", "target filer HTTP URL")
	mvvPath        = cmdMigrateVolumeVerify.Flag.String("path", "/collector/{yyyy}/{yyyyMMdd}/{volume_id}/{hash12}/{fid}", "path template; must include {fid}")
	mvvLevel       = cmdMigrateVolumeVerify.Flag.String("level", "", "compare level: exists|size|crc|md5; empty = size (filer) or crc (blob)")
	mvvMode        = cmdMigrateVolumeVerify.Flag.String("mode", "filer", "filer or blob")
	mvvMasters     = cmdMigrateVolumeVerify.Flag.String("masters", "", "target masters host:port for -mode=blob")
	mvvDecompress  = cmdMigrateVolumeVerify.Flag.Bool("decompress", true, "decompress gzip needles before compare")
	mvvOnlySuccess = cmdMigrateVolumeVerify.Flag.Bool("onlySuccess", false, "only compare fids in stateDir success logs")
	mvvStateDir    = cmdMigrateVolumeVerify.Flag.String("stateDir", "", "migration state dir (required with -onlySuccess)")
	mvvWorkers     = cmdMigrateVolumeVerify.Flag.Int("workers", 4, "concurrent compare workers")
	mvvLimit       = cmdMigrateVolumeVerify.Flag.Int("limit", 0, "stop after N compared files (0=unlimited)")
	mvvReport      = cmdMigrateVolumeVerify.Flag.String("report", "", "write JSON report to this path")
	mvvJSON        = cmdMigrateVolumeVerify.Flag.Bool("json", false, "print full JSON report to stdout")
	mvvNewer       = cmdMigrateVolumeVerify.Flag.String("newer", "", "only compare LastModified after this time")
	mvvTimeZone    = cmdMigrateVolumeVerify.Flag.String("timeZone", "Local", "Local or UTC")
)

func runMigrateVolumeVerify(cmd *Command, args []string) bool {
	util.LoadSecurityConfiguration()

	*mvvDir = util.ResolvePath(*mvvDir)
	*mvvStateDir = util.ResolvePath(*mvvStateDir)
	*mvvReport = util.ResolvePath(*mvvReport)

	level, err := migrate.ParseVerifyLevel(*mvvLevel)
	if err != nil {
		fmt.Printf("invalid -level: %v\n", err)
		return false
	}
	if level == migrate.VerifyNone {
		fmt.Println("-level=none is not valid for verify; use exists|size|crc|md5")
		return false
	}
	if strings.TrimSpace(*mvvLevel) == "" {
		if strings.EqualFold(*mvvMode, "blob") {
			level = migrate.VerifyCRC
		} else {
			level = migrate.VerifySize
		}
	}

	cfg := migrate.VerifyConfig{
		Dir:          *mvvDir,
		Collection:   *mvvCollection,
		Filer:        *mvvFiler,
		PathTemplate: *mvvPath,
		Level:        level,
		Decompress:   *mvvDecompress,
		OnlySuccess:  *mvvOnlySuccess,
		StateDir:     *mvvStateDir,
		Workers:      *mvvWorkers,
		Limit:        *mvvLimit,
		ReportPath:   *mvvReport,
		Mode:         *mvvMode,
		Masters:      *mvvMasters,
		GrpcDial:     security.LoadClientTLS(util.GetViper(), "grpc.client"),
	}

	if *mvvVolumeIds != "" {
		for _, p := range strings.Split(*mvvVolumeIds, ",") {
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

	switch strings.ToUpper(*mvvTimeZone) {
	case "UTC":
		cfg.TimeZone = time.UTC
	default:
		cfg.TimeZone = time.Local
	}

	if *mvvNewer != "" {
		t, err := time.ParseInLocation("2006-01-02T15:04:05", *mvvNewer, cfg.TimeZone)
		if err != nil {
			fmt.Printf("invalid -newer: %v\n", err)
			return false
		}
		cfg.Newer = t
		cfg.HasNewer = true
	}

	rep, err := migrate.RunVerify(cfg)
	if err != nil {
		fmt.Printf("migrate.volume.verify: %v\n", err)
		return false
	}

	if *mvvJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
	} else {
		fmt.Print(migrate.FormatVerifyText(rep))
		if len(rep.Details) > 0 && len(rep.Details) <= 20 {
			for _, d := range rep.Details {
				fmt.Printf("  %s vid=%d fid=%s path=%s %s\n", d.Result, d.VID, d.FID, d.Path, d.Err)
			}
		} else if len(rep.Details) > 20 {
			fmt.Printf("  (%d detail rows; see -report)\n", len(rep.Details))
		}
	}

	if rep.HasFailures() {
		return false
	}
	return true
}
