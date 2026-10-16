package command

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/migrate"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	cmdSyncVolumeStatus.Run = runSyncVolumeStatus
}

var cmdSyncVolumeStatus = &Command{
	UsageLine: "sync.volume.status -stateDir=./sync-state",
	Short:     "show per-volume sync progress from the local state directory",
	Long: `Read sync.volume progress files and print files, synced count, failures, and lag.

  Lag is now minus the oldest unsynced needle LastModified. The watermark is the
  last LastModified second that is fully synced.

`,
}

var (
	svsStateDir = cmdSyncVolumeStatus.Flag.String("stateDir", "", "sync state directory")
	svsJSON     = cmdSyncVolumeStatus.Flag.Bool("json", false, "print JSON instead of text")
)

func runSyncVolumeStatus(cmd *Command, args []string) bool {
	*svsStateDir = util.ResolvePath(*svsStateDir)
	if *svsStateDir == "" {
		fmt.Println("-stateDir is required")
		return false
	}
	rep, err := migrate.ReadSyncStatus(*svsStateDir)
	if err != nil {
		fmt.Printf("sync.volume.status: %v\n", err)
		return false
	}
	if *svsJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
		return true
	}
	fmt.Print(migrate.FormatSyncStatus(rep))
	return true
}
