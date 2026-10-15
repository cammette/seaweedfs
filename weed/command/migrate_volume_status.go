package command

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/migrate"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	cmdMigrateVolumeStatus.Run = runMigrateVolumeStatus
}

var cmdMigrateVolumeStatus = &Command{
	UsageLine: "migrate.volume.status -stateDir=./migrate-state",
	Short:     "show volume migration progress from local stateDir",
	Long: `Read migration progress files and print how many volumes and fids were migrated.

  Reports:
    - volumes done / running / pending
    - fids migrated per volume (from success/*.log)
    - total fids migrated

`,
}

var (
	mvsStateDir = cmdMigrateVolumeStatus.Flag.String("stateDir", "", "migration state directory")
	mvsJSON     = cmdMigrateVolumeStatus.Flag.Bool("json", false, "print JSON instead of text")
)

func runMigrateVolumeStatus(cmd *Command, args []string) bool {
	*mvsStateDir = util.ResolvePath(*mvsStateDir)
	if *mvsStateDir == "" {
		fmt.Println("-stateDir is required")
		return false
	}

	rep, err := migrate.ReadStatus(*mvsStateDir)
	if err != nil {
		fmt.Printf("migrate.volume.status: %v\n", err)
		return false
	}

	if *mvsJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			fmt.Printf("encode: %v\n", err)
			return false
		}
		return true
	}

	fmt.Print(migrate.FormatStatusText(rep))
	return true
}
