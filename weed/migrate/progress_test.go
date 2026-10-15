package migrate

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProgressStoreAndStatus(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenProgressStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Dir:          "/data",
		Collection:   "c",
		Filer:        "http://filer:8888",
		PathTemplate: "/x/{fid}",
		StateDir:     dir,
	}
	if err := store.LoadOrInit(cfg); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveVolume(VolumeProgress{VolumeID: 3, Status: "done", IdxCursor: 10, IdxEntries: 10}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkSuccess(3, "3_abc"); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkSuccess(3, "3_def"); err != nil {
		t.Fatal(err)
	}
	ok, err := store.HasSuccess(3, "3_abc")
	if err != nil || !ok {
		t.Fatalf("HasSuccess: %v %v", ok, err)
	}
	_ = store.Close()

	rep, err := ReadStatus(dir)
	if err != nil {
		t.Fatal(err)
	}
	if rep.VolumesTotal != 1 || rep.VolumesDone != 1 || rep.FIDsMigratedTotal != 2 {
		t.Fatalf("unexpected report: %+v", rep)
	}
	text := FormatStatusText(rep)
	if text == "" {
		t.Fatal("empty status text")
	}
	// ensure success log exists
	if _, err := os.Stat(filepath.Join(dir, "success", "3.log")); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoverVolumes(t *testing.T) {
	dir := t.TempDir()
	// collection photos volume 7
	base := filepath.Join(dir, "photos_7")
	if err := os.WriteFile(base+".dat", []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(base+".idx", make([]byte, 16), 0644); err != nil {
		t.Fatal(err)
	}
	// missing idx
	_ = os.WriteFile(filepath.Join(dir, "photos_8.dat"), []byte("x"), 0644)

	refs, skipped, err := DiscoverVolumes(dir, "photos", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].VolumeID != 7 {
		t.Fatalf("refs=%v skipped=%v", refs, skipped)
	}
	if len(skipped) == 0 {
		t.Fatal("expected skipped missing idx")
	}
}
