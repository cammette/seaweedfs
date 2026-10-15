package migrate

import (
	"testing"
	"time"
)

func TestPathTemplateRender(t *testing.T) {
	tmpl, err := NewPathTemplate("/collector/{yyyy}/{yyyyMMdd}/{volume_id}/{hash12}/{fid}")
	if err != nil {
		t.Fatal(err)
	}
	mod := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	path, err := tmpl.Render(PathContext{
		Fid:        "3,01637037d6",
		VolumeID:   3,
		Collection: "photos",
		Modified:   mod,
		TimeZone:   time.UTC,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := "/collector/2024/20240115/3/"
	if path[:len(wantPrefix)] != wantPrefix {
		t.Fatalf("got %s want prefix %s", path, wantPrefix)
	}
	if path[len(path)-len("3_01637037d6"):] != "3_01637037d6" {
		t.Fatalf("basename wrong: %s", path)
	}
}

func TestPathTemplateRequiresFid(t *testing.T) {
	if _, err := NewPathTemplate("/collector/{volume_id}/x"); err == nil {
		t.Fatal("expected error")
	}
}

func TestFidPathSafe(t *testing.T) {
	if got := FidPathSafe("3,abc"); got != "3_abc" {
		t.Fatalf("got %s", got)
	}
}

func TestHash12Stable(t *testing.T) {
	a := hashMod("3,01637037d6", 12)
	b := hashMod("3,01637037d6", 12)
	if a != b || a >= 12 {
		t.Fatalf("hash unstable or out of range: %d", a)
	}
}
