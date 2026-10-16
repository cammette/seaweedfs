package migrate

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestParseVerifyLevel(t *testing.T) {
	cases := map[string]VerifyLevel{
		"none":     VerifyNone,
		"exists":   VerifyExists,
		"size":     VerifySize,
		"meta":     VerifySize,
		"md5":      VerifyMD5,
		"crc":      VerifyCRC,
		"checksum": VerifyCRC,
		"etag":     VerifyCRC,
		"":         VerifySize,
	}
	for in, want := range cases {
		got, err := ParseVerifyLevel(in)
		if err != nil || got != want {
			t.Fatalf("%q: got %v err=%v want %v", in, got, err, want)
		}
	}
	if _, err := ParseVerifyLevel("bogus"); err == nil {
		t.Fatal("expected error")
	}
}

func TestVerifyLevelAtLeast(t *testing.T) {
	if !VerifyMD5.AtLeast(VerifySize) || !VerifySize.AtLeast(VerifyExists) {
		t.Fatal("ordering broken")
	}
	if VerifyExists.AtLeast(VerifyMD5) {
		t.Fatal("exists should not cover md5")
	}
	if VerifyCRC.AtLeast(VerifyMD5) || !VerifyCRC.AtLeast(VerifySize) {
		t.Fatal("crc should cover size and not md5")
	}
}

func TestNeedleCRCHexMatchesETag(t *testing.T) {
	data := []byte("needle-body")
	n := &needle.Needle{Data: data, Checksum: needle.NewCRC(data)}
	if got := NeedleCRCHex(uint32(n.Checksum)); got != n.Etag() {
		t.Fatalf("crc hex %s etag %s", got, n.Etag())
	}
	if NormalizeETag(`"`+n.Etag()+`"`) != n.Etag() {
		t.Fatal("etag quotes")
	}
}

func TestDigestOf(t *testing.T) {
	d := DigestOf([]byte("hello"))
	if d.Size != 5 || !d.Has || d.Hex() == "" || d.Base64() == "" {
		t.Fatalf("%+v", d)
	}
}
