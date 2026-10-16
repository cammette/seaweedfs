package migrate

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Digest holds size and optional MD5 of payload bytes.
type Digest struct {
	Size int64
	MD5  [16]byte
	Has  bool
}

func DigestOf(data []byte) Digest {
	sum := md5.Sum(data)
	return Digest{Size: int64(len(data)), MD5: sum, Has: true}
}

func (d Digest) Hex() string {
	if !d.Has {
		return ""
	}
	return hex.EncodeToString(d.MD5[:])
}

func (d Digest) Base64() string {
	if !d.Has {
		return ""
	}
	return base64.StdEncoding.EncodeToString(d.MD5[:])
}

func DigestReader(r io.Reader) (Digest, []byte, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return Digest{}, nil, err
	}
	return DigestOf(data), data, nil
}

func FormatMD5Mismatch(src, dst Digest) string {
	return fmt.Sprintf("md5 mismatch src=%s dst=%s size=%d/%d", src.Hex(), dst.Hex(), src.Size, dst.Size)
}

// NeedleCRCHex is the volume ETag body: 8 lowercase hex chars of the raw CRC32.
func NeedleCRCHex(sum uint32) string {
	var b [4]byte
	util.Uint32toBytes(b[:], sum)
	return hex.EncodeToString(b[:])
}

// NormalizeETag strips quotes and a weak-validator prefix from an HTTP ETag.
func NormalizeETag(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "W/")
	s = strings.TrimSpace(s)
	s = strings.Trim(s, `"`)
	return strings.ToLower(s)
}

func FormatCRCMismatch(srcHex, dstETag string) string {
	return fmt.Sprintf("crc mismatch src=%s dst=%s", srcHex, NormalizeETag(dstETag))
}
