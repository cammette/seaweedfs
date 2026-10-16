package migrate

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// FilerHTTP talks to a SeaweedFS Filer over HTTP.
type FilerHTTP struct {
	bases       []string
	client      *http.Client
	collection  string
	replication string
	ttl         string
	disk        string
}

func NewFilerHTTP(bases []string, collection, replication, ttl, disk string) *FilerHTTP {
	return &FilerHTTP{
		bases:       bases,
		client:      &http.Client{Timeout: 0}, // per-request context controls timeout
		collection:  collection,
		replication: replication,
		ttl:         ttl,
		disk:        disk,
	}
}

func (f *FilerHTTP) urlFor(base, fullPath string, query url.Values) string {
	u := strings.TrimRight(base, "/") + fullPath
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	return u
}

func (f *FilerHTTP) authHeader(isWrite bool) string {
	tok := util_http.JwtForFilerServer(isWrite)
	if tok == "" {
		return ""
	}
	return "Bearer " + tok
}

func contentLength(resp *http.Response) int64 {
	size := resp.ContentLength
	if size >= 0 {
		return size
	}
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		n, err := strconv.ParseInt(cl, 10, 64)
		if err == nil {
			return n
		}
	}
	return -1
}

// Exists returns whether the path exists on filer (HEAD, fallback GET).
func (f *FilerHTTP) Exists(ctx context.Context, fullPath string) (bool, int64, error) {
	var last error
	for _, base := range f.bases {
		exists, size, err := f.existsOnBase(ctx, base, fullPath)
		if err == nil {
			return exists, size, nil
		}
		last = err
	}
	if last != nil {
		return false, 0, last
	}
	return false, 0, fmt.Errorf("no filer base available")
}

func (f *FilerHTTP) existsOnBase(ctx context.Context, base, fullPath string) (bool, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, f.urlFor(base, fullPath, nil), nil)
	if err != nil {
		return false, 0, err
	}
	if a := f.authHeader(false); a != "" {
		req.Header.Set("Authorization", a)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return false, 0, err
	}
	resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return true, contentLength(resp), nil
	case http.StatusNotFound:
		return false, 0, nil
	case http.StatusMethodNotAllowed:
		// fall through to GET
	default:
		return false, 0, fmt.Errorf("HEAD %s: status %d", fullPath, resp.StatusCode)
	}

	req, err = http.NewRequestWithContext(ctx, http.MethodGet, f.urlFor(base, fullPath, nil), nil)
	if err != nil {
		return false, 0, err
	}
	if a := f.authHeader(false); a != "" {
		req.Header.Set("Authorization", a)
	}
	resp, err = f.client.Do(req)
	if err != nil {
		return false, 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<20))
	switch resp.StatusCode {
	case http.StatusOK:
		return true, contentLength(resp), nil
	case http.StatusNotFound:
		return false, 0, nil
	default:
		return false, 0, fmt.Errorf("GET %s: status %d", fullPath, resp.StatusCode)
	}
}

// UploadResult captures optional server-side digest headers.
type UploadResult struct {
	ContentMD5 string // base64 or hex from response, if any
}

// Upload POSTs file content to filer path (autoChunk on server).
// contentMD5Base64, when non-empty, is sent as Content-Md5 for filer-side check.
func (f *FilerHTTP) Upload(ctx context.Context, fullPath string, body io.Reader, size int64, mime, contentMD5Base64 string) (*UploadResult, error) {
	q := url.Values{}
	if f.collection != "" {
		q.Set("collection", f.collection)
	}
	if f.replication != "" {
		q.Set("replication", f.replication)
	}
	if f.ttl != "" {
		q.Set("ttl", f.ttl)
	}
	if f.disk != "" {
		q.Set("disk", f.disk)
	}

	var last error
	for _, base := range f.bases {
		var rdr io.Reader = body
		if br, ok := body.(*bytes.Reader); ok {
			_, _ = br.Seek(0, io.SeekStart)
			rdr = br
		}
		// PUT accepts a raw body; POST requires multipart/form-data.
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, f.urlFor(base, fullPath, q), rdr)
		if err != nil {
			last = err
			continue
		}
		if size >= 0 {
			req.ContentLength = size
		}
		if mime == "" {
			mime = "application/octet-stream"
		}
		req.Header.Set("Content-Type", mime)
		if contentMD5Base64 != "" {
			req.Header.Set("Content-Md5", contentMD5Base64)
		}
		if a := f.authHeader(true); a != "" {
			req.Header.Set("Authorization", a)
		}
		resp, err := f.client.Do(req)
		if err != nil {
			last = err
			continue
		}
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
			return &UploadResult{ContentMD5: resp.Header.Get("Content-MD5")}, nil
		}
		last = fmt.Errorf("PUT %s: status %d body %s", fullPath, resp.StatusCode, strings.TrimSpace(string(respBody)))
		if resp.StatusCode >= 500 {
			continue
		}
		return nil, last
	}
	return nil, last
}

// UploadWithRetry wraps Upload with simple retries.
func (f *FilerHTTP) UploadWithRetry(ctx context.Context, fullPath string, data []byte, mime string, dig Digest, retries int) (*UploadResult, error) {
	var (
		err error
		res *UploadResult
	)
	md5b64 := ""
	if dig.Has {
		md5b64 = dig.Base64()
	}
	for i := 0; i <= retries; i++ {
		r := bytes.NewReader(data)
		res, err = f.Upload(ctx, fullPath, r, int64(len(data)), mime, md5b64)
		if err == nil {
			return res, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(1<<uint(min(i, 5))) * 200 * time.Millisecond):
		}
	}
	return nil, err
}

// Delete removes a filer path. A 404 is treated as success.
func (f *FilerHTTP) Delete(ctx context.Context, fullPath string) error {
	var last error
	for _, base := range f.bases {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, f.urlFor(base, fullPath, nil), nil)
		if err != nil {
			last = err
			continue
		}
		if a := f.authHeader(false); a != "" {
			req.Header.Set("Authorization", a)
		}
		resp, err := f.client.Do(req)
		if err != nil {
			last = err
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusNoContent {
			return nil
		}
		last = fmt.Errorf("DELETE %s: status %d", fullPath, resp.StatusCode)
	}
	return last
}

// Download fetches file bytes from filer via HTTP GET.
func (f *FilerHTTP) Download(ctx context.Context, fullPath string) ([]byte, error) {
	var last error
	for _, base := range f.bases {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.urlFor(base, fullPath, nil), nil)
		if err != nil {
			last = err
			continue
		}
		if a := f.authHeader(false); a != "" {
			req.Header.Set("Authorization", a)
		}
		resp, err := f.client.Do(req)
		if err != nil {
			last = err
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			last = readErr
			continue
		}
		if resp.StatusCode == http.StatusOK {
			return body, nil
		}
		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("not found: %s", fullPath)
		}
		last = fmt.Errorf("GET %s: status %d", fullPath, resp.StatusCode)
		if resp.StatusCode >= 500 {
			continue
		}
		return nil, last
	}
	return nil, last
}

// destIO is where a migrated needle is written.
type destIO interface {
	PrepareVolume(ctx context.Context, vid uint32) error
	Stat(ctx context.Context, key string) (exists bool, size int64, err error)
	Head(ctx context.Context, key string) (exists bool, size int64, etag string, err error)
	Put(ctx context.Context, key, name, mime string, data []byte, dig Digest, retries int) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

type filerDest struct{ f *FilerHTTP }

func (d filerDest) PrepareVolume(context.Context, uint32) error { return nil }

func (d filerDest) Stat(ctx context.Context, key string) (bool, int64, error) {
	return d.f.Exists(ctx, key)
}

func (d filerDest) Head(ctx context.Context, key string) (bool, int64, string, error) {
	exists, size, err := d.f.Exists(ctx, key)
	return exists, size, "", err
}

func (d filerDest) Put(ctx context.Context, key, _, mime string, data []byte, dig Digest, retries int) error {
	_, err := d.f.UploadWithRetry(ctx, key, data, mime, dig, retries)
	return err
}

func (d filerDest) Get(ctx context.Context, key string) ([]byte, error) {
	return d.f.Download(ctx, key)
}

func (d filerDest) Delete(ctx context.Context, key string) error {
	return d.f.Delete(ctx, key)
}

type blobDest struct{ b *BlobClient }

func (d blobDest) PrepareVolume(ctx context.Context, vid uint32) error {
	return d.b.EnsureVolume(ctx, vid)
}

func (d blobDest) Stat(ctx context.Context, key string) (bool, int64, error) {
	exists, size, _, err := d.b.Head(ctx, key)
	return exists, size, err
}

func (d blobDest) Head(ctx context.Context, key string) (bool, int64, string, error) {
	return d.b.Head(ctx, key)
}

func (d blobDest) Put(ctx context.Context, key, name, mime string, data []byte, dig Digest, _ int) error {
	return d.b.Write(ctx, key, data, name, mime, dig)
}

func (d blobDest) Delete(ctx context.Context, key string) error {
	return d.b.Delete(ctx, key)
}

func (d blobDest) Get(ctx context.Context, key string) ([]byte, error) {
	body, status, err := d.b.Fetch(ctx, key)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return body, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
