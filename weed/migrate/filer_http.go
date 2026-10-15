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
	bases      []string
	client     *http.Client
	collection string
	replication string
	ttl        string
	disk       string
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

// Upload POSTs file content to filer path (autoChunk on server).
func (f *FilerHTTP) Upload(ctx context.Context, fullPath string, body io.Reader, size int64, mime string) error {
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
		// body may need rewind on retry — callers should pass bytes.Reader or similar
		if br, ok := body.(*bytes.Reader); ok {
			_, _ = br.Seek(0, io.SeekStart)
			rdr = br
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.urlFor(base, fullPath, q), rdr)
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
			return nil
		}
		last = fmt.Errorf("POST %s: status %d body %s", fullPath, resp.StatusCode, strings.TrimSpace(string(respBody)))
		if resp.StatusCode >= 500 {
			continue
		}
		return last
	}
	return last
}

// UploadWithRetry wraps Upload with simple retries.
func (f *FilerHTTP) UploadWithRetry(ctx context.Context, fullPath string, data []byte, mime string, retries int) error {
	var err error
	for i := 0; i <= retries; i++ {
		r := bytes.NewReader(data)
		err = f.Upload(ctx, fullPath, r, int64(len(data)), mime)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(1<<uint(min(i, 5))) * 200 * time.Millisecond):
		}
	}
	return err
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
