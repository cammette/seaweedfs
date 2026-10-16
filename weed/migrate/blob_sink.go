package migrate

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// BlobClient copies needles onto a target cluster by master lookup and volume HTTP write.
// The destination fid is the source fid. Missing volumes are allocated on a target
// volume server with that same volume id; the master learns them on the next heartbeat.
type BlobClient struct {
	masters     []pb.ServerAddress
	dial        grpc.DialOption
	collection  string
	replication string
	ttl         string
	disk        string
	httpClient  *http.Client
	rr          uint32
	mu          sync.Mutex
	ready       map[uint32]struct{}
}

func NewBlobClient(cfg Config) *BlobClient {
	dial := cfg.GrpcDial
	if dial == nil {
		dial = grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	var masters []pb.ServerAddress
	for _, m := range splitCSV(cfg.Masters) {
		masters = append(masters, pb.ServerAddress(m))
	}
	col := cfg.CollectionDst
	if col == "" {
		col = cfg.Collection
	}
	rep := cfg.Replication
	if rep == "" {
		rep = "000"
	}
	return &BlobClient{
		masters:     masters,
		dial:        dial,
		collection:  col,
		replication: rep,
		ttl:         cfg.TTL,
		disk:        cfg.Disk,
		httpClient:  &http.Client{},
		ready:       map[uint32]struct{}{},
	}
}

func (b *BlobClient) masterFn(ctx context.Context) pb.ServerAddress {
	b.mu.Lock()
	defer b.mu.Unlock()
	addr := b.masters[int(b.rr)%len(b.masters)]
	b.rr++
	return addr
}

// EnsureVolume makes sure vid exists on the target cluster and is visible to the master.
func (b *BlobClient) EnsureVolume(ctx context.Context, vid uint32) error {
	b.mu.Lock()
	if _, ok := b.ready[vid]; ok {
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()

	locs, err := b.lookup(ctx, vid)
	if err != nil {
		return err
	}
	if len(locs) == 0 {
		if err := b.allocate(ctx, vid); err != nil {
			return err
		}
		deadline := time.Now().Add(20 * time.Second)
		for {
			operation.InvalidateVolumeIdLocationCache(fmt.Sprint(vid))
			locs, err = b.lookup(ctx, vid)
			if err == nil && len(locs) > 0 {
				break
			}
			if time.Now().After(deadline) {
				if err == nil {
					err = fmt.Errorf("volume %d not visible on master after allocate", vid)
				}
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
	b.mu.Lock()
	b.ready[vid] = struct{}{}
	b.mu.Unlock()
	return nil
}

func (b *BlobClient) lookup(ctx context.Context, vid uint32) ([]operation.Location, error) {
	operation.InvalidateVolumeIdLocationCache(fmt.Sprint(vid))
	res, err := operation.LookupVolumeId(b.masterFn, b.dial, fmt.Sprint(vid))
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if res == nil || res.Error != "" && len(res.Locations) == 0 {
		if res != nil && isNotFound(fmt.Errorf("%s", res.Error)) {
			return nil, nil
		}
		if res != nil && res.Error != "" {
			return nil, fmt.Errorf("lookup volume %d: %s", vid, res.Error)
		}
		return nil, nil
	}
	return res.Locations, nil
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "not found") || strings.Contains(s, "notfound")
}

func (b *BlobClient) allocate(ctx context.Context, vid uint32) error {
	var nodes []*master_pb.DataNodeInfo
	err := operation.WithMasterServerClient(ctx, false, b.masterFn(ctx), b.dial, func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(ctx, &master_pb.VolumeListRequest{TopologyOnly: true})
		if err != nil {
			return err
		}
		if resp.GetTopologyInfo() == nil {
			return fmt.Errorf("master returned empty topology")
		}
		for _, dc := range resp.TopologyInfo.DataCenterInfos {
			for _, rack := range dc.RackInfos {
				nodes = append(nodes, rack.DataNodeInfos...)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("list volume servers: %w", err)
	}
	if len(nodes) == 0 {
		return fmt.Errorf("no volume server registered on target masters")
	}
	node := nodes[int(vid)%len(nodes)]
	addr := pb.NewServerAddressFromDataNode(node)
	err = pb.WithVolumeServerClient(false, addr, b.dial, func(client volume_server_pb.VolumeServerClient) error {
		_, e := client.AllocateVolume(ctx, &volume_server_pb.AllocateVolumeRequest{
			VolumeId:    vid,
			Collection:  b.collection,
			Replication: b.replication,
			Ttl:         b.ttl,
			DiskType:    b.disk,
			Version:     uint32(needle.GetCurrentVersion()),
		})
		return e
	})
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return nil
	}
	if err != nil {
		return fmt.Errorf("allocate volume %d on %s: %w", vid, addr, err)
	}
	return nil
}

// Write uploads data as the given source fid (volumeId,keycookie).
func (b *BlobClient) Write(ctx context.Context, fid string, data []byte, name, mime string, dig Digest) error {
	vid := strings.SplitN(fid, ",", 2)[0]
	lookup, err := operation.LookupVolumeId(b.masterFn, b.dial, vid)
	if err != nil {
		return err
	}
	if lookup == nil || len(lookup.Locations) == 0 {
		return fmt.Errorf("volume %s has no location", vid)
	}
	loc := lookup.Locations[0]
	uploadURL := "http://" + loc.Url + "/" + fid
	uploader, err := operation.NewUploader()
	if err != nil {
		return err
	}
	md5 := ""
	if dig.Has {
		md5 = dig.Base64()
	}
	_, err = uploader.UploadData(ctx, data, &operation.UploadOption{
		UploadUrl: uploadURL,
		Filename:  name,
		MimeType:  mime,
		Jwt:       security.EncodedJwt(lookup.Jwt),
		Md5:       md5,
	})
	return err
}

// Head reads existence, Content-Length, and ETag (needle CRC) without the body.
func (b *BlobClient) Head(ctx context.Context, fid string) (exists bool, size int64, etag string, err error) {
	vid := strings.SplitN(fid, ",", 2)[0]
	lookup, err := operation.LookupVolumeId(b.masterFn, b.dial, vid)
	if err != nil {
		return false, 0, "", err
	}
	if lookup == nil || len(lookup.Locations) == 0 {
		return false, 0, "", nil
	}
	var last error
	for _, loc := range lookup.Locations {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, "http://"+loc.Url+"/"+fid, nil)
		if err != nil {
			last = err
			continue
		}
		if lookup.Jwt != "" {
			req.Header.Set("Authorization", "Bearer "+lookup.Jwt)
		}
		resp, err := b.httpClient.Do(req)
		if err != nil {
			last = err
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			return false, 0, "", nil
		}
		if resp.StatusCode != http.StatusOK {
			last = fmt.Errorf("HEAD %s: status %d", fid, resp.StatusCode)
			continue
		}
		size = -1
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			if n, perr := strconv.ParseInt(cl, 10, 64); perr == nil {
				size = n
			}
		}
		return true, size, NormalizeETag(resp.Header.Get("ETag")), nil
	}
	if last != nil {
		return false, 0, "", last
	}
	return false, 0, "", nil
}

// Delete removes fid on the target volume. A 404 is treated as success.
func (b *BlobClient) Delete(ctx context.Context, fid string) error {
	vid := strings.SplitN(fid, ",", 2)[0]
	lookup, err := operation.LookupVolumeId(b.masterFn, b.dial, vid)
	if err != nil {
		return err
	}
	if lookup == nil || len(lookup.Locations) == 0 {
		return nil
	}
	var last error
	for _, loc := range lookup.Locations {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://"+loc.Url+"/"+fid, nil)
		if err != nil {
			last = err
			continue
		}
		if lookup.Jwt != "" {
			req.Header.Set("Authorization", "Bearer "+lookup.Jwt)
		}
		resp, err := b.httpClient.Do(req)
		if err != nil {
			last = err
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusNoContent {
			return nil
		}
		last = fmt.Errorf("DELETE %s: status %d", fid, resp.StatusCode)
	}
	return last
}

// Fetch downloads a fid from the target volume servers.
func (b *BlobClient) Fetch(ctx context.Context, fid string) ([]byte, int, error) {
	vid := strings.SplitN(fid, ",", 2)[0]
	lookup, err := operation.LookupVolumeId(b.masterFn, b.dial, vid)
	if err != nil {
		return nil, 0, err
	}
	if lookup == nil || len(lookup.Locations) == 0 {
		return nil, http.StatusNotFound, nil
	}
	var last error
	for _, loc := range lookup.Locations {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+loc.Url+"/"+fid, nil)
		if err != nil {
			last = err
			continue
		}
		if lookup.Jwt != "" {
			req.Header.Set("Authorization", "Bearer "+lookup.Jwt)
		}
		resp, err := b.httpClient.Do(req)
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
		if resp.StatusCode == http.StatusNotFound {
			return nil, http.StatusNotFound, nil
		}
		if resp.StatusCode == http.StatusOK {
			return body, http.StatusOK, nil
		}
		last = fmt.Errorf("GET %s: status %d", fid, resp.StatusCode)
	}
	if last != nil {
		return nil, 0, last
	}
	return nil, http.StatusNotFound, nil
}
