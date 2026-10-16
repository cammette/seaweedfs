package migrate

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// VerifyConfig configures offline volume↔filer comparison.
type VerifyConfig struct {
	Dir          string
	Collection   string
	VolumeIDs    []uint32
	Filer        string
	PathTemplate string
	Level        VerifyLevel
	Decompress   bool
	OnlySuccess  bool
	StateDir     string
	Workers      int
	Limit        int
	ReportPath   string
	TimeZone     *time.Location
	Newer        time.Time
	HasNewer     bool
	Mode         string
	Masters      string
	GrpcDial     grpc.DialOption
}

func (c *VerifyConfig) IsBlob() bool {
	return strings.EqualFold(c.Mode, "blob")
}

func (c *VerifyConfig) Normalize() error {
	if c.Mode == "" {
		c.Mode = "filer"
	}
	c.Mode = strings.ToLower(c.Mode)
	if c.Mode != "filer" && c.Mode != "blob" {
		return fmt.Errorf("mode must be filer or blob, got %q", c.Mode)
	}
	if c.Dir == "" {
		return fmt.Errorf("dir is required")
	}
	if c.IsBlob() {
		masters := splitCSV(c.Masters)
		if len(masters) == 0 {
			return fmt.Errorf("blob verify requires -masters=host:9333[,host:9334]")
		}
		for i, m := range masters {
			masters[i] = strings.TrimPrefix(strings.TrimPrefix(m, "http://"), "https://")
			if !strings.Contains(masters[i], ":") {
				return fmt.Errorf("master address %q must be host:port", m)
			}
		}
		c.Masters = strings.Join(masters, ",")
	} else {
		if c.Level == VerifyCRC {
			return fmt.Errorf("level=crc compares needle checksums and requires -mode=blob")
		}
		if c.Filer == "" {
			return fmt.Errorf("filer is required")
		}
		if c.PathTemplate == "" {
			return fmt.Errorf("path template is required")
		}
		if _, err := NewPathTemplate(c.PathTemplate); err != nil {
			return err
		}
	}
	if c.OnlySuccess && c.StateDir == "" {
		return fmt.Errorf("stateDir is required when -onlySuccess")
	}
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.TimeZone == nil {
		c.TimeZone = time.Local
	}
	if c.Level < VerifyExists {
		c.Level = VerifySize
	}
	c.Filer = trimFilerCSV(c.Filer)
	return nil
}

func trimFilerCSV(filer string) string {
	bases := (&Config{Filer: filer}).FilerBases()
	if len(bases) == 0 {
		return filer
	}
	out := bases[0]
	for i := 1; i < len(bases); i++ {
		out += "," + bases[i]
	}
	return out
}

// VerifyDetail is one mismatch/error row in the report.
type VerifyDetail struct {
	VID     uint32 `json:"vid"`
	FID     string `json:"fid"`
	Path    string `json:"path"`
	Result  string `json:"result"`
	SrcSize int64  `json:"src_size,omitempty"`
	DstSize int64  `json:"dst_size,omitempty"`
	SrcMD5  string `json:"src_md5,omitempty"`
	DstMD5  string `json:"dst_md5,omitempty"`
	SrcCRC  string `json:"src_crc,omitempty"`
	DstCRC  string `json:"dst_crc,omitempty"`
	Err     string `json:"err,omitempty"`
}

// VerifyReport is written by migrate.volume.verify.
type VerifyReport struct {
	Level        string         `json:"level"`
	Volumes      []uint32       `json:"volumes"`
	Checked      int64          `json:"checked"`
	OK           int64          `json:"ok"`
	Missing      int64          `json:"missing"`
	SizeMismatch int64          `json:"size_mismatch"`
	MD5Mismatch  int64          `json:"md5_mismatch"`
	CRCMismatch  int64          `json:"crc_mismatch"`
	Errors       int64          `json:"errors"`
	Details      []VerifyDetail `json:"details"`
}

func (r *VerifyReport) HasFailures() bool {
	return r.Missing > 0 || r.SizeMismatch > 0 || r.MD5Mismatch > 0 || r.CRCMismatch > 0 || r.Errors > 0
}

type verifyJob struct {
	vid  uint32
	rec  *NeedleRecord
	path string
}

// RunVerify compares source volume needles against filer HTTP paths.
func RunVerify(cfg VerifyConfig) (*VerifyReport, error) {
	if err := cfg.Normalize(); err != nil {
		return nil, err
	}
	var tmpl *PathTemplate
	if !cfg.IsBlob() {
		var err error
		tmpl, err = NewPathTemplate(cfg.PathTemplate)
		if err != nil {
			return nil, err
		}
	}
	var blob *BlobClient
	if cfg.IsBlob() {
		blob = NewBlobClient(Config{Masters: cfg.Masters, GrpcDial: cfg.GrpcDial})
	}
	refs, skipped, err := DiscoverVolumes(cfg.Dir, cfg.Collection, cfg.VolumeIDs)
	if err != nil {
		return nil, err
	}
	for _, s := range skipped {
		fmt.Fprintf(os.Stderr, "skip volume: %s\n", s)
	}
	if len(refs) == 0 {
		return nil, fmt.Errorf("no volumes found under %s collection=%q", cfg.Dir, cfg.Collection)
	}

	filer := NewFilerHTTP((&Config{Filer: cfg.Filer}).FilerBases(), "", "", "", "")
	report := &VerifyReport{Level: cfg.Level.String()}
	for _, ref := range refs {
		report.Volumes = append(report.Volumes, uint32(ref.VolumeID))
	}

	jobs := make(chan verifyJob, cfg.Workers*2)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		checked  atomic.Int64
		okN      atomic.Int64
		missing  atomic.Int64
		sizeMis  atomic.Int64
		md5Mis   atomic.Int64
		crcMis   atomic.Int64
		errorsN  atomic.Int64
		limitHit atomic.Bool
	)

	addDetail := func(d VerifyDetail) {
		mu.Lock()
		if len(report.Details) < 10000 {
			report.Details = append(report.Details, d)
		}
		mu.Unlock()
	}

	worker := func() {
		defer wg.Done()
		for job := range jobs {
			if cfg.Limit > 0 && checked.Load() >= int64(cfg.Limit) {
				limitHit.Store(true)
				continue
			}
			checked.Add(1)
			ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout(job.rec.DataSize)+30*time.Second)
			var result string
			var detail VerifyDetail
			if cfg.IsBlob() {
				result, detail = compareBlob(ctx, blob, cfg.Level, job)
			} else {
				result, detail = compareOne(ctx, filer, cfg.Level, job)
			}
			cancel()
			switch result {
			case "ok":
				okN.Add(1)
			case "missing":
				missing.Add(1)
				addDetail(detail)
			case "size_mismatch":
				sizeMis.Add(1)
				addDetail(detail)
			case "md5_mismatch":
				md5Mis.Add(1)
				addDetail(detail)
			case "crc_mismatch":
				crcMis.Add(1)
				addDetail(detail)
			default:
				errorsN.Add(1)
				addDetail(detail)
			}
		}
	}

	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go worker()
	}

	var successFilter map[uint32]map[string]struct{}
	if cfg.OnlySuccess {
		successFilter = map[uint32]map[string]struct{}{}
	}

scanLoop:
	for _, ref := range refs {
		vid := uint32(ref.VolumeID)
		var successSet map[string]struct{}
		if cfg.OnlySuccess {
			set, err := LoadSuccessFIDs(cfg.StateDir, vid)
			if err != nil {
				close(jobs)
				wg.Wait()
				return nil, err
			}
			successSet = set
			successFilter[vid] = set
		}

		err = ScanVolume(ref, 0, cfg.Newer, cfg.HasNewer, cfg.Decompress,
			func(_ uint64, rec *NeedleRecord) error {
				if rec == nil {
					return nil
				}
				if cfg.Limit > 0 && checked.Load() >= int64(cfg.Limit) {
					limitHit.Store(true)
					return ioEOF
				}
				if successSet != nil {
					if _, ok := successSet[rec.FidPathSafe]; !ok {
						return nil
					}
				}
				var path string
				if cfg.IsBlob() {
					path = rec.Fid
				} else {
					var err error
					path, err = tmpl.Render(PathContext{
						Fid:         rec.Fid,
						FidPathSafe: rec.FidPathSafe,
						VolumeID:    vid,
						Collection:  cfg.Collection,
						Modified:    rec.Modified,
						TimeZone:    cfg.TimeZone,
					})
					if err != nil {
						checked.Add(1)
						errorsN.Add(1)
						addDetail(VerifyDetail{VID: vid, FID: rec.FidPathSafe, Result: "error", Err: err.Error()})
						return nil
					}
				}
				cp := *rec
				cp.Data = append([]byte(nil), rec.Data...)
				jobs <- verifyJob{vid: vid, rec: &cp, path: path}
				return nil
			})
		if err != nil && err != ioEOF {
			close(jobs)
			wg.Wait()
			return nil, fmt.Errorf("scan volume %d: %w", ref.VolumeID, err)
		}
		if limitHit.Load() {
			break scanLoop
		}
		glog.V(0).Infof("verify scanned volume %d", ref.VolumeID)
		_ = successFilter
	}

	close(jobs)
	wg.Wait()

	report.Checked = checked.Load()
	report.OK = okN.Load()
	report.Missing = missing.Load()
	report.SizeMismatch = sizeMis.Load()
	report.MD5Mismatch = md5Mis.Load()
	report.CRCMismatch = crcMis.Load()
	report.Errors = errorsN.Load()

	if cfg.ReportPath != "" {
		b, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return report, err
		}
		if err := os.WriteFile(cfg.ReportPath, b, 0644); err != nil {
			return report, err
		}
	}
	return report, nil
}

func compareBlob(ctx context.Context, blob *BlobClient, level VerifyLevel, job verifyJob) (string, VerifyDetail) {
	detail := VerifyDetail{
		VID:     job.vid,
		FID:     job.rec.FidPathSafe,
		Path:    job.path,
		SrcSize: job.rec.DataSize,
		SrcCRC:  NeedleCRCHex(job.rec.Checksum),
	}
	exists, dstSize, etag, err := blob.Head(ctx, job.path)
	if err != nil {
		detail.Result = "error"
		detail.Err = err.Error()
		return "error", detail
	}
	if !exists {
		detail.Result = "missing"
		return "missing", detail
	}
	detail.DstSize = dstSize
	detail.DstCRC = etag
	if level == VerifyExists {
		detail.Result = "ok"
		return "ok", detail
	}
	if dstSize >= 0 && dstSize != job.rec.DataSize {
		detail.Result = "size_mismatch"
		return "size_mismatch", detail
	}
	if level == VerifySize {
		detail.Result = "ok"
		return "ok", detail
	}
	if level == VerifyCRC {
		if etag == "" || etag != detail.SrcCRC {
			detail.Result = "crc_mismatch"
			detail.Err = FormatCRCMismatch(detail.SrcCRC, etag)
			return "crc_mismatch", detail
		}
		detail.Result = "ok"
		return "ok", detail
	}
	body, status, err := blob.Fetch(ctx, job.path)
	if err != nil || status == 404 {
		detail.Result = "error"
		if err != nil {
			detail.Err = err.Error()
		} else {
			detail.Err = "not found"
		}
		return "error", detail
	}
	dstDig := DigestOf(body)
	srcDig := DigestOf(job.rec.Data)
	detail.DstSize = dstDig.Size
	detail.SrcMD5 = srcDig.Hex()
	detail.DstMD5 = dstDig.Hex()
	if dstDig.Size != srcDig.Size {
		detail.Result = "size_mismatch"
		return "size_mismatch", detail
	}
	if srcDig.MD5 != dstDig.MD5 {
		detail.Result = "md5_mismatch"
		detail.Err = FormatMD5Mismatch(srcDig, dstDig)
		return "md5_mismatch", detail
	}
	detail.Result = "ok"
	return "ok", detail
}

func compareOne(ctx context.Context, filer *FilerHTTP, level VerifyLevel, job verifyJob) (string, VerifyDetail) {
	detail := VerifyDetail{
		VID:     job.vid,
		FID:     job.rec.FidPathSafe,
		Path:    job.path,
		SrcSize: job.rec.DataSize,
	}
	exists, dstSize, err := filer.Exists(ctx, job.path)
	if err != nil {
		detail.Result = "error"
		detail.Err = err.Error()
		return "error", detail
	}
	if !exists {
		detail.Result = "missing"
		return "missing", detail
	}
	if level == VerifyExists {
		detail.Result = "ok"
		return "ok", detail
	}

	srcDig := DigestOf(job.rec.Data)
	detail.SrcSize = srcDig.Size
	detail.DstSize = dstSize
	if dstSize >= 0 && dstSize != srcDig.Size {
		detail.Result = "size_mismatch"
		return "size_mismatch", detail
	}
	if level == VerifySize && dstSize >= 0 {
		detail.Result = "ok"
		return "ok", detail
	}

	// size without Content-Length, or md5: download body from filer
	body, err := filer.Download(ctx, job.path)
	if err != nil {
		detail.Result = "error"
		detail.Err = err.Error()
		return "error", detail
	}
	dstDig := DigestOf(body)
	detail.DstSize = dstDig.Size
	if dstDig.Size != srcDig.Size {
		detail.Result = "size_mismatch"
		return "size_mismatch", detail
	}
	if level == VerifySize {
		detail.Result = "ok"
		return "ok", detail
	}
	detail.SrcMD5 = srcDig.Hex()
	detail.DstMD5 = dstDig.Hex()
	if srcDig.MD5 != dstDig.MD5 {
		detail.Result = "md5_mismatch"
		detail.Err = FormatMD5Mismatch(srcDig, dstDig)
		return "md5_mismatch", detail
	}
	detail.Result = "ok"
	return "ok", detail
}

// FormatVerifyText prints a human summary.
func FormatVerifyText(r *VerifyReport) string {
	return fmt.Sprintf(
		"level=%s volumes=%v checked=%d ok=%d missing=%d size_mismatch=%d crc_mismatch=%d md5_mismatch=%d errors=%d\n",
		r.Level, r.Volumes, r.Checked, r.OK, r.Missing, r.SizeMismatch, r.CRCMismatch, r.MD5Mismatch, r.Errors,
	)
}
