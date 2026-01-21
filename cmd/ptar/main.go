package main

import (
	"fmt"

	"github.com/alecthomas/kingpin/v2"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/zgiles/ptar/pkg/index"
	"github.com/zgiles/ptar/pkg/ptar"
	"github.com/zgiles/ptar/pkg/scanner"
)

type rootConfig struct {
	Compression string
	Prefix      string
	Input       string
	Debug       bool
	Index       bool
	Threads     int
	ScanWorkers int // 并行扫描worker数量
	Verbose     bool
	GOGCPercent int
	GOMAXProcs  int
	Create      bool
	StatsEvery  int
	PprofAddr   string
	MaxSize     string // 每个tar文件的最大大小，如 "20G"
}

func RegularFileCreate(filename string) (io.WriteCloser, error) {
	// Auto-create parent directories for output files, so users don't need to mkdir -p manually.
	if dir := filepath.Dir(filename); dir != "." && dir != "/" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return os.Create(filename)
}

var version string
var config rootConfig
var logger *log.Logger

// parseSize 解析大小字符串，如 "20G", "100M", "1T" 等
func parseSize(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(sizeStr)
	if len(sizeStr) == 0 {
		return 0, nil
	}

	// 获取单位
	lastChar := strings.ToUpper(sizeStr[len(sizeStr)-1:])
	var multiplier int64 = 1
	var numStr string

	if lastChar >= "0" && lastChar <= "9" {
		// 没有单位，默认字节
		numStr = sizeStr
	} else {
		numStr = sizeStr[:len(sizeStr)-1]
		switch lastChar {
		case "K":
			multiplier = 1024
		case "M":
			multiplier = 1024 * 1024
		case "G":
			multiplier = 1024 * 1024 * 1024
		case "T":
			multiplier = 1024 * 1024 * 1024 * 1024
		default:
			return 0, fmt.Errorf("unknown unit: %s", lastChar)
		}
	}

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, err
	}

	return int64(float64(multiplier) * num), nil
}

func main() {
	/*
		Example Desired end state
		./ptar \
		--partition dirdepth2,unixgroup \
		--input /things \
		--output ./files/ \
		--compression gzip \
		--parallel 2 \
		--maxoutputsize 5TB \
		--manifest yes
	*/

	config = rootConfig{}

	app := kingpin.New("ptar", "Parallel Tar")
	app.UsageTemplate(kingpin.CompactUsageTemplate)
	app.Flag("create", "Create").Short('c').BoolVar(&config.Create)
	app.Flag("threads", "Threads (tar writing workers)").Short('t').Default("16").IntVar(&config.Threads)
	app.Flag("scan-workers", "Scan workers (parallel directory scanning, 0=auto)").Default("0").IntVar(&config.ScanWorkers)
	app.Flag("debug", "Enable debug output").BoolVar(&config.Debug)
	app.Flag("verbose", "Verbose Mode").Short('v').BoolVar(&config.Verbose)
	app.Flag("gogcpercent", "GO GC Percent").Default("0").IntVar(&config.GOGCPercent)
	app.Flag("gomaxprocs", "GO Max Procs").Default("0").IntVar(&config.GOMAXProcs)
	app.Flag("stats-interval", "Print runtime stats every N seconds (0=disable)").Default("0").IntVar(&config.StatsEvery)
	app.Flag("pprof", "Enable pprof http server, e.g. :6060 (empty=disable)").Default("").StringVar(&config.PprofAddr)
	app.Flag("compression", "Compression type").HintOptions("gz", "gzip", "lz4", "none").StringVar(&config.Compression)
	app.Flag("file", "(File) Prefix to use for output files. Ex: output => output.tar.gz").Required().Short('f').StringVar(&config.Prefix)
	app.Flag("index", "Enable Index output").BoolVar(&config.Index)
	app.Flag("max-size", "Maximum size per tar file (e.g., 20G, 100M). When exceeded, switches to new tar file. 0=unlimited (default: one tar per thread)").StringVar(&config.MaxSize)
	app.Arg("input", "Input Path(s)").Required().StringVar(&config.Input)
	app.Version(version)
	kingpin.MustParse(app.Parse(os.Args[1:]))

	// Normalize compression values (CLI hints historically used "gzip", while the archive expects "gz").
	switch strings.ToLower(strings.TrimSpace(config.Compression)) {
	case "gzip":
		config.Compression = "gz"
	case "none":
		config.Compression = ""
	}

	if config.Debug {
		log.Println("Config:")
		log.Println("  Parallel: ", config.Threads)
		log.Println("  Scan Workers: ", config.ScanWorkers)
		log.Println("  Compression: ", config.Compression)
		log.Println("  Prefix: ", config.Prefix)
		log.Println("  Input: ", config.Input)
		log.Println("  Debug: ", config.Debug)
		log.Println("  Indexes: ", config.Index)
	}

	// Apply runtime tuning flags (if provided).
	if config.GOMAXProcs > 0 {
		runtime.GOMAXPROCS(config.GOMAXProcs)
	}
	if config.GOGCPercent > 0 {
		debug.SetGCPercent(config.GOGCPercent)
	}

	// Parse max-size if provided
	var maxSizeBytes int64 = 0
	if config.MaxSize != "" && config.MaxSize != "0" {
		var err error
		maxSizeBytes, err = parseSize(config.MaxSize)
		if err != nil {
			log.Fatalf("Invalid max-size format: %v (examples: 20G, 100M, 1T)", err)
		}
	}

	// NewArchive(inputpath string, outputpath string, tarthreads int, compression string, index bool) (*Archive)
	arch := ptar.NewArchive(config.Input, config.Prefix, config.Threads, config.Compression, config.Index)
	arch.Verbose = config.Verbose
	arch.StatsEverySeconds = config.StatsEvery
	if arch.StatsEverySeconds == 0 {
		// 默认每2秒输出一次统计信息
		arch.StatsEverySeconds = 2
	}
	arch.PprofAddr = config.PprofAddr
	arch.TarMaxSize = maxSizeBytes
	sc := scanner.NewScanner()
	sc.ScanWorkers = config.ScanWorkers
	arch.Scanner = sc
	arch.Indexer = index.NewIndex
	arch.FileMaker = RegularFileCreate
	arch.Begin()
}
