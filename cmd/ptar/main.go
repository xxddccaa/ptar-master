package main

import (
	"github.com/alecthomas/kingpin/v2"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
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

	// NewArchive(inputpath string, outputpath string, tarthreads int, compression string, index bool) (*Archive)
	arch := ptar.NewArchive(config.Input, config.Prefix, config.Threads, config.Compression, config.Index)
	arch.Verbose = config.Verbose
	arch.StatsEverySeconds = config.StatsEvery
	arch.PprofAddr = config.PprofAddr
	sc := scanner.NewScanner()
	sc.ScanWorkers = config.ScanWorkers
	arch.Scanner = sc
	arch.Indexer = index.NewIndex
	arch.FileMaker = RegularFileCreate
	arch.Begin()
}
