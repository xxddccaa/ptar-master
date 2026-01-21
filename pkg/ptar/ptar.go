package ptar

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha1"
	"encoding/hex"
	"fmt"

	"github.com/pierrec/lz4"
	index "github.com/zgiles/ptar/pkg/index"
	// "github.com/zgiles/ptar/pkg/scanner"
	"github.com/zgiles/ptar/pkg/writecounter"

	// xz "github.com/remyoudompheng/go-liblzma"
	"io"
	"os"
	"runtime"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Indexer interface {
	IndexWriter(io.WriteCloser)
	Close()
	Channel() chan index.IndexItem
}

type Scanner interface {
	Scan(string, chan string, chan error)
}

/*
type Partition struct {
	filename string
	entries  chan string
}
*/

type Archive struct {
	InputPath    string
	OutputPath   string
	TarThreads   int
	TarMaxSize   int
	Compression  string
	Index        bool
	Verbose      bool
	StatsEverySeconds int
	PprofAddr         string
	Scanner      Scanner
	Indexer      func() *index.Index
	FileMaker    func(string) (io.WriteCloser, error)
	globalwg     *sync.WaitGroup
	scanwg       *sync.WaitGroup
	partitionswg *sync.WaitGroup
	entries      chan string
	errors       chan error

	stats *archiveStats
}

type archiveStats struct {
	start time.Time

	entriesRead uint64
	filesReg    uint64
	bytesWritten uint64
	lstatErr    uint64
	openErr     uint64
}

func NewArchive(inputpath string, outputpath string, tarthreads int, compression string, index bool) *Archive {
	arch := &Archive{
		InputPath:   inputpath,
		OutputPath:  outputpath,
		TarThreads:  tarthreads,
		Compression: compression,
		Index:       index,
	}
	// need to probably do a default scanner
	// 	ScanFunc:    scanner.Scan,
	return arch
}

func (arch *Archive) Begin() {
	arch.globalwg = new(sync.WaitGroup)
	arch.scanwg = new(sync.WaitGroup)
	arch.partitionswg = new(sync.WaitGroup)
	// 增大缓冲区到 100000，减少 channel 锁竞争，避免 CPU 热点
	arch.entries = make(chan string, 100000)
	arch.errors = make(chan error, 1024)
	arch.stats = &archiveStats{start: time.Now()}

	if arch.Scanner == nil {
		return
	}

	// Optional: pprof server for profiling (cpu/block/trace) while running.
	if arch.PprofAddr != "" {
		go func() {
			_ = http.ListenAndServe(arch.PprofAddr, nil)
		}()
	}

	// Optional: periodic stats
	if arch.StatsEverySeconds > 0 {
		arch.globalwg.Add(1)
		go arch.statsReporter(time.Duration(arch.StatsEverySeconds) * time.Second)
	}

	arch.globalwg.Add(1)
	arch.scanwg.Add(1)
	go func() {
		arch.Scanner.Scan(arch.InputPath, arch.entries, arch.errors)
		arch.scanwg.Done()
		arch.globalwg.Done()
	}()

	arch.globalwg.Add(1)
	go arch.errornotice()

	// for all partitions
	// arch.globalwg.Add(1)
	for i := 0; i < arch.TarThreads; i++ {
		arch.partitionswg.Add(1)
		arch.globalwg.Add(1)
		go arch.tarChannel(i)
	}
	// go channelcounter(wg, "files", entries)
	arch.scanwg.Wait()
	// arch.globalwg.Done()
	arch.partitionswg.Wait()

	close(arch.errors)
	arch.globalwg.Wait()
}

func (arch *Archive) statsReporter(every time.Duration) {
	defer arch.globalwg.Done()
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	var lastEntries, lastFiles, lastBytes uint64
	var m runtime.MemStats

	for range ticker.C {
		entries := atomic.LoadUint64(&arch.stats.entriesRead)
		files := atomic.LoadUint64(&arch.stats.filesReg)
		bytes := atomic.LoadUint64(&arch.stats.bytesWritten)
		lstatErr := atomic.LoadUint64(&arch.stats.lstatErr)
		openErr := atomic.LoadUint64(&arch.stats.openErr)

		sec := every.Seconds()
		dEntries := float64(entries - lastEntries) / sec
		dFiles := float64(files - lastFiles) / sec
		dMB := float64(bytes-lastBytes) / (1024 * 1024) / sec

		lastEntries, lastFiles, lastBytes = entries, files, bytes
		runtime.ReadMemStats(&m)

		// Scanner stats if available (best-effort type assertion).
		var scanDirs, scanEntries uint64
		type scanStats interface{ ScanStats() (dirs uint64, entries uint64) }
		if ss, ok := arch.Scanner.(scanStats); ok {
			scanDirs, scanEntries = ss.ScanStats()
			_ = scanEntries
		}

		fmt.Printf(
			"STATS uptime=%s goroutines=%d entries=%.0f/s files=%.0f/s write=%.2fMiB/s totalEntries=%d totalFiles=%d totalWrite=%.2fGiB lstatErr=%d openErr=%d scanDirs=%d heap=%.2fGiB\n",
			time.Since(arch.stats.start).Truncate(time.Second),
			runtime.NumGoroutine(),
			dEntries, dFiles, dMB,
			entries, files,
			float64(bytes)/(1024*1024*1024),
			lstatErr, openErr,
			scanDirs,
			float64(m.HeapAlloc)/(1024*1024*1024),
		)
	}
}

func channelcounter(wg *sync.WaitGroup, t string, c chan string) {
	counter := 0
	for {
		_, ok := <-c
		if !ok {
			break
		} else {
			counter++
		}
	}
	fmt.Printf("number of %s: %d\n", t, counter)
	wg.Done()
}

func (arch *Archive) errornotice() {
	for {
		err, ok := <-arch.errors
		if !ok {
			break
		} else {
			if err != nil {
				fmt.Printf("ERROR: %s\n", err)
			}
		}
	}
	arch.globalwg.Done()
}

func (arch *Archive) tarChannel(threadnum int) {
	defer arch.partitionswg.Done()
	defer arch.globalwg.Done()
	var r io.WriteCloser
	var idx *index.Index
	var ientries chan index.IndexItem
	// ientries := make(chan IndexItem, 1024)
	indexwg := new(sync.WaitGroup)

	filename := arch.OutputPath + "." + strconv.Itoa(threadnum) + ".tar" + arch.Compression
	f, ferr := arch.FileMaker(filename)
	if ferr != nil {
		panic(ferr)
	}
	// defer f.Sync()
	defer f.Close()

	switch arch.Compression {
	case "gz":
		var err error
		r = gzip.NewWriter(f)
		if err != nil {
			panic(err)
		}
		defer r.Close()
	case "lz4":
		r = lz4.NewWriter(f)
		defer r.Close()
	case "":
		r = f
	default:
		panic("Not implemented")
	}

	if arch.Index {
		idx = arch.Indexer()
		indexwg.Add(1)
		f, ferr := arch.FileMaker(filename + ".index")
		if ferr != nil {
			panic(ferr)
		}
		ientries = idx.Channel()
		go func() {
			idx.IndexWriter(f)
			indexwg.Done()
		}()
		// go indexwriter(indexwg, filename+".index", ientries)
	}

	cw := writecounter.NewWriteCounter(r)
	tw := tar.NewWriter(cw)
	defer tw.Close()

	// Bigger buffer reduces syscall overhead for large file copies.
	// Keep per-goroutine buffer to avoid contention.
	copyBuf := make([]byte, 256*1024)

	for {
		i, ok := <-arch.entries
		if !ok {
			break
		}
		atomic.AddUint64(&arch.stats.entriesRead, 1)
		if arch.Verbose {
			fmt.Printf("%s\n", i)
		}
		s, serr := os.Lstat(i)
		if serr != nil {
			atomic.AddUint64(&arch.stats.lstatErr, 1)
			arch.errors <- serr
			panic(serr)
		}

		var ientry index.IndexItem
		if arch.Index {
			ientry = index.IndexItem{Name: i}
		}

		var link string
		var linkerr error
		if s.Mode()&os.ModeSymlink != 0 {
			link, linkerr = os.Readlink(i)
			if linkerr != nil {
				panic(linkerr)
			}
		}

		hdr, err := tar.FileInfoHeader(s, link)
		if err != nil {
			panic(linkerr)
		}
		hdr.Name = i
		hdr.Format = tar.FormatGNU
		if hdr.Typeflag == tar.TypeDir {
			hdr.Name += "/"
		}

		if arch.Index {
			ientry.Pos = cw.Pos()
		}

		if err := tw.WriteHeader(hdr); err != nil {
			arch.errors <- err
			panic(err)
		}

		// Only call Write if it's a regular file; all others are invalid
		if hdr.Typeflag == tar.TypeReg {
			atomic.AddUint64(&arch.stats.filesReg, 1)
			var hash hashWriter
			if arch.Index {
				hash = sha1.New()
			}
			sf, sferr := os.Open(i)
			if sferr != nil {
				atomic.AddUint64(&arch.stats.openErr, 1)
				arch.errors <- sferr
				panic(sferr)
			}
			// Read -> (optional hash) -> tar writer
			// Note: tar stream itself is sequential per partition file; this keeps it efficient.
			before := cw.Pos()
			if arch.Index {
				_, _ = io.CopyBuffer(io.MultiWriter(tw, hash), sf, copyBuf)
			} else {
				_, _ = io.CopyBuffer(tw, sf, copyBuf)
			}
			after := cw.Pos()
			if after >= before {
				atomic.AddUint64(&arch.stats.bytesWritten, after-before)
			}
			closeerr := sf.Close()
			if closeerr != nil {
				panic(closeerr)
			}
			if arch.Index {
				ientry.Hash = hex.EncodeToString(hash.Sum(nil))
			}
		}

		if arch.Index {
			ientry.Size = cw.Pos() - ientry.Pos
			ientries <- ientry
			// log.Printf("%v", ientry)
		}
	}
	if arch.Index {
		idx.Close()
	}
	indexwg.Wait()
	// fmt.Printf("Pre Write Pos: %d\n", cw.Pos())
	// fmt.Printf("Closing Write Pos: %d\n", cw.Pos())
	// close(ientries)
}

// Minimal interface we need from hash.Hash (to avoid importing hash just for the type name in multiple places).
type hashWriter interface {
	io.Writer
	Sum([]byte) []byte
}
