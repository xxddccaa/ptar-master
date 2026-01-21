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

// tarFileManager 管理共享的文件编号分配（无锁设计，每个线程独立写入）
type tarFileManager struct {
	outputPath    string
	compression   string
	fileMaker     func(string) (io.WriteCloser, error)
	indexer       func() *index.Index
	indexEnabled  bool
	maxSize       int64 // 0表示不限制
	fileCounter   int64 // 全局文件计数器（使用atomic，无锁）
}

// threadTarFile 表示线程的当前tar文件（每个线程独立，无锁写入）
type threadTarFile struct {
	filename      string
	file          io.WriteCloser
	writer        io.WriteCloser // 可能是压缩writer
	tarWriter     *tar.Writer
	writeCounter  *writecounter.WriteCounter
	index         *index.Index
	indexEntries  chan index.IndexItem
	indexFile     io.WriteCloser
	indexWg       sync.WaitGroup
	fileNum       int64
	createdAt     time.Time
	mgr           *tarFileManager // 指向管理器，用于获取新文件编号
}

func newTarFileManager(outputPath, compression string, maxSize int64, fileMaker func(string) (io.WriteCloser, error), indexer func() *index.Index, indexEnabled bool) *tarFileManager {
	return &tarFileManager{
		outputPath:   outputPath,
		compression:  compression,
		fileMaker:    fileMaker,
		indexer:      indexer,
		indexEnabled: indexEnabled,
		maxSize:      maxSize,
		fileCounter:  0,
	}
}

// allocateFileNumber 分配新的文件编号（无锁，使用atomic）
func (m *tarFileManager) allocateFileNumber() int64 {
	return atomic.AddInt64(&m.fileCounter, 1) - 1
}

// createNewFile 为线程创建新的tar文件（线程独立调用，无锁）
func (m *tarFileManager) createNewFile() (*threadTarFile, error) {
	fileNum := m.allocateFileNumber()
	// 文件编号从 0 开始，但输出时固定 5 位补零：0->00000, 1->00001
	filename := fmt.Sprintf("%s.%05d.tar%s", m.outputPath, fileNum, m.compression)

	f, err := m.fileMaker(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create tar file %s: %v", filename, err)
	}

	var writer io.WriteCloser = f
	switch m.compression {
	case "gz":
		writer = gzip.NewWriter(f)
	case "lz4":
		writer = lz4.NewWriter(f)
	case "":
		writer = f
	default:
		f.Close()
		return nil, fmt.Errorf("unsupported compression: %s", m.compression)
	}

	cw := writecounter.NewWriteCounter(writer)
	tw := tar.NewWriter(cw)

	ttf := &threadTarFile{
		filename:     filename,
		file:         f,
		writer:       writer,
		tarWriter:    tw,
		writeCounter: cw,
		fileNum:      fileNum,
		createdAt:    time.Now(),
		mgr:          m,
	}

	if m.indexEnabled {
		idx := m.indexer()
		indexFilename := filename + ".index"
		indexFile, err := m.fileMaker(indexFilename)
		if err != nil {
			// 清理已创建的文件
			tw.Close()
			if writer != f {
				writer.Close()
			}
			f.Close()
			return nil, fmt.Errorf("failed to create index file %s: %v", indexFilename, err)
		}
		ttf.index = idx
		ttf.indexEntries = idx.Channel()
		ttf.indexFile = indexFile
		ttf.indexWg.Add(1)
		go func() {
			idx.IndexWriter(indexFile)
			ttf.indexWg.Done()
		}()
	}

	fmt.Printf("[TAR-FILE] Created new tar file: %s (file #%05d)\n", filename, fileNum)
	return ttf, nil
}

// closeFile 关闭线程的tar文件
func (ttf *threadTarFile) closeFile() {
	if ttf == nil {
		return
	}

	finalSize := ttf.writeCounter.Pos()
	duration := time.Since(ttf.createdAt)

	// 关闭tar writer
	ttf.tarWriter.Close()

	// 关闭压缩writer（如果不是文件本身）
	if ttf.writer != ttf.file {
		ttf.writer.Close()
	}

	// 关闭文件
	ttf.file.Close()

	// 关闭索引
	if ttf.index != nil {
		ttf.index.Close()
		ttf.indexWg.Wait()
	}

	fmt.Printf("[TAR-FILE] Closed tar file: %s (size=%.2fGiB, duration=%s, file #%d)\n",
		ttf.filename,
		float64(finalSize)/(1024*1024*1024),
		duration.Truncate(time.Second),
		ttf.fileNum)
}

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
	TarMaxSize   int64 // 改为int64以支持大文件大小
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

	stats         *archiveStats
	tarFileMgr    *tarFileManager // 共享的tar文件管理器
	useSharedPool bool            // 是否使用共享文件池（当TarMaxSize > 0时）
}

type archiveStats struct {
	start time.Time

	entriesRead uint64
	filesReg    uint64
	bytesWritten uint64
	lstatErr    uint64
	openErr     uint64

	// 新增：用于排查性能瓶颈的统计
	lstatTimeNs    uint64 // lstat 累计耗时（纳秒）
	openTimeNs     uint64 // open 累计耗时（纳秒）
	readTimeNs     uint64 // read 累计耗时（纳秒）
	lstatCount     uint64 // lstat 调用次数
	openCount      uint64 // open 调用次数
	readCount      uint64 // read 调用次数（文件读取次数）
	waitTimeNs     uint64 // 从 channel 等待条目的累计耗时（纳秒）
	waitCount      uint64 // 等待次数
}

func NewArchive(inputpath string, outputpath string, tarthreads int, compression string, index bool) *Archive {
	arch := &Archive{
		InputPath:   inputpath,
		OutputPath:  outputpath,
		TarThreads:  tarthreads,
		Compression: compression,
		Index:       index,
		TarMaxSize:  0, // 默认不限制
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

	// 如果设置了TarMaxSize，使用共享文件池模式
	arch.useSharedPool = arch.TarMaxSize > 0
	if arch.useSharedPool {
		arch.tarFileMgr = newTarFileManager(arch.OutputPath, arch.Compression, arch.TarMaxSize, arch.FileMaker, arch.Indexer, arch.Index)
		fmt.Printf("[INIT] Using shared tar file pool with max-size=%d bytes (%.2fGiB)\n", arch.TarMaxSize, float64(arch.TarMaxSize)/(1024*1024*1024))
	} else {
		fmt.Printf("[INIT] Using per-thread tar files (threads=%d)\n", arch.TarThreads)
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

	// 注意：在共享池模式下，每个线程会自己关闭文件，这里不需要额外操作
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
		var scanSpeed float64
		var scanWorkers int
		var queuedEntries int64 = 0 // 已扫描但未写入的条目数（队列中等待的）
		type scanStats interface{ ScanStats() (dirs uint64, entries uint64) }
		type scanWorkersGetter interface{ GetScanWorkers() int }
		if ss, ok := arch.Scanner.(scanStats); ok {
			scanDirs, scanEntries = ss.ScanStats()
			// 计算扫描速度（每秒扫描的条目数）
			if sec > 0 {
				scanSpeed = float64(scanEntries) / time.Since(arch.stats.start).Seconds()
			}
			// 计算队列中等待的条目数：已扫描 - 已写入
			if scanEntries >= entries {
				queuedEntries = int64(scanEntries - entries)
			}
		}
		if swg, ok := arch.Scanner.(scanWorkersGetter); ok {
			scanWorkers = swg.GetScanWorkers()
		}

		// 获取当前tar文件信息（如果使用共享池）
		// 注意：由于每个线程独立管理文件，这里只显示文件总数
		var currentTarInfo string
		if arch.useSharedPool && arch.tarFileMgr != nil {
			totalFiles := atomic.LoadInt64(&arch.tarFileMgr.fileCounter)
			currentTarInfo = fmt.Sprintf(" totalTarFiles=%d", totalFiles)
		}

		// 新增性能统计
		lstatTime := atomic.LoadUint64(&arch.stats.lstatTimeNs)
		openTime := atomic.LoadUint64(&arch.stats.openTimeNs)
		readTime := atomic.LoadUint64(&arch.stats.readTimeNs)
		lstatCount := atomic.LoadUint64(&arch.stats.lstatCount)
		openCount := atomic.LoadUint64(&arch.stats.openCount)
		readCount := atomic.LoadUint64(&arch.stats.readCount)
		waitTime := atomic.LoadUint64(&arch.stats.waitTimeNs)
		waitCount := atomic.LoadUint64(&arch.stats.waitCount)

		// 计算平均值
		var avgLstatMs, avgOpenMs, avgReadMs, avgWaitMs float64
		var avgFileSizeMB float64
		if lstatCount > 0 {
			avgLstatMs = float64(lstatTime) / float64(lstatCount) / 1e6 // 纳秒转毫秒
		}
		if openCount > 0 {
			avgOpenMs = float64(openTime) / float64(openCount) / 1e6
		}
		if readCount > 0 {
			avgReadMs = float64(readTime) / float64(readCount) / 1e6
		}
		if waitCount > 0 {
			avgWaitMs = float64(waitTime) / float64(waitCount) / 1e6
		}
		if files > 0 {
			avgFileSizeMB = float64(bytes) / float64(files) / (1024 * 1024)
		}

		// 获取 entries channel 的实际长度（队列积压）
		channelLen := len(arch.entries)

		fmt.Printf(
			"STATS uptime=%s goroutines=%d threads=%d scanWorkers=%d entries=%.0f/s files=%.0f/s write=%.2fMiB/s totalEntries=%d totalFiles=%d totalWrite=%.2fGiB lstatErr=%d openErr=%d scanDirs=%d scanSpeed=%.0f/s queuedEntries=%d channelLen=%d avgFileSize=%.2fMB avgLstat=%.2fms avgOpen=%.2fms avgRead=%.2fms avgWait=%.2fms heap=%.2fGiB%s\n",
			time.Since(arch.stats.start).Truncate(time.Second),
			runtime.NumGoroutine(),
			arch.TarThreads,
			scanWorkers,
			dEntries, dFiles, dMB,
			entries, files,
			float64(bytes)/(1024*1024*1024),
			lstatErr, openErr,
			scanDirs,
			scanSpeed,
			queuedEntries,
			channelLen,
			avgFileSizeMB,
			avgLstatMs,
			avgOpenMs,
			avgReadMs,
			avgWaitMs,
			float64(m.HeapAlloc)/(1024*1024*1024),
			currentTarInfo,
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

	// Bigger buffer reduces syscall overhead for large file copies.
	// Keep per-goroutine buffer to avoid contention.
	copyBuf := make([]byte, 256*1024)

	// 统计信息
	filesProcessed := uint64(0)
	bytesProcessed := uint64(0)
	threadStartTime := time.Now()

	if arch.useSharedPool {
		// 共享文件池模式：所有线程共享文件管理器
		arch.tarChannelShared(threadnum, copyBuf, &filesProcessed, &bytesProcessed, threadStartTime)
	} else {
		// 原有模式：每个线程创建自己的文件
		arch.tarChannelPerThread(threadnum, copyBuf, &filesProcessed, &bytesProcessed, threadStartTime)
	}

	duration := time.Since(threadStartTime)
	fmt.Printf("[THREAD-%d] Completed: files=%d bytes=%.2fGiB duration=%s\n",
		threadnum, filesProcessed, float64(bytesProcessed)/(1024*1024*1024), duration.Truncate(time.Second))
}

// tarChannelShared 共享文件池模式的tar处理（每个线程独立写入，无锁）
func (arch *Archive) tarChannelShared(threadnum int, copyBuf []byte, filesProcessed *uint64, bytesProcessed *uint64, startTime time.Time) {
	var currentFile *threadTarFile

	for {
		// 记录从 channel 等待的时间
		waitStart := time.Now()
		i, ok := <-arch.entries
		waitDuration := time.Since(waitStart)
		if waitDuration > 0 {
			atomic.AddUint64(&arch.stats.waitTimeNs, uint64(waitDuration.Nanoseconds()))
			atomic.AddUint64(&arch.stats.waitCount, 1)
		}
		if !ok {
			break
		}

		// 检查是否需要创建新文件或切换文件
		if currentFile == nil {
			// 创建第一个文件
			var err error
			currentFile, err = arch.tarFileMgr.createNewFile()
			if err != nil {
				arch.errors <- err
				panic(err)
			}
		} else if arch.tarFileMgr.maxSize > 0 {
			// 检查当前文件是否达到大小限制
			currentSize := int64(currentFile.writeCounter.Pos())
			if currentSize >= arch.tarFileMgr.maxSize {
				// 关闭当前文件
				currentFile.closeFile()
				// 创建新文件
				var err error
				currentFile, err = arch.tarFileMgr.createNewFile()
				if err != nil {
					arch.errors <- err
					panic(err)
				}
			}
		}

		// 处理文件条目（无锁，因为每个线程有自己的tar writer）
		arch.processEntryShared(i, currentFile, copyBuf, filesProcessed, bytesProcessed)
	}

	// 关闭当前文件
	if currentFile != nil {
		currentFile.closeFile()
	}
}

// processEntryShared 在共享模式下处理单个条目（无锁，每个线程独立写入）
func (arch *Archive) processEntryShared(entryPath string, ttf *threadTarFile, copyBuf []byte, filesProcessed *uint64, bytesProcessed *uint64) {
	atomic.AddUint64(&arch.stats.entriesRead, 1)
	if arch.Verbose {
		fmt.Printf("%s\n", entryPath)
	}

	// 记录 lstat 耗时
	lstatStart := time.Now()
	s, serr := os.Lstat(entryPath)
	lstatDuration := time.Since(lstatStart)
	atomic.AddUint64(&arch.stats.lstatTimeNs, uint64(lstatDuration.Nanoseconds()))
	atomic.AddUint64(&arch.stats.lstatCount, 1)
	if serr != nil {
		atomic.AddUint64(&arch.stats.lstatErr, 1)
		arch.errors <- serr
		panic(serr)
	}

	var ientry index.IndexItem
	if arch.Index && ttf.index != nil {
		ientry = index.IndexItem{Name: entryPath}
	}

	var link string
	var linkerr error
	if s.Mode()&os.ModeSymlink != 0 {
		link, linkerr = os.Readlink(entryPath)
		if linkerr != nil {
			panic(linkerr)
		}
	}

	// 创建简化的 tar header，只保留必要信息（文件名、大小、类型），去掉时间戳、权限、uid/gid等
	hdr := arch.createMinimalTarHeader(s, entryPath, link)

	// 无锁写入（每个线程有自己的tar writer）
	if arch.Index && ttf.index != nil {
		ientry.Pos = ttf.writeCounter.Pos()
	}

	if err := ttf.tarWriter.WriteHeader(hdr); err != nil {
		arch.errors <- err
		panic(err)
	}

	// Only call Write if it's a regular file; all others are invalid
	if hdr.Typeflag == tar.TypeReg {
		atomic.AddUint64(&arch.stats.filesReg, 1)
		*filesProcessed++
		var hash hashWriter
		if arch.Index && ttf.index != nil {
			hash = sha1.New()
		}

		// 记录 open 耗时
		openStart := time.Now()
		sf, sferr := os.Open(entryPath)
		openDuration := time.Since(openStart)
		atomic.AddUint64(&arch.stats.openTimeNs, uint64(openDuration.Nanoseconds()))
		atomic.AddUint64(&arch.stats.openCount, 1)
		if sferr != nil {
			atomic.AddUint64(&arch.stats.openErr, 1)
			arch.errors <- sferr
			panic(sferr)
		}

		before := ttf.writeCounter.Pos()
		// 记录 read 耗时
		readStart := time.Now()
		if arch.Index && ttf.index != nil {
			_, _ = io.CopyBuffer(io.MultiWriter(ttf.tarWriter, hash), sf, copyBuf)
		} else {
			_, _ = io.CopyBuffer(ttf.tarWriter, sf, copyBuf)
		}
		readDuration := time.Since(readStart)
		atomic.AddUint64(&arch.stats.readTimeNs, uint64(readDuration.Nanoseconds()))
		atomic.AddUint64(&arch.stats.readCount, 1)
		after := ttf.writeCounter.Pos()

		closeerr := sf.Close()
		if closeerr != nil {
			panic(closeerr)
		}

		bytesWritten := after - before
		if bytesWritten > 0 {
			atomic.AddUint64(&arch.stats.bytesWritten, bytesWritten)
			*bytesProcessed += bytesWritten
		}

		if arch.Index && ttf.index != nil {
			ientry.Hash = hex.EncodeToString(hash.Sum(nil))
		}
	}

	if arch.Index && ttf.index != nil {
		ientry.Size = ttf.writeCounter.Pos() - ientry.Pos
		ttf.indexEntries <- ientry
	}
}

// tarChannelPerThread 原有模式：每个线程创建自己的文件
func (arch *Archive) tarChannelPerThread(threadnum int, copyBuf []byte, filesProcessed *uint64, bytesProcessed *uint64, startTime time.Time) {
	var r io.WriteCloser
	var idx *index.Index
	var ientries chan index.IndexItem
	indexwg := new(sync.WaitGroup)

	filename := arch.OutputPath + "." + strconv.Itoa(threadnum) + ".tar" + arch.Compression
	f, ferr := arch.FileMaker(filename)
	if ferr != nil {
		panic(ferr)
	}
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
		indexFile, ferr := arch.FileMaker(filename + ".index")
		if ferr != nil {
			panic(ferr)
		}
		ientries = idx.Channel()
		go func() {
			idx.IndexWriter(indexFile)
			indexwg.Done()
		}()
	}

	cw := writecounter.NewWriteCounter(r)
	tw := tar.NewWriter(cw)
	defer tw.Close()

	for {
		// 记录从 channel 等待的时间
		waitStart := time.Now()
		i, ok := <-arch.entries
		waitDuration := time.Since(waitStart)
		if waitDuration > 0 {
			atomic.AddUint64(&arch.stats.waitTimeNs, uint64(waitDuration.Nanoseconds()))
			atomic.AddUint64(&arch.stats.waitCount, 1)
		}
		if !ok {
			break
		}
		atomic.AddUint64(&arch.stats.entriesRead, 1)
		if arch.Verbose {
			fmt.Printf("%s\n", i)
		}
		// 记录 lstat 耗时
		lstatStart := time.Now()
		s, serr := os.Lstat(i)
		lstatDuration := time.Since(lstatStart)
		atomic.AddUint64(&arch.stats.lstatTimeNs, uint64(lstatDuration.Nanoseconds()))
		atomic.AddUint64(&arch.stats.lstatCount, 1)
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

		// 创建简化的 tar header，只保留必要信息（文件名、大小、类型），去掉时间戳、权限、uid/gid等
		hdr := arch.createMinimalTarHeader(s, i, link)

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
			*filesProcessed++
			var hash hashWriter
			if arch.Index {
				hash = sha1.New()
			}
			// 记录 open 耗时
			openStart := time.Now()
			sf, sferr := os.Open(i)
			openDuration := time.Since(openStart)
			atomic.AddUint64(&arch.stats.openTimeNs, uint64(openDuration.Nanoseconds()))
			atomic.AddUint64(&arch.stats.openCount, 1)
			if sferr != nil {
				atomic.AddUint64(&arch.stats.openErr, 1)
				arch.errors <- sferr
				panic(sferr)
			}
			before := cw.Pos()
			// 记录 read 耗时
			readStart := time.Now()
			if arch.Index {
				_, _ = io.CopyBuffer(io.MultiWriter(tw, hash), sf, copyBuf)
			} else {
				_, _ = io.CopyBuffer(tw, sf, copyBuf)
			}
			readDuration := time.Since(readStart)
			atomic.AddUint64(&arch.stats.readTimeNs, uint64(readDuration.Nanoseconds()))
			atomic.AddUint64(&arch.stats.readCount, 1)
			after := cw.Pos()
			bytesWritten := after - before
			if bytesWritten > 0 {
				atomic.AddUint64(&arch.stats.bytesWritten, bytesWritten)
				*bytesProcessed += bytesWritten
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
		}
	}
	if arch.Index {
		idx.Close()
	}
	indexwg.Wait()
}

// Minimal interface we need from hash.Hash (to avoid importing hash just for the type name in multiple places).
type hashWriter interface {
	io.Writer
	Sum([]byte) []byte
}

// createMinimalTarHeader 创建简化的 tar header，只保留必要信息
// 去掉时间戳、权限、uid/gid 等元数据，只保留文件名、大小、类型
// 这样可以减少系统调用开销和 tar header 写入开销，加速归档
func (arch *Archive) createMinimalTarHeader(fi os.FileInfo, name string, linkname string) *tar.Header {
	hdr := &tar.Header{
		Name:     name,
		Format:   tar.FormatGNU,
		ModTime:  time.Time{}, // 不保存时间戳，设为零值
		Mode:     0644,        // 统一权限，避免读取和写入权限信息
		Uid:      0,           // 不保存用户ID
		Gid:      0,           // 不保存组ID
		Uname:    "",          // 不保存用户名
		Gname:    "",          // 不保存组名
		Linkname: linkname,    // 符号链接目标（如果有）
	}

	// 设置文件类型和大小
	mode := fi.Mode()
	switch {
	case mode.IsDir():
		hdr.Typeflag = tar.TypeDir
		hdr.Name += "/"
		hdr.Size = 0
	case mode&os.ModeSymlink != 0:
		hdr.Typeflag = tar.TypeSymlink
		hdr.Size = 0
	case mode.IsRegular():
		hdr.Typeflag = tar.TypeReg
		hdr.Size = fi.Size()
	default:
		// 其他类型（设备文件等），设为普通文件
		hdr.Typeflag = tar.TypeReg
		hdr.Size = fi.Size()
	}

	return hdr
}
