package scanner

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/karrick/godirwalk"
)

// unboundedDirQueue 是一个无界阻塞队列：
// - Push：无容量上限，不会因“队列满”阻塞/失败（除非已 Close）
// - Pop：队列为空时阻塞等待；Close 后且队列清空时返回 ok=false
//
// 用它替代有界 channel，可以避免扫描 worker 在向同一个有界队列追加子目录时“全部阻塞，导致没人消费队列”的死锁。
type unboundedDirQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	items  []string
	head   int // 避免 Pop 时 O(n) copy 造成热点；通过 head 前移实现近似无锁队列语义
}

func newUnboundedDirQueue() *unboundedDirQueue {
	q := &unboundedDirQueue{items: make([]string, 0, 1024), head: 0}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *unboundedDirQueue) Push(dir string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	q.items = append(q.items, dir)
	q.cond.Signal()
	return true
}

func (q *unboundedDirQueue) Pop() (dir string, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.head >= len(q.items) && !q.closed {
		q.cond.Wait()
	}
	if q.head >= len(q.items) {
		return "", false
	}
	dir = q.items[q.head]
	q.head++

	// 定期收缩底层 slice，避免无限增长；阈值取折中，减少 copy 次数
	if q.head > 4096 && q.head*2 >= len(q.items) {
		// 把剩余元素搬到新 slice（均摊成本，避免每次 Pop copy）
		remaining := make([]string, len(q.items)-q.head)
		copy(remaining, q.items[q.head:])
		q.items = remaining
		q.head = 0
	}
	return dir, true
}

func (q *unboundedDirQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

type Scanner struct {
	ScanWorkers int // 并行扫描的worker数量，0表示使用默认值

	// Stats (optional): these counters can be read by the caller for diagnosing bottlenecks.
	DirsScanned   uint64
	EntriesEmitted uint64
}

// GetScanWorkers 返回实际使用的扫描worker数量
func (scanner *Scanner) GetScanWorkers() int {
	if scanner.ScanWorkers > 0 {
		return scanner.ScanWorkers
	}
	return 8 // 默认值
}

func NewScanner() *Scanner {
	return &Scanner{
		ScanWorkers: 0, // 0表示使用默认值
	}
}

// ScanStats exposes scanner counters for diagnostics (best-effort).
func (scanner *Scanner) ScanStats() (dirs uint64, entries uint64) {
	return atomic.LoadUint64(&scanner.DirsScanned), atomic.LoadUint64(&scanner.EntriesEmitted)
}

// 并行扫描实现：使用 worker pool + 目录队列，多个 goroutine 同时扫描不同目录。
// 重要：绝不丢目录（队列满则阻塞等待），并用 WaitGroup 精确判断扫描完成，避免忙等。
// 函数会阻塞直到所有目录扫描完成并 entries channel 关闭。
// 新增：当 entries channel 积压超过阈值时，暂停扫描等待消费，避免浪费资源。
func (scanner *Scanner) Scan(inputpath string, entries chan string, errors chan error) {
	// 如果ScanWorkers为0，使用默认值
	workers := scanner.ScanWorkers
	if workers <= 0 {
		workers = 8 // 默认8个扫描worker，对于1亿文件可以设置更大（如16-64）
	}

	dirQueue := newUnboundedDirQueue()
	var workersWg sync.WaitGroup // 等待所有 worker 退出
	var dirsWg sync.WaitGroup    // 统计“待处理目录”数量（入队 +1，处理完 -1）

	// 启动worker pool
	for i := 0; i < workers; i++ {
		workersWg.Add(1)
		go func() {
			defer workersWg.Done()
			for {
				dir, ok := dirQueue.Pop()
				if !ok {
					return
				}
				scanner.scanDir(dir, entries, errors, dirQueue, &dirsWg)
				dirsWg.Done() // 当前目录处理完成
			}
		}()
	}

	// 启动初始目录
	dirsWg.Add(1)
	dirQueue.Push(inputpath)

	// 等待所有目录扫描完成后关闭目录队列，让 worker 退出
	go func() {
		dirsWg.Wait()
		dirQueue.Close()
	}()

	// 等待所有 worker 退出后关闭 entries（通知 tar writer 没有更多条目）
	workersWg.Wait()
	close(entries)
}

// 扫描单个目录：只扫描直接子项，发现的子目录加入队列
// 当 entries channel 积压超过阈值（80000，留20%缓冲）时，暂停发送等待消费
func (scanner *Scanner) scanDir(dirPath string, entries chan string, errors chan error, dirQueue *unboundedDirQueue, dirsWg *sync.WaitGroup) {
	const queuePauseThreshold = 80000 // 当队列长度超过此值时暂停扫描
	const queueResumeThreshold = 60000 // 当队列长度降到此值以下时恢复扫描
	const pauseCheckInterval = 10 * time.Millisecond // 检查队列长度的间隔

	// 发送当前目录本身（带队列积压控制）
	scanner.sendEntryWithBackpressure(dirPath, entries, queuePauseThreshold, queueResumeThreshold, pauseCheckInterval)
	atomic.AddUint64(&scanner.EntriesEmitted, 1)
	atomic.AddUint64(&scanner.DirsScanned, 1)

	// 读取目录内容
	des, err := godirwalk.ReadDirents(dirPath, nil)
	if err != nil {
		errors <- err
		return
	}

	// 处理目录中的每个条目
	for _, de := range des {
		fullPath := filepath.Join(dirPath, de.Name())

		// 发送所有条目（文件和目录）- 带队列积压控制
		scanner.sendEntryWithBackpressure(fullPath, entries, queuePauseThreshold, queueResumeThreshold, pauseCheckInterval)
		atomic.AddUint64(&scanner.EntriesEmitted, 1)

		// 如果是目录，加入队列让其他worker处理
		if de.IsDir() {
			// 先计数再入队；无界队列不会因为"队列满"丢目录
			dirsWg.Add(1)
			if !dirQueue.Push(fullPath) {
				// 队列已关闭：补偿计数，避免 WaitGroup 永远等不到归零
				dirsWg.Done()
			}
		}
	}
}

// sendEntryWithBackpressure 带背压控制的条目发送
// 当队列积压超过阈值时暂停发送，等待消费
func (scanner *Scanner) sendEntryWithBackpressure(entryPath string, entries chan string, pauseThreshold, resumeThreshold int, checkInterval time.Duration) {
	for {
		// 检查队列长度
		queueLen := len(entries)
		if queueLen < pauseThreshold {
			// 队列未满，直接发送（可能阻塞，但这是正常的背压）
			entries <- entryPath
			return
		}
		// 队列接近满，等待消费
		// 使用 ticker 定期检查，避免忙等
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()
		for range ticker.C {
			queueLen = len(entries)
			if queueLen < resumeThreshold {
				// 队列降到阈值以下，恢复发送
				entries <- entryPath
				return
			}
		}
	}
}

// MissFileScanner 从 miss.txt 文件读取路径列表的 Scanner
type MissFileScanner struct {
	MissFilePath string
	EntriesEmitted uint64
}

// NewMissFileScanner 创建新的 MissFileScanner
func NewMissFileScanner(missFilePath string) *MissFileScanner {
	return &MissFileScanner{
		MissFilePath: missFilePath,
	}
}

// GetScanWorkers 返回扫描worker数量（miss模式不需要worker）
func (scanner *MissFileScanner) GetScanWorkers() int {
	return 1
}

// ScanStats 返回统计信息
func (scanner *MissFileScanner) ScanStats() (dirs uint64, entries uint64) {
	return 0, atomic.LoadUint64(&scanner.EntriesEmitted)
}

// Scan 从 miss.txt 文件读取路径并发送到 entries channel
// 文件格式：每行是 "文件路径\t原因" 或 "文件路径 原因"（制表符或空格分隔）
func (mfs *MissFileScanner) Scan(inputpath string, entries chan string, errors chan error) {
	file, err := os.Open(mfs.MissFilePath)
	if err != nil {
		errors <- err
		close(entries)
		return
	}
	defer file.Close()

	const queuePauseThreshold = 80000
	const queueResumeThreshold = 60000
	const pauseCheckInterval = 10 * time.Millisecond

	fileScanner := bufio.NewScanner(file)
	for fileScanner.Scan() {
		line := strings.TrimSpace(fileScanner.Text())
		if line == "" {
			continue
		}

		// 解析文件路径（第一列，可能是制表符或空格分隔）
		var filePath string
		if idx := strings.Index(line, "\t"); idx >= 0 {
			filePath = strings.TrimSpace(line[:idx])
		} else if idx := strings.Index(line, "  "); idx >= 0 {
			// 多个空格分隔
			filePath = strings.TrimSpace(line[:idx])
		} else {
			// 没有分隔符，整行就是路径
			filePath = line
		}

		if filePath == "" {
			continue
		}

		// 发送路径（带背压控制）
		mfs.sendEntryWithBackpressure(filePath, entries, queuePauseThreshold, queueResumeThreshold, pauseCheckInterval)
		atomic.AddUint64(&mfs.EntriesEmitted, 1)
	}

	if err := fileScanner.Err(); err != nil {
		errors <- err
	}

	close(entries)
}

// sendEntryWithBackpressure 带背压控制的条目发送（与 Scanner 相同）
func (mfs *MissFileScanner) sendEntryWithBackpressure(entryPath string, entries chan string, pauseThreshold, resumeThreshold int, checkInterval time.Duration) {
	for {
		queueLen := len(entries)
		if queueLen < pauseThreshold {
			entries <- entryPath
			return
		}
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()
		for range ticker.C {
			queueLen = len(entries)
			if queueLen < resumeThreshold {
				entries <- entryPath
				return
			}
		}
	}
}
