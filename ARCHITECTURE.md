# ptar 代码架构与工作原理

本文档详细说明 `ptar` 的代码架构、工作流程以及生产-消费模型。

## 目录结构

```
ptar-master/
├── cmd/ptar/main.go          # 命令行入口，参数解析
├── pkg/ptar/ptar.go          # 核心归档逻辑
├── pkg/scanner/scanner.go    # 目录扫描器（生产者）
└── pkg/index/index.go        # 索引文件生成器
```

## 整体架构

`ptar` 采用**生产者-消费者模式**，通过 Go channel 实现扫描和写入的解耦：

```
┌─────────────┐         ┌──────────────┐         ┌─────────────┐
│   Scanner   │────────▶│ entries      │────────▶│ tarChannel  │
│  (生产者)   │  推送    │  channel     │  消费    │  (消费者)   │
│             │         │ (缓冲100000) │         │             │
└─────────────┘         └──────────────┘         └─────────────┘
    扫描目录                 路径队列                 写入tar文件
```

## 核心组件

### 1. Archive（归档管理器）

**位置**：`pkg/ptar/ptar.go`

**职责**：
- 管理整个归档流程的生命周期
- 协调 Scanner（生产者）和 tarChannel workers（消费者）
- 收集统计信息（吞吐、错误计数、性能指标）
- 管理 tar 文件创建策略（共享池 vs 每线程文件）

**关键字段**：
```go
type Archive struct {
    InputPath    string          // 输入目录路径
    OutputPath   string          // 输出文件前缀
    TarThreads   int             // 写入 worker 数量
    TarMaxSize   int64           // 单个 tar 文件最大大小（0=不限制）
    Compression  string          // 压缩算法（gz/lz4/空）
    entries      chan string      // 文件路径队列（缓冲100000）
    errors       chan error       // 错误队列（缓冲1024）
    tarFileMgr   *tarFileManager  // tar 文件管理器（共享模式）
    useSharedPool bool            // 是否使用共享文件池
    isMissMode   bool            // 是否是 miss 模式
    stats        *archiveStats    // 性能统计
}
```

### 2. Scanner（目录扫描器 - 生产者）

**位置**：`pkg/scanner/scanner.go`

**职责**：
- 并行扫描目录树，发现所有文件和目录
- 将文件路径推送到 `entries` channel
- 实现背压控制，避免队列积压过多

**工作流程**：

```
1. 创建无界目录队列（unboundedDirQueue）
2. 启动 N 个扫描 worker（默认8个，可通过 --scan-workers 配置）
3. 将根目录推入队列
4. 每个 worker 循环：
   a. 从队列 Pop 一个目录
   b. 扫描该目录的直接子项
   c. 将所有条目（文件+目录）推送到 entries channel
   d. 发现的子目录 Push 回队列
5. 所有目录扫描完成后，关闭 entries channel
```

**关键特性**：
- **无界目录队列**：避免"队列满导致所有 worker 阻塞"的死锁
- **背压控制**：当 `entries` channel 积压超过 80000 时暂停发送，降至 60000 时恢复
- **并行扫描**：多个 worker 同时处理不同目录，充分利用文件系统元数据访问

**代码示例**：
```go
func (scanner *Scanner) Scan(inputpath string, entries chan string, errors chan error) {
    // 启动 worker pool
    for i := 0; i < workers; i++ {
        go func() {
            for {
                dir, ok := dirQueue.Pop()
                if !ok { return }
                scanner.scanDir(dir, entries, errors, dirQueue, &dirsWg)
            }
        }()
    }
    // 等待所有目录扫描完成
    workersWg.Wait()
    close(entries)
}
```

### 3. tarChannel（tar 写入器 - 消费者）

**位置**：`pkg/ptar/ptar.go::tarChannel()`

**职责**：
- 从 `entries` channel 消费文件路径
- 读取文件内容并写入 tar 文件
- 处理文件元数据（lstat、open、read）
- 支持超时控制和错误处理

**工作流程**：

```
1. 创建 256KB 的复制缓冲区（每个 worker 独立）
2. 从 entries channel 循环读取路径
3. 对每个路径：
   a. lstat 获取文件信息
   b. 创建 tar header
   c. 如果是普通文件：
      - open 打开文件
      - 使用 io.CopyBuffer 读取并写入 tar
      - 支持超时控制（普通文件30秒，大文件60秒）
      - 如果超时，跳过文件并记录到 miss.txt
   d. 更新统计信息
4. 根据 max-size 决定是否创建新 tar 文件
```

**两种模式**：

#### 模式1：共享文件池（`useSharedPool = true`）

**触发条件**：
- 设置了 `--max-size`（每个 tar 文件大小限制）
- 使用 `--miss-file`（miss 模式）

**特点**：
- 所有 worker 共享一个 `tarFileManager`
- 每个 worker 独立管理自己的 `threadTarFile`
- 当文件达到 `max-size` 时，worker 自动创建新文件
- 文件编号通过 atomic 无锁分配

**代码流程**：
```go
func (arch *Archive) tarChannelShared(threadnum int, ...) {
    var currentFile *threadTarFile
    for {
        entry := <-arch.entries
        // 检查是否需要创建新文件
        if currentFile == nil || 
           (arch.tarFileMgr.maxSize > 0 && 
            currentFile.writeCounter.Pos() >= arch.tarFileMgr.maxSize) {
            currentFile = arch.tarFileMgr.createNewFile()
        }
        arch.processEntryShared(threadnum, entry, currentFile, ...)
    }
}
```

#### 模式2：每线程文件（`useSharedPool = false`）

**触发条件**：
- 未设置 `--max-size` 且非 miss 模式

**特点**：
- 每个 worker 创建独立的 tar 文件：`{prefix}.{threadnum}.tar`
- 文件数量 = worker 数量
- 简单直接，无文件切换逻辑

### 4. tarFileManager（tar 文件管理器）

**位置**：`pkg/ptar/ptar.go`

**职责**：
- 管理 tar 文件的创建和编号分配
- 支持压缩（gzip/lz4/none）
- 支持索引文件生成

**关键方法**：
```go
// 分配新的文件编号（无锁，使用 atomic）
func (m *tarFileManager) allocateFileNumber() int64

// 为线程创建新的 tar 文件
func (m *tarFileManager) createNewFile() (*threadTarFile, error)
```

**文件命名规则**：
- 普通模式：`{prefix}.00000.tar`, `{prefix}.00001.tar`, ...
- Miss 模式：`{prefix}_miss.tar`（单文件，不使用编号）

### 5. 统计与监控

**位置**：`pkg/ptar/ptar.go::statsReporter()`

**职责**：
- 定期输出性能统计（默认每2秒）
- 监控队列积压、吞吐、错误计数
- 检测卡住情况并输出慢文件信息

**关键指标**：
- `entries/s`, `files/s`, `write MiB/s`：实时吞吐
- `queuedEntries`：队列积压（扫描速度 - 写入速度）
- `avgLstat`, `avgOpen`, `avgRead`：平均耗时（定位瓶颈）
- `channelLen`：entries channel 当前长度

## 数据流详解

### 正常归档流程

```
1. 用户执行：./ptar -c -f output --threads 16 /input/dir

2. main.go 解析参数，创建 Archive 实例

3. Archive.Begin() 启动：
   ├─ 创建 entries channel（缓冲100000）
   ├─ 创建 errors channel（缓冲1024）
   ├─ 启动 Scanner.Scan() goroutine（生产者）
   ├─ 启动 N 个 tarChannel() goroutine（消费者）
   ├─ 启动 statsReporter() goroutine（统计）
   └─ 启动 errornotice() goroutine（错误处理）

4. Scanner 扫描目录：
   ├─ 多个 scan worker 并行扫描
   ├─ 发现文件/目录 → 推送到 entries channel
   └─ 扫描完成 → 关闭 entries channel

5. tarChannel workers 消费：
   ├─ 从 entries channel 读取路径
   ├─ lstat → open → read → write to tar
   └─ 更新统计信息

6. 所有 worker 完成后：
   ├─ 关闭 tar 文件
   ├─ 输出最终统计
   └─ 程序退出
```

### Miss 模式流程

**用途**：重试打包之前因超时等原因失败的文件

**触发**：使用 `--miss-file` 参数指定 miss.txt 文件

**流程**：
```
1. 使用 MissFileScanner（而非普通 Scanner）
2. 从 miss.txt 读取文件路径列表（格式：路径\t原因）
3. 强制单线程模式（确保只创建一个 tar 文件）
4. 输出文件：{prefix}_miss.tar
5. 不使用超时限制（这是最后一次兜底打包）
6. 显示读取进度（每10秒输出一次）
```

**miss.txt 格式**：
```
/path/to/file1.txt	read timeout: elapsed=60s, timeout=60s, padded=0 bytes
/path/to/file2.txt	file too large: 60GB, exceeds threshold 50GB
```

## 关键设计决策

### 1. 为什么使用无界目录队列？

**问题**：如果使用有界 channel 作为目录队列，可能出现：
- 所有 scan worker 都在等待 Push 目录到队列
- 但队列已满，无法 Push
- 而消费队列的也是这些 worker
- 导致死锁

**解决**：使用无界队列（`unboundedDirQueue`），支持动态扩容，避免死锁。

### 2. 为什么 entries channel 缓冲是 100000？

**原因**：
- 解耦扫描和写入，允许扫描先跑一段
- 在大量小文件场景下，减少两边互相等待
- 峰值内存占用：100000 个路径字符串（约几十MB）

**权衡**：
- 如果太小：写入线程经常等待扫描
- 如果太大：内存占用增加，但收益递减

### 3. 为什么共享文件池模式使用无锁设计？

**设计**：
- 文件编号分配：使用 `atomic.AddInt64`（无锁）
- 每个线程独立管理自己的 `threadTarFile`（无锁写入）
- 文件切换时，线程独立创建新文件（无锁）

**优势**：
- 避免锁竞争，提高并发性能
- 每个线程的 tar writer 独立，不会并发写入同一个 writer

### 4. 超时控制机制

**问题**：网络文件系统（如 JuiceFS）可能出现读取卡住

**解决**：
```go
func (arch *Archive) copyWithTimeout(w io.Writer, r io.ReadCloser, 
                                     buf []byte, timeout time.Duration) {
    done := make(chan copyResult, 1)
    go func() {
        n, err := io.CopyBuffer(w, r, buf)
        done <- copyResult{n, err}
    }()
    
    select {
    case res := <-done:
        return res.n, false, res.err
    case <-time.After(timeout):
        r.Close()  // 关闭文件，让阻塞的 read 退出
        res := <-done
        // 补 0 填充剩余字节，保证 tar 流合法
        arch.padToTarSize(w, fileSize - res.n)
        return res.n, true, res.err
    }
}
```

**关键点**：
- 超时后关闭文件，让阻塞的 read 退出
- 补 0 填充，保证 tar 流结构合法
- 记录失败文件到 miss.txt，后续可重试

## 性能优化要点

### 1. 简化 tar header

**优化**：`createMinimalTarHeader()` 只保留必要信息
- 去掉时间戳（`ModTime = time.Time{}`）
- 统一权限（`Mode = 0644`）
- 去掉 UID/GID/用户名/组名

**收益**：减少系统调用和 tar header 写入开销

### 2. 大缓冲区

**优化**：每个 worker 使用 256KB 的复制缓冲区
```go
copyBuf := make([]byte, 256*1024)  // 每个 goroutine 独立
```

**收益**：减少 syscall 次数，提高大文件复制效率

### 3. 背压控制

**优化**：当 entries channel 积压超过 80000 时，暂停扫描

**收益**：避免内存无限增长，平衡扫描和写入速度

### 4. 并行扫描

**优化**：多个 scan worker 并行处理不同目录

**收益**：充分利用文件系统元数据访问的并行性

## 错误处理

### 错误收集

所有错误通过 `errors` channel 统一收集：
```go
errors := make(chan error, 1024)  // 缓冲1024，避免阻塞
```

### 错误类型

1. **lstat 失败**：`lstatErr++`，发送到 errors channel
2. **open 失败**：`openErr++`，发送到 errors channel
3. **read 超时**：跳过文件，记录到 miss.txt
4. **文件过大**：跳过文件，记录到 miss.txt

### Miss 文件记录

```go
func (arch *Archive) recordMissedFile(filePath string, reason string) {
    // 线程安全写入 miss.txt
    line := fmt.Sprintf("%s\t%s\n", filePath, reason)
    arch.missFile.Write([]byte(line))
}
```

## 索引文件（可选）

**启用**：使用 `--index` 参数

**格式**：`{tarfile}.index`

**内容**：每行一个条目
```
{pos}:{size}:{hash}:{name}
```

**用途**：快速定位 tar 文件中的条目位置，用于随机访问

**生成流程**：
1. 写入文件时，记录当前位置（`writeCounter.Pos()`）
2. 计算文件 SHA1 哈希
3. 通过 channel 发送到索引写入 goroutine
4. 异步写入索引文件

## 总结

`ptar` 的核心设计思想：

1. **生产者-消费者解耦**：通过大缓冲 channel 连接扫描和写入
2. **并行处理**：扫描和写入都支持多 worker 并行
3. **无锁设计**：共享文件池模式下，使用 atomic 和无锁数据结构
4. **容错机制**：超时控制、miss 文件记录、错误收集
5. **性能监控**：丰富的统计指标，便于定位瓶颈

这种设计使得 `ptar` 能够高效处理海量小文件的归档任务，充分利用多核 CPU 和并行 IO。
