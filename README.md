Parallel Tar (`ptar`)
---
`ptar` 是一个并行打包工具（Go 实现），目标是**更快地归档“海量小文件/深目录树”**：它会把同一个输入目录拆分成**多个独立且标准的 `.tar` 分卷**；把所有分卷全部解开后，可以重建出与原目录等价的结构。

## 适用场景 / 不适用场景

- **适用**
  - 大量小文件（几十万/上百万）快速归档
  - 追求吞吐（通常配合 `--compression none`）
  - 希望每个分卷都是标准 tar（方便恢复、容错更好）
- **不适用/注意**
  - 你只想生成“单个 tar 文件”：本工具默认输出多个分卷
  - 你要强压缩比：压缩会显著降低吞吐（且可能转为 CPU 受限）
  - 超高单线程 CPU 利用率不是目标：很多场景瓶颈在文件系统/IO/元数据

## 输出是什么样的

使用 `-f /path/to/out/prefix` 时，输出通常包括：

- `prefix.0.tar`, `prefix.1.tar`, ...（**分卷**，每个都是标准 tar）
- （可选）`prefix.0.tar.index`, ...（启用 `--index` 时生成的索引文件）

> 重要：**分卷可以任意顺序解包**，但要把所有分卷都解开才能完整还原。

**特殊模式：miss 模式**（使用 `--miss-file` 时）：
- 输出为单个文件：`prefix_miss.tar`（不是分卷）
- 用于重试打包之前因超时等原因失败的文件

## 编译（不改 Go 代码的前提下使用本项目）

### 依赖
- Go >= 1.18
- `make`（可选，但推荐）

### 编译步骤

```bash
cd /mnt/s3fs/ptar-master

# 如果你的环境没有 go.mod（例如从打包文件/拷贝来的代码），先初始化一次：
go mod init github.com/zgiles/ptar

# 国内网络环境建议（避免 proxy/sumdb 超时）：
export GOPROXY=https://goproxy.cn,direct
export GOSUMDB=off

go mod tidy
make

./ptar --help
```

编译产物为项目根目录下的可执行文件：`./ptar`

## 快速开始

### 1) 归档（创建分卷 tar）

```bash
./ptar -c \
  -f /path/to/output/archive_prefix \
  --compression none \
  /path/to/input_dir
```

### 2) 解包（恢复目录）

**基本解包**（如果所有文件都打包成功）：
```bash
mkdir -p /path/to/restore
cd /path/to/restore
for f in /path/to/output/archive_prefix.*.tar; do
  tar -xf "$f"
done
```

**完整恢复**（如果有文件失败，需要两步覆盖）：
```bash
mkdir -p /path/to/restore
cd /path/to/restore

# 第一步：解压主 tar 文件（包含大部分完整文件 + 一些不完整文件）
for f in /path/to/output/archive_prefix.*.tar; do
  tar -xf "$f"
done

# 第二步：如果有 miss.tar，解压并覆盖不完整文件
if [ -f /path/to/output/archive_prefix_miss.tar ]; then
  tar -xf /path/to/output/archive_prefix_miss.tar --overwrite
fi
```

> **说明**：第一次打包时，如果某些文件因读取超时被补 0 填充，这些文件是不完整的。需要先用 miss 模式重试打包生成 `{prefix}_miss.tar`，然后先解压主 tar，再解压 miss.tar 覆盖，才能得到所有完整文件。

### 3) 重试打包失败的文件（miss 模式）

如果打包过程中有文件因读取超时等原因失败，会生成 `{prefix}_miss.txt` 文件记录失败的文件路径。可以使用 `--miss-file` 参数重试打包这些文件：

```bash
./ptar -c \
  -f /path/to/output/archive_prefix \
  --miss-file /path/to/output/archive_prefix_miss.txt \
  --compression none
```

这会生成单个 tar 文件：`archive_prefix_miss.tar`，包含所有失败的文件（这次是完整的）。

## 常用参数说明（结合你实际跑大数据时最常用的）

> 下面参数名以 `./ptar --help` 为准；本 README 解释其用途与调参思路。

- **`-c`**：创建归档（create）
- **`-f <prefix>`**：输出前缀（会生成 `<prefix>.<n>.tar` 多个分卷）
- **`--compression <algo>`**
  - **`none`**：不压缩（通常最快、也最适合海量小文件归档）
  - 其它压缩算法：更省空间但更慢，且可能变成 CPU 受限
- **`--threads <N>`**：写入/打包侧 worker 数量
  - 经验：先从 **\(1–4\)×CPU 核心数**范围内试；盲目拉到 1024 往往只会增加上下文切换/锁竞争/FD 压力，不一定变快
- **`--scan-workers <N>`**：目录扫描 worker 数量
  - 经验：目录很深/元数据压力大/小文件极多时，`scan-workers` 往往比 `threads` 更值得加
- **`--gomaxprocs <N>`**：设置 Go 的并行度上限（等价于 `GOMAXPROCS`）
  - 通常设为**可用 CPU 核心数**（或略小于 cgroup 限制）
- **`--stats-interval <sec>`**：周期输出 `STATS`（吞吐、累计写入、错误计数、heap 等）
- **`--max-size <size>`**：单个 tar 文件“目标大小”上限（例如 `20G`/`100M`/`1T`）。达到上限后**下一个条目**会切到新 tar（因此单个 tar 可能略大于设定值，这是为了保证每个 tar 都是可独立解压的完整 tar 流）。
- **`--miss-file <path>`**：指定 miss.txt 文件路径，用于重试打包之前失败的文件
  - 当指定此参数时，工具会从文件中读取文件路径列表（每行格式：`文件路径\t原因` 或 `文件路径 原因`）
  - 输出为单个 tar 文件：`{prefix}_miss.tar`（不使用分卷）
  - miss 模式下自动使用单线程，确保只创建一个文件
  - 适用于处理因读取超时、文件过大等原因被跳过的文件
- **`--pprof <addr>`**：开启 pprof（例如 `:6060`），用于定位瓶颈（CPU/阻塞/锁/内存）
- **`--index`**：输出 `*.tar.index` 索引文件（用于快速定位条目；是否需要取决于你的下游流程）
- **`--verbose`**：输出更详细日志（调试时用，线上跑大数据不建议一直开）

## 日志说明（`STATS` / `TAR-FILE`）

你运行时看到的日志主要有两类：

### `STATS ...`

示例（字段顺序与实际输出一致，括号内为单位/口径）：

```
STATS uptime=42s goroutines=141 threads=128 scanWorkers=8 entries=23/s files=10/s write=5000.00MiB/s totalEntries=8152 totalFiles=396 totalWrite=89.31GiB lstatErr=0 openErr=0 scanDirs=89 scanSpeed=2575/s queuedEntries=1234 channelLen=50000 avgFileSize=2.50MB avgLstat=0.15ms avgOpen=0.20ms avgRead=50.30ms avgWait=0.05ms heap=0.22GiB totalTarFiles=128
```

- **`uptime`**：程序运行时长
- **`goroutines`**：当前 Go `goroutine` 数（不是线程数；可粗略看是否有堆积/泄漏）
- **`threads`**：`--threads` 写入 worker 数
- **`scanWorkers`**：`--scan-workers` 扫描 worker 数（0 时会用默认值）
- **`entries`**：写入侧处理的“条目数”速度（条目/秒；目录 + 文件 + 其它 tar 条目）
- **`files`**：写入侧写入的“普通文件”速度（文件/秒；仅 tar `TypeReg`）
- **`write`**：写入吞吐（MiB/s；包含 tar header + 文件内容；开压缩时为“压缩后写入”吞吐）
- **`totalEntries`**：累计处理的条目数
- **`totalFiles`**：累计写入的普通文件数
- **`totalWrite`**：累计写入总字节（GiB）
- **`lstatErr`**：`os.Lstat` 失败计数
- **`openErr`**：`os.Open` 失败计数
- **`scanDirs`**：累计扫描过的目录数（扫描侧计数器；每扫描一个目录 +1）
- **`scanSpeed`**：扫描侧累计条目/秒（从启动到现在的平均扫描速度；用于判断扫描是否瓶颈）
- **`queuedEntries`**：已扫描但未写入的条目数（在 `entries` channel 队列中等待写入的条目数；`scanEntries - totalEntries`）
  - **解读**：如果 `queuedEntries` 持续接近 100000（channel 缓冲上限），说明写入侧慢于扫描侧；如果 `queuedEntries` 接近 0，说明写入侧在等扫描侧供给
- **`channelLen`**：`entries` channel 的当前实际长度（队列积压数；0-100000）
  - **解读**：与 `queuedEntries` 类似，但这是实时快照；如果持续接近 100000，说明写入侧处理不过来
- **`avgFileSize`**：平均文件大小（MB；`totalWrite / totalFiles`）
  - **解读**：帮助判断是大量小文件还是少量大文件；小文件多时，`lstat/open` 耗时占比更高
- **`avgLstat`**：平均 `lstat` 耗时（毫秒）
  - **解读**：如果很高（>1ms），说明文件系统元数据访问慢（可能是网络 FS/分布式 FS 瓶颈）
- **`avgOpen`**：平均 `open` 耗时（毫秒）
  - **解读**：如果很高，说明文件打开慢（可能是文件系统/网络延迟）
- **`avgRead`**：平均文件读取耗时（毫秒；包含 `io.CopyBuffer` 的完整读取时间）
  - **解读**：如果很高但文件不大，可能是 IO 带宽不足或文件系统慢
- **`avgWait`**：平均从 `entries` channel 等待条目的耗时（毫秒）
  - **解读**：如果很高，说明写入线程经常在等扫描侧供给；如果接近 0，说明扫描侧供给充足
- **`heap`**：当前 Go heap 使用量（GiB）
- **`totalTarFiles`**：累计创建的 tar 文件数量（`--max-size` 模式下尤其有用）

> 如何解读你贴的那段日志：`write` 很高但 `entries/files` 很低，通常意味着正在写大文件/少量大文件；`entries/files` 高则更像是大量小文件持续流入。

### `[TAR-FILE] ...`

- **`[TAR-FILE] Created new tar file: ... (file #xxxxx)`**：创建了一个新的 tar 分卷文件（`--max-size` 模式下会频繁出现）
- **`[TAR-FILE] Closed tar file: ... (size=..., duration=..., file #xxxxx)`**：该分卷写入完成并关闭（size 为最终大小，可能略大于 `--max-size`）

### `[THREAD-x] Completed ...`

- 表示某个写入 worker 退出时的汇总（处理文件数/写入字节/耗时）。

## “队列限制 100000/1024” 是什么？

代码里有两个关键的 channel 缓冲区大小（你可能记成 10000，但当前代码是 100000）：

- **`entries` channel 缓冲 = 100000**：扫描侧把“条目路径”（目录 + 文件）推到这里，写入侧从这里消费并写入 tar。  
  - **意义**：把“扫描”和“写入”解耦，允许扫描先跑一段把路径攒起来，减少两边互相等的概率；在大量小文件/深目录时更稳。  
  - **代价**：峰值会多占一些内存（主要是 100000 个路径字符串在队列里时的内存）。  
  - **现象**：如果写入侧慢，`entries` 会堆积；如果扫描侧慢，写入线程会等 `entries` 供给。

- **`errors` channel 缓冲 = 1024**：各处错误通过这个 channel 汇报并打印。  
  - **意义**：避免短时间内大量错误时生产者被阻塞（但真实出错一般也会直接中断/影响流程）。

另外，扫描器内部还有一个 **无界目录队列**（`unboundedDirQueue`），它是为了避免“有界队列满了导致扫描 worker 全部阻塞、没人继续消费”的死锁风险；它不是 100000 这种固定上限，而是按需增长（但会有内部收缩逻辑）。

## 性能调优建议（不改代码只调参数）

- **优先保证 IO 通路够快**
  - 源数据与输出路径所在文件系统（本地 NVMe/网络盘/JuiceFS 等）会决定上限
  - `STATS write=...` 长期稳定在某个值且 CPU 很闲时，通常是 IO/元数据瓶颈，不是线程不够
- **线程/worker 的建议顺序**
  - 先把 `--compression none` 用起来（如果你目标是速度）
  - 再调 `--scan-workers`（小文件/深目录一般更敏感）
  - 最后才考虑调 `--threads`
- **遇到"CPU0 满，其它核很低"一般正常**
  - Go 运行时/聚合写入/内核 IO 等会造成看起来像单核更忙
  - 是否需要调整以吞吐为准：看 `STATS write/files/entries` 是否上升

## 使用日志排查写入变慢问题

新增的性能指标可以帮助你快速定位瓶颈：

### 1. 判断是扫描瓶颈还是写入瓶颈
- **`queuedEntries` 接近 0 且 `avgWait` 很高** → 写入线程在等扫描侧，**扫描是瓶颈**
  - 解决：增加 `--scan-workers` 或检查文件系统元数据性能
- **`queuedEntries` 接近 100000 或 `channelLen` 很高** → 扫描侧快但写入侧慢，**写入是瓶颈**
  - 解决：检查 `avgLstat/avgOpen/avgRead` 是否异常高

### 2. 判断文件系统性能问题
- **`avgLstat > 1ms`** → 元数据访问慢（常见于网络 FS/分布式 FS）
  - 解决：检查元数据服务器负载、网络延迟；考虑降低 `--scan-workers` 减少并发压力
- **`avgOpen > 5ms`** → 文件打开慢
  - 解决：检查文件系统性能、网络延迟（如果是网络 FS）
- **`avgRead` 很高但文件不大** → IO 带宽不足或文件系统慢
  - 解决：检查存储 IOPS/带宽、网络带宽（如果是网络 FS）

### 3. 判断文件大小分布
- **`avgFileSize < 1MB` 且 `files/s` 很高** → 大量小文件，瓶颈通常在元数据操作（`lstat/open`）
- **`avgFileSize > 10MB` 且 `files/s` 很低** → 少量大文件，瓶颈通常在 `read` 操作

### 4. 综合判断示例
```
# 场景1：扫描瓶颈
scanSpeed=400/s, queuedEntries=0, avgWait=10ms, channelLen=0
→ 扫描侧慢，写入线程在等

# 场景2：元数据瓶颈（JuiceFS/网络FS常见）
avgLstat=5ms, avgOpen=8ms, scanSpeed=500/s, queuedEntries=50000
→ 元数据操作慢，导致整体变慢

# 场景3：IO瓶颈
avgRead=200ms, avgFileSize=50MB, write=100MiB/s
→ 文件读取慢，可能是存储带宽不足
```

## pprof 怎么用（定位到底卡在哪）

你可以像这样启动：

```bash
./ptar -c \
  -f /path/to/output/archive_prefix \
  --threads 128 \
  --scan-workers 8 \
  --gomaxprocs 72 \
  --compression none \
  --stats-interval 2 \
  --pprof :6060 \
  /path/to/input_dir
```

然后在另一台终端抓取分析（示例）：

```bash
go tool pprof -top http://127.0.0.1:6060/debug/pprof/profile?seconds=30
go tool pprof -top http://127.0.0.1:6060/debug/pprof/heap
```

如果你看到主要时间在 syscall / 文件系统相关函数，通常就说明瓶颈在 IO/元数据；反之如果在压缩/拷贝/哈希等，则更多是 CPU 侧瓶颈。

## 测试与完整性校验

`TESTING.md` 提供了：
- 小规模冒烟（3 文件）
- 中等规模（1000 文件）带解包校验
- 更大规模（约 5 万文件）带解包校验

## 项目目标（路线图）

- 快、正确、尽量兼容 GNU Tar
- 多分卷并行归档（全部解开后等价于原目录结构）
- 索引文件（Index）✅
- Manifest 文件：未完成
- 多节点并行：未完成
- 并行解压：未完成

## 性能样例（上游作者给出的历史数据，供参考）

```
Linux kernel source 1.5GB 47K files GPFS hot-pagepool no compression
Run     Time
gnutar  6m4s
1       6m33s
2       3m15s
4       1m30sec
8       50sec
16      23sec
32      12sec
64      9.4sec
128     9.9sec
```

```
Test dataset 10TB 500K files
Run     Time
gnutar  10hr20m
32      44m39s
32gzip  8hr22m
```

## 实用指令

```
这个是纯io操作，没有任何计算逻辑，后续可以用来测速：
dd if=/dev/zero of=10G bs=1M count=10240 status=progress
```

```
cp -f /mnt/s3fs/ptar-master/ptar /mnt/jfs6/
chmod 777 /mnt/jfs6/ptar
/mnt/jfs6/ptar -c \
  -f /mnt/jfs5/llamafactory_train_data_style_processed_tar/data \
  --threads 64 \
  --scan-workers 256 \
  --gomaxprocs 72 \
  --compression none \
  --max-size 20G \
  /mnt/jfs5/llamafactory_train_data_style_processed
```

### 重试打包失败的文件

如果上述命令执行后生成了 `data_miss.txt`，可以使用以下命令重试打包失败的文件：

```bash
/mnt/jfs6/ptar -c \
  -f /mnt/jfs5/llamafactory_train_data_style_tar/data \
  --miss-file /mnt/jfs5/llamafactory_train_data_style_tar/data_miss.txt \
  --compression none
```

这会生成 `data_miss.tar` 文件，包含所有失败的文件。
