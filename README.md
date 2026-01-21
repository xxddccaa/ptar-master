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

```bash
mkdir -p /path/to/restore
cd /path/to/restore
for f in /path/to/output/archive_prefix.*.tar; do
  tar -xf "$f"
done
```

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
- **`--pprof <addr>`**：开启 pprof（例如 `:6060`），用于定位瓶颈（CPU/阻塞/锁/内存）
- **`--index`**：输出 `*.tar.index` 索引文件（用于快速定位条目；是否需要取决于你的下游流程）
- **`--verbose`**：输出更详细日志（调试时用，线上跑大数据不建议一直开）

## 性能调优建议（不改代码只调参数）

- **优先保证 IO 通路够快**
  - 源数据与输出路径所在文件系统（本地 NVMe/网络盘/JuiceFS 等）会决定上限
  - `STATS write=...` 长期稳定在某个值且 CPU 很闲时，通常是 IO/元数据瓶颈，不是线程不够
- **线程/worker 的建议顺序**
  - 先把 `--compression none` 用起来（如果你目标是速度）
  - 再调 `--scan-workers`（小文件/深目录一般更敏感）
  - 最后才考虑调 `--threads`
- **遇到“CPU0 满，其它核很低”一般正常**
  - Go 运行时/聚合写入/内核 IO 等会造成看起来像单核更忙
  - 是否需要调整以吞吐为准：看 `STATS write/files/entries` 是否上升

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
./ptar -c \
  -f /mnt/jfs5/stage1_tar/stage1 \
  --threads 128 \
  --scan-workers 8 \
  --gomaxprocs 72 \
  --compression none \
  /mnt/jfs5/stage1
```