# TESTING / 测试与验证方法

本项目面向“大量小文件快速归档”。这里记录可复现的测试数据生成、归档、解包与完整性校验方法。

> 注意：测试数据与产物已在 `.gitignore` 忽略，请不要提交到仓库。

## 0. 环境准备

```bash
cd /mnt/s3fs/ptar-master

export GOPROXY=https://goproxy.cn,direct
export GOSUMDB=off

go mod tidy
make
```

## 1. 小规模冒烟测试（3 个文件）

```bash
cd /mnt/s3fs/ptar-master
rm -rf test_data/simple_test
mkdir -p test_data/simple_test
cd test_data/simple_test

mkdir -p a/b/c
echo "test1" > a/file1.txt
echo "test2" > a/b/file2.txt
echo "test3" > a/b/c/file3.txt

../../ptar -c -f test --threads 2 --scan-workers 2 --compression none --verbose a

rm -rf extract_test
mkdir -p extract_test
cd extract_test
for f in ../test.*.tar; do tar -xf "$f"; done

find . -type f | wc -l   # 期望输出：3
```

## 2. 中等规模测试（1000 个文件）+ 完整性验证

### 2.1 生成测试数据

```bash
cd /mnt/s3fs/ptar-master
rm -rf test_data/medium_test test_data/medium_output
mkdir -p test_data

python3 << 'PY'
import os
base = "test_data/medium_test"
os.makedirs(base, exist_ok=True)

for i in range(20):
    dir1 = os.path.join(base, f"dir_{i}")
    os.makedirs(dir1, exist_ok=True)
    for j in range(10):
        dir2 = os.path.join(dir1, f"subdir_{j}")
        os.makedirs(dir2, exist_ok=True)
        for k in range(5):
            fp = os.path.join(dir2, f"file_{k}.txt")
            with open(fp, "w") as f:
                f.write("Content\n" * 10)

file_count = 0
for _, _, files in os.walk(base):
    file_count += len(files)
print(file_count)  # 期望输出：1000
PY
```

### 2.2 归档

```bash
cd /mnt/s3fs/ptar-master
mkdir -p test_data/medium_output
./ptar -c -f test_data/medium_output/medium --threads 8 --scan-workers 4 --compression none test_data/medium_test
```

### 2.3 解包并校验文件数

```bash
cd /mnt/s3fs/ptar-master
rm -rf test_data/medium_output/extract_test
mkdir -p test_data/medium_output/extract_test
cd test_data/medium_output/extract_test
for f in ../medium.*.tar; do tar -xf "$f"; done

find . -type f | wc -l    # 期望输出：1000
```

## 3. 大规模测试（示例：50000 个文件）+ 完整性验证

### 3.1 生成测试数据（约 5 万文件）

```bash
cd /mnt/s3fs/ptar-master
rm -rf test_data/test_input test_data/test_output test_data/test_extract
mkdir -p test_data

python3 << 'PY'
import os
base = "test_data/test_input"
os.makedirs(base, exist_ok=True)

for i in range(100):        # 100 个一级目录
    d1 = os.path.join(base, f"dir_{i}")
    os.makedirs(d1, exist_ok=True)
    for j in range(50):     # 每个 50 个二级目录
        d2 = os.path.join(d1, f"subdir_{j}")
        os.makedirs(d2, exist_ok=True)
        for k in range(10): # 每个 10 个文件
            fp = os.path.join(d2, f"file_{k}.txt")
            with open(fp, "w") as f:
                f.write(f"{fp}\n" * 10)

file_count = 0
for _, _, files in os.walk(base):
    file_count += len(files)
print(file_count)  # 期望输出：50000
PY
```

### 3.2 归档

```bash
cd /mnt/s3fs/ptar-master
mkdir -p test_data/test_output

./ptar -c \
  -f test_data/test_output/test_archive \
  --threads 16 \
  --scan-workers 8 \
  --compression none \
  test_data/test_input
```

### 3.3 解包并校验文件数

```bash
cd /mnt/s3fs/ptar-master
rm -rf test_data/test_extract
mkdir -p test_data/test_extract
cd test_data/test_extract
for f in ../test_output/test_archive.*.tar; do tar -xf "$f"; done

echo "解压后文件数:"
find test_input -type f | wc -l

echo "原始文件数:"
find ../test_input -type f | wc -l
```

