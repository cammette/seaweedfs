# blob 模式测试步骤

当前步骤已改到 [`blob_migrate/测试步骤.md`](blob_migrate/测试步骤.md)。filer 迁移见 [`file_migrate/测试步骤.md`](file_migrate/测试步骤.md)，常驻同步见 [`blob_sync/测试步骤.md`](blob_sync/测试步骤.md)。

用 `test-env` 里的两套单节点集群验证：从集群 A 的卷文件读取 needle，经集群 B 的 master 和 volume 写入，目标 fid 与源 fid 相同，不调用 Filer。

| 集群 | 角色 | master | volume | filer | 卷目录 |
|------|------|--------|--------|-------|--------|
| A | 源 | http://127.0.0.1:9333 | http://127.0.0.1:8080 | http://127.0.0.1:8888 | `a/data/volume` |
| B | 目标 | http://127.0.0.1:9334 | http://127.0.0.1:8081 | http://127.0.0.1:9888 | `b/data/volume` |

以下命令都在 `test-env` 目录执行。

## 1. 准备

```bash
cd /ex-bingo/github/seaweedfs/test-env

# 使用包含 blob 模式的二进制
(cd .. && go build -o test-env/weed ./weed)

./start.sh a
./start.sh b
./status.sh
```

两个集群的 master、volume、filer 都应显示 `ok`。

灌入源数据（collection 为 `src_demo`）：

```bash
./prepare_demo_data.sh
```

确认源卷里有存活文件，记下 fid（第一列）：

```bash
for f in a/data/volume/src_demo_*.idx; do
  vid=$(basename "$f" .idx | sed 's/src_demo_//')
  sz=$(stat -c%s "$f")
  [[ "$sz" -gt 0 ]] || continue
  echo "=== volume $vid ==="
  ./weed export -dir=./a/data/volume -collection=src_demo -volumeId="$vid"
done
```

目标侧此时不应已有这些卷：

```bash
ls b/data/volume/src_demo_*.dat 2>/dev/null || echo "目标还没有 src_demo 卷"
```

若要重跑整次测试，先清掉进度和目标卷（目标集群需先停再删文件，或只删未挂载的空目录）。已迁过的卷在 `migrate-state` 里是 `done`，不删进度会被跳过。

```bash
./stop.sh b
rm -rf b/data migrate-state/blob-a-to-b
./start.sh b
```

## 2. 迁移

```bash
./weed migrate.volume \
  -mode=blob \
  -masters=127.0.0.1:9334 \
  -dir=./a/data/volume \
  -collection=src_demo \
  -collectionDst=src_demo \
  -replication=000 \
  -stateDir=./migrate-state/blob-a-to-b \
  -verifyMode=crc \
  -workers=2 \
  -decompress=true
```

只迁某一个卷时加上 `-volumeIds=9`。

期望：

- 标准错误里该卷 `uploaded` 大于 0，`failed=0`
- 结束行 `failed=0`
- 退出码为 0

## 3. 看进度与断点

迁移过程中标准错误每 10 秒打印 `uploaded/skipped/failed/bytes`。另开终端：

```bash
./weed migrate.volume.status -stateDir=./migrate-state/blob-a-to-b
```

期望：相关卷 `status=done`，`fids` 等于该卷成功条数，总 fid 数等于各卷之和。输出里能看到 `Mode: blob` 和目标 master。

`Ctrl-C` 后用第 2 步同一条命令再跑。已成功且 CRC 仍一致的 fid 记为 skipped，`idx_cursor` 从上次提交的位置继续。`status` 里的 `fids` 只增不减。

## 4. 离线 CRC 比对

```bash
./weed migrate.volume.verify \
  -mode=blob \
  -masters=127.0.0.1:9334 \
  -dir=./a/data/volume \
  -collection=src_demo \
  -level=crc \
  -onlySuccess \
  -stateDir=./migrate-state/blob-a-to-b
```

期望：`crc_mismatch=0`，退出码 0。比对的是源 needle CRC 与目标 volume `ETag`。

## 5. 核对目标卷和 fid

目标目录出现与源相同的 volumeId 文件：

```bash
ls -l b/data/volume/src_demo_*.dat
```

用源卷上的某个 fid 导出目标卷，fid 应一致。把 `9` 和 `9,xxxxxxxx` 换成第 1 步记下的值：

```bash
./weed export -dir=./b/data/volume -collection=src_demo -volumeId=9
```

期望：输出的 `key` 列与源卷 `weed export` 的 fid 相同，`name` 与源文件名相同。

直接向集群 B 的 volume 读取该 fid（blob 模式不经过 Filer）：

```bash
curl -sf "http://127.0.0.1:8081/9,xxxxxxxx" | head
```

期望：返回文件内容，而不是 404。

## 6. 确认没有写入 Filer 目录

blob 模式不创建 Filer 元数据。集群 B 的 Filer 上不应出现这些对象路径：

```bash
curl -sf -o /dev/null -w "%{http_code}\n" "http://127.0.0.1:9888/src_demo/"
```

这个请求只说明 Filer 根目录可访问。用 `weed export` 能读到、而 Filer 上没有对应业务目录，即表示数据只在 volume 上。

## 7. 结束

```bash
./stop.sh a
./stop.sh b
```

只停其中一个：`./stop.sh a` 或 `./stop.sh b`。
