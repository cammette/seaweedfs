# Blob 块同步工具使用说明

在源 volume 服务器上读取 SeaweedFS 卷文件，把存活 needle 按**源 fid** 写到另一套集群的 volume 上，并把每个卷的进度写到本地监控目录。同步不调用目标 Filer。

相关命令：

| 命令 | 作用 |
|------|------|
| `weed sync.volume` | 常驻同步。扫描卷目录，按卷启动同步任务 |
| `weed sync.volume.status` | 只读本地监控目录，输出每个卷的文件数、成功数和延迟 |

一次性搬迁见文末附录里的 `migrate.volume -mode=blob`。

---

## 1. 概述

同步只依赖两类本地数据：

1. **SeaweedFS 卷文件**（`-dir`）。每个卷是一对 `{collection}_{卷号}.dat` 与 `.idx`（空 collection 时为 `{卷号}.dat`）。`.idx` 给出当前存活 needle 的位置，`.dat` 里是文件内容和 needle 尾部的 CRC32。已删除或被覆盖的旧记录不参与同步。
2. **本地监控日志目录**（`-stateDir`）。管理循环和每个卷任务都把状态写在这里。监控命令不访问源卷，也不访问目标集群，只读这个目录。

```
源卷目录  -dir                         本地监控目录  -stateDir
┌────────────────────────┐            ┌─────────────────────────────┐
│ photos_9.dat / .idx    │            │ volumes/9.json   每卷进度   │
│ photos_10.dat / .idx   │            │ sync.log         每轮一行   │
└───────────┬────────────┘            └──────────────▲──────────────┘
            │ 读存活 needle                          │ 写入水位、计数、延迟
            ▼                                        │
     ┌──────────────┐    按源 fid 写入        ┌──────┴───────┐
     │ 同步管理循环 │ ──────────────────────► │ 目标集群      │
     │ 每 interval  │   master + volume HTTP  │ master/volume │
     │ 扫描卷目录   │                         └──────────────┘
     └──────┬───────┘
            │ 卷文件 mtime 变新，或上一轮失败
            ▼
     ┌──────────────┐
     │ 卷同步任务   │  一个卷同时只有一个 goroutine
     │ 按时间水位   │  LastModified 秒为续传索引
     └──────────────┘
```

三部分职责：

| 部分 | 做什么 |
|------|--------|
| 同步管理 | 按 `-interval` 扫描 `-dir` 里该 collection 的卷。`.dat` 或 `.idx` 的修改时间新于该卷上次成功记录，或上一轮状态是 `error`，就为这个卷启动一个任务。目录里后出现的新卷，下一轮扫描会接上。 |
| 卷同步任务 | 按 `.idx` 读出存活 needle，只处理时间水位之后的文件，`POST http://{目标volume}/{源fid}`。成功后把水位、文件数、已同步数、失败数、延迟写回 `volumes/{卷号}.json`，并往 `sync.log` 追加一行。 |
| 监控 | `sync.volume.status` 读取 `volumes/*.json`，打印每个卷同步了多少、还差多少、当前延迟。 |

写入与 `migrate.volume -mode=blob` 相同：目标没有这个 volumeId 时，向目标 master 查拓扑，在一台 volume server 上按**同一个 volumeId** 建卷，再按源 fid 写入。源、目标必须是两套集群，因为 volumeId 在一个集群内全局唯一。

续传索引是 needle 的 `LastModified`（Unix 秒），不是 `.idx` 行号。某一秒里的存活文件全部成功，水位才推过这一秒。同一秒只成功一部分时，fid 记在 `partial_fids`，下次只补剩余的。没有 `LastModified` 的 needle 单独记在 `untimed_fids`，成功一次后不再重复。

一致性默认比对 needle CRC32。目标 volume 把该校验放在 HTTP `ETag` 里，同步用 `HEAD /{fid}` 核对，不把正文再下载一遍。`-decompress=false` 时，这个 CRC 就是源卷磁盘上的 Needle Checksum。源文件是 gzip 且默认解压后再写入时，比对的是解压后字节的 CRC。

延迟的定义：当前时间减去最早一条尚未同步的 needle 的 `LastModified`。没有待同步的带时间文件时，延迟为 0。

---

## 2. 运行条件

运行同步的机器需要：

1. 只读访问源卷目录，其中要有成对的 `.dat` 和 `.idx`。
2. 访问目标 master 的 HTTP 端口（如 `192.168.1.10:9333`）。多个地址用逗号分隔，查询时轮询。可写 `http://host:9333`，程序会去掉协议前缀。对应 gRPC 端口默认为 HTTP 端口 + 10000。建卷时还会直连目标 volume server 的 gRPC。
3. 可写 `-stateDir`。这个目录就是监控日志目录。

建议在源卷写入的低峰执行。进程会按卷文件的修改时间反复打开 `.dat` / `.idx`，与正在追加的卷存在竞态时，下一轮扫描会再同步一次。

目标若开启 JWT，按集群惯例配置 `security.toml`。volume 写读使用 lookup 返回的 JWT；gRPC 使用其中 `grpc.client` 的 TLS 配置。

---

## 3. 命令：`sync.volume`

常驻进程。`Ctrl-C` 或 `SIGTERM` 后退出。下次用同一个 `-dir`、`-collection`、`-masters`、`-stateDir` 启动，从各卷已提交的时间水位继续。

```bash
weed sync.volume \
  -dir=/data/volumes \
  -collection=photos \
  -masters=192.168.1.10:9333,192.168.1.11:9333 \
  -collectionDst=photos \
  -replication=000 \
  -stateDir=./sync-state \
  -interval=10s \
  -verifyMode=crc \
  -decompress=true \
  -center=a \
  -centerDst=b
```

只同步部分卷时加 `-volumeIds=9,10`。

### 3.1 参数

| 参数 | 默认 | 说明 |
|------|------|------|
| `-dir` | `.` | 源 volume 数据目录 |
| `-collection` | `""` | collection；空则匹配 `^\d+\.dat$` |
| `-volumeIds` | `""` | 逗号分隔卷 ID；空表示该 collection 下全部有 `.dat+.idx` 的卷 |
| `-masters` | 必填 | 目标 master，`host:port`，逗号分隔 |
| `-stateDir` | 必填 | 本地监控日志目录 |
| `-interval` | `10s` | 扫描卷目录的间隔 |
| `-collectionDst` | `""` | 目标上新建卷时使用的 collection；空则用 `-collection` |
| `-replication` | `000` | 新建卷的 replication |
| `-ttl` | `""` | 新建卷的 ttl |
| `-disk` | `""` | 新建卷的 disk type |
| `-decompress` | `true` | 源 needle 为 gzip 时，是否解压后再上传 |
| `-verifyMode` | `crc` | `none` / `exists` / `size` / `crc` / `md5`。空值即 `crc` |
| `-retries` | `3` | 单次上传重试次数 |
| `-center` | 必填 | 源集群标识。没有中心前缀的文件名写成 `{center}::原文件名`，例如 `a::photo.jpg` |
| `-centerDst` | 必填 | 目标集群标识。文件名已是 `{centerDst}::...` 时不复制，也不在目标上删除 |

`-filer`、`-path` 不使用。同步不生成 Filer 目录。

### 3.2 一轮同步做什么

管理循环发现某个卷需要同步后：

1. 向目标 master 查询该 volumeId。不存在则 `AllocateVolume` 建出同号卷，并等到 master 能查到它（最多约 20 秒）。
2. 扫描该卷全部存活 needle。`LastModified` 不晚于已提交水位的跳过；水位所在秒只补 `partial_fids` 里还没有的 fid。
3. 按时间从早到晚 `POST /{源fid}`。`-verifyMode=crc` 时再 `HEAD`，核对长度和 `ETag`。
4. 把新水位写入 `volumes/{卷号}.json`，并追加 `sync.log`。
5. 本轮失败数为 0，且卷文件修改时间没有再变，任务结束，该卷变为 `idle`。文件在本轮期间又被写入，任务立刻再跑一轮。有失败时本轮结束，管理循环在下一个 `-interval` 再启动该卷。

源卷 `.idx` 里最新记录是删除墓碑时，对目标 volume 发 `DELETE /{源fid}`。返回 404 视为已经删掉，记入 `deleted/{卷号}.log`，下次不再请求。文件名带有目标中心前缀时，既不复制也不删除，避免把对方集群的数据再送回去。

已经带有其他中心前缀的文件名保持不变，例如源上的 `c::photo.jpg` 仍以这个名字写入。没有前缀的才加上 `-center`。

标准错误会打印 `sync start volume`、追平后的 `sync volume N idle`，以及失败数。

### 3.3 监控目录

```text
{stateDir}/
  sync.log                 # 每卷每一轮一行，便于 tail
  volumes/{卷号}.json      # 该卷的进度，监控命令读这里
  deleted/{卷号}.log       # 已在目标删除或按中心规则跳过的 fid
```

`volumes/{卷号}.json` 里常用字段：

| 字段 | 含义 |
|------|------|
| `status` | `idle` 已追平，`running` 正在同步，`error` 上一轮有失败 |
| `files` | 本轮扫描到的存活文件数 |
| `synced` | 已落在水位之内的文件数 |
| `failed` | 本轮失败数 |
| `lag_seconds` | 当前同步延迟（秒） |
| `committed_unix` | 已完整同步到的 `LastModified` 秒 |
| `partial_unix` / `partial_fids` | 尚未完整的那一秒，以及其中已成功的 fid |
| `bytes` / `pass_uploaded` / `pass_bytes` / `pass_duration_ms` | 累计字节、本轮成功条数、本轮字节和耗时 |
| `source_mtime_nano` | 上一轮成功时看到的卷文件修改时间 |
| `last_fid` / `last_error` | 最近成功的 fid，或最近一条错误 |

`sync.log` 的一行类似：

```text
2026-10-16T14:00:00+08:00 vid=9 files=1000 synced=1000 failed=0 lag=0s watermark=1760000000 uploaded=12
```

失败的单条 fid 也会追加到这个文件，带 `fid=` 和 `error=`。

重跑全量时，停掉进程后删除整个 `-stateDir`。只删某一个 `volumes/{卷号}.json`，该卷会从时间水位 0 再同步；目标上已有且 CRC 一致的 fid 会被跳过，不会再写一份。

---

## 4. 命令：`sync.volume.status`

只读 `-stateDir`，不打开卷文件，也不访问目标集群。

```bash
weed sync.volume.status -stateDir=./sync-state

# JSON
weed sync.volume.status -stateDir=./sync-state -json
```

文本输出包括：卷总数，以及 `running` / `idle` / `error` 各多少；全部卷的文件数、已同步数、失败数、最大延迟。每个卷一行：`files`、`synced`、`failed`、`lag`、水位时间、最近 fid。`error` 卷会多打一行 `last_error`。

另开终端也可以直接看追加日志：

```bash
tail -f ./sync-state/sync.log
```

---

## 5. 使用约束

- 源、目标必须是不同集群。对仍挂着该 volumeId 的同一套集群写，会打到源卷上。
- 目标上这个 volumeId 必须空闲，或已经是本次要写入的同一 collection。同号卷属于其他 collection 时，数据会写进那个卷。
- 目标 master 上要已有 volume server 注册，否则无法建卷。
- 不写 Filer 元数据。同步完成后，目标 Filer 目录里看不到这些文件，只能按 fid 从 volume 读取。
- 大文件整段读入内存再 POST，不做 Filer 的 autoChunk。
- 缺少完整 `.dat` 的 EC-only 卷不会进入同步。
- `LastModified` 早于已提交水位、但事后才写入的 needle 不会被这次水位补上。正常写入的新文件时间更晚，会在卷文件修改时间更新后的下一轮同步。

---

## 附录. 一次性迁移 `migrate.volume`

`sync.volume` 适合持续追平。只搬一次时用 `migrate.volume`。`-mode=blob` 同样读本地卷文件、按源 fid 写目标 volume，进度也在本地 `stateDir`，但续传索引是 `.idx` 行游标，进程结束即停止，不会按卷文件修改时间再启动。

```bash
weed migrate.volume \
  -mode=blob \
  -masters=192.168.1.10:9333,192.168.1.11:9333 \
  -dir=/data/volumes \
  -collection=photos \
  -collectionDst=photos \
  -replication=000 \
  -stateDir=./migrate-state/photos-blob \
  -verifyMode=crc
```

进度用 `weed migrate.volume.status -stateDir=...`。离线 CRC 比对：

```bash
weed migrate.volume.verify \
  -mode=blob \
  -masters=192.168.1.10:9333 \
  -dir=/data/volumes \
  -collection=photos \
  -level=crc \
  -onlySuccess \
  -stateDir=./migrate-state/photos-blob
```

filer 模式则把文件经 HTTP PUT 写到目标 Filer，由 Filer 分配新的 fid，路径模板必须包含 `{fid}`。

### 附录 A. blob 一次性迁移的要点

- 每个卷先 lookup；没有该 volumeId 就 `AllocateVolume`，然后 `POST /{源fid}`。
- 未指定 `-verifyMode` 时使用 `crc`（`HEAD` 的 `ETag`）。`md5` 会下载全文。
- 断点文件是 `volumes/{vid}.json` 的 `idx_cursor` 和 `success/{vid}.log`。中断后用相同 `-dir`、`-collection`、`-mode=blob`、`-masters`、`-stateDir` 再跑。
- `-dryRun` 不建卷、不上传，但会把 fid 记入 success。试跑后要换一个 `stateDir`，或删掉该目录再正式迁移。

### 附录 B. filer 模式

```bash
weed migrate.volume \
  -dir=/data/volumes \
  -collection=photos \
  -filer=http://filer:8888 \
  -path='/collector/{yyyy}/{yyyyMMdd}/{volume_id}/{hash12}/{fid}' \
  -stateDir=./migrate-state/photos \
  -verifyMode=md5 \
  -workers=4
```

| 变量 | 含义 |
|------|------|
| `{fid}` | 源 fid 的路径安全形式（`,` → `_`），用作文件名 |
| `{volume_id}` | 源卷 ID |
| `{collection}` | 源 collection |
| `{yyyy}` / `{yyyyMM}` / `{yyyyMMdd}` / `{HH}` / `{mm}` | needle `LastModified`；没有则用当前时间 |
| `{hash12}` | `hash(fid) % 12`（FNV-1a） |
| `{hashN}` | 如 `{hash64}` 表示 `hash(fid) % 64` |

filer 模式的进度目录：

```text
{stateDir}/
  job.json
  metrics.json
  volumes/{vid}.json
  success/{vid}.log
  failed.ndjson
  SKIPPED_VOLUMES.log
```

`migrate.volume.verify -level=exists|size|md5` 按路径模板对比目标 Filer。`-level=crc` 只适用于 `-mode=blob`。
