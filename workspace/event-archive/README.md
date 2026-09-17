# Event Archive —— 多来源设备遥测事件归档服务

面向可能离线数小时后补传的设备遥测场景，解决：

- **迟到 / 重复 / 时钟回拨** 的识别与共存；
- 不破坏**每台设备业务顺序**的前提下持续形成可查询分段；
- 任意时间点**冻结视图**并从该视图分页**回放**，冻结后新数据物理上不混入冻结批次；
- 写入确认（HTTP 200）即 **fsync 落盘**，节点重启 / kill -9 不丢已确认数据；
- 分段损坏时**隔离影响范围**，报告可读位置与可继续处理的 WAL 序列号，并可从 WAL 重建。

实现只依赖 **Python 3.11 标准库**，无需外部数据库或消息队列。

---

## 1. 快速开始（容器）

```bash
cd event-archive

# 构建并启动（数据在命名卷 archive-data 中）
docker compose up -d --build

# 健康检查
curl -s http://localhost:8080/healthz
```

一键端到端验证（含 kill -9 持久性与分段损坏注入，需访问数据卷，
在本地用测试 profile 跑）：

```bash
docker compose --profile test run --rm e2e --remote
# 本地全量（含崩溃/损坏注入，不建议在容器网络内做）：
python3 tests/e2e.py
```

## 2. 本地直接运行

```bash
EA_DATA_DIR=./data python3 -m app.main
```

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `EA_DATA_DIR` | `/data` | 数据根目录 |
| `EA_HOST` / `EA_PORT` | `0.0.0.0` / `8080` | 监听地址 |
| `EA_WINDOW_MS` | `3600000` | 分段事件时间窗口（1 小时） |
| `EA_ROTATE_COUNT` | `100000` | 单分段最大事件数，超出开新段 |
| `EA_MAX_OPEN` | `16` | 同时打开的活动分段句柄数，超出封存最老窗口 |
| `EA_LATE_GRACE_MS` | `300000` | 迟到/时钟回拨判定宽限（5 分钟） |
| `EA_FLUSH_INTERVAL` | `0.5` | 刷盘线程空转间隔秒 |

## 3. HTTP API

### 写入

```bash
curl -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"sensor-7","seq":42,"event_ts":1758000000000,"data":{"t":21.5}}'

# 批量（离线补传典型用法）：
curl -X POST http://localhost:8080/v1/events \
  -d '{"events":[{"device_id":"sensor-7","seq":43,...},{...}]}'
```

返回：

```json
{"results":[{"device_id":"sensor-7","seq":42,"accepted":true,
  "duplicate":false,"flags":{"late":true,"rollback":false},
  "wal_seq": 105}], "errors":[]}
```

`flags` 取值：

| 标记 | 含义 |
| --- | --- |
| `late` | 事件时间显著早于接收时间（设备离线后补传的旧数据） |
| `gap_fill` | 序列号跳号，说明设备侧中间序号可能后补 |
| `rollback` | 业务序号前进、事件时间却大幅倒退，判定设备**时钟回拨** |
| `duplicate` | 同设备同业务序号的重复提交，幂等忽略、不分配新 WAL 号 |

**顺序保证**：接收线程在一把锁内完成「分类 → 分配 WAL 序列号 → fsync」，
每台设备看到的确认顺序与 WAL 顺序一致；查询固定按设备业务序列号 `seq` 升序。

### 查询

```bash
curl 'http://localhost:8080/v1/events?device=sensor-7&from=1758000000000&to=1758003600000&limit=1000'
```

迟到但属于仍可写窗口的数据按事件时间窗口落位；时钟回拨的极旧数据
**不回写已封存分段**，进入当前活动窗口并保留 `rollback` 标记。

### 冻结与回放

```bash
# 1) 冻结此刻视图（封存全部活动分段，记录 WAL 高水位 HWM）
curl -X POST http://localhost:8080/v1/snapshots -d '{"name":"incident-2026-09","note":"回放基线"}'

# 2) 从冻结点回放（cursor 翻页；永远只返回 HWM 之内事件）
curl 'http://localhost:8080/v1/replay/incident-2026-09?limit=1000'
curl 'http://localhost:8080/v1/replay/incident-2026-09?cursor=<next_cursor>&limit=1000'
```

回放期间新到数据继续写入，但因为冻结时已封存所有分段，新数据只会进入
**冻结点之后新建的分段**，从物理上不可能混入冻结批次；回放按
`wal_seq` 顺序、硬截止到冻结 HWM。

### 运维

```bash
curl http://localhost:8080/v1/devices     # 设备水位
curl http://localhost:8080/v1/segments    # 分段清单（open/sealed/quarantined）
curl http://localhost:8080/v1/stats       # HWM、计数、最近隔离报告
curl -X POST http://localhost:8080/v1/admin/verify    # 校验并隔离损坏分段
curl -X POST http://localhost:8080/v1/admin/rebuild   # 从 WAL 重建被隔离数据
curl -X POST http://localhost:8080/v1/admin/flush     # 等待已确认数据全部落分段
```

## 4. 磁盘布局与损坏处理

```
$EA_DATA_DIR/
├── catalog.db            # SQLite 目录：分段元数据 / 事件唯一索引 / 设备水位 / 快照
├── wal/
│   └── wal-0000000001.log  # 追加 + fsync 的预写日志，按大小轮转、安全后 GC
├── segments/
│   └── seg-<窗口起点ms>-<段序号>.seg  # CRC32 长度前缀帧，END 帧标识干净封存
└── quarantine/
    └── seg-*.seg.<时间>.bad          # 校验失败被移走的损坏分段
```

启动恢复顺序：

1. 仍为 `open` 的分段：截断尾部撕裂帧（其已确认副本仍在 WAL），补写 END 封存；
2. 逐个 CRC 校验已封存分段：失败则把该段移入 `quarantine/`、
   其事件标记受影响——**影响范围严格限定在该分段**，其它设备查询不受阻；
3. 扫描 WAL，把所有未进分段的已确认记录重放重建；
4. 推进高水位；存在隔离分段时，其所需的 WAL 不予回收。

隔离报告给出：

```json
{"segment":"seg-1789610400000-000002.seg",
 "reason":"CRC 校验失败；可继续位置 offset=1240，其前 18 帧可读 ...",
 "quarantine_path":"/data/quarantine/seg-...bad",
 "resume_after_wal_seq": 59}
```

`POST /v1/admin/rebuild` 即以 `resume_after_wal_seq` 之后的 WAL 记录
把隔离数据重放进全新分段，完成后可正常查询。

## 5. 持久化语义

- 写入路径：WAL 追加帧 → `flush()` + `fsync()` → 返回 200。未返回 200
  的请求不承诺；崩溃留下的尾部撕裂帧在重启时丢弃。
- catalog（SQLite）使用 `synchronous=FULL`，事件唯一约束 `(device_id,seq)`
  跨重启生效；进程内另维护已确认键集合，覆盖「已 fsync 但尚未落分段」
  瞬间的重复请求，保证幂等。
- 分段帧写入同样逐帧 fsync；END 帧 + SHA-256 摘要用于封存校验。

## 6. 设计取舍说明

- 单节点、单卷部署；多副本/跨节点一致性超出当前范围，可由
  上游写入侧把 WAL 目录放到多副本块存储，或后续加复制层。
- 时钟回拨数据按「当前窗口」归档而不改写历史分段，保证已封存内容
  （以及任何冻结视图）永不变异；查询靠事件标记暴露该事实。
- WAL 回收上界在存在隔离分段时自动收窄，直到重建完成。
