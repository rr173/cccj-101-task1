# research_harvester

科研成果接口的持续增量采集项目：可控的本地模拟服务 + 多租户 SQLite 采集器 +
可执行验收断言。**仅依赖 Python 标准库（≥3.9），完全离线可重复运行。**

## 布局

```
server/mock_server.py    可控模拟服务：增量扫描 API + 脚本化控制面（虚拟时间、
                         故障注入、触发器、请求日志、在途统计）
collector/db.py          SQLite 持久化：raw_pages / result_versions /
                         scan_state / watermarks
collector/collector.py   多租户采集器：每租户一线程 + 全局在途信号量、
                         可缩放退避 Sleeper、410 复合边界重开、401 停租户
run_fetch.py             可配置抓取入口（CLI），输出每租户已确认区间与版本计数
tests/acceptance.py      可执行验收断言（8 个场景，全部离线）
```

## 离线运行命令

```bash
cd research_harvester

# 1) 启动模拟服务（数据 API + 控制面）
python3 server/mock_server.py --port 8734

# 2) 运行采集器（每租户一轮或多轮；--retry-scale 缩放退避等待）
python3 run_fetch.py --server http://127.0.0.1:8734 \
    --db out/harvest.db --tenants alpha,beta --rounds 2 \
    --page-size 50 --in-flight 4 --retry-scale 0.05

# 3) 验收断言（自带服务，无需先启动 1）
python3 tests/acceptance.py
```

`run_fetch.py` 结束时打印每个租户的已确认区间 `[origin, high_watermark]`、
完成轮数、版本数、去重后成果数、墓碑数与页数，退出码仅在全部租户成功时为 0。

## 接口协议（服务端）

`GET /v1/results`（请求头 `X-Tenant-Id`）

- 开扫：`since`（modified_at 闭区间下界）+ `after_accession`（相等时间的
  复合边界 tie-breaker：仅返回 `accession > after_accession` 的同时间记录）+
  可选 `until`（固定闭区间上界，缺省取服务端当前最大 modified_at）+ `limit`。
- 翻页：`cursor=<token>&limit=<n>`。
- 扫描视图在开扫时**冻结**：期间新写入（包括回填到过去时间的写入）不会混入；
  上界 `until` 在首轮响应中固定并随每页返回。
- 撤回成果以 tombstone 返回（`retracted: true`，无 payload）；每条记录版本带
  单调递增 `revision`；服务端保留测试时间段内的全部修改版本。
- 错误：`410` cursor 失效（客户端须从已持久化的复合边界重开，保持原 `until`）；
  `429` 每租户独立限额，带 `Retry-After`；`503` 短暂重试；`401` 停止该租户。

控制面（`POST /control/...`）：`record` / `retract` / `time`（虚拟时间）/
`fault`（按租户注入 429/503/401，次数与 Retry-After 可配）/ `trigger`
（某租户第 N 个请求到达时执行动作序列，用于确定性制造“扫描期间写入”）/
`expire_cursors` / `latency` / `reset`；`GET /control/requests`、`/control/stats`
用于断言（请求日志、最大在途数）。

## 采集器设计要点

- **版本身份去重**：`result_versions` 主键 `(tenant, accession, revision)`，
  `INSERT OR IGNORE` 吸收已确认页的重传；同一成果的新 revision 作为新版本行插入。
- **共同提交**：每页的原始响应（raw_pages）、规范化版本与分页进度
  （scan_state 中已确认的复合边界 + 固定上界）在一个 SQLite 事务内提交；
  整轮高水位（watermarks）仅随最后一页同事务推进。
- **410 恢复**：从 scan_state 读取已确认 `(modified_at, accession)` 与本轮固定
  `until` 重新开扫，相等时间由 `after_accession` 精确接续。
- **并发**：每租户一个线程，退避只睡本线程，不阻塞其他租户；全局
  `BoundedSemaphore` 限制在途请求数；`cancel()` 立即生效，状态已持久化可重启续跑。
- **退避可测**：所有等待经 `Sleeper` 缩放（`--retry-scale`），测试用缩小比例
  验证 Retry-After 行为，不做真实长时间等待。

## 验收场景（tests/acceptance.py）

| 场景 | 断言 |
|---|---|
| T1 | 250 条同秒成果跨 5 页不丢项，高水位正确 |
| T2 | 同时间组内 cursor 过期，从复合边界重开，无缺口无重复，410 落库 |
| T3 | 429 按 Retry-After 等候（缩放计时），退避期间租户 B 持续推进，在途上限生效 |
| T4 | 503 短促重试后恢复，数据完整 |
| T5 | 401 后该租户零后续请求，已确认页保留，其他租户不受影响 |
| T6 | 写入期间 SIGKILL：页写入与进度原子、水位未推进；重启后收敛无缺口 |
| T7 | 扫描期间新增/回填/撤回不进入冻结视图，下一轮全部收敛（含 tombstone） |
| T8 | cancel 及时停止、在途=1 严格成立，新实例从持久化状态续跑收敛 |
