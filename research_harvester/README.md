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
quality/rules.py         可配置质量契约：加载校验、DOI 规范化、逐字段判定
quality/rules/v1.json    规则文件（明确 rule_version，可另存新版本）
quality/store.py         质量投影存储：代次、原子发布指针、读者视图、审计表
quality/processor.py     质量隔离处理器：增量识别、逐租户重建、冲突解析、
                         代次准备→核对→原子切换
run_fetch.py             可配置抓取入口（CLI），输出每租户已确认区间与版本计数
run_quality.py           独立质量处理入口（CLI）：process/status/trace/
                         publish/discard-staging/generations/rules-check
tests/acceptance.py      采集器验收断言（8 个场景，全部离线）
tests/quality_acceptance.py  质量隔离层验收断言（16 个场景，全部离线）
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
python3 tests/quality_acceptance.py
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

# 质量隔离层（quality isolation）

采集器只负责“把资料落地”；质量隔离层是下游研究统计的**唯一读取口径**。
它只读 `raw_pages` / `result_versions`（绝不联网重取、绝不补猜历史），把合规
成果投影到 `quality_clean`，把不合格的放到 `quality_quarantine`，撤回放到
`quality_retracted`，通过一个 pointer 原子发布。

## 身份与质量契约

- 身份：`(tenant, accession, revision)`；同一身份重放不会产生第二条有效行或
  第二条隔离记录（投影按 `(tenant, accession, generation)` 唯一）。
- 每个成果只判定**最新 revision**。最新修订破坏契约时整条成果隔离，**不会**
  沿用旧的合格内容冒充当前数据。撤回优先级最高：撤回后下游不可见。
- 规则文件 `quality/rules/v1.json` 带明确 `rule_version`，可配置：
  - `title`：非空文本、长度上限、禁止控制字符；
  - `publication_year`：必须是整数（bool/浮点拒绝）且在范围内；
  - `doi`：可选；规范化（小写、去 `doi:`/`https://doi.org/` 等前缀、去首尾
    空白），并匹配 `10.<前缀>/<后缀>` 正则；标识符**内部空格不会被删除**，
    否则会“拼”出一个不存在的 DOI。
- 规则编号固定并写入隔离记录：`R-PAYLOAD-00x`、`R-TITLE-00x`、
  `R-YEAR-00x`、`R-DOI-00x`。每条隔离记录保留 `rule_ids`、命中的字段/原值
  （`reasons`）、完整原始字段（`original`）与来源 `raw_page_id`。
- DOI 冲突：两个仍有效成果归一化后 DOI 相同 → **两篇都隔离**（`R-DOI-004`）。
  判定每次都从落地资料重算，因此一方修订改正或撤回后，另一方**自动解除**。

## 代次、原子发布、续跑

- 新代次先写入带 `generation_id` 的 staging 行；完成核对（无重复 clean、
  clean/quarantine/retracted 互斥、DOI 唯一、计数一致）后，用**一个事务**
  提升输入账本、推进进度、写入迁移事件、翻转 `quality_pointer`。
- 旧代次随即 `retired` 并清空其行；读者通过视图
  `quality_current_clean` / `quality_current_quarantine` /
  `quality_current_retracted` 读取，永远只看到**一个完整代次**。
- 中途退出只留下未发布的 staging，原代次不受影响。`--resume` 从已发布基准
  重新对齐后继续（沿用该批开始时读取的规则快照）；`discard-staging` 可安全
  丢弃；对已发布代次重复 publish 是无副作用的幂等操作。
- 处理失败（核对失败、异常）不会推进已发布代次。

## 规则版本与重评

- 一次处理只使用**开始时读取**的那份规则（快照同时存入代次表），处理中替换
  文件不影响本批判断。
- 新规则版本默认会被拒绝（退出码 3），必须显式 `--reevaluate`；重评会重判
  所有已读资料并同步撤销过时的合格/隔离状态，事件表保留前后状态、前后规则
  版本与原因。同一 `rule_version` 内容不同也会被拒绝，除非显式 `--reevaluate`。

## 增量进度

- `quality_progress` 按租户记录 rowid 水位与累计输入数；但识别新输入时不会
  只看“每篇最大 revision”——每条落地行都用身份 + 内容哈希与
  `quality_inputs` 账本比对，因此迟到行、较小 revision、落在 rowid 空洞里的
  行、原地改写都能被识别。

## 命令

```bash
cd research_harvester

# 首次处理（读落地库，生成并原子发布代次）
python3 run_quality.py process --db out/harvest.db \
    --rules quality/rules/v1.json

# 只看当前代次/规则/计数
python3 run_quality.py status --db out/harvest.db

# 追踪某成果：源资料、采用的规则、隔离/恢复原因
python3 run_quality.py trace --db out/harvest.db \
    --tenant alpha --accession a0001

# 只准备并核对、不发布；可安全丢弃
python3 run_quality.py process --db out/harvest.db --stage-only
python3 run_quality.py discard-staging --db out/harvest.db

# 中断后续跑（沿用该批规则快照）
python3 run_quality.py process --db out/harvest.db --resume

# 规则升级后显式重评
python3 run_quality.py process --db out/harvest.db \
    --rules quality/rules/v2.json --reevaluate

# 代次历史与规则文件校验
python3 run_quality.py generations --db out/harvest.db
python3 run_quality.py rules-check --rules quality/rules/v1.json
```

数据库为空的租户可用 `--tenants alpha,beta` 限定处理范围；未落地任何行的租户
会被报告为 `skipped`，不会凭空生成数据。

## 新增表（全部以 `quality_` 前缀，增量创建，不改动原库）

| 表/视图 | 用途 |
|---|---|
| `quality_generations` | 代次元数据：状态、规则版本/哈希/原文、基准代次、计数 |
| `quality_pointer` | 唯一发布指针（id=1），读者视角的原子切换点 |
| `quality_clean` / `quality_quarantine` / `quality_retracted` | 带 `generation_id` 的投影行 |
| `quality_current_*` | 三个读者视图，只解析当前 pointer |
| `quality_inputs` | 已消费落地身份的持久账本（含内容哈希） |
| `quality_pending_inputs` / `quality_pending_events` | 代次内暂存，发布时提升 |
| `quality_progress` | 每租户 rowid 水位与累计输入数 |
| `quality_runs` | 每次处理尝试的状态与计数 |
| `quality_events` | 每次判定变化的前后状态、规则版本、原因（供追查） |

## 质量层验收场景（tests/quality_acceptance.py）

| 场景 | 断言 |
|---|---|
| Q1 | 非法年份/缺字段/格式错误被隔离，其余正常发布；记录规则号、原值、raw_page_id |
| Q2 | 较新坏修订遮蔽旧合格内容；撤回优先级最高、下游不可见 |
| Q3 | DOI 大小写/前缀归一触发双向冲突；改正或撤回后自动解除 |
| Q4 | 不同租户同 DOI 互不影响 |
| Q5 | 迟到行、较小 revision、低 rowid 复用、原地改写都能识别（非最大 revision 进度） |
| Q6 | 规则升级显式重评，原隔离资料重新纳入，保留前后原因 |
| Q7 | 处理中替换规则文件不改变本批判断；代次保存启动时的规则快照 |
| Q8 | 同版本号不同内容默认拒绝发布，显式重评才允许 |
| Q9/Q11 | 异常（含真实 SIGKILL）退出时下游仍是完整旧代次；恢复后只切换一次 |
| Q10 | 不一致的 staging 拒绝发布；处理失败不推进已发布代次 |
| Q12 | `--stage-only`/丢弃对读者不可见，未发布的输入不会丢失 |
| Q13 | 相同身份重放不产生重复有效行/隔离记录 |
| Q14 | 采集器原表与历史数据逐字节保留；trace 可回溯源页 |
| Q15 | 直接消费采集器真实落地输出（含 raw_page_id 关联） |
| Q16 | 租户范围处理不改动其他租户；无新输入为 no-op |

```bash
python3 tests/quality_acceptance.py   # 全部离线，退出码 0 表示全通过
python3 tests/acceptance.py           # 原采集器验收，保持兼容
```
