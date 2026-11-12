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
archive/shard.py         压缩 JSONL 分片格式：manifest 头、逐页记录、footer，
                         整份与单页 SHA256，原子写入 + 旁车 manifest
archive/store.py         归档登记（archive_batches / archive_shards /
                         archive_pages），含冻结快照、幂等登记与清理
archive/archiver.py      写文件→从盘校验→登记→清理正文的发布编排与崩溃恢复
archive/inspect.py       只读预览、按租户/日期列出分片、整份与逐页核验
search/tokenizer.py      NFKC+casefold 确定性切词，区间映射回未经规范化的原文
search/parser.py         布尔表达式解析器：字段/括号/短语/AND/OR/NOT，
                         语法错误带字符位置，绝不拼 SQL、绝不回退文本搜索
search/store.py          独立检索库（search_ 前缀表）：词典、带位置/区间的
                         倒排表、文档、水位、消费账本、跳过原因
search/indexer.py        增量索引器：rowid 水位追平、账本识别迟到/改写、
                         单事务原子落定、撤回/恢复、硬退出屏障
search/engine.py         AST 在完整命中文档集合上做集合运算、快照读、
                         相关性/编号排序、分页与原文半开区间高亮
search/naive.py          朴素逐篇参考解释器，验收时与倒排引擎逐篇对照
run_search.py            独立检索入口（CLI）：index/update/rebuild/query/
                         status/explain
tests/search_acceptance.py
                         本地检索验收断言（11 组场景，含真实 SIGKILL，全离线）
run_fetch.py             可配置抓取入口（CLI），输出每租户已确认区间与版本计数
run_quality.py           独立质量处理入口（CLI）：process/status/trace/
                         publish/discard-staging/generations/rules-check
run_archive.py           独立归档入口（CLI）：archive/preview/list/verify/batches
tests/acceptance.py      采集器验收断言（8 个场景，全部离线）
tests/quality_acceptance.py  质量隔离层验收断言（16 个场景，全部离线）
tests/crash_consistency_acceptance.py
                         发布资格/崩溃一致性验收（6 个场景，真实 SIGKILL
                         子进程故障 + 旧库迁移，全部离线）
tests/archive_acceptance.py  原始响应保留/归档验收断言（12 个场景，全部离线，
                         含两处真实 SIGKILL 中断与重跑收敛）
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
python3 tests/crash_consistency_acceptance.py
python3 tests/archive_acceptance.py
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

### 发布资格与崩溃一致性

投影内容（clean/quarantine/retracted 行）、汇总计数（`counts`）和变更履历
（`quality_pending_events` → 发布时提升进 `quality_events`）分属不同的事务。
**“counts 非空”不是可发布的充分条件**：在计数提交之后、履历事务提交之前被
SIGKILL，会留下计数已耐久但履历缺失的 staging。旧实现仅凭 `counts` 放行
`publish`，于是读者看到了新内容，`trace` 却只有旧原因。

`quality_generations.build_phase` 是显式的完整性标记，取代对计数/事件条数的
猜测：

- `building`：构建中或在履历事务完成前被中断，**不可发布**；
- `events_staged`：履历事务已提交，投影与履历都已耐久，**可发布**。

标记与履历行在**同一个事务**写入，因此二者要么同时可见、要么同时回滚。判定
永远基于标记而不是事件数量——一个正常的零事件批次（重评后所有判定都不变）
也会被显式标记为 `events_staged`，不会被误拦；系统也不会临时编造一条
“恢复事件”来掩盖缺失的履历。发布入口（`publish_generation` 与 `publish`
子命令）统一通过 `staging_readiness()` 把关，不完整的批次返回退出码 4 并提示
恢复命令。

`--resume` 会先识别持久状态：标记为 `events_staged` 的批次履历已完整，直接
核对并发布，不重建、不重复写履历；标记缺失或仍为 `building` 的批次按原逻辑
从已发布基准 rebase 重建。恢复始终读回**该代次存储的规则快照**
（`rule_content`），即使本地规则文件在停机期间被改写，也不会重新解释同一批
数据。

### 旧库迁移（build_phase 列）

`init()` 以增量 `ALTER TABLE` 给旧库补列，历史行的标记保持 `NULL`，**不会**
被统一盖成完成标记。发布入口对 `NULL` 标记的 staging 一律拒绝（无法证明其
履历完整），按下列形态处置，命令均为
`run_quality.py process --db <db> --resume`：

| 旧 staging 形态 | 处置 |
|---|---|
| 只有汇总计数、无履历（本次缺陷的现场） | 拒绝 `publish`；`--resume` 重建并补全履历 |
| 残留半个租户的投影行 | 拒绝 `publish`；`--resume` 清空该代次投影后整体重建 |
| 投影/履历/计数看起来齐全，仅缺标记 | 同样拒绝（完整性不可推断）；`--resume` 重建后发布，履历不重复 |
| 已发布/已退役的旧代次 | 原样保留，标记维持 `NULL`，不改动读者可见数据 |

操作人员遇到旧 staging 时：能确认要继续这批数据就执行
`process --resume`（沿用批次内的规则快照）；确认该批作废则执行
`discard-staging` 丢弃后重新处理。不要直接 `publish`。

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
| `quality_generations` | 代次元数据：状态、规则版本/哈希/原文、基准代次、计数、`build_phase` 完整性标记 |
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
python3 tests/crash_consistency_acceptance.py  # 崩溃一致性与旧库迁移
```

## 崩溃一致性验收场景（tests/crash_consistency_acceptance.py）

故障注入只在设置 `RH_QUALITY_CRASH_POINT` 环境变量时生效（生产路径零开销、
行为不变）：子进程在命名屏障处写出标记文件并阻塞，测试驱动随后发送**真实
SIGKILL**，因此复现的是已提交事务的持久状态，而非可回滚的普通异常。

| 场景 | 断言 |
|---|---|
| C1 | 计数提交后、履历事务前被 kill：`publish` 退出码 4（旧实现为 0），读者停留旧代次；`--resume` 重建后论文转 clean、读取进度推进一次、前后原因完整；重复恢复不产生重复履历 |
| C2 | 履历事务执行期间被 kill：事件行与标记随事务原子回滚，拒绝发布，resume 重建无重复 |
| C3 | 最终发布事务期间被 kill：履历已耐久（`events_staged`），可直接发布或 resume 完成，指针只翻转一次、事件只提升一次 |
| C4 | 零事件批次（重评后判定全不变）凭显式标记正常发布，不被误拦；后续无变化仍为 no-op |
| C5 | 旧库三种 staging 形态（仅汇总/半个租户/看似齐全）均拒绝发布、由 resume 重建；已发布旧代次保持原样 |
| C6 | 回归：年份质量判定与 DOI 双向冲突/自动解除在崩溃恢复后仍然正确，履历完整 |

# 原始响应保留与归档（retention / archive）

采集器把每次 HTTP 回包（200 页与错误响应）原样留在 `raw_pages`。本层是把
**过期正文**移出主库的独立能力：`raw_pages` 行（页号、状态码、URL、抓取时间、
`scan_until`）与 `result_versions.raw_page_id` **一律保留**，正文改写为 `NULL`
后改从压缩分片读取。范围仅限原始响应的保留与追溯，不新增质量判断、报表或界面。

## 快照与截止边界

- 每轮归档在**启动时**固定两个值：UTC 截止时间 `cutoff_at` 与当前最大页序号
  `max_page_id`，并写入 `archive_batches`。之后到达的回包（即便回填到过去时间）
  页号超出上限，不属于本轮。
- 截止是**闭区间**：`fetched_at <= cutoff_at` 视为到期。恰好等于截止时刻的
  记录会被归档（有独立断言固定该语义）。
- `--retention-days N`（默认 30）等价于 `cutoff_at = now - N*86400`；
  `--cutoff-at <ts>` 可直接指定。日期分片桶按 **UTC** 日历日
  `YYYY-MM-DD` 划分（`time.gmtime`），与本地时区无关。
- 同一 `cutoff_at` + 租户/日期筛选若存在**未完成**批次，重跑会复用该批次
  已冻结的 `max_page_id`，不会围绕新行重新冻结；`cleared` 终态批次不再复用，
  下一次即新快照。

## 分片格式与布局

路径 `<archive-dir>/<tenant>/<UTC-date>/part-<shard_no>-<snapshot_sig>-<first>-<last>.jsonl.gz`：

- 文件名内嵌冻结快照签名与页号范围：**相同输入快照**总映射到同一路径，
  **另一份快照**永远落到不同路径，不会被旧文件覆盖。
- 文件为 gzip 文本，逐行一个 JSON 对象：首行 manifest、随后按页号升序的页记录、
  末行 footer。manifest 记录租户、UTC 日期、分片序号、`cutoff_at`、
  `max_page_id`、页数、起止页号、每页原文 SHA256 映射与正文摘要；页记录保存
  完整原文 `body` 及其 `body_sha256`；footer 重复摘要与页数以防截断。
- 旁车 `*.manifest.json` 记录整份文件 SHA256 与压缩字节数。
- 整份摘要按“页行按 `\n` 连接 + 末尾换行”的 UTF-8 文本计算，可用
  `gunzip | sha256sum` 之外的自带算法离线复算（验收 A12 用标准库独立复算）。

## 跨资源发布：可中断、可重试

文件与 SQLite 无法放进一个事务，因此发布拆成幂等阶段，批次 `phase` 记录位置：

| 阶段 | 含义 | 重跑行为 |
|---|---|---|
| `planned` | 快照已冻结，尚未写文件 | 正常写入 |
| `shard_written` | 文件已落盘，登记未完成（**崩溃窗口 1**） | 用同一路径重新渲染→发现文件已在→从盘校验→**收养**并登记，不丢数据、不重复 |
| `registered` | 分片已登记，正文可能未清（**崩溃窗口 2**） | 不重写、不重复登记，直接清理正文 |
| `cleared` | 已登记且正文已清 | 幂等 no-op |
| `failed` | 有分片未通过校验，该租户未登记、正文保留 | 可再次运行同一快照收敛 |

- 写入用临时文件 + `fsync` + `link`（目标已存在则失败），永不覆盖。
- **只有从磁盘重新计算整份与单页摘要均通过的分片才会被登记或读取。**
- 登记与正文清理是独立事务；清理按 `archive_pages` 归属逐页执行，因此
  “登记后再崩溃”也能在下一次运行收敛，且不会碰到批次之外的新行。
- 若已有分片内容与校验值不符：**停止并给出定位**（租户、路径、页号），
  不覆盖文件、不清空原正文、不报告成功；该租户其余步骤跳过，
  **其他尚未开始的租户继续归档**（租户隔离）。

## 追踪行为

`run_quality.py trace` 读取页正文时：SQLite 有正文则取自 `sqlite`；正文已移出
则按 `archive_pages` 找到分片、校验后取回，来源标为 `archive`。`raw_pages`
仍按原页号升序返回，**混合状态**（部分已归档、部分在线）的来源顺序与原始内容
保持一致。分片损坏时 trace 以退出码 4 报错，不回退成“查无记录”，也不猜测。

## 命令

```bash
cd research_harvester

# 只读预览：各租户可释放正文容量与分片数量，不写文件、不清正文
python3 run_archive.py preview --db out/harvest.db --retention-days 30

# 归档到期正文（按机构、抓取日期、页序分片）；--no-clear 只登记不清正文
python3 run_archive.py archive --db out/harvest.db \
    --archive-dir out/archive --retention-days 30
python3 run_archive.py archive --db out/harvest.db \
    --archive-dir out/archive --cutoff-at 1700000000 \
    --tenants alpha,beta --fetch-date 2026-01-01 --page-group-limit 200

# 按机构/日期找分片文件
python3 run_archive.py list --db out/harvest.db \
    --tenant alpha --fetch-date 2026-01-01

# 完整性核验：重新计算整份与单页摘要，报告在线/已迁出/校验问题页数
python3 run_archive.py verify --db out/harvest.db --tenant alpha

# 批次编号与当前阶段（供值班决定重跑或停止）
python3 run_archive.py batches --db out/harvest.db
python3 run_archive.py archive --db out/harvest.db \
    --archive-dir out/archive --resume-batch b17000-1234-abcd
```

所有命令均为标准库、完全离线可重复；`preview`/`list`/`verify`/`batches` 不
创建表、不写行、不触碰抓取游标 `scan_state`、水位 `watermarks`、质量投影、
规则代次或处理进度。

## 新增表（均以 `archive_` 前缀，增量创建）

| 表 | 用途 |
|---|---|
| `archive_batches` | 批次编号、冻结的 `cutoff_at`/`max_page_id`、筛选、当前 `phase` |
| `archive_shards` | 每个分片的路径、整份 SHA256、压缩字节数、页数、起止页号、摘要 |
| `archive_pages` | 页号 → 所属分片的归属、单页摘要、清理时间 |

## 归档验收场景（tests/archive_acceptance.py）

| 场景 | 断言 |
|---|---|
| A1 | 归档后 `trace` 读回与归档前逐字节一致的回包；状态码/URL/抓取时间/页号/版本引用仍可查 |
| A2 | 截止闭区间边界；启动后写入（含回填时间）的记录页号超限，不进入本轮 |
| A3 | 按机构和 UTC 日期分片；一个租户坏分片被定位且不阻断另一租户 |
| A4 | 文件落盘后、登记前 SIGKILL：重跑收养同一文件，不丢数据、不生成重复 |
| A5 | 登记后、清理前 SIGKILL：重跑收敛清理，不重写、不重复登记 |
| A6 | 校验失败：保留 SQLite 原文、不登记坏分片、拒绝覆盖、`verify`/`trace` 定位 |
| A7 | 归档扫描期间新写入的行保持未归档；下一快照再处理 |
| A8 | 混合归档状态：成果版本、质量投影与 trace 仍可读，来源顺序一致 |
| A9 | 只读预览不生成文件、不清正文、不建表，容量与分片数可信 |
| A10 | 相同快照重跑复用已验证文件；另一快照不覆盖旧路径 |
| A11 | 不修改抓取游标、水位、质量代次/进度；批次编号与阶段可查询 |
| A12 | 分片格式可用标准库独立复算整份与逐页摘要 |

```bash
python3 tests/archive_acceptance.py   # 退出码 0 表示全通过
```

# 本地布尔检索层（local search）

采集器只负责落地，质量层负责合规口径，**检索层只回答“哪些成果的标题/摘要
同时提到了哪些实验条件”**。它是完全离线的独立模块：

- 检索数据来自落地库 `result_versions` 的 `title` 与 `abstract`（取自 payload
  JSON），**检索库是独立 SQLite 文件**，落地库以只读方式打开，检索层不改任何
  采集表、不联网、不接入抓取线程；建索引/跟进更新只在显式命令里运行。
- 表达式**绝不拼接成 SQL**：查询先解析成语法树，SQL 只做词典精确查找和倒排行
  读取，命中集合在 Python 里按语法树做并/交/差；解析失败直接报带位置的语法错，
  **不会**退化成普通文本搜索。

## 查询语法（规则即契约）

```
or_expr  := and_expr (OR and_expr)*
and_expr := factor ((AND? factor) | (AND? NOT factor))*
factor   := primary (NOT primary)*
primary  := '(' or_expr ')' | leaf
leaf     := [字段 ':'] ( 短语 | 词 )
短语     := '"' ... '"'          # 引号必须闭合
字段     := title | abstract     # 其他字段名是未知字段错误
```

- **运算符**：`AND`/`OR`/`NOT` 仅识别大写 ASCII 关键字。相邻检索项不写运算符
  时按 **默认 AND** 处理（如 `title:x title:y`）。
- **NOT 只作二元差集**：`A NOT B` 与 `A AND NOT B` 等价，都是集合 `A − B`；
  前导 `NOT`、`OR NOT` 没有左操作数，报语法错。
- **字段**：只允许 `title`、`abstract`；不带字段的词项同时检索两个字段。
  字段限定与否定表达式都在查询入口给定的机构（tenant）内求值，不会带进其他
  机构的文档。
- **短语**：双引号内的内容经同一套规范化/切词后，要求这些词项在**同一字段内
  占据连续词位**。短语不可能从标题末尾跨到摘要开头（两个字段分别编号词位）。
- **未加引号的多词项词**（如汉字 `蛋白`）切出多个词项，按隐式 AND 处理；只有
  加引号才要求连续词位。
- **必报的语法错误（带 0 起算字符位置和 `^` 指示）**：未知字段、空括号 `()`、
  空短语 `""`、残缺运算符（`a AND`、`a OR`、`a NOT`、`AND a`、`a AND OR b`）、
  前导/`OR` 后的 NOT、未闭合引号、未闭合括号、多余右括号、裸冒号。
  退出码 3，且不产生任何检索结果。

## 切词与原文区间约定（确定性）

每个字段独立编号词位，顺序固定：

1. 先做 **Unicode NFKC**，再做 **`str.casefold()`**；
2. 连续拉丁字母或十进制数字组成一个词；**每个汉字（CJK 表意文字）单独占一个
   词位**；标点与空白只起分隔作用，**不占词位**；其他文字/符号同样只分隔。
3. 返回的命中区间是**半开 `[start, end)` 字符区间，按未经规范化原文的 Unicode
   字符计数**，`text[start:end]` 切回原文。规范化长度变化也不标错：
   - 全角 `ＡＢＣ` → 词 `abc`，区间仍是原文 3 个全角字符；
   - 连字 `ﬁle`（ﬁ 是单字符）→ 词 `file`，区间覆盖原文 3 个字符；
   - `Straße` casefold 成 `strasse`（ß 展开为两个字符），区间仍按 6 个源字符；
   - 组合字符（如 `e` + U+0301）在 NFKC 下被合成，区间按合并前的源字符计数。

实现先逐源字符做 NFKC 并保留其源区间，再处理跨字符的规范合成，最后对每个
对齐片做 casefold；词的区间是其所有片源区间的并集。

## 排序、评分与分页

- **相关性（默认 `order=relevance`）**：得分 = 该文档命中的**不同查询叶节点
  数**（同一个叶在文中出现多次不重复加分；NOT 右支、OR 未命中的分支不加分也不
  产生高亮）。同分按 `(tenant, accession)` 字典序，排序完全确定。
- **按成果编号核对（`order=accession`）**：直接按 `(tenant, accession)` 排序。
- `total` 是**完整匹配集合**的大小，与分页无关；`offset/limit` 在排序后的完整
  集合上切片，页之间无缺口、无重复。
- 一次查询在一个 WAL 读快照事务内完成（进入事务立即用一次读固定快照），因此
  查询内的 total、排序、摘录来自同一代索引；查询进行中索引更新提交也不影响
  本次结果。
- 每个返回项带原文摘录（`【…】` 标注高亮）与 `title_spans`/`abstract_spans`
  两组半开区间；否定分支的词不高亮，OR 未命中分支不制造区间。

## 文档身份、增量与生命周期

- 身份沿用采集库的 `(tenant, accession, revision)`。**每个机构同一成果只展示
  最新修订**：索引里 `(tenant, accession)` 唯一，仅存当前修订的原文与倒排行。
- 消费进度：每机构 `search_cursor` 记录已消费到的 `result_versions.rowid`
  水位；另有 `search_ledger` 记录每个身份的内容哈希。首次建索引全量扫描，之后
  `update` 只读 `rowid > 水位` 的新行，**不会每次全文重扫源表**。采集落地是只
  追加的（仅 `INSERT OR IGNORE`，rowid 单调），因此迟到版本的 rowid 也高于水位、
  必被增量读到；账本让重传的相同身份幂等吸收。若有人在库外原地改写旧行，可用
  显式 `rebuild` 整体对账。
- 新版本：在**同一事务**内删旧倒排、写新倒排、换当前修订原文，并把水位推进到
  本提交分块已处理的最大 rowid，因此不会出现“搜到旧词却显示新摘要”。迟到的
  较小修订永远不覆盖当前文本。
- 撤回（最新修订 `retracted=1`）：文档从检索中删除（结果里消失）；之后更高的
  非撤回修订到达时重新写入、恢复可搜。
- **空摘要**：不产生摘要倒排，但标题照常可检索，并在返回里标注“空摘要”。
- **非文本字段**：对象、数组、数字等绝不 `str()` 成字符串混入索引；该字段跳过，
  原因写入 `search_skips` 并随文档行的 `title_skip`/`abstract_skip` 返回。
- **原子落定与续跑**：数据按固定分块（默认每块 500 个成果）提交，每个分块的
  词位、原文、当前修订与该块水位在一个事务里落定。进程在事务提交前被硬退出
  （验收用真实 SIGKILL/`os._exit`）时整个事务回滚，读者仍看到上一完整代，
  水位停在前一分块；再次 `update` 从持久水位接续，账本保证重放幂等，未处理的
  后续分块不会被跳过。

## 命令

```bash
cd research_harvester

# 首次建立索引（全量扫描落地库；落地库只读打开）
python3 run_search.py index --db out/harvest.db --index out/search.db

# 追平后来写入的资料（增量，只读新 rowid；可限定机构）
python3 run_search.py update --db out/harvest.db --index out/search.db \
    --tenants alpha,beta

# 显式丢弃并整体重建（需要时才全量重扫）
python3 run_search.py rebuild --db out/harvest.db --index out/search.db

# 查询：--tenant 是必填的机构入口；表达式作为一个参数传入
python3 run_search.py query --index out/search.db --tenant alpha \
    'title:光谱 AND (abstract:"mass spectrometry" OR abstract:蛋白)' \
    --order relevance --limit 10 --offset 0
python3 run_search.py query --index out/search.db --tenant alpha \
    'abstract:mass NOT abstract:balance' --order accession --json

# 查看语法树与规范化后的叶词项（不访问索引）
python3 run_search.py explain 'title:光谱 AND abstract:"mass spectrometry"'

# 索引元数据、每机构水位、文档/词项/跳过计数
python3 run_search.py status --index out/search.db --json
```

查询退出码：`0` 成功（含 total=0）；`2` 入口参数错误；`3` 表达式语法错误。
JSON 输出包含 `total/offset/limit/order/results`，每个结果含原文、半开区间
（`title_spans`、`abstract_spans`）、摘录与跳过原因。

## 检索库表（全部 `search_` 前缀，独立文件，增量创建）

| 表 | 用途 |
|---|---|
| `search_meta` | schema 版本、上次同步时间 |
| `search_cursor` | 每机构 rowid 水位（读取位置） |
| `search_ledger` | 已消费身份 `(tenant,accession,revision)` + 内容哈希，识别迟到/改写 |
| `search_docs` | 每 `(tenant,accession)` 当前修订的原文、词数、撤回态、跳过原因 |
| `search_terms` | 规范化词项词典 |
| `search_postings` | 词项→文档→字段：词位 positions 与原文半开区间 spans、tf |
| `search_skips` | 非文本/缺字段的跳过审计（字段、类型、原因） |

连接以 `isolation_level=None` 显式管理 `BEGIN/COMMIT`，并开启 WAL；只读落地
连接带 `mode=ro` 与 `query_only`。

## 验收场景（tests/search_acceptance.py）

样本固定覆盖：跨字段假短语、全角/连字/casefold/组合字符等不同规范化长度原文、
只命中 OR 一侧的论文、空摘要、非文本字段；再经历改摘要、迟到旧修订、撤回与
恢复，并在索引写事务中真实 SIGKILL 后接续。引擎结果与 `search/naive.py`
朴素解释器逐篇对照（集合、得分、高亮区间、排序、total、分页），不盲信倒排引擎。

| 场景 | 断言 |
|---|---|
| S1 | 合法语法全接受；未知字段/空括号/空短语/残缺运算符/未闭合引号等均带位置报错；引擎不回退文本搜索 |
| S2 | 全角、连字展开、casefold 变长、组合字符合并下，半开区间仍对应未经规范化原文 |
| S3 | 复杂表达式真实命中与高亮，区间切回原文字符 |
| S4 | 跨字段假短语不命中；字段限定 AND 仍可独立命中 |
| S5 | OR 单侧命中不制造另一侧高亮；NOT 右支不高亮；差集语义正确 |
| S6 | 非文本字段给原因、不字符串化；空摘要可按标题检索 |
| S7 | 引擎与朴素解释器在全部表达式、两种排序、分页下集合/得分/区间/total 全一致 |
| S8 | 改摘要移除旧词；迟到低修订不覆盖；撤回消失；高修订恢复；追平后零行重扫 |
| S9 | 写事务内真实 SIGKILL：水位不前移、无部分文档、旧词配旧文；接续后收敛到新修订 |
| S10 | 机构为必填且不串机构；字段限定与否定不带来其他机构文档 |
| S11 | 一次查询固定单一代快照，并发提交不改变本次 total/排序/摘录 |
| S12 | 多分块提交时在第二个分块事务内真实硬退出：仅首个分块可见、水位不跳过未处理行；接续后无丢失无重复，水位到达真实最大 rowid |
| 边界 | 重复词短语（`"long long"`）不塌缩；标点不占词位、短语可跨标点；空集 total=0 |

```bash
python3 tests/search_acceptance.py   # 退出码 0 表示全通过（343 项断言）
python3 tests/acceptance.py          # 原采集器验收，保持兼容
python3 tests/quality_acceptance.py
python3 tests/crash_consistency_acceptance.py
python3 tests/archive_acceptance.py
```
