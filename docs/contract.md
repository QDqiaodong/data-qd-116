# Change replay v1

为离线数据副本核验者，把customers/orders基线与完整事务日志重放成截止LSN快照及客户金额汇总。
不是支付对账、实体事件生命周期或真实数据库连接器；输入是自制的明确变更协议。
基线含lsn及两张key/value表，customers值仅name，orders值仅customer_id/amount_cents。
标识为非空不带首尾空白文本<=128字符，金额为非负整数分<=10^9。基线引用须完整。

事务id、commit_lsn全局唯一，LSN须大于基线；最多2000个事务、每个1000操作。
相同事务完整内容重复算一次；ID或LSN冲突拒绝，不用后到数据覆盖。输入事务可以乱序，
按commit_lsn顺序处理；操作顺序保留。before/after为精确行镜像，null表示不存在。
before须与当前值一致，双方null非法；删除不存在或错误前镜像均失败。
同事务操作先在私有副本执行，事务结束才核外键；允许先写订单再补客户，提交态不得孤儿。

through_lsn>=基线lsn。只应用commit_lsn<=截止值的完整事务，不把大事务切半；LSN可以有空洞。
未来事务校验字段、行形状和身份，但不提前执行before/外键状态约束。
输出真实last_applied_lsn、完整两表、汇总、应用/未来事务清单、重复数与确定性state_sha256。
请求截止点不是已应用进度。样例T1/T2应用，T1重传一次，T3未来；最终仅C2和O2=500分。
相同输入全量重建，不累加、不写外部数据库。整个批次成功后才发布报告。

--history在result追加history数组，未启用时输出契约不变。每条版本含table/key/value/from_lsn/to_lsn，
区间左闭右开，当前版本to_lsn为null；按table、key、from_lsn排序。基线行从baseline.lsn生效，
之后只记录截止点内各事务提交后的净变化，不暴露事务内中间态；无净变化不拆版本，删除关闭区间，
重建同键开启新段，缺失期间不生成墓碑。历史在截止点不截断仍有效版本。前镜像、事务末外键、
重复与未来事务校验与默认路径一致，历史由提交态差异推导，不做逐操作快照。未来事务不产生版本。
任一[baseline.lsn, through_lsn]内LSN（含空洞）按from_lsn<=L且(to_lsn为null或to_lsn>L)
还原两表，与以该LSN为截止点的原重放状态一致。样例历史：C1[10,12)、C2[11,null)、
O1[10,11)/[11,12)、O2[11,null)。失败仍退出2并保留旧报告。

不包含日志采集、真实WAL解析、增量checkpoint恢复、schema演进、并发复制或事务隔离模拟。
这些能力不通过题面预置；后续可按真实需求演进。源快照与输出运行文件分离。

## 持久接收链路 v2（SQLite）

在离线重放之外提供持久接收。状态全部在一个SQLite库文件中，业务运行只用标准库sqlite3；
旧`--source/--output/--history`路径的行为、输出和退出码不变，持久链路是独立子命令。

- `init --baseline <现有基线或input.json> --db <库文件>`：只读输入里的`baseline`（若给的是
  旧input文档，忽略其transactions/through_lsn），建v2表并写入两表、baseline_lsn，初始
  `watermark=last_applied_lsn=baseline.lsn`。库文件已存在或为符号链接则拒绝，失败不留库。
- `ingest --db ... --batch <{transactions,watermark}>`：批次字段恰为transactions与watermark，
  事务结构沿用上面的契约（id/commit_lsn/operations，形状、before/after镜像、金额与标识约束）。
- `export --db ... [--output ...]`：输出两表、`customer_order_cents`、`watermark`、
  `last_applied_lsn`、`pending_transactions`（id/commit_lsn/操作数）和schema_version；
  可读schema_version 1与2，v1库按原JSON行读出，不要求先迁移。

水位语义：`watermark`单调不减，是接收前沿，表示此后不会再有`commit_lsn<=watermark`的事务
首次到达；LSN空洞因此永久存在。它不是已应用进度，`last_applied_lsn`可以小于水位。

身份与幂等：批内可乱序；同ID且同LSN且完整内容相同的重复算`duplicate_transactions`，跨批
重传同样幂等。同ID不同LSN、同LSN不同内容或同ID不同内容一律按身份冲突拒绝；`commit_lsn`
不高于旧水位的未知事务同样拒绝（不再首次到达），此类拒绝不区分位于批内何处。

应用：新水位内所有未应用的持久事务按commit_lsn顺序（同LSN按id）逐笔消费，以私有工作集执行
完整操作、事务末核外键；before镜像不符或提交态外键错误则整批不生效。高于新水位的已收事务
只验形状，持久暂存为待处理；later批次水位越过其LSN时转为可应用，同样受完整校验。已应用事务的
ID/内容留在库中，重启直接复用，不重放已应用事务，也不重读旧源文件。

按键工作集：每笔事务只从库中按主键读取它实际触及(customers.key/orders.key)的行，工作集仅保存
这些键；未触及的行不载入内存，也不参与全表校验。操作顺序保留，before为精确前镜像；事务内
允许先写订单后补客户，外键在事务末按工作集最终态检查（客户在本事务内被删除后又被重建等
中间态不暴露）。删除客户时，若存在未被本事务触及、仍以`customer_id`引用该客户的订单，事务
失败并整批回滚；本事务内一并删除或改挂的订单不阻删。`orders`按`(customer_id,key)`建持久
索引，引用检查按键分页走该索引，不扫描全表。相反方向的孤儿订单（工作集内订单引用了不存在的
客户）同样在事务末拒绝。

写回只针对工作集：存在的客户与订单按主键UPSERT，删除按主键DELETE，不再`DELETE`全表后重写；
被应用事务的journal行逐条`UPDATE ... applied=1`。普通批次的SQL只触及：progress全表（常数行）、
按id/commit_lsn点查identity、`applied=0`的待应用日志前缀（`journal_pending(applied,commit_lsn,id)`
覆盖索引、keyset分页，已应用日志不扫描）以及按键的行读写。`export`本就是全量快照，允许全表读。

原子性：每批把身份日志（journal）、被触及的物化行、`watermark`与`last_applied_lsn`放进同一
SQLite事务（`BEGIN IMMEDIATE`……`COMMIT`，`synchronous=FULL`）。提交前被杀只留旧版本且重试
可按新批应用；提交后被杀只留新版本，重试全为重复。测试用独立子进程在提交前后`os._exit`证明。
前置的JSON输入校验失败与任何应用错误均整批回滚，不留下半批日志，退出码2，已有库文件与旧输出
保持原样。

### v1 → v2 原地迁移

v1把两表存为`(key, value)` JSON文本且无订单引用索引；v2改为类型化列并新增
`orders_customer(customer_id,key)`与更宽的`journal_pending`。`init`只建v2。`export`两种版本
都可读。v1库的**首次`ingest`**在`BEGIN IMMEDIATE`内原地迁移：重命名旧表、建v2表与索引、
按512行分块流式搬迁两表、重建索引并写`schema_version=2`；随后同批日志应用、新表物化、
watermark/last_applied与迁移本身在同一个事务中一起提交或回滚。迁移失败（如某笔待应用事务
before不符）时库仍停在v1，旧表、旧索引、journal与进度原样保留，重试可再迁移。提交点前后
被杀同样只暴露完整v1或完整v2+本批。迁移后旧JSON列不再存在；导出字段保持不变。

不包含：并发写入者协调、网络接收服务、跨库事务、日志压缩/checkpoint裁剪与历史（SCD2）的
持久化；`--history`仍只适用于旧离线路径。

### 离线副本核验 `audit`

`audit --db <库文件> --reference <NDJSON 快照> --output <报告>` 在离线副本核对数据库与外部
快照，**只读**：数据库以`mode=ro` URI 打开并加`query_only`，不迁移、不 checkpoint、不改写
源库；v1/v2 均可读，v1按旧JSON列读出。

参考快照为无界面的行式 NDJSON（可超过8MB、十万行），行序任意：第一行恰为`{"watermark":N}`，
此后每行为`{"table","key","value"}`，两表的 key/value 约束与仓库其余路径相同（标识、金额上限、
值字段恰为name或customer_id/amount_cents）。全文件流式校验：空文件、首行非水位、坏行/坏末行
（含非UTF-8、重复JSON字段、超过1MB的单行）、重复键（按表）、未知表、非法值和孤儿订单均拒绝，
错误带行号。
参考行按`(table,key)`规范化为临时SQLite文件，**不把整个文件装进内存**；成功或失败都删除临时
文件。水位必须等于库的`watermark`，否则拒绝核验。

数据库在单个`BEGIN`（deferred read transaction）内先读`progress`再按主键序流式扫描两表；
库表与进度来自同一读快照，因此WAL 下与`ingest`交错提交时报告不会混合两个版本。比较为按键
归并连接，逐行流式，报告只保留每类前20条：

```json
{"schema_version":1,"source_sha256":"...","progress":{"schema_version":2,"baseline_lsn":10,
 "watermark":12,"last_applied_lsn":12},"reference_watermark":12,"sample_limit":20,
 "tables":{"customers":{"db_only":{"count":0,"samples":[]},
                        "reference_only":{"count":0,"samples":[]},
                        "value_differs":{"count":0,"samples":[]}},
           "orders":{...}}}
```

`source_sha256`是参考文件原始字节的SHA256。每类的`count`是精确总数，`samples`按key排序取前20
条，每项含`key`、`db`（仅库有的一方为null）与`reference`（仅快照有的一方为null）的双方值。
表存在差异是正常结果，退出0；坏快照、水位不等、库不存在/非本工具库或发布失败退出2并保留旧
报告。全文件验证并比较成功后报告才原子发布，失败清理临时库文件。仅用标准库。

不包含：修复数据、合并差异、写回库或快照、跨库比较、参考快照的压缩/分片传输。旧
`--source/--output/--history`与`init/ingest/export`行为不变。

### 规模与测量

同一合法小批（新增客户、跨客户转单、同键两次修改、一笔未来事务）分别作用于1,000行和
100,000行（客户与订单各N行）的库，`tracemalloc`在`ingest`调用的峰值增量均为约0.02 MiB，
要求不超过8 MiB且不随库增大；库文件字节在批前后不变（除journal新增行）。`export`是全量
路径，峰值随库增长（100k行约69 MiB），不作为普通批次指标。`tools/bench_ingest.py`记录造数
与测量命令；`tests/test_store_v2.py`用同样的批与阈值断言，并用`sqlite3` trace回调证明普通
批次没有未按键限定的表访问、没有全表DELETE、引用检查走`orders_customer`索引。

