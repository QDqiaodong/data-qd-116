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

## 持久接收链路 v1（SQLite）

在离线重放之外提供持久接收。状态全部在一个SQLite库文件中，业务运行只用标准库sqlite3；
旧`--source/--output/--history`路径的行为、输出和退出码不变，持久链路是独立子命令。

- `init --baseline <现有基线或input.json> --db <库文件>`：只读输入里的`baseline`（若给的是
  旧input文档，忽略其transactions/through_lsn），建表并写入两表、baseline_lsn，初始
  `watermark=last_applied_lsn=baseline.lsn`。库文件已存在或为符号链接则拒绝，失败不留库。
- `ingest --db ... --batch <{transactions,watermark}>`：批次字段恰为transactions与watermark，
  事务结构沿用上面的契约（id/commit_lsn/operations，形状、before/after镜像、金额与标识约束）。
- `export --db ... [--output ...]`：输出两表、`customer_order_cents`、`watermark`、
  `last_applied_lsn`、`pending_transactions`（id/commit_lsn/操作数）和schema_version。

水位语义：`watermark`单调不减，是接收前沿，表示此后不会再有`commit_lsn<=watermark`的事务
首次到达；LSN空洞因此永久存在。它不是已应用进度，`last_applied_lsn`可以小于水位。

身份与幂等：批内可乱序；同ID且同LSN且完整内容相同的重复算`duplicate_transactions`，跨批
重传同样幂等。同ID不同LSN、同LSN不同内容或同ID不同内容一律按身份冲突拒绝；`commit_lsn`
不高于旧水位的未知事务同样拒绝（不再首次到达），此类拒绝不区分位于批内何处。

应用：新水位内所有未应用的持久事务按commit_lsn顺序，以私有副本执行完整操作、事务末核外键；
before镜像不符或提交态外键错误则整批不生效。高于新水位的已收事务只验形状，持久暂存为待处理；
later批次水位越过其LSN时转为可应用，同样受完整校验。已应用事务的ID/内容留在库中，重启直接
复用，不重放已应用事务，也不重读旧源文件。

原子性：每批把身份日志（journal）、两表的物化结果、`watermark`与`last_applied_lsn`放进同一
SQLite事务（`BEGIN IMMEDIATE`……`COMMIT`，`synchronous=FULL`）。提交前被杀只留旧版本且重试
可按新批应用；提交后被杀只留新版本，重试全为重复。测试用独立子进程在提交前后`os._exit`证明。
前置的JSON输入校验失败与任何应用错误均整批回滚，不留下半批日志，退出码2，已有库文件与旧输出
保持原样。

不包含：并发写入者协调、网络接收服务、跨库事务、日志压缩/checkpoint裁剪与历史（SCD2）的
持久化；`--history`仍只适用于旧离线路径。

