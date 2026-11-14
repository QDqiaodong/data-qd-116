# data-qd-116 · 事务变更日志重放

Transaction-aware change log replay with before-image validation。业务输入、计算和当前边界详见 [数据契约](docs/contract.md)。

## 安装与演示

Python >=3.11，运行和测试仅用标准库；先确认 `python3 --version`。
每仓建立独立环境，构建工具锁版本。首次安装无公共缓存时可能联网；业务运行不访问网络。

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
.venv/bin/python -m pip check
.venv/bin/change-replay --source fixtures/input.json --output run/report.json
.venv/bin/python -m unittest discover -s tests -v
```

本地已有wheel时，安装构建工具可加 `--no-index --find-links <公共wheel缓存目录>`。
修改源码后重新安装。本仓不共享其他仓环境，不依赖Docker、外部账号或持续服务。

输入JSON最多8MB，拒绝重复键、非有限数值和未定义字段；源字节SHA256随结果保留。
输出是可独立读取的JSON；成功后原子替换，输入/计算失败退出2并保留旧报告。
输入和输出不得相同，输出符号链接拒绝。单操作者批处理，不承诺并发发布或断电恢复。
同输入重跑不累加。测试使用临时目录，运行产物在忽略的run/。

src/change_replay/model.py 为业务模型，相关算法模块独立组织；common.py 提供受限JSON及报告发布，
cli.py 衔接数据链路。fixtures 为明确自制样例，代码与样例采用MIT许可。

## 业务与样例结果

最终快照仅客户C2、订单O2=500分；应用T1/T2、等价重传1次、T3保留未来。
transaction.py处理有序镜像与提交时外键，model.py处理日志身份、截止点与状态摘要。
history.py从相邻提交态差异推导SCD2版本；`--history`在result追加history数组，
样例历史为C1[10,12)、C2[11,null)、O1[10,11)/[11,12)、O2[11,null)，未启用时输出不变。
当前无外部CDC采集或持久增量恢复。

## 持久接收链路

store.py 用标准库sqlite3把基线、已应用身份、待处理日志、两表和进度放进一个库文件；
CLI新增三个子命令，原`--source/--output/--history`不变：

```sh
.venv/bin/change-replay init --baseline fixtures/input.json --db run/state.db
.venv/bin/change-replay ingest --db run/state.db --batch fixtures/batch.json --output run/ingest.json
.venv/bin/change-replay export --db run/state.db --output run/export.json
```

`fixtures/batch.json` 为 `{transactions,watermark}`，样例watermark为12、保留重复T1和未来T3：
ingest应用T1/T2、T3持久待处理；export显示watermark=12、last_applied_lsn=12和待处理清单。
水位单调不减且是接收前沿而非应用进度，空洞永久；跨批重传按完整内容幂等，冲突或迟到未知事务
整批拒绝。每批日志、被触及物化行与进度在一个SQLite事务提交，跨进程故障注入在提交前后杀进程
只能看到完整旧版或新版。

v2按主键读写：每笔事务只载入它触及的行，逐笔消费待应用日志；操作顺序与前镜像、事务末外键
不变（允许先写订单后补客户），删除客户时若仍有未触及订单引用则拒绝。`orders`按
`(customer_id,key)`建索引，引用检查不扫全表；普通批次不扫全部客户、订单或已应用日志，也不再
删表重写。init只建v2；export可读v1/v2；首次对v1库ingest时在同一事务内原地迁移，迁移与该批
日志、物化行和进度一起提交或回滚。详见[数据契约](docs/contract.md)。

规模测量（同一合法小批作用于1,000和100,000客户/订单的库）：

```sh
PYTHONPATH=src python3 tools/bench_ingest.py --rows 1000 --sql-trace
PYTHONPATH=src python3 tools/bench_ingest.py --rows 100000 --sql-trace
```

两者ingest的tracemalloc峰值均约0.02 MiB（阈值8 MiB），SQL跟踪无未按键限定的表访问、无全表
DELETE。`export`是全量快照路径，峰值随库增长，不作为批处理指标。对应断言在
`tests/test_store_v2.py`。
