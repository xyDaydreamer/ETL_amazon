# 阶段1验收记录

验收日期：2026-11-03（Asia/Shanghai）。本阶段只处理有界真实样本和明确标记的构造测试；未执行全量解析、网络采集或电影匹配。

## 阶段1修正（v2，最终有效验收）

本节晚于下方v1初验，若统计或表名冲突，以本节为准。未删除v1证据；业务库迁移前基线为1000原始记录、1000个v1结果（993有效/7隔离）、16产品、976用户、2检查点、2运行。

### 实现与迁移

- `003_stage1_versioned_reparse.sql`把旧结果原位保留为`staging.review_parse_result`的v1历史，并回填1000个稳定`staging.review_record`身份。原始记录保持1000条，无副本。
- v2结果以`(source_review_id, parser_version)`唯一；当前视图只选择显式激活版本。检查点主键包含解析版本，另固定范围起止。
- ASIN观察与评论整体有效性分离；用户ID不做有损规范化；显示名按评论时间、记录号、稳定ID确定性选择。
- 全部原始字节以Base64安全表示；NUL仅在可读派生值中置换并记录问题。评分、票数和时间越界时规范值为NULL，不舍入、不截断。
- v2定位单独保存在`staging.review_parse_locator`；`end_line`是最后一个内容行，空行分隔符不计入，字节范围同样排除分隔空行。
- 正式默认装载策略是COPY临时表后集合合并；逐条SQL路径保留为基线。

迁移在独立测试库重复执行后应用业务库。业务库回填结果：

| 指标 | v1 | v2/current |
|---|---:|---:|
| 解析结果 | 1000 | 1000 |
| 有效 | 993 | 993 |
| 隔离 | 7 | 7 |
| 可用ASIN观察 | 未独立记录 | 1000 |
| 原始记录/稳定身份 | 1000/1000（共享） | 1000/1000（共享） |

v1/v2分类未变化，因为7条仍无依据修复其非法UTF-8；但7条隔离评论全部贡献可靠ASIN。产品仍为16，因为这些ASIN已由其他评论出现。用户从976增至982：v2允许6个隔离评论中的可靠用户ID进入用户表；真实样本v1/v2用户ID本身差异为0，并非规范化合并造成。按时间/记录号/稳定ID重算期望显示名后，与目标表不一致数为0。当前视图恰好1000条，不会把两个版本重复计数。

### 异常、定位与编码证据

17个解析单元测试全部通过，覆盖空行/连续空行/EOF、正文冒号、缺失/空/超长/冲突ASIN、相同ASIN重复、NUL、NaN、正负Infinity、评分精度、integer边界、bigint边界前后值及超大时间戳。

独立测试数据库的9条构造记录实际落库通过：正文非法UTF-8仍创建唯一测试ASIN产品；NUL出现在正文、摘要、显示名和未知字段时JSONB可写且Base64往返一致；4种非有限/精度异常评分均为NULL；冲突ASIN不选值；相同ID不同内容抛出`ContentConflictError`并保留原文。

真实7条非法UTF-8位于记录415、439、489、525、604、738、745；字段为5个显示名和2个正文。错误字节分别包括`f8/fc/ae/f8/fc/ef/f4/c1`，位置与短十六进制上下文由`encoding-audit`导出。字节和值分布不构成可靠统一编码证据，因此继续隔离，不指定备用编码。

真实记录415按v2定位恢复1263字节，SHA-256为`49ad4dd780ae0e1d81c47c6203525452da2f122920363f863f1b3c786fdbc6b8`，与数据库一致且未输出正文。1000条v2定位均与旧v1行号不同，原因恰是v1把分隔空行计入`end_line`；v2统一排除。

### 幂等、恢复与性能

- v1存在后，v2首次回填确实插入1000个新解析结果，原始/身份计数仍1000；同版本新分区重放插入0、识别已有1000。
- 构造测试提交前中断：检查点0，恢复插入完整6条。提交后中断：检查点2，恢复从头扫描并跳过2条，最终无漏无重。
- 业务检查点对同一分区同时保留v1与v2两行，均为范围1–1000、`sample_complete`，版本不会互相阻止重算。

独立数据库、批量500、同一机器实测（数据库增长含索引/WAL可见数据页，不代表全量保证）：

| 规模 | 策略 | 首次秒数/吞吐 | 首次库增长 | 新分区重复秒数/结果 |
|---:|---|---|---:|---|
| 1000 | 逐条SQL | 3.453 / 289.6条秒 | 8,167,424 B | 0.824 / 0新增、1000已有 |
| 1000 | COPY集合 | 0.500 / 2001.0条秒 | 8,208,384 B | 0.362 / 0新增、1000已有 |
| 10000 | 逐条SQL | 34.008 / 294.0条秒 | 75,513,856 B | 8.684 / 0新增、10000已有 |
| 10000 | COPY集合 | 11.450 / 873.4条秒 | 75,276,288 B | 18.532 / 0新增、10000已有 |

首次装载COPY明显更快，因此被选为默认。10000条“新分区重复”COPY较慢，原因是每批仍做集合冲突检查；正常故障恢复沿同一检查点跳过已提交前缀，不等同于该人工场景。以上只支持有界选型，不外推为791万条全量保证。

### Airflow最终复验

- DAG导入错误0，显式记录v2、范围和COPY策略；导入期没有大文件、网络或数据库I/O。
- 首个v2运行因DAG仍引用v1指标名，在业务装载完成后的日志阶段失败并自动尝试3次；未产生重复解析结果。修正指标名后，`manual__stage1_v2_success_after_fix_20261103`一次成功。
- `manual__stage1_v2_retry_proof_20261103`第一次尝试在首批事务提交后注入中断，状态`up_for_retry`；第二次跳过已提交500条并成功，最终任务`try_number=2`、DAG成功、检查点1000。
- 日志位于`airflow/logs/dag_id=stage1_review_sample_ingest/run_id=manual__stage1_v2_retry_proof_20261103/`。XCom只含版本、路径、批次、范围和少量计数。

### 结论和未验证事项

阶段1修正通过，代码、数据库和Airflow均已真实运行。已具备进入阶段2“受控全量评论处理”的技术前提，但尚未证明整个gzip完整、全量时长/磁盘/WAL上限或最终791万条统计；进入全量前应先定义容量监控、保留空间阈值和全量验收查询。本轮未执行全量解析、网络采集或电影匹配。

## 修改内容

- 新增 `review_parser.py`：gzip二进制流逐行解析、空行/EOF边界、完整字段前缀、稳定ID、原始/规范字段、异常与定位。
- 新增 `review_loader.py`：分批事务装载、检查点、稳定键内容比较、冲突报告、产品/用户关系和质量问题。
- 新增 `002_stage1_review_ingest.sql`：样本范围、恢复策略、gzip状态、评论原始/规范字段和条件约束/索引。
- 新增 `stage1_review_sample_ingest.py`：手动触发、无历史补跑、单输入并发限制、重试、失败分支和小型XCom。
- 新增7项构造解析测试和1项构造内容冲突测试；构造测试未写入业务库。
- 更新全新数据库初始化脚本、README、要求矩阵、决策与AI使用记录。

## 源文件与范围

- 路径：`D:\amazon\movies.txt.gz`。
- 当前大小：3,321,791,660字节，与阶段0记录完全一致；当前最后写入时间为2026-10-27 08:14:47 UTC。
- 复用阶段0已计算SHA-256：`4B6EE09EA4880F3FB197B1407D45DFB08A892FBB48A9E7A2D74AA0572CDCC715`。因路径和大小一致且无变化证据，本阶段没有重复扫描3.3GB计算哈希。
- 阶段1只成功读取前1000条并另在恢复测试库读取到前1400条。这证明样本可解压，不证明整个gzip流完整；数据库状态为 `sample_readable`，不是 `verified_complete`。
- 文件未被程序写入；Compose以只读方式挂载。Windows文件系统只读属性为false，因此“不修改”由只读挂载和程序打开模式保证，而非文件属性位。

## 解析验收

真实前1000条统计：

| 指标 | 结果 |
|---|---:|
| 读取 | 1000 |
| 有效 | 993 |
| 隔离 | 7 |
| 分类平衡 | 1000 = 993 + 7 |
| 字段质量问题 | 7个 `invalid_utf8` |
| helpfulness为0/0 | 230；`helpful_ratio`均为NULL |

实际helpfulness格式为 `7/7` 等斜线格式。初版规则误按方括号格式解析并得到1000条隔离；该结果被判定无效，依据真实值修正规则后重跑得到上表结果。非法UTF-8记录没有静默丢弃：记录被隔离，替换文本仅供查看，原始字节定位、内容哈希和Base64证据均保留。

构造测试共7项，全部通过：正文冒号、空字段、EOF无空行、非法UTF-8、重复字段、未知字段、异常评分/票数、0/0与方括号helpfulness变体、稳定ID。构造数据与真实样本在测试文件和输出中明确区分。

记录1的数据库定位为行1–9、解压偏移0–2106。`verify-locator`恢复2106字节，SHA-256与数据库 `f1bfc43...84dd5` 完全一致，且没有输出正文。解压偏移不是压缩字节随机访问点，恢复需从gzip起点解压到目标位置。

## PostgreSQL与恢复验收

业务库最终保持阶段1范围：1000源记录、1000暂存评论、993有效、7隔离、16产品、976用户、7质量问题、无暂存孤儿。

测试数据库：`amazon_movies_stage1_test`；恢复数据库：`amazon_movies_stage1_recovery_test`。二者与业务库隔离，不代表后续全量业务结果。

| 场景 | 实际结果 |
|---|---|
| 首次装载 | 5批×200；插入1000；检查点1000 |
| 相同检查点重跑 | 从头扫描1000、跳过1000、零新增；所有计数不变 |
| 新分区逐条重放相同内容 | `records_already_present=1000`、零新增；所有计数不变 |
| 同ID不同内容 | 明确标记的构造测试抛出 `ContentConflictError`；原内容哈希保持不变 |
| 第1批提交前中断 | 预期退出1；源记录0、评论0、检查点0；恢复后完整1000 |
| 1001–1200提交后中断 | 预期退出1；记录与检查点均为1200；恢复扫描1200并全部跳过 |
| 修正后1201–1400提交前中断 | 数据/检查点保持1200，失败统计已提交数为0；恢复只插入200，最终1400 |

每批的源记录、评论、产品/用户、质量问题和检查点使用同一个PostgreSQL事务。恢复策略为 `scan_from_start`，成本是重新解压并解析已提交前缀；没有把任意压缩偏移当作可直接恢复位置。

## Airflow验收

- `stage1_review_sample_ingest`实际导入，导入错误0；`schedule=None`、`catchup=False`、`max_active_runs=1`，样本参数最大1000。
- 首次运行 `manual__stage1_sample_1000_20261103`：DAG success；装载任务一次成功；1000条写入业务库；成功分支success、失败分支skipped。
- 相同输入重跑 `manual__stage1_sample_1000_rerun_20261103`：DAG success；数据库仍1000条、质量问题仍7、检查点仍1000。
- 自动重试运行 `manual__stage1_airflow_retry_proof_20261103`：第一次尝试提交200条检查点后受控失败；第二次尝试跳过200、确认其余800条已存在并成功；任务 `try_number=2`，数据库仍1000条。
- 最大XCom `return_value` 为312字节，只含批次ID、容器路径、范围和少量计数。
- 任务日志位于 `airflow/logs/dag_id=stage1_review_sample_ingest/`。首次日志记录 `inserted=1000 valid=993 quarantined=7 checkpoint=1000`；重试成功日志记录 `existing=800 skipped_checkpoint=200 checkpoint=1000`。

## 可复制PowerShell命令

```powershell
$sha = '4B6EE09EA4880F3FB197B1407D45DFB08A892FBB48A9E7A2D74AA0572CDCC715'

docker compose run --rm --no-deps `
  -v 'D:\amazon\tests:/opt/airflow/project/tests:ro' `
  airflow-init python -m unittest discover -s /opt/airflow/project/tests -v

docker compose run --rm --no-deps airflow-init `
  python -m amazon_movies.cli parse-sample `
  --data-file /opt/airflow/data/movies.txt.gz --source-sha256 $sha --limit 1000 `
  --parser-version snap-movies-review-v2

docker compose run --rm --no-deps airflow-init `
  python -m amazon_movies.cli load-sample `
  --data-file /opt/airflow/data/movies.txt.gz `
  --source-uri file:///D:/amazon/movies.txt.gz --source-sha256 $sha `
  --batch-id stage1-manual-sample --partition-key real-sample-1-1000 `
  --limit 1000 --batch-size 500 --parser-version snap-movies-review-v2 `
  --load-strategy copy

docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec postgres psql -U postgres -d amazon_movies -c `
  "select parser_version,parse_status,count(*) from staging.review_parse_result group by 1,2;"
```

测试目录没有挂入长期服务；上述测试命令只在一次性容器中临时只读挂载。

## 未验证与剩余缺口

- 未读取第1401条之后的评论，未做全量gzip完整性验证，未核对全量7,911,684条/253,059产品/889,176用户。
- 未生成全量产品清单，未进行网络页面采集、第三方补充、电影/人物匹配或逻辑评论去重。
- 样本中的7条非法UTF-8目前按整条隔离；后续若制定源编码修复规则，必须版本化并保留原始字节证据后复验。
- 字段级候选、转换依赖和最终选择表尚无电影/人物真实实例；阶段1只完成评论源定位。
- 独立测试数据库与容器卷仍保留，未删除。

阶段1到此停止，等待阶段2指令。
