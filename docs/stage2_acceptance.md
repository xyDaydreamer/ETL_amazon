# 阶段2验收记录

验收日期：2026-11-03（Asia/Shanghai）。本阶段完成评论文件全量读取、v3版本化装载、产品清单、Airflow恢复与全量核对；未执行产品页面采集、第三方电影补充或实体匹配。

## 结论

阶段2通过。程序读到真实gzip EOF，Python gzip流未报告CRC/截断错误；数据库检查点为7,911,696个物理空行分隔块，状态为`full_complete`、`reached_eof=true`、`source_integrity_status=verified_complete`。原始记录、稳定身份、v3解析结果、v3定位和v3 ASIN观察均为7,911,696，无覆盖缺口；v3在覆盖复核后成为唯一当前解析版本。

官方口径与本地结果的关系：

| 指标 | 本地结果 | 官方参考 | 解释 |
|---|---:|---:|---|
| 物理空行分隔块 | 7,911,696 | 7,911,684评论 | 源内另有12个缺少productId/userId/profileName的畸形块 |
| 有可靠ASIN的评论 | 7,911,684 | 7,911,684 | 完全一致 |
| 不同产品ASIN | 253,059 | 253,059 | 完全一致 |
| 不同可靠用户ID | 889,176 | 889,176 | 完全一致 |

没有为匹配官方数字删除、合并或补造记录。12个畸形块仍作为隔离原始证据保留。

## 完成状态修正与数据库迁移

- `review_parser_v2.py`中的当前规则版本为`snap-movies-review-v3`。流状态明确区分到达请求上限和干净EOF。
- 样本检查点只保存实际提交位置。短文件请求不足、起点超过EOF、gzip截断/损坏和低磁盘停止具有不同状态。
- 全量入口不接受预计记录数作为停止条件，持续读取到gzip真实EOF。
- `004_stage2_full_ingest.sql`增加实际读取末尾、EOF、源完整性、停止原因、磁盘余量和版本覆盖字段。
- 全量激活前核对记录号1..EOF连续，并核对原始记录、稳定身份、指定版本解析结果、定位和ASIN观察等量。
- v1/v2历史保留；全量期间v2仍是当前版本，只有v3完成覆盖复核后才原子激活。
- 冲突reviewer_id不写用户观察；同原文重放不重写既有版本化解析结果。
- COPY质量问题计数取数据库实际插入行数，事务回滚不计入已提交统计。

## 边界与扩大验证

独立数据库`amazon_movies_stage2_test`的构造验收均实际通过：

| 场景 | 结果 |
|---|---|
| 请求1000、文件600 | 检查点600；`sample_range_short`；真实EOF |
| 起点1001、文件600 | 检查点0；实际读取末尾600；`sample_start_past_eof` |
| 截断gzip | 已提交检查点400；实际读到599；`source_failed`；未伪造EOF |
| 低磁盘阈值 | 检查点0；`stopped_low_disk` |
| 300旧结果+300新结果COPY | `quality_issues_inserted=302`，为实际行数 |
| 冲突reviewer_id | 用户观察0；冲突问题1 |
| 20条完整gzip | `full_complete`、`verified_complete` |

解析单元测试18项在最终代码上复跑全部通过。覆盖空行/连续空行/EOF、非法UTF-8、NUL、ASIN缺失/重复/冲突、reviewer_id重复/冲突、非有限评分、票数和时间边界等。

累计到100,000条的有界扩大验证：99,275有效、725隔离、100,000个可用ASIN观察；耗时60.09秒，约1,664条/秒。业务库从约22.5 MB增长到约795.3 MB，基线后产生约1.57 GB WAL。相同检查点恢复重扫100,000条耗时7.18秒、零写入。该结果用于容量估算，不冒充全量性能保证。

## 全量运行与恢复

Airflow运行`manual__stage2_full_v3_resume_20261103`最终为success：

- `load_to_verified_eof`：success，`try_number=2`；
- `validate_and_activate`：success；
- `fail_after_logging`：skipped；
- DAG导入错误0，`max_active_runs=1`，任务业务重试为0，避免低磁盘停止被无限重试；
- XCom只返回路径、批次、版本、检查点、EOF/完整性和耗时等小型统计；最大`return_value`实测307字节。

首次运行曾因Windows宿主长时间无调度器心跳而在2,875,000检查点被外部判失败；旧DAG的成功失败叶任务又错误掩盖为DAG success。失败叶已改为记录后抛错。第二次运行恢复到4,570,000时，为迁移Docker数据盘安全停止：DAG暂停、Airflow停止、检查点连续两次不变。迁移后计数完全一致，再复用同一DAG run恢复。

最终恢复尝试从gzip开头扫描7,911,696块，跳过已提交4,570,000，新增3,341,696；669个批次，耗时4,729.786秒。恢复策略仍是`scan_from_start`，没有把压缩字节偏移当作随机恢复点。

## 全量评论和异常

| 指标 | 结果 |
|---|---:|
| 物理读取块 | 7,911,696 |
| 有效 | 7,849,544 |
| 隔离 | 62,152 |
| 分类平衡 | 7,911,696 = 7,849,544 + 62,152 |
| 隔离但ASIN可用 | 62,140 |
| 隔离且ASIN缺失 | 12 |
| 隔离评论覆盖的不同ASIN | 36,493 |

问题实例与受影响记录：

| 问题 | 实例 | 受影响记录 |
|---|---:|---:|
| `invalid_utf8` | 63,501 | 62,085 |
| `unknown_field` | 47 | 46 |
| `helpful_votes_exceed_total` | 43 | 43 |
| `missing_required_field` | 60 | 24 |
| `missing_or_unreliable_reviewer_id` | 12 | 12 |

问题类型可以重叠，因此不能把问题实例数当成隔离记录数。

### 多出的12个物理块

12块的记录号、行号、解压偏移和SHA-256已导出到`data/work/stage2/malformed_blocks.tsv`，没有输出正文。字段前缀序列均为：

`<unknown> → review/helpfulness → review/score → review/time → review/summary → review/text`

它们缺少`product/productId`、`review/userId`和`review/profileName`，分属4个内容哈希（重复次数7、2、2、1）。由于`review/text`是正常记录的最后字段，这种序列不能由正文内空行拆分产生；它们是源文件自身重复出现的畸形块。官方评论数对应有可靠productId的7,911,684块。

## 产品与用户

- 可靠ASIN观察7,911,684条，不同ASIN 253,059。
- `target.product_edition`和`staging.product_harvest`均为253,059；全部状态为`not_attempted`，状态总和等于产品数。
- 本阶段没有页面访问，`not_attempted`不冒充失败或成功。
- 不同可靠用户ID 889,176；有效评论中的用户886,311；2,865个用户只出现在隔离评论中。
- 12个畸形块没有可靠用户；全量冲突reviewer_id为0。

## 容量、WAL与Docker迁移

全量后业务库为61,415,626,431字节（PostgreSQL显示约57 GiB）。主要关系含索引/TOAST：`raw.source_record`约27 GiB，`review_parse_result`约18 GiB，定位约3.52 GiB，ASIN观察约2.77 GiB，稳定身份约2.55 GiB，显示名观察约2.53 GiB。

从10万条LSN基线累计生成约385,628,929,224字节WAL；这是累计写入量，`max_wal_size=1GB`且WAL循环复用，不能与数据库/VHDX大小相加。

Docker Desktop 4.97.0的活动磁盘从C盘迁移至：

`D:\DockerDesktopData\DockerDesktopWSL\disk\docker_data.vhdx`

迁移后活动VHDX为82,838,552,576字节；采样时D盘可用537,603,723,264字节，C盘可用147,271,811,072字节。Airflow磁盘监控挂载已改为D盘，最终检查点记录余量547,699,974,144字节。

迁移前完整停止Docker后生成整盘备份：

`D:\amazon\backups\stage2-pre-migration-20261103-0840\docker_data-pre-migration.vhdx`

备份大小46,742,372,352字节，源/备份SHA-256均为`28FFADAAD20FE7636880BBE820521B7AFC34AC153123DE44D466381C2887F19A`。压缩逻辑备份曾因内存不足失败，其文件保留为`.failed-partial`并明确不可恢复。未删除卷、WAL、数据库或历史结果。

Docker重启曾遇到官方已记录的Windows AF_UNIX stale socket错误1920。确认Docker进程退出后，仅把`%LOCALAPPDATA%\docker-secrets-engine`父目录重命名为带时间戳的`broken`目录；没有删除数据或恢复出厂。随后由用户从开始菜单正常启动，数据库计数与检查点复核一致。

## 导出物与可重算查询

可重算SQL：`sql/queries/stage2_full_validation.sql`和`sql/queries/stage2_product_list.sql`。

被Git忽略的本地导出：

| 文件 | 数据行 | SHA-256 |
|---|---:|---|
| `data/work/stage2/product_list.tsv` | 253,059 | `EE821E788B801308770B319AA627BE9AD06D7E795457C40117826A698144B455` |
| `data/work/stage2/anomaly_summary.tsv` | 5 | `C397D1DF9ECB9CB9DB86AD740014960A031A560BE3A6AA596B62B41738E472C4` |
| `data/work/stage2/malformed_blocks.tsv` | 12 | `0E801888098A0C647B72C491D3E19003BEDE22FB4F1FB3A1235BE0BEFFA46F85` |

Airflow任务日志位于`airflow/logs/dag_id=stage2_review_full_ingest/`；日志目录绑定在D盘项目目录。业务完成证据以数据库检查点、覆盖计数、EOF/源完整性和任务状态共同判断，不以绿色状态单独判断。

## 未验证与下一阶段边界

- 未执行Amazon页面采集、第三方电影/人物补充、作品分类或实体匹配。
- 未将任何算法输出写成人工标签；电影/人物人工评价集仍需学生依据证据完成。
- 注册SHA-256沿用阶段0对同一原文件的计算；本阶段全流读取验证gzip EOF/CRC和数据库覆盖，但没有再次扫描压缩文件重算SHA-256。
- 本阶段已具备进入后续数据源采集与电影匹配开发的评论/产品基础条件；全量评论处理本身无需再运行。
