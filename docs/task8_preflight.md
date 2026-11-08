# 任务8全量逻辑评论去重运行手册

当前状态为“全量已完成并激活”。`logical-review-exact-v1`已达到`full_verified/is_current=true`，最终统计和验收见`docs/task8_acceptance.md`。以下命令继续作为恢复与复算手册保留，不应再次启动同一个已完成run。

## 去重规则

- 输入只取当前激活的`snap-movies-review-v3`且`parse_status='valid'`的记录；62,152条隔离记录保留原始证据但不进入逻辑评论。
- 仅当ASIN、可靠`reviewer_id`、`review_time_raw`、评分、原始摘要和原始正文全部相等时归为同一逻辑评论。SHA-256只作索引键；每批均把实际字段与已有逻辑评论逐列核对，内容不一致即停止。
- `reviewer_id`、时间、评分、摘要或正文缺失时，以`source_review_id`生成独立稳定键，不尝试合并。
- ASIN属于精确键，跨ASIN相同文本始终是不同逻辑评论。
- helpfulness不属于身份键。组内票数一致时保留该值；不一致时逻辑层置NULL并标记`conflict_preserved_at_source`，每个来源值仍在版本化解析结果和映射证据中可查。
- 代表记录是组内`record_number`最小者，若并列再按`source_review_id`；规则版本为`logical-review-exact-v1`。

## 小样本实际结果

独立数据库`amazon_movies_task8_test`构造10,000条输入，包含两组跨批次精确重复、相似正文、跨ASIN同文、缺用户、缺时间、缺评分和helpfulness冲突：

| 指标 | 结果 |
|---|---:|
| 输入与映射 | 10,000 / 10,000 |
| 逻辑评论 | 9,998 |
| 重复组/重复记录 | 2 / 2 |
| 缺关键字段而独立保留 | 3 |
| helpfulness冲突组 | 1 |
| 同版本重跑新增 | 0逻辑评论、0映射 |
| 提交前故障检查点 | 2,000，恢复后10,000 |
| 提交后故障检查点 | 3,000，恢复后10,000 |
| 跨ASIN同文 | 2个逻辑评论 |

最终一次基线耗时2.002秒，约4,995条/秒。三个规则版本合计增长59,777,024字节，折算单版本约19.93 MB/10,000条；按7,849,544条线性外推约14.57 GiB持久数据。真实正文分布、TOAST压缩、索引、重复率和WAL会改变结果，因此运行预留至少50 GiB空闲，不把合成吞吐直接视为全量保证。当前D盘和容器监控路径实测空闲532,410,294,272字节，业务库61,914,060,479字节。

证据：`data/work/task8/small_sample_acceptance.json`和`small_sample_database_validation.txt`。

## 启动

条件：Docker四个服务健康；没有同一DAG运行中；D盘剩余大于50 GiB。普通PowerShell：

```powershell
Set-Location 'D:\amazon'
$runId = 'manual__task8_full_20261106'
docker compose exec -T airflow-scheduler airflow dags trigger task8_logical_review_dedupe `
  --run-id $runId `
  --conf '{"batch_id":"task8-logical-review-full-v1","batch_size":10000,"min_free_bytes":53687091200,"monitor_interval_batches":5}'
```

预期：命令返回DAG run已创建。业务任务从检查点0开始，完整成功前不会激活逻辑评论视图。不要同时运行CLI入口。

## 查看进度

```powershell
docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run `
  task8_logical_review_dedupe $runId

docker compose exec -T airflow-scheduler python -m amazon_movies.cli task8-dedupe-status `
  --rule-version logical-review-exact-v1

Get-PSDrive D | Select-Object Used,Free
```

预期：`committed_record_number`和`processed_input_count`递增；目标表只反映已提交批次。日志位于`airflow/logs/dag_id=task8_logical_review_dedupe/run_id=<runId>/`。

## 失败恢复

先确认原DAG已进入failed，检查状态中的`stop_reason`。若为`stopped_low_disk`，先释放或扩展D盘空间；不要删除卷、目标表或检查点。使用新的Airflow run ID、完全相同的业务`batch_id`和参数：

```powershell
$resumeRunId = 'manual__task8_resume_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
docker compose exec -T airflow-scheduler airflow dags trigger task8_logical_review_dedupe `
  --run-id $resumeRunId `
  --conf '{"batch_id":"task8-logical-review-full-v1","batch_size":10000,"min_free_bytes":53687091200,"monitor_interval_batches":5}'
```

预期：从`meta.logical_review_progress.committed_record_number`之后继续。业务写入和检查点在同一事务；提交前失败重做该批，提交后失败跳过已提交批次。

## 最终核对

DAG的第二个任务会先执行全量覆盖、映射唯一性和孤儿检查，只有通过后才把版本设为`full_verified/is_current`并切换业务统计。DAG成功后执行：

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -v rule_version=logical-review-exact-v1 `
  -f /opt/project/sql/queries/task8_full_validation.sql |
  Tee-Object -FilePath 'data\work\task8\full_validation.txt'
```

必须满足：映射数=有效输入数；`重复记录数=有效输入数-逻辑评论数`；缺失映射、多重映射、输入外映射、孤儿和无映射逻辑评论均为0；`counting_mode=logical_exact_dedupe`；合集成员的`logical_reviews_copied_to_members=0`。

若装载任务已`full_complete`但激活任务因临时查询问题失败，修复原因后可单独重验并激活：

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli task8-dedupe-activate `
  --rule-version logical-review-exact-v1
```

全量已完成；要求矩阵、课程报告、任务8验收和2026-11-07提交包已经据实更新。
