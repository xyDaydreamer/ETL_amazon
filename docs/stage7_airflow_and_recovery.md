# 阶段7 Airflow、日志与恢复手册

## 作业定义

| DAG | 用途 | 依赖/配置 | 重试与失败边界 | 恢复入口 |
|---|---|---|---|---|
| `stage0_smoke` | DB连接、成功/失败分支 | `amazon_movies_db`连接，`force_failure`参数 | 主任务2次；显式`one_failed`分支 | 换run id重触发 |
| `stage1_review_sample_ingest` | 离线小样本 | 只读`movies.txt.gz`、v3、limit≤1000 | 2次；事务幂等；测试可注入提交后失败 | 相同batch/partition重跑 |
| `stage2_review_full_ingest` | 全量评论至真实EOF | 磁盘阈值、COPY、单活跃run | 业务任务0次，低磁盘不形成重试风暴 | 修复资源后复用batch和检查点 |
| `stage3_product_movie_sample` | UCSD/IMDb候选/页面有界验证 | 配置文件与缓存 | 离线/候选1次，受限页面不绕过 | 复用业务batch，换Airflow run id |
| `stage4_first_movie_matching` | 冻结50条development缓存重评 | 不联网，limit≤50 | 评估任务1次，验收0次 | 稳定键和版本化规则重跑 |
| `stage5_imdb_selective_integration` | 固定IMDb快照选择性装载 | 五文件、scope、snapshot version | 长扫描任务1次；事务检查点 | 同scope从gzip起点扫描到检查点 |
| `stage6_candidate_range` | development外共享候选扫描 | 固定排除、seed、scope | 0次，避免长任务自动重试 | 同scope/seed/排除文件显式恢复 |

所有DAG均`schedule=None`、`catchup=False`；阶段1以后限制同一逻辑输入并发。DAG导入不打开大文件、不联网、不查询业务库。XCom只返回路径、批次ID、版本和少量统计。

## 离线小样本演示

```powershell
Set-Location 'D:\amazon'
docker compose up -d postgres airflow-api-server airflow-scheduler airflow-dag-processor
docker compose exec -T airflow-scheduler airflow dags trigger stage1_review_sample_ingest `
  --run-id manual__stage7_sample `
  --conf '{"batch_id":"stage7-sample","sample_limit":10,"batch_size":5,"parser_version":"snap-movies-review-v3","scope_start_record":1,"load_strategy":"copy","verify_retry_after_commit":false}'
docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run `
  stage1_review_sample_ingest manual__stage7_sample
```

预期主任务与success叶任务为success；数据库只增加或复用前10条v3结果。同一命令换run id但保留business batch后，稳定键与内容冲突检查保证不重复。

## 全量运行与恢复

全量入口、磁盘阈值、实际SHA和恢复命令见README“阶段2全量评论”。恢复不是从任意gzip压缩偏移跳转，而是从头解压并丢弃到已提交记录；目标写入和检查点在同一事务。IMDb长扫描使用相同策略，`meta.imdb_selective_progress`逐文件记录EOF。

日志路径：`airflow/logs/dag_id=<dag_id>/run_id=<run_id>/task_id=<task_id>/`。检查顺序：DAG run状态→失败任务日志→`meta.etl_run`→对应progress表→目标计数与孤儿查询。不要用绿色任务状态代替数据核对。

