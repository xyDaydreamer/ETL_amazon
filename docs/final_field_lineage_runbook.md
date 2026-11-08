# 最终字段血缘全量审计运行手册

前提：Docker Desktop运行，`postgres`健康，迁移019已应用；任务8当前版本仍为`logical-review-exact-v1/full_verified`。

## 启动（由用户手动执行）

```powershell
Set-Location 'D:\amazon'
docker --% compose exec -T airflow-scheduler airflow dags trigger final_field_lineage_audit --run-id manual__final_lineage_full_20261107 --conf "{\"batch_id\":\"final-lineage-full-v1\",\"scope_key\":\"all-current-final-fields-v1\",\"full\":true,\"limit\":10000}"
```

预期：run先进入`queued/running`；`repair_saved_links`应success且新增量为0，随后`audit_coverage`运行。不要同时启动第二个同范围run，DAG已设置`max_active_runs=1`。

## 查看进度

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli lineage-audit-status `
  --batch-id final-lineage-full-v1

docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run `
  final_field_lineage_audit manual__final_lineage_full_20261107
```

`meta.lineage_audit_progress`按ProductEdition、Movie、Person、PersonAlias、relationships、LogicalReview和ReviewMapping显示`running/completed/failed`。单个集合SQL执行中不会伪造记录级进度。

## 失败恢复

先读取Airflow任务日志和`lineage-audit-status`。资源问题解决后，用新的Airflow run ID、相同`batch_id/scope_key`重触发。Windows PowerShell 5应继续使用`docker --%`，避免JSON双引号被本机参数解析移除：

```powershell
docker --% compose exec -T airflow-scheduler airflow dags trigger final_field_lineage_audit --run-id manual__final_lineage_full_20261107_retry1 --conf "{\"batch_id\":\"final-lineage-full-v1\",\"scope_key\":\"all-current-final-fields-v1\",\"full\":true,\"limit\":10000}"
```

修复入口和审计结果均可重入；失败组件会重新计算，已保存的业务来源和历史决定不会删除。该只读审计不声称具备单条记录级检查点。

## 最终核对与导出

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli lineage-audit-status `
  --batch-id final-lineage-full-v1 `
  --output /opt/airflow/project/data/work/lineage_audit/full_status.json

Get-Content .\sql\queries\final_lineage_audit_validation.sql -Raw |
  docker compose exec -T postgres psql -U postgres -d amazon_movies -X -v batch_id=final-lineage-full-v1

Get-Content .\sql\queries\stage7_lineage_queries.sql -Raw |
  docker compose exec -T postgres psql -U postgres -d amazon_movies -X

Get-Content .\sql\queries\stage7_business_queries.sql -Raw |
  docker compose exec -T postgres psql -U postgres -d amazon_movies -X
```

验收条件：run为`completed/full_scope=true`；31个目录项都有结果；所有非空最终字段的`missing_lineage_count=0`，或者每个非零项都有`lineage_audit_gap`与待核实原因；候选电影库存与46部采用作品分母仍明确分开；四类血缘查询和五类业务查询均能返回且仍采用当前关系/当前逻辑评论口径。

本次实际结果：run10于UTC 06:07:07开始、07:20:21结束；两个Airflow任务success。78,641,193个非空值完整链同数，`missing_lineage_sum=0`，审计gap 0。
