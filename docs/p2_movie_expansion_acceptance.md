# 433个电影商品离线扩展有界验收

验收时间：2026-11-07。固定范围为UCSD Movies & TV离线元数据中分类为`movie`的433个去重ASIN，规则版本为`p2-movie-expansion-v1-stage4-evidence`，IMDb快照为`imdb-noncommercial-2026-11-05`。本轮只复用阶段4/6缓存、已装载IMDb证据和既有人工决定，没有访问网络，也没有重新扫描五份IMDb gzip。

## 固定范围和基线

- 范围键：`p2-movie-433-v1`。
- 范围SHA-256：`7151797772520e6c407d8f549ee16fcfda857b521543b4c23a81d12f0d1657b3`。
- 注册时已有采用：41个ASIN、40部去重作品；待处理392个ASIN。
- 阶段6候选范围覆盖381/433个ASIN；真实合集成员ASIN另计1个。
- 基线清单：`data/work/p2/movie_433_baseline.tsv`，434行（含表头），SHA-256 `3f88be2d84c76a26feeaa8c56612ba6608fb4ce99692fce2100645714de3fd23`。
- 当前互斥对账：`data/work/p2/movie_433_reconciliation.tsv`，434行（含表头），SHA-256 `df0927a947be1af4bc96ee695c38165691b35fba67b6405cab0f05281fdc6268`。

## 实际执行结果

Airflow run `manual__p2_bounded_rerun_20261107`的`register`、`evaluate`和`apply`均为`success`。注册、候选评估和采用分别保存独立ETL run；DAG `catchup=False`、`max_active_runs=1`，任务只传少量统计。

433个ASIN的当前互斥对账如下：

| 状态 | ASIN数 | 说明 |
|---|---:|---|
| `existing_adopted` | 41 | 固定范围登记前已有当前`single_work`关系 |
| `newly_adopted` | 6 | 本轮有界采用并写入关系及来源链接 |
| `needs_review` | 216 | 包含168条原始复核项及48条尚未应用的规则确认项 |
| `explicit_mismatch` | 30 | 缓存候选存在硬冲突或均不合适 |
| `no_candidate` | 140 | 已有缓存中没有候选 |
| **合计** | **433** | 去重ASIN数与固定范围一致 |

当前范围内采用47个ASIN、对应46部去重作品，覆盖率`47/433 = 10.8545%`。这是ASIN覆盖率，不是网页采集覆盖率。全库当前有53条活动`single_work`商品、48部活动去重作品；该全库分母包含固定433范围之外的已验收关系，不能与P2范围数字混用。

验收SQL实际返回：多个活动`single_work`映射的ASIN为0、孤儿候选来源为0、孤儿决定为0、已采用作品字段冲突为0；6条新增映射全部具有`relationship_source_link`。冻结的电影和人物v2评价结果仍各有一条`evaluated`记录，没有重算或覆盖。

## 六条新增采用

| ASIN | 商品原始标题 | IMDb | 作品 | 年份 | 采用路径 | 关系来源链接数 |
|---|---|---|---|---:|---|---:|
| B000009EEG | Superfights VHS | tt0114582 | Superfights | 1995 | 精确标题和可定位署名 | 7 |
| B00002AFVH | Blondie Brings Up Baby VHS | tt0031106 | Blondie Brings Up Baby | 1939 | 精确标题和可定位署名 | 11 |
| B00006ADEX | Russ Meyer's Up! | tt0075376 | Up! | 1976 | 精确标题和可定位署名 | 17 |
| B000Y01X9I | The Annunciation | tt0123381 | The Annunciation | 1984 | 精确标题和可定位署名 | 11 |
| B001HY3B40 | Way of War [Blu-ray] | tt1133995 | The Way of War | 2009 | 精确标题和可定位署名 | 8 |
| B001U41838 | Direct Contact | tt1182609 | Direct Contact | 2009 | 精确标题和可定位署名 | 7 |

六条均无硬冲突。来源链接包含UCSD商品记录、IMDb标题/别名记录、title.basics记录以及实际参与确认的人物署名记录；哈希只用于稳定冲突检测，不替代原始定位。

## 易混淆项抽查

- `B007L6VR6M` Moonrise Kingdom：`tt1748122`和`tt41082087`均达到确认资格，保留`needs_review`，没有用IMDb rank消歧。
- `B004LWZW2Y` Scream 4：`tt1262416`和`tt1962637`均有强证据，保留`needs_review`。
- `B00008EYF7` Nabucco：三个IMDb ID均达到确认资格，保留`needs_review`。
- `B004G6NKKG` Anuvahood：候选分数虽高，但存在`work_type_conflict`，归为`explicit_mismatch`，硬冲突没有被分数抵消。
- `B000006PZE` Gerbert VHS及`6305535310` Saber Marionette R VHS：类型冲突，需扩大检索，未采用。

## 幂等、测试和合集回归

使用已成功批次`p2-airflow-bounded-v1-apply`重新调用采用入口，返回同一run 104、`idempotent_replay=true`、`applied_this_execution=0`，数据库仍为6条新增采用，证明成功批次重放不会再次写入。

运行镜像和主机Python均未安装`pytest`，因此没有临时联网安装依赖。`tests/test_p2_movie_expansion.py`的3个无fixture测试函数通过容器Python直接执行，结果为3/3；这与完整pytest发现运行明确区分。Airflow真实DAG和数据库验收不能由这3个断言替代。

真实合集`B001PO54OM`仍有4个成员、383条商品源评论，复制到成员作品的逻辑评论为0，P2没有改变合集评论归因。

此前全量字段血缘run 10在P2写入前完成，属于历史验收，不能冒充本轮新增关系的全量重审。本轮以关系来源链接、来源孤儿、采用冲突及字段冲突SQL对6条新增关系做了局部实测；若以后应用剩余48条，应再运行一次新的全量字段血缘审计。

## 人工继续应用、进度和恢复

当前48条`rule_verified_pending`没有被自动应用。先查看状态：

```powershell
Set-Location 'D:\amazon'
docker compose exec -T airflow-scheduler python -m amazon_movies.cli p2-movie-status `
  --scope-key p2-movie-433-v1
```

确认要继续后，每次使用新的批次ID有界采用，例如：

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli p2-movie-apply `
  --scope-key p2-movie-433-v1 `
  --batch-id p2-movie-manual-apply-02 `
  --limit 10
```

预期`applied`最多10，`remaining_verified_pending`下降，433条互斥对账总数保持不变，`single_work_conflict_asins`为0。若命令在提交后失去终端输出，先用状态命令检查；恢复时重跑同一批次ID。若该批次已提交，将返回`idempotent_replay=true`和`applied_this_execution=0`；进入下一批必须使用新的批次ID。

最终核对：

```powershell
Get-Content .\sql\queries\p2_movie_expansion_validation.sql -Raw |
  docker compose exec -T postgres psql -U postgres -d amazon_movies -X -v ON_ERROR_STOP=1

docker compose exec -T airflow-scheduler python -m amazon_movies.cli p2-movie-export `
  --scope-key p2-movie-433-v1 `
  --output /opt/airflow/project/data/work/p2/movie_433_reconciliation.tsv
```

仅当48条全部按批应用并复验后，才可把P2范围状态写成完整扩展；这仍不代表433个商品之外的Movies & TV产品、Amazon页面采集或IMDb全量装载完成。
