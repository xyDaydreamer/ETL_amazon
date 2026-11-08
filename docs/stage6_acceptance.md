# 阶段6候选范围与独立评价入口验收

验收日期：2026-11-05（Asia/Shanghai）。本轮完成代码、迁移、独立测试库微型快照验收和真实业务库范围预检；没有启动IMDb大型扫描，没有创建正式评价集，没有人工标签或指标。

## 实现范围

规则版本：`stage6-local-imdb-candidate-range-v1`。抽样版本：`stage6-independent-evaluation-sampling-v1`。预测版本：`stage6-evidence-baseline-v1`。

- 从UCSD `proposed_classification=movie`商品中排除阶段4全部50个ASIN以及阶段5跨版本development的2个ASIN。业务库实际待扫描范围381个商品，标题缺失0。
- `title.akas`与`title.basics`各共享扫描一次，不按商品重复扫描。候选通过完整规范标题精确匹配，或至少一个非通用信息词阻塞后使用固定`SequenceMatcher`；最低缓存阈值0.82，0.85/0.95分别标记close/strong。分数只用于候选排序，不是概率。
- 扫描命中行保存SourceRecord、源文件、数据记录号、源行号、内容SHA、原始字段和规则版本。只入库命中候选的aka/basics，不默认装载IMDb五表。
- basics扫描补齐作品类型、作品上映年、genre及原始/主标题。商品发售日期明确不作电影上映年份。
- 类型`movie/tvMovie/short`为compatible，`video`为unknown，其他为conflict。冲突候选保留，不被高标题分数删除；正片/花絮、系列、重拍、同名、续集差异留给后续证据和评价。
- 每个商品默认保存最多5个shortlist候选；完整缓存仍保留，可调整shortlist而不用重扫gzip。
- 检查点与候选批次同事务，每500,000条提交。恢复从gzip开头解压并丢弃至已提交记录；scope hash阻止换商品集合或排除清单后误续跑。
- Airflow DAG `stage6_candidate_range`只调用同一业务模块，`schedule=None`、`catchup=False`、`max_active_runs=1`、任务重试0。磁盘/进程中断后由相同scope显式恢复，避免长任务自动重试风暴。

## 评价入口

`stage6-prepare-evaluation`只有在candidate scope为`candidates_ready`且shortlist对应的选择性IMDb证据scope为`materialized`时才能运行。它不会把候选写成最终映射。

- 电影对按`predicted_same`、`type_or_title_conflict`、`insufficient_or_ambiguous`分层，固定随机种子6022026轮转抽样；自动确认、暂缓和冲突项都能进入评价。
- 人物左侧是UCSD原始商品证据中的真实姓名提及及上下文，右侧是固定快照nconst人物记录。正向候选依赖“原始提及+候选作品署名”，负向难例选择近名但未在该候选作品principal证据中出现的人物；不按姓名直接合并，也不是比较同一外部ID的自证任务。
- `title.principals`只作主要署名来源，不冒充完整演员表。UCSD描述漏掉姓名也不能证明人物不相关，属于候选生成限制。
- 抽样输入包含排除清单、seed、stratum、双方显示、来源定位、链接和独立算法预测。`evaluation-create`把预测写入`algorithm_prediction`，HTML导出不读取该表且答案为空。
- `cannot_determine`由人工单独保留，后续计算指标时不能强行改为正例或负例。

## 实际小样本验收

独立数据库`amazon_movies_stage6_test`使用构造IMDb gzip和构造商品，不混入业务数据：

- aka与basics均读到真实EOF；命中aka后由basics补齐作品类型/年份。
- 1个商品得到1个候选、1个shortlist tconst。
- 相同scope重复运行显示两文件`already_complete`，候选数不增加。
- shortlist导出1个唯一tconst并保存SHA-256。
- scope状态`candidates_ready`，两个检查点均`verified_complete`。

单元测试30项全部通过；迁移014在业务库重复执行可重入；CLI四个阶段6入口可导入；Airflow DAG导入错误0。业务库正式evaluation dataset和human label仍均为0。

## 首次真实运行故障与修复

真实scope首次运行已完成`title.akas`全部60,125,602条并提交15,779条命中；`title.basics`提交到500,000条后失败。PostgreSQL日志确认不是旧的Python CSV字段上限问题，而是候选补齐SQL在`startYear='\\N'`时仍预先把绑定参数转换为smallint。已改为Python先将IMDb缺失值映射为SQL NULL、genres映射为空数组，再绑定正确类型。含`\\N`年份和genres的微型数据库首次运行、重跑、EOF、评价冻结与盲评导出均重新通过；30项单元测试通过。已提交akas、basics检查点及6,380条候选未删除，恢复从basics第500,001条继续。

恢复运行已经完成。真实结果：381个目标商品中243个有候选、138个没有达到当前精确/信息词阻塞与0.82相似度门槛；缓存6,383个ASIN—tconst候选，涉及6,374个不同tconst。类型冲突候选3,162条被保留，没有用高标题分数覆盖。shortlist共839个商品—候选对、837个不同tconst，development ASIN交集0，缺basics记录0。导出文件`data/work/stage6/shortlisted_tconsts.txt`的SHA-256为`de3a13870346de97e07fef78ab3344b865dd71b17b338486befeec3902b28092`。

138个无候选商品不能被算作“不匹配”事实；它们反映当前本地快照和候选阻塞策略的召回限制。后续另保留no-candidate覆盖审计，不能从正式实体对中静默消失，也不能强行配一个低质量IMDb条目。

## 下一条需要执行的命令

在普通PowerShell执行，不需要管理员权限。该命令会长时间顺序扫描60,125,602条akas及12,931,235条basics；不会扫描另三文件：

```powershell
Set-Location 'D:\amazon'

docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage6-candidate-scan `
  --snapshot-dir /opt/airflow/project/data/raw/imdb/2026-11-05 `
  --snapshot-version imdb-noncommercial-2026-11-05 `
  --exclusions-file /opt/airflow/project/data/work/stage5/case_materials/development_exclusions.json `
  --scope-key stage6-independent-candidates-v1 `
  --batch-id stage6-independent-candidates-v1 `
  --random-seed 6022026 --batch-size 500000 `
  --progress-every 5000000 --limit-per-product 5
```

预期先登记381个目标商品，每500万源记录输出当前位置和累计命中。最终必须显示两个文件`reached_eof=true`、scope为`candidates_ready`，并报告实际候选、覆盖商品和shortlist数；这些数量不预填。

另开PowerShell查看已提交进度：

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage6-candidate-status `
  --snapshot-version imdb-noncommercial-2026-11-05 `
  --scope-key stage6-independent-candidates-v1
```

若中断，先检查状态，然后原样重跑第一条命令。不要换scope key、排除文件、seed或limit。它会从gzip开头扫描至检查点后继续，不删除已提交数据。

## 候选扫描通过后的后续命令（现在不要提前执行）

先运行`stage6_candidate_validation.sql`确认两个文件EOF、阶段4交集0，再导出唯一tconst：

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/queries/stage6_candidate_validation.sql

docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage6-export-tconsts `
  --snapshot-version imdb-noncommercial-2026-11-05 `
  --scope-key stage6-independent-candidates-v1 `
  --output /opt/airflow/project/data/work/stage6/shortlisted_tconsts.txt
```

随后需要第二次长任务，以现有选择性装载器扫描五文件并只装载shortlist作品及人物：

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli imdb-selective-load `
  --snapshot-dir /opt/airflow/project/data/raw/imdb/2026-11-05 `
  --snapshot-version imdb-noncommercial-2026-11-05 `
  --scope explicit --scope-key stage6-shortlist-evidence-v1 `
  --batch-id stage6-shortlist-evidence-v1 `
  --tconst-file /opt/airflow/project/data/work/stage6/shortlisted_tconsts.txt `
  --batch-size 500000 --progress-every 5000000
```

第二次长任务完成并验收后，才运行评价准备。若可用电影或人物对不足100，程序会明确失败并报告数量，不能用development补足：

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage6-prepare-evaluation `
  --snapshot-version imdb-noncommercial-2026-11-05 `
  --candidate-scope-key stage6-independent-candidates-v1 `
  --selective-scope-key stage6-shortlist-evidence-v1 `
  --exclusions-file /opt/airflow/project/data/work/stage5/case_materials/development_exclusions.json `
  --output-dir /opt/airflow/project/data/work/stage6/evaluation_inputs `
  --movie-pairs 100 --person-pairs 100 --random-seed 6022026
```

该任务已经完成：837个目标作品全部取得basics与crew，akas 9,085条、principals 10,349条、人物8,780个，关系事实10,346项；五文件均`verified_complete`，缺失作品/人物0/0。shortlist集合与选择性scope集合差异0，关系来源不可定位0。

评价准备实际生成电影100对和人物100对。电影分层为证据不足/歧义34、预测支持33、类型或标题冲突33；人物分层为作品上下文姓名候选50、近名负候选50。首次冻结的电影v1在人物支持证据中包含1个development nconst；冻结检查阻止人物集写入。旧电影清单未删除，已在无人工标签时标为`superseded`。修正版v2在候选生成阶段双侧排除全部development人物，电影与人物清单的development实体交集及阶段4 ASIN交集均为0。

当前正式可标注清单：

- `stage6-movie-eval-stage6-independent-candidates-v1-v2`：frozen，100对，100条独立预测，人工标签0；
- `stage6-person-eval-stage6-independent-candidates-v1-v2`：frozen，100对，100条独立预测，人工标签0。

两份盲评HTML均不含算法预测，答案TSV各100行且答案/理由全部为空。现在已经达到开始人工填写的条件。

## 电影人工评价导入

学生提交Excel `人工评价电脑100条.xlsx`，文件SHA-256为`060b24381f76c75e4267df4fb102d6df2edb201f83c82c4ff551edefa7a0e7ce`。实际100个review_item_id与电影v2冻结清单完全一致、无重复、理由无空缺。三条“不能确定/不能确认”按同义规范为`cannot_determine`；一条“不同实体或不能确定”经学生本次明确回复后记录为`cannot_determine`，AI未替代人工决定。原Excel未修改。

规范导入文件为`data/work/stage6/formal_evaluation/movie/imports/answers_canonical_20261105.tsv`，SHA-256为`d4088d6555dfa213fa146b28d661a59eb02077fe552fcd189469302eb93fa14b`。首次导入100，使用同一标注时间重复导入0，幂等通过。电影标签分布：same_entity 42、different_entity 53、cannot_determine 5；电影数据集状态更新为`labeled`。人物正式标签仍为0，因此尚未计算或报告最终电影/人物Precision、Recall、F1。

冻结与导出命令如下；只能在上一条准备命令成功且清单显示100/100后执行：

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli evaluation-create `
  --input /opt/airflow/project/data/work/stage6/evaluation_inputs/movie_evaluation_input.json

docker compose exec -T airflow-scheduler python -m amazon_movies.cli evaluation-create `
  --input /opt/airflow/project/data/work/stage6/evaluation_inputs/person_evaluation_input.json

docker compose exec -T airflow-scheduler python -m amazon_movies.cli evaluation-export `
  --dataset-key stage6-movie-eval-stage6-independent-candidates-v1-v2 `
  --output-dir /opt/airflow/project/data/work/stage6/formal_evaluation/movie

docker compose exec -T airflow-scheduler python -m amazon_movies.cli evaluation-export `
  --dataset-key stage6-person-eval-stage6-independent-candidates-v1-v2 `
  --output-dir /opt/airflow/project/data/work/stage6/formal_evaluation/person
```

development材料继续位于`data/work/stage5/case_materials/`，正式盲评材料位于`data/work/stage6/formal_evaluation/`，两者不共用答案文件。人工只编辑各目录的`answers.tsv`，不编辑JSON或数据库技术字段。superseded的电影v1不得导出、标注或计分。

## 尚未完成

- 381商品候选扫描、837作品五文件证据装载及电影/人物各100对冻结均已完成。
- 四类各5组案例仍沿用阶段5缺口；新缓存将提供候选，但证据不足者不能为凑数确认。
- 正式人工标签仍为0；人工完成前没有Precision、Recall、F1、复核率或无法确定率。
