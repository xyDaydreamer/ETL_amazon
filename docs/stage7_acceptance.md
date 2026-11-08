# 阶段7验收记录

验收日期：2026-11-06（Asia/Shanghai）。学生完成person v2盲评后，本记录追加答案导入、正式双指标、姓名构成和案例库存收尾。

## 变更

- 新增迁移`016_stage7_query_views.sql`：当前字段选择、源记录反向影响、商品/作品评论统计视图。
- 修复干净环境迁移链：初始化脚本补执行014、015、016。
- 新增四类血缘查询`stage7_lineage_queries.sql`和五类业务查询`stage7_business_queries.sql`。
- 新增质量/孤儿/覆盖核对`stage7_quality_validation.sql`。
- 新增当前环境证据脚本、隔离干净环境复现脚本和提交材料审计脚本。
- 新增课程报告、正式评价结果、ER/字段字典、Airflow恢复手册和10—15分钟演示步骤。

## 当前实际证据状态

### 已通过（沿用且未重跑大型任务）

- 评论全量EOF、gzip完整性、7,911,696物理块、7,911,684可靠ASIN评论、253,059产品、889,176可靠用户。
- UCSD离线元数据203,766条到EOF，73,813产品命中；IMDb五文件固定快照完整验证。
- stage5 38个采用映射的作品38、人物742、关系事实809，缺失作品/人物0。
- stage6候选缓存6,383条，243/381产品覆盖；837作品选择性证据五文件EOF；电影/人物v2各100对冻结且development交集0。
- 电影v2人工标签100条此前已导入；本轮人物v2也完成100条导入和幂等复验。

### 本轮新增实际验收

- 用户在管理员PowerShell中把异常目录可回退地重命名为`docker-secrets-engine.broken-20261106-014314`，Docker Engine 29.9.2恢复；未删除卷、VHDX、WAL或数据库。
- 迁移016在业务库重复执行成功；四类血缘和五类业务查询全部实际返回。最终演示案例使用movie_id 39 / ASIN `B0046MOVD0`，同时显示采用`tt1220634`与人工拒绝`tt0432021`、UCSD/IMDb源记录、字段候选、转换依赖和选择理由。
- 反向来源示例从UCSD source_record 7913697返回受影响product title断言；person示例从Eric Till的name.basics记录定位到`tt0078764`的crew导演关系。
- 质量核对实测：当前v3为7,849,544 valid + 62,152 quarantined；产品253,059；离线元数据88,666行/73,813产品；页面尝试5、成功0。
- 跨版本案例写入前，集成库为875作品、9,429人物、39条采用Product-Movie、11,157条Movie-Person、13,461条关系来源证据、101,611字段断言、12,931字段选择；六类孤儿计数均0。写入10个商品决定后Product-Movie为47（两个商品关系此前已存在），其他实体/关系统计不变。
- v3质量问题按事件计数：invalid_utf8 63,501次、影响62,085个SourceRecord；事件数可大于记录数，因为一个记录可有多个字段错误。另有unknown_field 47、缺可靠用户12、缺必填60、helpfulness票数错误43。
- 阶段4统一采用状态仍为人工4、规则34、暂缓11、非单电影排除1。电影与人物评价均100/100已标；预测只在盲评完成后用于正式计分。

### Airflow与幂等

- 当前环境10条样本run `manual__stage7_acceptance_20261106T0149`成功；0新增原始/解析结果、10条already present、检查点0→10。
- 同一business batch重跑`manual__stage7_acceptance_rerun_20261106T0150`成功；检查点10→10、inserted 0、skipped by checkpoint 10。
- 强制失败run `manual__stage7_forced_failure_20261106T0155`中数据库任务尝试3次（max_tries=2）后failed，失败分支success、成功分支upstream_failed。
- 恢复run `manual__stage7_recovery_20261106T0156`成功，失败分支skipped。日志位于`airflow/logs/dag_id=stage0_smoke/`及`dag_id=stage1_review_sample_ingest/`对应run目录。

### 干净环境复现

隔离项目`amazon-stage7-clean`使用新卷和端口55432/18080实际启动。新卷自动创建`meta.stage6_candidate_scope`、`evaluation.dataset`、`integration.source_record_impact`和`target.product_movie_review_attribution`，证明014—017迁移链完整；DAG导入错误0；`stage0_smoke`成功；10条v3样本全部valid且任务成功。脚本结束仅stop容器，clean卷保留，没有执行`down -v`。证据位于`data/work/stage7/clean_reproduction/`。

### 人物答案、正式评价与案例收尾

- 原始人物XLSX SHA-256为`83074aeaa769c7ab34554568407f65239d2c11985fd6a2e0a4b8e0c1b8cb4c5b`。100/100编号齐全，重复/意外/空标签/空理由/非法标签均0；标签49同一、49不同、2无法确定。项目归档副本哈希一致。
- 首次导入新增100，同文件、同标注时间、标注人和渠道重复导入新增0。电影/人物数据集最终均为100 pair、100 label、100 prediction、0标签冲突，状态`evaluated`。理由原文保留；人物中精确等于“没有关联”40条，包含该短语41条。
- 电影选择性计分63：TP27/FP5/FN6/TN25，P=0.84375、R=0.81818、F1=0.83077，覆盖明确人工样本63/95=66.32%；人工无法确定5、算法暂缓34。暂缓中人工正例9、负例23、无法确定2。将暂缓视为未自动合并时，95对明确样本为TP27/FP5/FN15/TN48，P=0.84375、R=27/42=0.64286、F1=0.72973。人物计分98：TP48/FP1/FN1/TN48，P/R/F1均0.97959；人工无法确定2、算法暂缓0。
- 人物姓名统计为：仅共享一个词26（25不同）、拼写相近3（2不同）、规范全名相同50（1不同）。全名相同负例只有1条，人物高分对同名不同人的解释能力有限。
- development案例现为人物合并5、人物近名不合并5、电影易混淆不合并5、电影跨商品版本合并5。跨版本规则`stage7-cross-product-version-v1`写入10个merge和1个The Crazies do_not_merge决定；同一命令重复执行后计数仍为10/1。查询五部作品均返回2个verified ASIN，孤儿0。
- 五组跨版本为Chocolate、The Color of Money、Initial D、Journey to the Center of the Earth和The Rundown。商品/作品SourceRecord分别可定位，详细版本差异、principal命中、内容哈希和选择理由位于`data/work/stage7/case_inventory/`。The Crazies被证实为1973原版与2010重拍版，不合并。
- 固定离线合集样本`B001PO54OM`实际核实The Cowboys、Fort Apache、Rio Bravo和The Searchers四部作品，写入4条`collection_member`关系。商品v3评论383条；四部成员的归因均为0，避免复制成1,532条作品评论。命令重复运行后关系和决定均未增加，证据见`data/work/stage7/collection_demo/john_wayne_collection.json`。
- 实际导出SHA-256：`case_inventory.tsv`=`673A4FC9B701EBF00E3A77C5877E155B5D31C6520715F4273B6D1135FF048497`，`case_inventory.json`=`36516250F0C45ED430D7EA77BAE765FFF6424E7D16B4AFA9FFB10C13D49D6036`，`cross_version_verification.json`=`4A8FB3D4733C13FB092AFB9C10473F13DBF9CD082C030CB8E96AFB5AA01C4DAF`；双评价结果JSON=`584391E26AE793C4280F3B460C14F8BE7155516E590E96D0CD53407F7B703E85`。
- 最终加入合集后再次执行`stage7_validate.ps1`成功：DAG导入错误为`No data found`，页面尝试5/成功0，47条`single_work`加4条`collection_member`，六类孤儿均0；当前集成范围为883部作品、9,527人物。刷新证据位于`data/work/stage7/evidence/`。
- 首次指标SQL因未限定`review_item_id`出现连接歧义，修正为显式表连接后重跑成功；首次`unittest discover`因运行容器未挂载`tests/`而无法发现目录，改用只读挂载的临时容器后新增2项评价测试和既有2项评价材料测试全部通过。5个修改Python文件在宿主绑定运行时编译通过。
- 同一人物答案在数据集已`evaluated`后再次导入仍新增0且保持`evaluated`；正式指标和案例导出再次运行后5个输出文件SHA-256全部不变。

### 存在局限与仍待完成

- 项目目录没有`.git`元数据；老师提交清单不要求Git历史，因此不列为提交缺口。正式提交按显式白名单打包并单独生成哈希与敏感信息审计。
- 固定运行镜像未安装pytest，本轮`python -m pytest`明确失败为`No module named pytest`；没有临时联网安装。随后把项目测试临时复制到容器`/tmp`，以自带`unittest discover`实际运行34项，全部通过；源码目录保持只读。DAG导入错误0，数据库和Airflow集成实跑通过。
- `compileall`因只读源码挂载无法写`__pycache__`而退出；随后对27个源码/DAG文件执行纯内存AST解析，全部通过。该失败是写权限行为，不是语法失败。
- 四类案例均达到5组；但这些是与正式评价集隔离的development展示案例，不能替代正式指标或全量页面采集。
- 页面实时采集仍只做有界尝试（5次、成功0），没有完成全量Amazon页面采集；离线UCSD覆盖不能替代页面覆盖。

## 提交材料静态审计实测

最终执行`.\scripts\stage7_submission_audit.ps1`成功：候选提交文本/代码113个，大文件9个且均命中预期排除边界，疑似敏感信息2处。人工复看两处均为初始化脚本中的环境变量引用`AIRFLOW_DB_PASSWORD`和`MOVIE_DB_PASSWORD`，不是实际密码。审计同时确认`.git`元数据不存在。明细位于被忽略的`data/work/stage7/submission_audit/`。

PowerShell三个新脚本均通过`[ScriptBlock]::Create`语法解析，clean Compose合并配置通过`docker compose ... config --quiet`。首次证据脚本暴露“SHA清单把自身纳入哈希”的文件占用问题，已排除manifest自身并复跑成功；clean脚本首次固定等待过短，已改为查询Airflow元数据库轮询终态并复跑成功。这些失败均保留为实际修正记录。

## 最终提交包验收

`scripts/build_submission.ps1`最初按显式白名单生成`submission/amazon_movies_etl_final_20261106/`，该目录作为任务8前的历史包保留。任务8全量验收后，最终目录更新为`submission/amazon_movies_etl_final_20261107/`：171个文件、19,342,735字节；产品清单253,059行（另1行表头），电影/人物冻结预测与人工答案逐对表各100行。`MANIFEST.sha256.csv`覆盖其余170个文件，逐项复算哈希不一致0；禁入扩展名/目录0，疑似嵌入密钥0。包内新增任务8全量状态、可复算SQL输出与小样本恢复证据，仍不含原始压缩数据、数据库、VHDX、日志、密码或令牌。

首次构建因PowerShell `TrimStart`字符参数错误在复制阶段停止，未完成目录被移动到`.tmp`保留诊断；修正后首次成功包又发现审计JSON在生成清单后被改写会使其哈希过期，已改为一次写入并重新构建。最终包158项哈希全部一致。

## 后续任务8追加验收

2026-11-07完成`logical-review-exact-v1`全量去重：7,849,544条有效输入形成7,835,611条逻辑评论和7,849,544条来源映射，重复记录13,933、重复组12,281；所有映射完整性与孤儿检查为0。Airflow构建及激活任务success、失败分支skipped。详细口径和证据见`docs/task8_acceptance.md`，最终提交包更新为2026-11-07版本，旧包保留但不再作为提交版本。

## 最终字段血缘追加验收

2026-11-07 Airflow run `manual__final_lineage_full_20261107`完成全范围审计，`repair_saved_links`与`audit_coverage`均success。run10固定活动解析`snap-movies-review-v3`及去重`logical-review-exact-v1`，31项、80,651,547个适用字段/关系成员，非空78,641,193、完整来源链78,641,193、缺来源0、gap明细0。2,010,354个缺值为NULL/pending，8,420个冲突是保留全部来源的helpfulness差异。

四类血缘查询复跑无SQL错误。五类业务查询首次复验发现迁移018后合集视图已把`attributed_source_review_count`改为`attributed_logical_review_count`；查询及合集演示代码改用当前逻辑评论列后完整复跑，B1/B1b/B2/B3/B4/B5a/B5b均返回。纯内存AST解析31个Python/DAG文件通过，血缘测试3项通过。原始`compileall`失败仅因容器源码只读、无法写`__pycache__`；原测试导入失败仅因容器未挂载tests，均已用不写源目录的方式复验。

机器证据位于`data/work/lineage_audit/`：`full_status.json`、`coverage_summary.csv`、`gap_detail.csv`、`repair_history.csv`、`full_validation.txt`、两类查询复验输出和影响分析JSON。详细口径见`docs/final_field_lineage_audit.md`。

最终白名单包重新构建为`submission/amazon_movies_etl_final_20261107_lineage_final/`，共187个文件；manifest覆盖其余186项，独立复算不一致0，禁入文件0，疑似嵌入密钥0，包含8份`evidence/lineage/`证据。旧任务8包及首次血缘构建包均保留为历史，不作为最终提交目录。

## 可重放命令

普通PowerShell：

```powershell
Set-Location 'D:\amazon'
.\scripts\stage7_validate.ps1
```

已实测生成`data/work/stage7/evidence/`下的迁移、九类查询、质量核对、DAG列表与SHA清单。脚本只读查询业务数据，唯一结构变更是可重入地创建迁移016视图。

干净环境复现（不使用当前卷，默认结束时只停止、不删除clean卷）：

```powershell
.\scripts\stage7_clean_reproduce.ps1
```

实测迁移链返回stage6/evaluation/stage7三个对象，DAG导入错误0，smoke和10条样本任务成功。失败后保留`amazon-stage7-clean`卷；修复原因后原命令重跑，不要加`down -v`。

