# 数据库迁移说明

## 003_stage1_versioned_reparse.sql

适用范围：阶段1修正，2026-11-03。迁移已先在`amazon_movies_stage1_test`重复执行，再应用到业务库`amazon_movies`。

- 将原有、已填充的`staging.review_record`重命名为`staging.review_parse_result`，原1000条结果标记为`snap-movies-review-v1`。
- 新建`staging.review_record`作为稳定身份表，并从v1结果回填1000个身份；`raw.source_record`不复制、不删除、不更新原始载荷。
- 解析结果主键改为`(source_review_id, parser_version)`，移除阻止多版本共存的旧`source_record_id`唯一索引。
- 新增解析版本注册/当前版本视图、字段级ASIN观察、逐评论显示名观察、版本化定位和含版本/范围的检查点。
- 票数改为`bigint`；规范化票数、评分精度/范围、ASIN状态均由约束保护。不可写文本原字节由Base64证据保存。
- v2成功回填后才原子激活为当前版本。业务库迁移后原始记录仍1000条，v1/v2结果各1000条。

全新卷初始化会依次运行001、002、003。对已有库可执行：

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/003_stage1_versioned_reparse.sql
```

003迁移自身可重入；它不会删除数据库、卷、原始记录或历史解析结果。001/002是基线建库脚本，不是003之后的可重放迁移：已有库升级只执行尚未应用的迁移，不要在003之后重新执行001/002。全新空库则只按001→002→003运行一次。应用前仍应记录计数并按课程提交要求备份数据库。

## 004_stage2_full_ingest.sql

适用范围：阶段2全量入口与完成状态，2026-11-03。该迁移已先应用于独立的`amazon_movies_stage2_test`，完成短文件、起点超过EOF、损坏gzip、低磁盘停止和混合新旧结果批次测试后，才应用业务库。

- 为检查点增加实际读取末尾、真实EOF、源完整性、停止原因和最后磁盘余量；请求范围与实际覆盖不再混为一谈。
- 检查点的`next_record_number`在尚未提交记录时保持逻辑范围起点，不伪造已处理位置。
- 为解析版本登记增加样本/全量覆盖、覆盖起止、EOF验证和激活时间。只有干净读到gzip EOF且关系覆盖核对通过的版本才能全量激活。
- 注册`snap-movies-review-v3`，但迁移本身不会激活它，也不会改写v1/v2结果。

已有库升级命令：

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/004_stage2_full_ingest.sql
```

004迁移不删除任何原始记录、解析历史、数据库或卷。全新空库按001→002→003→004运行；已有库只执行尚未应用的迁移。

## 005_stage3_product_metadata.sql

适用范围：阶段3首批商品元数据和电影候选，2026-11-03。迁移新增 `staging.product_metadata_record`、`integration.product_movie_candidate` 和 `integration.review_queue`，复用既有SourceRecord、FieldAssertion、Dependency、Selection及目标实体，不复制或删除阶段2评论数据。

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/005_stage3_product_metadata.sql
```

新表使用来源版本/记录号、候选规则版本和队列业务键保证重跑幂等。全新空库执行顺序为001→002→003→004→005。

## 006_stage4_first_movie_candidates.sql

适用范围：阶段4首批50个电影开发样本，2026-11-04。迁移新增开发样本表和按`(asin, rule_version)`版本化的IMDb候选结果表；两表均通过外键保留商品和候选响应的原始证据。

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/006_stage4_first_movie_candidates.sql
```

该迁移不创建人工标签，也不修改阶段2评论或阶段3商品元数据。全新空库执行顺序为001→002→003→004→005→006。

## 007_stage4_cached_candidate_reevaluation.sql

适用范围：阶段4现有50条开发样本的缓存候选池重评，2026-11-04。迁移只扩展候选状态约束，区分多候选歧义、证据不足、缓存无合适候选和缓存无候选；不删除或覆盖v1决策。

```powershell
docker compose exec -T postgres sh -c `
  'psql -U "$POSTGRES_USER" -d amazon_movies -v ON_ERROR_STOP=1 -f /opt/project/sql/007_stage4_cached_candidate_reevaluation.sql'
```

最终规则版本为`stage4-first50-movie-match-v2.1-cached-pool`。全新空库执行顺序为001→002→003→004→005→006→007。

## 017 合集评论归因语义

迁移`017_collection_review_semantics.sql`新增`target.product_movie_review_attribution`，并重建`target.movie_review_summary`使其只汇总`single_work`商品。合集、剧集及不确定关系的评论继续保留在ProductEdition，成员作品归因固定为0。迁移可重复执行，已用`B001PO54OM`四片合集的383条真实评论验证。

## 018 逻辑评论精确去重

迁移`018_logical_review_deduplication.sql`为LogicalReview补充规则版本、稳定键实际字段、代表来源、helpfulness冲突状态和运行来源；为ReviewMapping增加同一来源/规则唯一约束与映射证据；新增版本发布和事务检查点表。业务汇总在没有`full_verified`激活版本时继续使用v3源评论，只有全量覆盖验收通过后才切换到逻辑评论口径。当前全量版本已`full_verified/is_current`，结果见`docs/task8_acceptance.md`。

## 019 最终字段血缘审计

迁移`019_final_field_lineage_audit.sql`增加31项最终字段目录、版本化审计run/progress/result/gap、修复事件和关系来源链接。MoviePerson历史行新增`selected/superseded/conflicted/pending`状态，`target.current_movie_person`只暴露当前非退休关系；旧suggestion行不删除。`integration.final_source_record_impact`扩展反向影响到ProductMovie、LogicalReview和ReviewMapping。迁移已重复执行验证可重入。

## 008_stage4_v3_evidence_and_labels.sql

适用范围：阶段4 v3全缓存候选证据规则，2026-11-04。迁移为算法结果增加`confirmation_paths`、`review_reason_code`和`score_breakdown`，并新建独立的`integration.movie_match_human_label`。人工标签按内容哈希幂等追加，保存作用域、具体候选ID、规则版本、理由、证据、标注人和时间；不覆盖算法预测或旧规则结果。

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/008_stage4_v3_evidence_and_labels.sql
```

迁移已实际应用于业务库；v1、v2草案、v2.1和v3结果共存。全新空库执行顺序为001→002→003→004→005→006→007→008。

## 009_stage4_human_decision_views.sql

适用范围：阶段4 development人工决定采用，2026-11-05。迁移不复制或修改标签，只建立两个只读视图：

- `integration.current_movie_match_human_decision`：按ASIN、规则版本、作用域和候选ID选择最新的不可变人工决定。
- `integration.approved_development_movie_mapping`：仅暴露恰好一个当前`correct_match`候选、且没有当前`not_single_movie`商品排除的development映射。

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/009_stage4_human_decision_views.sql
```

迁移已实际应用；当前视图输出4条可采用开发映射。全新空库顺序为001→002→003→004→005→006→007→008→009。

## 010_stage4_unified_mapping_adoption.sql

适用范围：v3.1统一映射采用，2026-11-05。迁移明确009的`approved_development_movie_mapping`只是人工确认子集，并新增：

- `current_movie_match_human_decision_any_version`：跨预测规则版本读取每个候选/商品的最新人工决定，同时保留原标签版本。
- `unified_development_movie_mapping_decision`：人工决定优先；无人工决定时允许v3.1 `rule_verified`自动采用；冲突、歧义和不足暂缓。
- `adopted_development_movie_mapping`：统一输出人工确认和规则确认两种来源的已采用映射。
- `development_movie_mapping_review_queue`：保留11条暂缓商品，不要求当前一次性完成人工审核。

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/010_stage4_unified_mapping_adoption.sql
```

迁移已实际应用；当前人工采用4、规则采用34、排除1、暂缓11，采用映射38条。全新空库顺序为001→002→003→004→005→006→007→008→009→010。

## 011_stage5_imdb_snapshot_and_evaluation.sql

适用范围：IMDb固定快照、nconst人物实体和独立电影/人物评价集准备，2026-11-05。迁移只新增结构，不修改阶段4的50条开发预测、人工决定或统一采用视图：

- `meta.external_snapshot*`登记按日期和哈希固定的外部快照；有界表头检查与完整gzip EOF验证分开。
- `staging.imdb_*`为5个官方TSV的后续分批装载保留来源记录外键。
- `movie_external_identifier`、`person_external_identifier`和`person_match_candidate`区分外部ID事实与算法候选。
- `evaluation.*`把独立样本、算法预测和人工标签分开；人工标签为追加历史，不覆盖预测。

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/011_stage5_imdb_snapshot_and_evaluation.sql
```

迁移已在业务库重复执行验证可重入，并在独立`amazon_movies_stage5_test`走完001→011。迁移前后阶段4仍为38条采用映射、11条复核队列。全新空库顺序为001→…→011。

## 012_stage5_selective_imdb_integration.sql

新增固定快照的选择性scope、逐文件检查点和作品—人物来源证据。恢复从gzip开头扫描到已提交记录；选中行和检查点同事务提交。`movie_person_source_evidence`区分selected与supporting来源，不把principals冒充完整演员或主演。

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/012_stage5_selective_imdb_integration.sql
```

迁移在测试库和业务库重复执行通过；不修改阶段4预测或标签。

## 013_stage5_scope_product_mappings.sql

把scope中的商品成员独立建模，使多个ASIN版本可以指向同一只扫描一次的tconst。迁移会从012已有scope安全回填非空ASIN，不删除旧scope记录。

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/013_stage5_scope_product_mappings.sql
```

## 014_stage6_candidate_range.sql

新增阶段6独立范围登记、商品成员、`title.akas/title.basics`共享扫描检查点和可复用候选缓存。候选表只保存候选及来源，不建立最终ProductMovie关系。shortlist索引用于后续选择性人物证据装载；完整候选仍保留。

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/014_stage6_candidate_range.sql
```

迁移已在业务库重复执行验证可重入，并在独立`amazon_movies_stage6_test`从001顺序迁移至014。

## 015_evaluation_supersession.sql

评价清单冻结后不可覆盖。若排除规则或证据发生修正，旧清单改为`superseded`并保留实体对和预测历史，通过`superseded_by_dataset_id`指向替代清单；superseded清单不得导出、标注或计分。

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/015_evaluation_supersession.sql
```

构造数据库实际验证1个tconst可保留2个ASIN成员。全新空库最终顺序为001→…→013。

## 022_product_collection_sampling.sql

为产品页面候选抽样补充可审计元数据，不修改历史采集证据：批次保存选择方法、随机种子、候选上限（1—100）和访问依据；工作项保存稳定选择排名。候选规模与Scrapy限速、robots和网站访问控制保持独立。

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/022_product_collection_sampling.sql
```

迁移已在业务库和`amazon_movies_product_collection_test`执行通过。批次`product-page-sample100-plan-20261107`实际登记100个不同ASIN、0次请求；相同配置重复准备幂等。全新空库迁移顺序保持001→…→022，历史迁移编号和内容不变。
