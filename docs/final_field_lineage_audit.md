# 最终字段血缘与覆盖率审计

审计版本：`final-field-lineage-v1`；修复版本：`final-field-lineage-repair-v1`。有界验收后，已于2026-11-07完成全范围Airflow实跑（audit_run_id 10，batch `final-lineage-full-v1`）。31项审计结果中，78,641,193个非空最终值/关系成员全部具有完整来源链，缺来源为0。

## 审计范围和分母

- ProductEdition：审计范围内的商品行；`pending`分类按缺值而不是已知分类统计。
- Movie：只统计由`verified/manual/predicted` ProductMovie关系到达的46部已采用作品；数据库中的883部候选库存不充当已确认作品。
- Person/PersonAlias/MoviePerson：只统计已采用作品的当前、非退休人物关系。
- ProductMovie：51条已采用关系，包含`single_work`和`collection_member`。
- LogicalReview/ReviewMapping：只使用当前`logical-review-exact-v1`和当前`snap-movies-review-v3`。评论字段复用代表源评论、版本化解析、locator与原始记录链，不复制数百万条FieldAssertion。

## 字段清单

| 实体 | 业务字段/关系成员 | 语义与最终位置 | 来源链或生成规则 |
|---|---|---|---|
| ProductEdition | `asin` | 商品身份；`target.product_edition.asin` | 当前解析版本的usable ASIN observation → ReviewRecord → SourceRecord/locator/hash；也可由ASIN绑定的UCSD记录支持 |
| ProductEdition | `classification` | 商品分类；`pending`表示未知 | UCSD原始标题/分类/描述 → 保存的分类断言 → FieldSelection |
| ProductEdition | `media_format/language_codes/subtitle_codes/region_code/cut_label/publisher/product_release_date` | 商品版本字段，不与作品字段混用 | 非空时必须有FieldSelection及原始来源；当前有界范围均为空，不补造 |
| Movie | `canonical_title/original_title/release_year/release_type` | 已采用作品字段 | 固定IMDb快照记录 → FieldAssertion → FieldSelection |
| Movie | `release_precision` | 年/月/日精度 | 已选release字段 → AssertionDependency → 派生断言/选择 |
| Movie | `release_date/release_region` | 作品日期及地区语义 | 当前46部为空，保持缺失；不得由年份补1月1日 |
| Person | `canonical_name` | 稳定人物的显示名 | IMDb nconst固定身份的name.basics记录 → 断言/选择 |
| PersonAlias | `alias_raw` | 逐来源署名 | 直接SourceRecord链 |
| PersonAlias | `alias_normalized` | 仅检索规范化，不证明身份 | `alias_raw`的NFKC/casefold/空白规范化；技术派生 |
| ProductMovie | `product_id/movie_id/relationship_type/mapping_status` | 商品—作品采用关系；合集可多对多 | MergeDecision、规则/人工原因及被关系化的源记录ID |
| MoviePerson | `movie_id/person_id/role_type/credited_as/billing_order` | 当前作品—人物署名事实 | IMDb crew/principals与name记录；principals不宣称完整演员表 |
| LogicalReview | `product_id/reviewer_id/review_time_utc/score/summary/review_text` | 当前逻辑评论业务值 | 代表source_review_id → v3解析 → ReviewRecord → SourceRecord/locator/hash |
| LogicalReview | `helpful_votes/total_votes/helpfulness_resolution` | 一致时采用；冲突时逻辑值NULL并保留所有来源值 | 全部ReviewMapping来源及去重规则证据 |
| ReviewMapping | `logical_review_id/source_review_id/source_role/mapping_reason/rule_version` | 一个有效来源恰好映射一个当前逻辑评论 | 映射证据JSON、规则版本及源记录链 |
| 技术字段 | 各identity ID、`stable_key`、运行/检查点状态 | 数据库身份和运行控制 | PostgreSQL identity、明确SHA-256输入元组或事务状态机；不冒充外部事实 |

数据库中的规范化清单位于`integration.lineage_field_catalog`，共31项，包含字段语义、适用分母、最终位置、血缘模式和生成规则。

## 审计前基线与真实修复

基线：253,059个商品、51条采用ProductMovie、46部采用作品、9,527个人物库存、11,297条历史MoviePerson、7,835,611条当前LogicalReview和7,849,544条当前ReviewMapping。46部采用作品的4个非空核心字段均已有完整选择链；当前采用范围内882人中有2个退休suggestion人物仍留在关系表。

修复run_id 91实际写入：

- 88,666条既有UCSD分类候选断言，14,210条非pending分类选择；候选/冲突输出仍保留。
- 46条release_precision派生断言、46条选择及release_year依赖边。
- 2条旧suggestion人物关系标为`superseded`，明确指向已有merge decision中的正式nconst人物；没有删除旧行。当前采用范围变为880人、960条当前关系。
- 从既有ProductMovie决定JSON中关系化77条源记录链接；51条采用关系均可定位。

相同批次第二次运行六项新增量全部为0。修复只连接已有源记录、断言和决定，没有由最终值反推历史。

## 有界实跑覆盖率

审计run_id 6，范围为前10,000个商品、前10,000条LogicalReview/ReviewMapping，以及全部已采用作品/人物/关系：

| 指标 | 结果 |
|---|---:|
| 字段目录项 | 31 |
| 非空字段或关系成员 | 114,427 |
| 完整来源链 | 114,427 |
| 缺来源链 | 0 |
| 缺值 | 79,546 |
| 明确冲突 | 7 |

缺值由9,454个`pending`商品分类、7类尚未采集的商品版本字段各10,000项、46个作品release_date和46个release_region组成。7项冲突是helpfulness来源差异，已按任务8规则保留，不是来源缺失。

逐项结果位于`data/work/lineage_audit/bounded_10000.json`。第一次Airflow运行暴露并修复了映射查询别名错误；第二次run `manual__final_lineage_bounded_retry_20261107`的`repair_saved_links`与`audit_coverage`均success。

## 全量实跑覆盖率

Airflow run `manual__final_lineage_full_20261107`于北京时间2026-11-07 14:07开始，15:20完成；`repair_saved_links`和`audit_coverage`均为success。活动版本固定为`snap-movies-review-v3`和`logical-review-exact-v1`。

| 指标 | 全量结果 |
|---|---:|
| 字段目录项 | 31 |
| 适用字段/关系成员（各字段分母之和） | 80,651,547 |
| 非空字段/关系成员 | 78,641,193 |
| 完整来源链 | 78,641,193 |
| 非空但缺来源链 | 0 |
| 值缺失 | 2,010,354 |
| 明确冲突 | 8,420 |

组件分母为253,059个ProductEdition、46部已采用Movie、880个当前Person及880个别名、960条当前MoviePerson、51条ProductMovie、7,835,611条LogicalReview和7,849,544条ReviewMapping。数据库候选库存883部Movie及历史/非采用人物关系不混入最终采用分母。

2,010,354个缺值由238,849个pending商品分类、7类未采集商品版本字段各253,059项、46个作品release_date和46个release_region组成。它们是明确NULL/pending，不是已有值缺少血缘。8,420个冲突均为逻辑评论helpfulness的来源票数差异；逻辑值按任务8规则置NULL/冲突状态，所有来源值与映射仍保留。

机器可读证据：`data/work/lineage_audit/full_status.json`、`coverage_summary.csv`、`gap_detail.csv`（仅表头，0个缺口）、`repair_history.csv`及`full_validation.txt`。四类血缘查询和五类业务查询复验输出分别为`lineage_queries_revalidation.txt`和`business_queries_revalidation.txt`。

## 影响分析和局部重算

真实来源`source_record_id=8002472`、作品`imdb:tt0078764`的canonical_title用于隔离shadow重算。输出断言190460依赖原采用断言88691，选择同时保留原断言和派生断言ID；反向影响查询返回7行，目标作品值没有改变。重复运行返回同一断言ID，无新增。证据见`data/work/lineage_audit/impact_demo.json`。

## 查询计划和资源边界

`final_lineage_audit_explain.sql`已实跑。全量ASIN覆盖采用一次并行源链扫描和Hash Semi Join；LogicalReview与ReviewMapping分别采用一次并行顺序扫描并通过主键/locator索引连接。审计不会为每个评论字段各扫一次。执行计划的基数估计偏低，故这里只把它作为计划形态证据，不把cost换算为承诺耗时。

全量审计是只读大扫描，仅写31行汇总及少量审计元数据；本次实际墙钟约1小时13分。失败后用相同batch重新运行是幂等的；组件进度在autocommit下可见，但单个SQL组件内部不伪造细粒度检查点。

## 当前结论

- 已审计通过：全部253,059个ProductEdition、全部已采用Movie/ProductMovie/当前MoviePerson/Person/PersonAlias，以及当前7,835,611条LogicalReview和7,849,544条ReviewMapping；所有非空最终值缺来源为0。
- 已发现并修复：商品分类选择链、release_precision转换依赖、2条退休人物旧关系、ProductMovie决定中的源ID关系化。
- 仍缺证据：未采集的商品版本字段、238,849个全库pending分类、作品精确日期/地区；这些保持NULL或pending。
- 仍缺证据不等于审计失败：上述未采集商品版本字段、pending分类以及作品精确日期/地区继续保持NULL/pending；Amazon实时页面仍仅有5次有界失败尝试。
