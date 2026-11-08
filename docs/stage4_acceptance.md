# 阶段4 v3/v3.1 电影匹配验收记录

验收日期：2026-11-04至2026-11-05（Asia/Shanghai）。当前规则版本：`stage4-first50-movie-match-v3.1-evidence-extraction`；旧v3保留。范围严格限制为既有50条开发样本和既有IMDb缓存；未联网、未下载大型文件、未处理其余movie ASIN。算法预测与人工标签分开。

## 输入、迁移和历史保留

- 复用阶段3 UCSD来源SHA-256 `b7362b6c0bf92a6c47fbbb7855aea2bffd45042f6f10af73bc3afa3d8716c3e9`及50条`sample_role='development'`样本。
- v1、v2草案、v2.1、v3和v3.1各保留50条版本化决策；没有删除原始商品记录、IMDb响应或旧决策。
- 已应用`sql/008_stage4_v3_evidence_and_labels.sql`：决策表新增确认路径、复核原因和分项分数；新建独立人工标签表。2026-11-05学生通过聊天提交6项development标签；AI只整理可定位证据并执行导入，没有替学生决定标签。
- v3从每个缓存响应提取全部tt项并按ID去重，实际评估311项；v2.1只保留过220项前5候选。

## v3历史结果

50条结果互斥合计：

| 状态 | 数量 |
|---|---:|
| `rule_verified` | 31 |
| `needs_review_ambiguous` | 1 |
| `needs_review_insufficient` | 14 |
| `no_suitable_candidate` | 3 |
| `no_candidate` | 1 |

自动确认中的确认路径：18条同时走B（T_exact+explicit）和D（T_strong+两位explicit），13条仅B；另1条歧义结果的展示候选也同时具备B+D。当前没有可靠外部ID、可靠商品侧作品年份或已核实导演路径。

v2.1→v3状态变化：29条确认保持确认；5条确认降为证据不足；1条确认降为多候选歧义；1条旧歧义升级确认；1条旧证据不足升级确认；9条证据不足、2条无合适候选及1条空候选池保持；另1条证据不足变为无合适候选。v2.1为35/1/11/2/1，v3严格后为31/1/14/3/1，没有为提高确认数修改阈值。

## 代表案例

- Frozen `B003L1ZWG2`：选择`tt1323045`，T_exact，Shawn Ashmore和Emma Bell两位explicit，80分，路径B+D。2013同名动画`tt2294629`只有标题40分。状态`rule_verified`。
- The Red Baron `B0039USJBK`：选择`tt0365675`，Matthias Schweighöfer与Lena Headey按“角色（演员）”结构成为explicit；Manfred von Richthofen保持`character_or_subject`。80分，B+D，状态由v2.1歧义变为`rule_verified`。
- Castle Rock `B000PGTNCK`：展示电影候选`tt0240400`，T_exact但没有可定位的候选人物，40分；电视剧同名候选有类型硬冲突。状态`needs_review_insufficient`。
- Resident Evil: Afterlife `B0046MOVD0`：`tt1220634`为80分B+D；`tt0432021`因同系列两位演员交叉命中获得70分D，分差10。规则不以rank消歧，状态`needs_review_ambiguous`。
- UFC: Ultimate Knockouts 8 `B0047S4UKC`：商品描述明确25个终结片段，分类体育集锦；第2/3/4集候选有编号和类型硬冲突。状态`no_suitable_candidate`，原因`product_category_review`。

## 测试和数据库验收

- 解析器unittest：18项通过。
- 阶段3/4函数测试：v3最终29项通过；v3.1的`tests/test_stage4_v3.py`为21项通过，新增前置冠词、演员/主持人、top-billed、表演语句及元数据演员列表测试。
- 覆盖候选池全量提取、ID去重、Unicode与副标题、通用短标题限制、姓名边界、角色/演员、其他作品引用、品牌去重、年份缺失、类型unknown、Knockout误分类、3D/4K、罗马/阿拉伯编号、多候选歧义、硬冲突排序和池级标签。
- 最终业务CLI run 59：50条、311候选、31/1/14/3/1。
- 最终v3数据库行数50；两次相同Airflow重放后的内容摘要均为`7454eb227025f17a5d344411e212ccf9`，证明没有新增版本内重复行且计算内容稳定。算法重放只更新运行元数据，不删除标签。
- 空白TSV实际导入验收：`inserted=0`、`skipped_blank=50`，人工标签仍为0；没有用构造标签冒充人工事实。

## Airflow实际运行

DAG `stage4_first_movie_matching`已切换为v3缓存重评：手动触发、`catchup=False`、`max_active_runs=1`；选择和匹配任务各重试1次，验证任务不重试。DAG导入错误为0，导入时不联网、不读大文件、不查业务库；XCom只有run ID和少量计数。

第一次v3运行`manual__stage4_v3_evidence_1`中，选择和重评成功，但验证SQL将四个历史规则版本联接成200行而失败。错误日志保留在`airflow/logs/dag_id=stage4_first_movie_matching/run_id=manual__stage4_v3_evidence_1/`。修复为分别按开发样本和指定v3版本计数。

最终代码实际连续运行：

- `manual__stage4_v3_evidence_final_1`：2026-11-04 09:38:16Z开始，09:38:20Z成功；业务match run 61。
- `manual__stage4_v3_evidence_final_2`：09:38:22Z开始，09:38:26Z成功；业务match run 63。
- 两次均为50条、311候选、31/1/14/3/1；这两次运行发生在人工标签提交前，之后的标签导入没有改变程序预测。

## 导出与人工入口

最新版：`data/work/stage4/first50_movie_candidates.tsv`，50数据行、38列、1,327,230字节，SHA-256 `15EAA38DA2113C823BA4CA5A62EDE2FF28A9E37BFBA8465FC59BBC166DB6A7E6`。所有行规则版本为v3.1；5个商品有标签历史，共6项。`candidate_pool`保存全部候选与证据；`human_labels_json`跨规则版本保存完整标签历史。

Excel中只填写：

- 候选级：`human_label_scope=candidate`、`human_label_candidate_id=具体tt ID`、`human_label=correct_match|incorrect_match|uncertain`。
- 池中无正确作品：`human_label_scope=pool`、候选ID留空、`human_label=no_match_in_pool`。
- 商品不是单作品：`human_label_scope=product`、候选ID留空、`human_label=not_single_movie`。
- 所有非空标签还必须填`human_reason`、`human_evidence`（JSON）、`human_annotator`和带时区的`human_labeled_at`。不要修改ASIN或`match_rule_version`。

导入会验证候选ID属于该ASIN保存的候选池，并以内容哈希幂等追加；不会覆盖算法结果或清空旧标签。

## 2026-11-05 development人工标注

学生通过聊天提交5个商品、6项标签，统一人工时间为`2026-11-05T01:02:20+08:00`，标注人字段为`coursework_student_via_chat`。导入文件为`data/work/stage4/development_labels_20261105_chat.tsv`。

- Frozen `B003L1ZWG2` / `tt1323045`：`candidate/correct_match`。
- The Red Baron `B0039USJBK` / `tt0365675`：`candidate/correct_match`。
- Castle Rock `B000PGTNCK` / `tt0240400`：`candidate/correct_match`，因此人工决定可以覆盖程序的“证据不足”用于后续开发映射，但不改写算法状态。
- Resident Evil: Afterlife `B0046MOVD0`：`tt1220634`为`candidate/correct_match`；`tt0432021`为`candidate/incorrect_match`，理由是后者为同系列另一部作品Extinction。
- UFC: Ultimate Knockouts 8 `B0047S4UKC`：`product/not_single_movie`。人工检索只记录“本次找到第1至7部、未找到第8部”，并明确`nonexistence_claim=false`。

首次导入`inserted=6`，完全相同文件再次导入`inserted=0`；数据库仍为6项。迁移009提供`current_movie_match_human_decision`和`approved_development_movie_mapping`视图。后者只接受“恰好一个当前correct候选且没有not_single_movie排除”的development商品，实际输出Frozen、The Red Baron、Castle Rock和Resident Evil: Afterlife共4条；Extinction被拒绝，UFC被排除。v1、v2草案、v2.1和v3程序结果仍各50条。

标签导入后又实际执行无网络v3重评run 64：程序统计仍为31/1/14/3/1，标签仍6项、当前决定6项、采用映射4条。再次导出的TSV仍有6项标签历史，SHA-256保持`AEC4F2C144F54A0C3A37CEACDD99E99D7E4157584EDDF30FAE1903FFE308930C`，证明算法重算没有覆盖或丢失人工决定。

## v3.1证据提取修正与统一采用

2026-11-05对原14条`needs_review_insufficient`逐项复核。通用修正识别actor/host、括号后stars、top-billed、performance/portrayal和商品元数据前置演员列表，并把仅差英文前置冠词记录为T_strong而非别名或T_exact。5条升级为规则确认：The Color of Money、The Open Road、Pandorum、Disgrace、Paa。v3.1统计为36确认、1歧义、9证据不足、3无合适、1空池；其余所有状态不变。完整逐条依据见`docs/stage4_insufficient_audit.md`。

迁移010建立统一采用逻辑：人工决定优先；没有人工决定时允许v3.1 `rule_verified`自动采用；人工否定的选中候选、歧义、证据不足和冲突保持暂缓；`not_single_movie`排除单电影映射但不删除商品、评论或证据。50条实际为人工采用4、规则采用34、排除1、暂缓11，合计50；采用视图输出38条。人工否定的Extinction候选单独保留，但Resident Evil商品通过人工确认的Afterlife采用。

Airflow运行`manual__stage4_v3_1_evidence_extraction`实际成功，DAG导入错误0；运行后标签仍6项、统一采用统计不变。

## 实际命令

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage4-v3-reevaluate `
  --batch-id stage4-v3-final-cli --limit 50

docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage4-export `
  --rule-version stage4-first50-movie-match-v3.1-evidence-extraction `
  --output /opt/airflow/project/data/work/stage4/first50_movie_candidates.tsv

docker compose exec -T airflow-scheduler airflow dags trigger stage4_first_movie_matching `
  --run-id manual__stage4_v3_evidence_final_1 `
  --conf '{"batch_id":"stage4-v3-final-airflow-1","match_limit":50}'

docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/queries/stage4_first50_validation.sql

docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage4-import-labels `
  --input /opt/airflow/project/data/work/stage4/first50_movie_candidates.tsv `
  --import-batch stage4-v3-human-review-1
```

## 未验证和停止点

未取得IMDb官方TSV快照，未核实nconst、完整导演/演职员、别名或地区上映日期；没有独立人工评价集，不能报告准确率、Precision、Recall或F1；未处理其余ASIN。本轮停在现有50条：38条采用、1条排除、11条保留复核队列，不自行扩展网络来源或任务范围。

## 2026-11-05冻结确认

按学生指令，现有50条development至此冻结，不再围绕它们调整规则。后续固定IMDb快照、人物实体和独立评价集使用新版本与新样本；业务库复核保持38条采用映射、11条复核队列，另1条`not_single_movie`排除不进入单电影映射。迁移011应用前后这些计数不变。
