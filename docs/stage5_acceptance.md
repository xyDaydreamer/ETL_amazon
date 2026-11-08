# 阶段5固定IMDb快照与选择性人物集成验收

验收日期：2026-11-05（Asia/Shanghai）。固定快照、`tt0078764`真实小样本以及38条采用映射的选择性长扫描均已完成数据库验收；没有开始正式人工评价或计算指标。阶段4的50条development预测和6项人工标签保持冻结。

## 输入快照核对

用户实际完成了5文件下载、完整SHA-256与gzip EOF验证。清单`data/work/stage5/imdb-2026-11-05-full.json`：

| 文件 | 压缩字节 | 数据记录数 | SHA-256 |
|---|---:|---:|---|
| title.basics.tsv.gz | 229,783,200 | 12,931,235 | `10bcde40fee43a47adf9dec4534188f81af30b6fe3815f599da9091cae474ead` |
| title.akas.tsv.gz | 520,717,957 | 60,125,602 | `03f8228e4f22b85bbc0cb1f1ecee63643cbd8e6388afb3696d876766d70cbd4a` |
| title.crew.tsv.gz | 83,921,076 | 12,928,096 | `689a899de6e6f8fd1cbc514139b553a10779388c3a15d51e3879bc7ae5b60364` |
| title.principals.tsv.gz | 789,374,317 | 102,588,099 | `ac1334fbe775606aa6bb3a5f696239a7362d7f247fb6942a6790b01180ed79f5` |
| name.basics.tsv.gz | 312,374,951 | 15,786,283 | `eeff9e11fc02984cb52e9836bad414f0768fbff5b73476173374d6e1482a6406` |

快照ID 1、版本`imdb-noncommercial-2026-11-05`，数据库状态`fully_verified`。命令输出中的`431249…`是生成清单时对不含自引用字段的规范内容摘要；数据库中的`c9b472…`是最终清单文件字节哈希。二者语义不同，可分别复算，不是冲突。

源文件继续只读保留在`D:\amazon\data\raw\imdb\2026-11-05`，未重新下载或复制。

## 首次失败及修复

最初`imdb-title-evidence`在`title.akas`约3000万条处失败：Python `csv`默认单字段上限131,072字节。失败发生在证据扫描阶段，没有写业务数据库，也没有生成残缺证据文件。

修复后不再使用CSV字段缓冲，而是gzip逐行严格UTF-8解码、按TAB拆分、验证列数并对原始内容行计算SHA-256。新增140,000字符字段测试。真实复跑完整扫描60,125,602条akas与102,588,099条principals成功：

```json
{
  "tconst": "tt0078764",
  "title_found": true,
  "alias_count": 6,
  "principal_count": 19,
  "resolved_name_count": 19,
  "missing_name_ids": []
}
```

## 选择性装载实现

规则版本：`imdb-selective-integration-v1`。

- 扫描目标来自冻结的`adopted_development_movie_mapping`，或显式tconst/文件。只把目标作品、aka、crew、principals及由它们发现的nconst姓名写库；不默认全量装载IMDb五表。
- 扫描scope把唯一tconst和ASIN商品成员分表保存，支持后续多个商品版本指向同一作品；当前冻结映射为38个ASIN、38个不同tconst。
- 每500,000个源记录可提交一次。选中记录与`imdb_selective_progress`在同一事务提交。
- 恢复策略是每个gzip从头解压并丢弃到已提交记录后继续；不把压缩字节偏移当作随机恢复点。中断越晚，重复解压成本越高，但不会漏写或重复身份。
- 同一快照、scope使用PostgreSQL session advisory lock，CLI与Airflow不能并发写同一逻辑输入。
- 同一源文件位置若内容哈希变化会显式报冲突；同一snapshot业务主键若指向不同SourceRecord也会报冲突，不静默覆盖。
- 原始证据保存文件SHA、数据记录号、源文件行号、行SHA、字段JSON；NUL若出现则规范字段不写并以Base64保留原行。
- 作品与人物最终字段通过`field_assertion`和`field_selection`保存抽取版本、定位、采用理由。关系通过`movie_person_source_evidence`保存人物原名、IMDb类别、job、角色JSON、principal顺序、选择/支持状态及来源。
- `title.principals`仅映射为principal credit：actor/actress/self→`cast`，director→`director`，writer→`writer`，其他→`other`；从不自动写成`starring`，也不声称完整演员表。

## 真实`tt0078764`结果

作品：An American Christmas Carol，1979，TV movie。真实证据包选中：basics 1、akas 6、crew 1、principals 19、names 19，共46条SourceRecord。

证据文件`data/work/stage5/tt0078764-evidence.json`的SHA-256为`C7B70E1D3968BD6745978974B8A7555F0B323E459B616673EB1409FDBFE6C964`。

实际物化：

| 项目 | 数量 |
|---|---:|
| IMDb作品 | 1 |
| 作品外部tconst | 1 |
| nconst人物 | 19 |
| 人物外部nconst | 19 |
| 人物主名别名记录 | 19 |
| 独立作品—人物关系事实 | 19 |
| 关系来源证据行 | 22 |
| 作品字段断言 | 4 |
| 人物字段断言 | 19 |
| 最终字段选择 | 23 |
| 缺失作品/人物 | 0/0 |
| 本轮质量问题/字段冲突 | 0/0 |

19项关系事实分为：cast 10、director 1、writer 2、other 6。另有3条supporting证据：导演1、编剧2同时出现在crew和principals，但不重复生成关系事实。`credited_as`保持NULL，因为这5个IMDb文件没有逐作品credited-name字段；原始`primaryName`单独保存在关系证据中，不能冒充逐片署名。

旧suggestion人物没有按姓名直接合并。通用规则要求“规范姓名一致 + 两边通过不同SourceRecord关联同一已确认tconst + 唯一nconst候选”。Henry Winkler和David Wayne满足该组合，各生成一条`rule_verified`人物候选和不可变MergeDecision；旧`imdb-suggestion-name:*`记录标为retired但未删除，旧关系和来源继续可查。实际人物合并决定2条，歧义0，缺失0。

重复装载同一真实证据包多次后上述实体、外部ID、关系和字段选择计数不变。

## 构造数据库恢复验收

独立库`amazon_movies_stage5_test`使用构造IMDb格式文件，不混入业务结果：

- 提交前故障：注入成功，恢复前检查点0，恢复无丢失。
- 提交后故障：注入成功，恢复前检查点2，从gzip开头丢弃2条后继续。
- 同scope重复运行：幂等。
- 同一文件位置不同内容：显式冲突。
- 多ASIN→单tconst scope：1个扫描作品、2个商品成员，未丢映射。
- 构造结果：作品1、人物3、关系事实3；所有检查通过。

## Airflow验收

DAG：`stage5_imdb_selective_integration`，手动触发、`catchup=False`、`max_active_runs=1`。导入时不读大文件、不联网、不连接业务库；任务XCom只返回run ID和少量计数。装载任务重试1次，业务幂等和scope锁保证重试安全。

真实证据模式实际运行三次均success：

- `manual__stage5_real_evidence_1`
- `manual__stage5_real_evidence_2`
- `manual__stage5_real_evidence_3`（最终代码，person_merges=2）

最终运行约1.8秒，验证任务同时确认阶段4仍为38条采用、11条复核。日志位于`airflow/logs/dag_id=stage5_imdb_selective_integration/`。DAG导入错误0。

## 38条采用映射真实扫描与数据库验收

用户实际运行的`run_id=75`、scope `stage5-adopted-38-v1`已达到`materialized`。五个文件检查点均为`verified_complete`且`reached_eof=true`，目标集合SHA-256为`6be5bd9c503629d641296f6d0e35631b83e6752377a27146790d5e8a16f5e0a5`。选中记录为basics 38、akas 1,334、crew 38、principals 808、names 742；缺失作品和人物均为0，run 75质量问题0。

只读验收SQL为`sql/queries/stage5_adopted38_acceptance.sql`。实际结果：

| 核对项 | 结果 |
|---|---:|
| scope商品/不同ASIN/不同tconst | 38 / 38 / 38 |
| 与冻结采用视图的集合差异 | 0 |
| scope作品/nconst人物/关系事实 | 38 / 742 / 809 |
| 作品字段断言/人物字段断言/最终选择 | 152 / 742 / 894 |
| 无原始SourceRecord的字段断言 | 0 |
| 无关系来源或姓名来源的关系 | 0 |
| 缺作品外部ID/孤儿关系/缺目标关系 | 0 / 0 / 0 |

809项关系事实按角色为cast 380、director 39、writer 74、other 316。它们来自921条来源证据，其中809条`selected`、112条`supporting`。所有证据都有作品关系行和`name.basics`行定位，`primary_name_raw`缺失0。`characters_raw`为空并非血缘缺失：crew关系本来没有角色字段，principals中也允许`\N`；程序保留NULL语义，没有补造角色。

### 808条principals为何得到809项关系事实

808是`title.principals`的源记录数，不是最终关系去重数。其中三组同一作品、同一nconst的不同IMDb category折叠到相同role_type，故principals自身得到805个不同`(tconst,nconst,role_type)`事实。`title.crew`提供113个不同导演/编剧事实；其中109个已经出现在principals。集合并集为`805 + 113 - 109 = 809`。重复证据保留为supporting，不复制目标关系。

### 本轮新增、scope总数和数据库总数

run 75之前的真实pilot是1部作品、19个nconst人物、19项关系事实。本scope与pilot的人物无重叠，因此本轮增加38部IMDb作品、742个nconst人物、809项关系事实、2,960条IMDb原始记录和38条商品—作品映射；这些数恰好等于本scope总数，但不能推广为每次运行的“插入计数”。数据库当前总数为IMDb作品39、IMDb nconst人物761、IMDb关系事实828、关系来源证据943。`target.person`共763，还包括2个保留为retired的旧suggestion姓名实体；`target.movie_person`共830，还包括2条旧suggestion历史关系。历史记录没有删除或强行并入本scope统计。

`person_merges=0`只表示run 75的38部作品中没有需要把既有`imdb-suggestion-name:*`实体升级到nconst的唯一候选。742个人物均直接以IMDb nconst建立稳定身份，程序没有按姓名合并。它不表示“没有同名人物”、不表示自动证明742人互不重复，也不表示人物消歧失败。pilot另有Henry Winkler和David Wayne两条可查询的规则合并决定。

作品38个canonical title、original title、release year、release type以及人物742个canonical name均有非空规范值、字段断言、最终选择、文件行号及内容哈希，冲突0。关系保留principal category、job、角色原JSON、顺序和原始姓名。`title.principals`仍只称principal credits，不能声称完整演员表、全部演职员或“主演”。

## 开发案例与独立评价准备

有界命令`stage5-prepare-cases`只查询已入库数据，不扫描IMDb gzip、不联网、不改最终映射。实际生成：

- 10对近名人物development候选；双方nconst、姓名、职业/年代、known-for、scope作品、源记录行号与哈希、IMDb链接齐全，答案列为空。不同nconst不会因相似姓名自动合并。
- 2组电影跨版本候选：`B001L67A3M Chocolate → tt1183252`、`B002VRNJQ2 The Crazies → tt0455407`。当前只有标题/aka一致，明确标为candidate-only，尚未算作已验证合并。
- development排除清单：50个阶段4 ASIN、310个阶段4缓存候选tconst、761个pilot+38 scope nconst，并记录10个人物开发对。正式评价创建器现同时检查左右实体的排除键。
- HTML直接展示双方证据及可点击来源；TSV的`human_label`和`human_reason`均为空。正式评价集尚未创建，数据库正式人工标签仍为0。

材料位于`data/work/stage5/case_materials/`，清单为`case_prep_manifest.json`。就作业“各5组”而言：人物合并已有2组真实决定，仍缺3组；人物不合并有10组候选但尚无正式do-not-merge决定，仍缺5组已核实展示；电影跨版本有2组待核实候选、0组已确认，仍缺5组已确认；易混淆电影目前只有Resident Evil系列1个候选级人工否定可直接复用，仍缺4组已核实不合并案例。不能把候选数写成完成数。

电影与人物各至少100对的独立评价集应另起固定清单：排除上述development实体后，从阶段4外商品/作品和关联人物分层抽样；冻结双方证据、链接、抽样版本和清单哈希，再调用现有`evaluation-create`与`evaluation-export`。导出的HTML不含程序预测，答案TSV只留“同一实体／不同实体／无法确定”和理由。当前固定选择性库只覆盖development作品，尚不足以构造无污染的两套100对真实评价集；下一步须先对阶段4外候选做一次新的选择性范围准备。该扩大扫描必须另给用户手动命令，本轮未启动。

## 已执行的长扫描命令（保留作恢复说明）

执行条件：Docker服务健康；快照ID 1仍为`fully_verified`；阶段4仍为38条采用/11条复核；不要同时运行CLI和Airflow的同一scope。本命令只扫描并写相关记录，不会全量装载五表：

```powershell
Set-Location 'D:\amazon'

docker compose exec -T airflow-scheduler python -m amazon_movies.cli imdb-selective-load `
  --snapshot-dir /opt/airflow/project/data/raw/imdb/2026-11-05 `
  --snapshot-version imdb-noncommercial-2026-11-05 `
  --scope adopted-development `
  --scope-key stage5-adopted-38-v1 `
  --batch-id stage5-adopted-38-cli-v1 `
  --batch-size 500000 --progress-every 5000000
```

实际结果：5文件到真实EOF，作品38、人物742、关系事实809、缺失0。相同scope再次执行只用于恢复或幂等复验，不应为了获取相同统计重复启动长扫描。

可在另一个普通PowerShell窗口查看已提交进度：

```powershell
Set-Location 'D:\amazon'
docker compose exec -T airflow-scheduler python -m amazon_movies.cli imdb-selective-status `
  --snapshot-version imdb-noncommercial-2026-11-05 `
  --scope-key stage5-adopted-38-v1
```

若命令中断，直接重跑**完全相同**的装载命令。它会读取数据库检查点，从gzip开头扫描并丢弃到`committed_record_number`后继续；不要修改scope-key、快照版本或目标集合。原文件和已提交批次不需删除。

也可由Airflow运行长扫描，但不能与CLI同时运行：

```powershell
docker compose exec -T airflow-scheduler airflow dags trigger stage5_imdb_selective_integration `
  --run-id manual__stage5_adopted_38_v1 `
  --conf '{"batch_id":"stage5-adopted-38-airflow-v1","snapshot_version":"imdb-noncommercial-2026-11-05","snapshot_dir":"/opt/airflow/project/data/raw/imdb/2026-11-05","scope":"adopted-development","scope_key":"stage5-adopted-38-v1","load_mode":"scan","batch_size":500000,"progress_every":5000000}'
```

## 未完成要求

- 阶段4复核队列11条仍保持，不因IMDb装载自动采用。
- IMDb快照只选择性入库；未覆盖其他商品候选，更不是IMDb五表全量副本。
- 人物别名目前只有`name.basics.primaryName`；近名development材料已生成，但真实别名及跨作品消歧仍需来源与人工核实。
- 作业要求的人物5组合并、5组不合并展示尚未完成；当前只有2条真实合并决定，候选不能冒充决定。
- 作业要求的电影5组跨版本合并、5组易混淆不合并尚未完成；当前2组跨版本候选和1组人工否定只是准备进度。
- 电影和人物各100对独立评价集尚未生成；正式人工标签0，未计算Precision、Recall或F1。
- 页面采集、第三方电影补充和阶段4以外的电影匹配不在本轮范围。
