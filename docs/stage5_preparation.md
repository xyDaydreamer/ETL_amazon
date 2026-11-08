# IMDb固定快照、人物集成与独立评价集准备

准备日期：2026-11-05（Asia/Shanghai）。本文保留阶段5启动前的准备过程；后续实际快照、选择性长扫描和数据库验收结果以`docs/stage5_acceptance.md`为准。阶段4冻结基线仍为50个商品：人工采用4、规则采用34、排除1、复核队列11；统一采用映射共38条。

## 现有实现核对

| 能力 | 本轮前实际状态 | 本轮结果 |
|---|---|---|
| IMDb来源 | README只有5文件下载示例；数据库没有固定快照 | 后续已登记snapshot_id 1且五文件完整验证；见阶段5验收 |
| 作品证据 | suggestion缓存有tconst、标题、年份和姓名字符串 | 新增固定TSV中单个tconst→aka/crew/principals→nconst的可定位检查器 |
| 人物实体 | 现有2人仅有`imdb-suggestion-name:*`候选键，没有nconst | 新增nconst外部标识、IMDb人物暂存和人物候选接口；真实升级须等固定快照 |
| 评价集 | 只有阶段4 development标签，不能算正式指标 | 新增独立`evaluation`模式、冻结对、算法预测和人工标签分表、盲评HTML/空白TSV导出及幂等导入 |
| 选择性IMDb装载 | 无 | 后续已完成38作品范围：742人物、809关系事实；并未全量复制IMDb五表 |
| 正式评价 | 无 | 开发排除清单和盲评工具已完成；仍未抽取电影100对、人物100对，业务库正式评价标签为0 |

IMDb官方说明：非商业数据允许在相应条件下保存本地副本，每日刷新；文件是UTF-8、gzip压缩的TSV，`\\N`表示缺失。`title.principals`只表示主要演职员，不能冒充完整演员表。来源与字段定义见<https://data.imdb.com/non-commercial-datasets/>，使用条件见<https://help.imdb.com/article/imdb/general-information/can-i-use-imdb-data-in-my-software/>。

## 最短实施顺序

1. **固定同一获取批次**：手动把5个IMDb文件下载到D盘日期目录，保留响应头；不要把跨日变化的续传文件混成一个快照。
2. **先快检、再全检**：先检查文件集合、UTF-8表头和首条列数；再计算每文件SHA-256并把每个gzip读到真实EOF。只有全检清单可作为后续正式装载版本。
3. **真实小样本链**：对现有`tt0078764`执行单片扫描，核对title、aka、crew、principals和nconst姓名的文件行号与行哈希。此步通过后才补全全量COPY装载器。
4. **分批装载固定快照**：实现并验证5表COPY、同批检查点、内容冲突、提交前后恢复；之后由用户手动运行长批。不得把suggestion姓名直接升级为nconst。
5. **人物实体集成基线**：先按IMDb nconst建立稳定人物；姓名/别名只生成候选。合并旧姓名候选至少结合共同作品、角色/职业和年代，保存merge/do_not_merge/manual_review历史。
6. **扩大电影候选但冻结旧50条**：阶段4的50条不再参与调参。对其余商品用固定快照生成新候选，规则另起版本；38条冻结采用映射可用于数据链验证，但不得混入正式评价。
7. **冻结独立电影100对**：排除阶段4全部50个ASIN、其候选tconst及用于调规则的案例；保存抽样规则、分层、清单哈希后再导出盲评材料。
8. **建立人物开发集，再冻结人物100对**：先用独立的person-development样本开发姓名/别名/同名规则；正式person-evaluation排除所有开发nconst、左侧人物和关联开发案例。
9. **学生盲评**：HTML展示双方信息、可点击Amazon/IMDb来源、原始记录ID/位置、标题或姓名、年份/职业/共同作品和冲突。答案TSV只需填写`human_label`与`reason`；不显示程序预测，不预填答案。
10. **最后计算指标**：标签冻结后才联接算法预测，分别报告电影和人物的Precision/Recall/F1、无法确定比例、复核率和错误案例。分层抽样若不是总体等概率抽样，必须同时说明分层口径，不能把未加权结果冒充总体性能。

## 已完成代码和数据库准备

- `sql/011_stage5_imdb_snapshot_and_evaluation.sql`：快照、5个IMDb暂存表、tconst/nconst外部标识、人物候选、独立评价集、预测和人工标签。
- `src/amazon_movies/imdb_snapshot.py`：有界表头检查、全gzip/哈希验证、清单登记、单tconst人物链证据扫描。
- `src/amazon_movies/evaluation_material.py`：拒绝阶段4开发ASIN重叠，冻结pair payload；导出不含预测的HTML和空白答案TSV；人工答案追加且幂等。
- CLI：`imdb-snapshot-inspect`、`imdb-snapshot-register`、`imdb-title-evidence`、`evaluation-create`、`evaluation-export`、`evaluation-import`。

评价材料采用三个文件：`review_material.html`用于查看双方证据和点击来源；`answers.tsv`仅含程序生成的`review_item_id`以及两个待填列；`review_manifest.json`保存冻结哈希、规则版本及“未预填/未展示预测”的可核验证明。允许标签为`same_entity/different_entity/cannot_determine`，也接受“同一实体/不同实体/无法确定”。

## 实际小样本验收

本轮只用构造fixture验证技术行为，明确不作为事实、人工标签或正式评价集：

- 全部可发现unittest：23项通过，其中本轮新增5项；覆盖快检不冒充gzip全检、5文件完整读取和哈希、tconst→nconst定位、链接协议过滤、中文标签规范化。
- Python源码AST解析：13个文件通过。`compileall`因容器把源码只读挂载、无法写`__pycache__`而报只读错误；CLI实际导入和`--help`成功。
- 独立测试库`amazon_movies_stage5_test`迁移001→011成功。
- 构造评价集首次冻结`inserted=1`，相同输入重跑`inserted=0`；HTML含可点击HTTPS来源，答案模板标签/理由为空，清单明确`human_answers_prefilled=false`及`algorithm_predictions_in_material=false`。
- 空白答案导入：`inserted=0, skipped_blank=1`。构造答案在测试库首次导入1、重复导入0；业务库`evaluation.human_label=0`。
- 011在业务库重复执行成功；执行前后冻结状态均为采用38、复核11，未修改阶段4预测、人工决定或队列。
- 业务库当前`meta.external_snapshot=0`，因此尚不能声称固定IMDb快照或真实nconst人物链已验收。

构造渲染样例位于`data/work/stage5/constructed_smoke/`，只用于检查页面与导入行为，绝不能加入课程评价统计。

## 需用户手动运行：下载与完整验证

执行条件：Docker Desktop与当前Compose服务正常；D盘预留足够空间；一次获取尽量在同一天完成。IMDb文件每日刷新，下面的`2026-11-05`是本地获取批次标签，不是IMDb提供的不可变发布ID。若跨日后需要重新下载，不要续传旧日期目录，应换新的日期目录。

### A. 下载5个文件并保存HTTP响应头

在普通PowerShell执行；不需要管理员权限：

```powershell
Set-Location 'D:\amazon'
$snapshotDate = '2026-11-05'
$imdbDir = "D:\amazon\data\raw\imdb\$snapshotDate"
New-Item -ItemType Directory -Force -Path $imdbDir | Out-Null

$files = @(
  'title.basics.tsv.gz',
  'title.akas.tsv.gz',
  'title.crew.tsv.gz',
  'title.principals.tsv.gz',
  'name.basics.tsv.gz'
)

foreach ($name in $files) {
  curl.exe --fail --location --continue-at - `
    --dump-header (Join-Path $imdbDir "$name.headers.txt") `
    --output (Join-Path $imdbDir $name) `
    "https://datasets.imdbws.com/$name"
  if ($LASTEXITCODE -ne 0) { throw "下载失败：$name" }
}

Get-ChildItem -LiteralPath $imdbDir -Filter '*.tsv.gz' |
  Select-Object Name,Length,LastWriteTime
```

预期：5个`.tsv.gz`和5个响应头文件；curl逐文件显示进度。该步骤成功只表示下载命令完成，不等于gzip或字段已验证。

### B. 有界快检

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli imdb-snapshot-inspect `
  --snapshot-dir /opt/airflow/project/data/raw/imdb/2026-11-05 `
  --snapshot-date 2026-11-05 `
  --output /opt/airflow/project/data/work/stage5/imdb-2026-11-05-quick.json
```

预期：`complete_file_set=true`、`all_headers_verified=true`，但`all_gzip_streams_verified=false`。这是刻意的边界，不能据此登记为完整快照。

### C. 完整哈希和gzip EOF验证（长时间）

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli imdb-snapshot-inspect `
  --snapshot-dir /opt/airflow/project/data/raw/imdb/2026-11-05 `
  --snapshot-date 2026-11-05 `
  --full-verify --progress-every 5000000 `
  --output /opt/airflow/project/data/work/stage5/imdb-2026-11-05-full.json
```

预期：stderr每500万解压行报告一次进度；最终两个`all_*_verified`均为true，清单包含每文件大小、SHA-256和数据记录数。失败时保留原文件和输出，先报告错误；不要把失败清单登记为完整。

### D. 登记快照并检查一条真实作品—人物链

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli imdb-snapshot-register `
  --manifest /opt/airflow/project/data/work/stage5/imdb-2026-11-05-full.json

docker compose exec -T airflow-scheduler python -m amazon_movies.cli imdb-title-evidence `
  --snapshot-dir /opt/airflow/project/data/raw/imdb/2026-11-05 `
  --tconst tt0078764 --progress-every 5000000 `
  --output /opt/airflow/project/data/work/stage5/tt0078764-evidence.json
```

预期：登记状态`fully_verified`；单片命令报告title、alias、principal和已解析nconst数量，输出文件保存每项的源文件、行号、行SHA和IMDb可点击URL。该扫描不是全量数据库装载，可能需要顺序读取大型文件。

完成D后请把两条命令的JSON摘要发回。下一步先核对真实字段与计数，再补全并小样本验收COPY装载/人物物化代码；不会直接启动全量装载。

## 后续实际执行更新（2026-11-05）

用户已完成A—D。5个文件的完整SHA、gzip EOF和记录数均通过并登记为snapshot_id 1。首次`tt0078764`扫描因CSV字段上限失败；现已改为逐行TAB解析并完整复跑成功，实际取得1部作品、6条aka、19条principals和19个nconst人物。选择性装载、恢复、人物物化及Airflow小样本的完整结果见[阶段5验收记录](stage5_acceptance.md)。

## 未验证与阻塞

- 5个真实IMDb文件已完整验证；不再重复下载。
- 已实现按scope流式筛选、分批检查点和恢复，不默认全量COPY五表。38条采用映射的长扫描仍待用户手动运行。
- 现有2个`imdb-suggestion-name:*`已通过“姓名+同一已确认作品+唯一nconst”生成可追踪合并决定；旧记录保留为retired。
- 电影100对和人物100对尚未抽取；没有任何正式人工标签或指标。
- `title.episode`不在当前最小5文件范围；若后续正式处理单集/季映射，再按明确需求加入，不为当前电影/人物基线扩大快照。
