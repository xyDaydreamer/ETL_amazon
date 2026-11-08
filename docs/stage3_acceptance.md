# 阶段3小样本验收记录

验收日期：2026-11-03（Asia/Shanghai）。先完成有界商品元数据、作品候选、人物关系和单页采集闭环；随后由用户手动完成UCSD全量元数据关联。未运行Amazon全量页面采集、IMDb全量TSV处理或全量作品匹配。

## 基线与迁移

- 复用阶段2当前解析版本 `snap-movies-review-v3`、253,059个 `target.product_edition` 和 `data/work/stage2/product_list.tsv`。
- 新增迁移 `sql/005_stage3_product_metadata.sql`，建立离线商品证据、作品候选和算法复核队列；未删除或改写评论、原始记录和历史解析结果。
- 依赖实测发现 Airflow 3.3.2/Python 3.12 约束固定 `psycopg 3.3.5`，项目从3.3.6修正为3.3.5；Scrapy固定2.19.0。镜像内实测版本为Scrapy 2.19.0、psycopg 3.3.5。

## UCSD真实小样本

命令读取UCSD gzip JSONL流，最多允许100条；目标6个ASIN全部在前20条内命中，因此实际只读20条。没有下载或保存整份105,021,691字节文件。

- 来源：`meta_Movies_and_TV.json.gz`
- 版本证据：ETag `"64280fb-62be3295ab953"`，Last-Modified `Fri, 17 Jan 2025 09:01:51 GMT`
- 请求6、命中6、离线覆盖率6/6=100%；这是有意选择的首批样本，不能外推到253,059个产品。
- 分类：movie 1、non_movie 2、pending 3。每条保存JSONL记录号、原始JSON哈希、原始标题/描述/分类、规范标题及规则依据。
- `0005019281` 位于记录18，商品标题为 `An American Christmas Carol VHS`，规则只去除载体词用于候选生成；商品本身仍保留VHS语义。

## UCSD全量元数据关联

用户按README手动执行本地gzip全量入口，run 39于2026-11-03 10:53:44Z开始、10:56:39Z完成：

- 文件：105,021,691字节；SHA-256 `b7362b6c0bf92a6c47fbbb7855aea2bffd45042f6f10af73bc3afa3d8716c3e9`。
- 实际干净读到gzip EOF，共203,766条JSONL；网页声明203,970，两者相差204，以本次固定文件哈希和真实EOF为验收事实，不补造记录。
- 检查点：committed 203,766、next 203,767、`verified_complete`。
- 与253,059个阶段2商品交集为73,813个，离线覆盖率29.1683%；未覆盖179,246个。
- 保存88,660条匹配来源记录；14,847个ASIN各有2条记录。重复记录规范标题冲突0、分类冲突0，因此没有按源顺序任意选值。
- 按不同ASIN分类：collection 6,835、movie 433、non_movie 2,672、series 4,270、pending 59,603，合计73,813。
- 全部商品最终分类：collection 6,835、movie 433、non_movie 2,672、series 4,270、pending 238,849；pending等于59,603个已覆盖但证据不足商品加179,246个未覆盖商品。
- 原始证据孤儿0，元数据对应未知商品0；业务库验收时约58 GB。

可重算SQL为`sql/queries/stage3_full_validation.sql`。全量覆盖率与实时页面成功率仍必须分报：前者29.1683%，后者0/1。

被Git忽略的D盘交付物：

- `data/work/stage3/review_queue.tsv`：59,603条开放商品分类复核项加表头，共59,604行；SHA-256 `c93df04b1b8cd7927f8433eb59634e5cdcbd36460c089be175ca54f99c3ae7e9`。
- `data/work/stage3/ucsd_coverage_summary.tsv`：1条覆盖摘要加表头；SHA-256 `9b857034f6d3ca921b78f49a7647841a3b782dba9197b0bc11f225148cde83c5`。

## 真实作品与人物链路

采用链路：

`ASIN 0005019281` → UCSD记录18商品证据 → IMDb候选 `tt0078764` → 作品 `An American Christmas Carol`（1979）→ starring `Henry Winkler`、`David Wayne`。

最终采用规则版本为 `stage3-product-movie-v2`。v1保留首次过严阈值的review/manual_review历史，未覆盖或删除。标题规范值精确一致；IMDb建议响应提供作品外部ID、标题、TV movie类型、1979年和两位principal names；Wikipedia支持摘要独立给出1979年和导演Eric Till。两处年份一致且标题、类型、人物/导演共同满足多事实阈值。Wikipedia摘要不是剧情摘要，描述相似度0.04178只作观测，不作为否决条件。人物名称来自IMDb建议响应，但该响应没有nconst，因此人物稳定键明确标成 `imdb-suggestion-name:*` 候选身份，不能据此完成同名消歧。

原始HTTP JSON、字段定位、候选断言、标题规范化依赖、年份双来源断言、最终字段选择和Product-Movie合并决策均可查询。`sql/queries/stage3_sample_validation.sql`可重算示例。

## 复核队列

开放3条，均为商品分类证据不足：`0005054885`、`0005059836`、`0005055121`。首次过严的作品候选阈值曾把 `0005019281` 放入复核；规则按真实摘要语义修正并复验后，该历史队列项保留且状态为resolved，没有冒充人工标签。

## 页面采集

Scrapy设置：遵守robots.txt、每域并发1、下载延迟5秒、重试上限2、HTTP缓存开启、单次只请求一个ASIN。独立运行真实发出robots和商品页两个请求，均HTTP 200；商品响应为验证页，记为 `access_restricted`，成功率0/1，受限率1/1。快照SHA-256为 `512EA675A0D08B0CE8CF484ED6C9F1C4E88ACFEB9AAE82335C84D3A1C943DA69`，文件位于被Git忽略的 `data/snapshots/stage3/`。

第一次Scrapy 2.19运行因旧 `start_requests` 接口没有调度请求；当时错误地生成的run 21未删除，已纠正为failed并记录 `attempted=0`。改用异步 `start()` 后复验通过。

## Airflow与幂等

DAG `stage3_product_movie_sample` 为手动触发、关闭catchup、`max_active_runs=1`；DAG导入不联网、不读大文件、不查库。网络任务分别设置有限重试，页面受限作为真实业务结果而不是调度失败；XCom只传run id和少量计数。

- `manual__stage3_sample_20261103`、`manual__stage3_sample_rerun_20261103`：初始规则下均为4任务success。
- `manual__stage3_final_v2_20261103`：最终v2代码4任务全部success。
- 最终v2重跑后：UCSD raw记录6、商品元数据6、版本化作品候选2（v1 review、v2 verified）、Product-Movie关系1、Movie-Person关系2。相同规则版本重跑不增长；采集尝试按事实追加，响应内容按哈希复用。
- 日志目录：`airflow/logs/dag_id=stage3_product_movie_sample/`。

## 测试和实际命令

临时在可丢弃scheduler容器安装pytest 9.0.2后执行全部测试：21 passed、8 subtests passed；唯一警告是复制到只读临时目录后pytest不能写缓存。DAG导入错误为0。

核心复验：

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage3-offline-sample `
  --config /opt/airflow/project/config/stage3_sample.json `
  --batch-id stage3-manual-offline --max-records 100

docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage3-imdb-verify `
  --config /opt/airflow/project/config/stage3_sample.json `
  --batch-id stage3-manual-candidate

docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/queries/stage3_sample_validation.sql
```

## 未验证与进入下一阶段条件

- 未测UCSD对全部253,059个ASIN的覆盖，当前100%只是选定6条样本的覆盖。
- 本地UCSD gzip全量入口和事务检查点先在独立测试库以3条构造gzip验证，随后真实105 MB文件已由用户手动完成；结果见上文。
- 未下载IMDb每日TSV快照；本轮使用IMDb的有界建议JSON及可核实支持来源。正式批量作品/人物处理前应固定TSV文件日期、大小和SHA-256，并遵守IMDb非商业许可。
- 未建立电影/人物独立人工评价集，不得报告匹配Precision/Recall/F1，也不得把规则结果称作人工确认。
- Amazon页面仍受限，不绕过验证。全量页面采集当前不具备开始条件。
- 已具备继续扩大离线元数据批次和准备人工评价集的代码/模型基础；尚不具备宣布阶段3全量电影集成完成的条件。
