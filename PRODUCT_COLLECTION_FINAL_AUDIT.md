# 产品页面采集最终审计

审计日期：2026-11-07（Asia/Shanghai）  
审计对象：Amazon Movies & TV ETL 项目的产品页面采集任务  
最终结论：**部分完成**

结论依据：253,059 个不同 ASIN 的清单、候选 URL、采集器、证据模型、数据库领取、缓存、限速、有限重试、恢复和幂等机制已经实现并验收；但实际只尝试过 1 个不同 ASIN，Amazon 返回 HTTP 200 验证页，真实有效 Amazon 商品页面为 0。当前官方条款限制未经明确许可使用数据挖掘、机器人或类似采集工具，历史响应又明确要求验证码，因此没有新增实时商品页请求、没有绕过限制，也没有启动 253,059 页采集。代码完成和 Airflow 可导入不等同于页面采集完成。

## 1. 最新全量状态

分母始终为 `staging.product_harvest` 中 253,059 个去重 ASIN；每个 ASIN 只有一个当前互斥状态。

| 状态 | 产品数 |
|---|---:|
| 未采集 `not_attempted` | 253,058 |
| 成功 `success` | 0 |
| 页面不存在 `not_found` | 0 |
| 访问受限 `access_restricted` / `robots_denied` | 1 |
| 超时 `timeout` | 0 |
| 解析失败 `parse_failed` | 0 |
| 网络失败 `network_failed` | 0 |
| 合计 | 253,059 |

清单核对结果：ASIN 数 253,059、不同 ASIN 数 253,059、不同原始 URL 数 253,059；全部 URL 均为 `https://www.amazon.com/dp/<ASIN>`，不匹配数为 0。

## 2. 请求、重试与成功口径

| 指标 | 实际值 | 口径 |
|---|---:|---|
| 实际尝试的不同 ASIN | 1 | `product_harvest_attempt` 去重 ASIN |
| 历史逻辑采集执行 | 5 | 五个旧批次均针对同一 ASIN |
| 已证实的目标页网络响应 | 1 | 首次 CLI 运行；响应正文已保存 |
| HTTP 缓存重放 | 4 | 四次 Airflow 日志均有 `httpcache/hit`；已回填 `cache_hit=true` |
| 重试请求 | 0 | 历史五条是独立运行，不是同一逻辑任务的重试 |
| 真实有效 Amazon 页面 | 0 | 必须同时通过状态、模板和目标 ASIN 核验 |
| 不存在 / 受限 / 超时 / 解析失败 | 0 / 1 / 0 / 0 | 当前产品状态分布 |
| 扩大验证候选计划 | 100个不同ASIN | 从全量清单按种子选择；不是HTTP请求 |
| 本轮新增网络请求 | 0 | 现有CAPTCHA与无书面授权仍适用 |

历史 ASIN `0005019281` 返回 HTTP 200，但正文包含 `validatecaptcha`，没有可证明的目标 ASIN，也没有商品详情模板。修复后的 `amazon-product-page-collector-v2` 对实际快照重算结果为：

```text
outcome=access_restricted
error_code=verification_page
observed_asins=[]
product_markers=[]
final_url_asin=0005019281
```

因此 HTTP 200 验证页没有被算作成功。

## 3. 原始响应与证据完整性

- 五条历史尝试均关联 `raw.source_record`；它们复用同一份不可变响应证据，不制造五份正文副本。
- 响应文件：`data/snapshots/stage3/amazon-0005019281-512ea675a0d08b0c.html`
- 文件大小：3,781 字节。
- SHA-256：`512ea675a0d08b0ce8cf484ed6c9f1c4e88acfeb9aae82335c84d3a1c943da69`。
- 数据库记录的请求 URL、最终 URL、HTTP 200、获取时间、快照路径、内容哈希和访问状态完整。
- 5/5 有响应的尝试均有快照路径和内容哈希；缺证据数为 0；孤儿尝试数为 0。
- 新采集器额外保存重定向链、缓存命中、请求序号、重试原因以及 ASIN/模板验证明细。
- 历史缓存属性按既有运行证据回填：首次 CLI 请求为网络响应，后四次 Airflow 运行由任务日志证明为缓存命中；回填依据写入 `validation_evidence.cache_provenance`。

原始响应未被修改；重分类结果作为新的验证证据追加保存。

## 4. 实际修复的缺陷

| 原缺陷 | 修复结果 |
|---|---|
| HTTP 200 且含 `<title>` 即成功 | 成功必须同时具备商品页结构和目标 ASIN 证据 |
| 未识别 HTTP 200 验证页 | CAPTCHA/Robot Check 标记统一为 `access_restricted` |
| 未核验目标 ASIN | 从 canonical、ASIN input、`data-asin`、结构化 JSON 抽取并核对；重定向到其他 ASIN 为失败 |
| 404、软404、403、429、5xx 混淆 | 分类为不存在、受限、可重试限流或网络失败，并保留错误码 |
| Scrapy 隐式重试不逐次落库 | 关闭隐式重试；应用级有限重试逐次保存，429/超时/5xx/网络错误才可重试 |
| 新失败会覆盖历史成功 | `valid_source_record_id` 永久保留最近有效快照；`latest_source_record_id` 与 `last_attempt_status` 单独记录新失败 |
| 没有数据库领取与恢复 | `meta.product_harvest_batch` + `staging.product_harvest_work_item`；`FOR UPDATE SKIP LOCKED` 原子领取，活动领取互斥，陈旧领取可恢复 |
| 逻辑任务、请求、缓存和成功数混为一谈 | 分别保存 logical task、attempt、`request_ordinal`、`cache_hit` 和有效成功数 |
| 缓存无时效、重试不可解释 | 缓存有效期 24 小时；每个重试保存前一失败原因 |
| User-Agent 未明确代理身份 | 改为 `Agent/amazon-movie-course-etl/0.2`，明确学术有界验证且不绕过访问控制 |

迁移 `sql/021_product_page_collection.sql` 与 `sql/022_product_collection_sampling.sql` 可重入，保留旧表、旧记录和历史快照。原 DAG `stage3_product_movie_sample` 未改名且继续复用同一业务模块；两个DAG的页面网络入口现在都默认关闭。独立 `product_page_collection` DAG 的人为候选上限由10改为可配置的1—100，`max_active_runs=1`；请求控制仍为并发1、间隔至少5秒、最多3次显式请求。候选上限只控制不同ASIN的计划规模，不等于真实HTTP次数，也不覆盖robots或网站访问控制。

候选计划`product-page-sample100-plan-20261107`从253,059条清单使用`seeded-md5-unattempted-v1`、种子20261107选择100条，排除了已有HTTP尝试的ASIN以及其他活动计划中的ASIN。数据库保存`selection_method`、`random_seed`、`candidate_limit`、`selection_rank`和`access_basis`。实际结果为100个不同ASIN、100个零请求工作项、`claimed_count=0`、`completed_count=0`、批次状态`prepared`。

## 5. 自动化与数据库验收

### 分类测试

`tests/test_product_page_collection.py`：8/8 通过，覆盖：

1. HTTP 200 只有 title 不成功；
2. 目标 ASIN + 商品模板成功；
3. HTTP 200 验证页为访问受限；
4. 403 不重试、429 有界重试；
5. 404 与软404；
6. 重定向 ASIN 不一致；
7. ASIN 存在但商品模板不足；
8. 候选上限1—100校验，与请求侧限制独立。

### 独立测试数据库

测试库：`amazon_movies_product_collection_test`，未向生产业务表写入测试产品或模拟响应。

`tests/integration_product_page_collection.py` 实际通过：

- 3个不同ASIN按固定种子生成计划，未产生HTTP attempt；同批计划重放候选顺序不变且不新增；
- 同一批次活动领取不能并发重复领取；
- 两次请求分别保存，request attempt 数为 2；
- 先成功后受限时，当前有效快照仍为原成功快照，最新失败证据另存；
- 工作项完成位置与业务写入一致；
- 相同已成功批次重放不再领取，不新增请求或结果。

迁移在测试库重入成功。生产迁移重入成功且历史 253,059 条清单未改变。

### Airflow

- `airflow dags list-import-errors --output json` 实际返回 `[]`。
- `product_page_collection` 可导入且默认暂停。
- 原 `stage3_product_movie_sample` 仍可导入。
- `manual__sample100_plan_20261107`的`collect`和`verify`均成功，但参数明确为`network_authorized=false`，数据库attempt总数前后仍为5；运行结束后DAG恢复暂停。
- 没有为了显示绿色状态而发出新的 Amazon 商品请求；该Airflow成功只证明计划与重放，不可声称网络采集成功。

## 6. 断点续跑和幂等

领取和恢复语义如下：

1. 逻辑批次先写 `product_harvest_batch`，每个 ASIN 写一个 work item；
2. 使用事务和 `FOR UPDATE SKIP LOCKED` 原子领取；
3. 活跃领取在一小时内拒绝同批并发运行；进程崩溃留下的陈旧领取一小时后可由相同批次恢复；
4. 每个响应证据、attempt、产品摘要和 work item 在一次数据库事务中提交；
5. 已成功批次使用相同 `batch_id` 重跑时返回 `idempotent_replay=true`，不会再请求；
6. 快照按内容 SHA 命名，相同响应复用文件和 `source_record`；
7. 历史有效快照不因后续 403、验证页、超时或解析失败丢失。

上述语义在独立测试库已实际验证；由于访问条件不允许，本轮没有用新的真实 Amazon 请求测试恢复。

## 7. 访问规则与本轮停止条件

核对日期为 2026-11-07。Amazon 官方公开条件限制未经明确许可的数据挖掘、机器人或类似采集工具，并限制收集商品列表/描述；官方 Agent Policy 还要求代理明确身份且不得规避 CAPTCHA 或其他访问控制：

- Amazon Conditions/相关官方条款：<https://sellercentral.amazon.com/gp/help/external/201824360>
- Amazon Agent Policy：<https://sellercentral.amazon.com/help/hub/reference/external/GS83KH2MA7HM69PH?locale=en-gb>

项目没有保存 Amazon 对 253,059 页自动化采集的书面授权。已有真实响应是 CAPTCHA 验证页。按“遇到明确访问限制立即停止、不绕过验证”的要求，本轮没有新增商品页请求。只有获得适用于本课程用途的明确授权或改用 Amazon 正式许可的 API/数据馈送后，才应解除 DAG 暂停并进行新的有界验证。

## 8. 离线元数据与实时页面严格分开

- UCSD Movies & TV 离线元数据匹配 73,813 / 253,059 ASIN，覆盖率 29.1683%。
- Amazon 实时有效页面为 0 / 253,059。
- UCSD 记录的标题、描述、分类和品牌不标记为 Amazon 实时页面字段。
- 测试构造 HTML 只用于分类测试，不写入生产 `raw.source_record`，不参与产品、电影或字段血缘。
- 因没有真实有效 Amazon 页面，本轮没有新增 Amazon 页面字段断言，也没有向电影关系写入页面来源。

## 9. 老师任务 2 逐条验收

| 要求 | 结果 | 证据/缺口 |
|---|---|---|
| 253,059 个 ASIN 的清单和候选 URL | 已完成 | 253,059/253,059，唯一且 URL 规则一致 |
| Scrapy/同类工具并保存响应 | 部分完成 | Scrapy 已实现；历史1份真实受限响应完整保存；无有效页 |
| 断点、缓存、限速、有限重试 | 已完成（机制） | 隔离库实测领取/恢复/幂等；缓存、并发1、至少5秒、最多3次 |
| URL、状态、HTTP、次数、时间、快照、哈希 | 已完成（已尝试范围） | 历史5条尝试字段齐全；253,058条保持未采集 |
| 成功/不存在/受限/超时/解析失败/未采集 | 已完成（分类机制） | 状态约束和7项测试；真实只出现未采集与受限 |
| 200验证页不得成功 | 已完成 | 真实快照复判为受限，成功0 |
| 目标 ASIN 一致性 | 已完成（机制） | 多位置抽取、重定向核对；真实受限页没有ASIN证据 |
| 重定向、模板变化和失败证据 | 部分完成 | 模型和测试完成；真实仅有验证页，未观察真实重定向/模板变更 |
| 遵守规则、不绕过限制 | 已完成 | 因条款与 CAPTCHA 停止；未扩大请求 |
| 全部产品页面采集 | 未完成 | 253,058未采集，0个有效页；缺少明确授权/许可数据接口 |

## 10. 可复算命令

```powershell
Set-Location 'D:\amazon'

# 全量状态、请求口径、证据完整性和孤儿检查（只读）
docker compose exec -T postgres psql -U movie_app -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/queries/product_collection_final_validation.sql

# 分类测试
Get-Content -LiteralPath tests\test_product_page_collection.py -Raw | `
  docker compose exec -T airflow-scheduler sh -lc `
  "PYTHONPATH=/opt/airflow/project/src python - -v"

# 隔离数据库集成测试
Get-Content -LiteralPath tests\integration_product_page_collection.py -Raw | `
  docker compose exec -T airflow-scheduler sh -lc `
  "PYTHONPATH=/opt/airflow/project/src python -"

# DAG 导入检查
docker compose exec -T airflow-scheduler airflow dags list-import-errors --output json

# 历史真实响应文件复核
docker compose exec -T airflow-scheduler sha256sum `
  /opt/airflow/project/data/snapshots/stage3/amazon-0005019281-512ea675a0d08b0c.html
```

不要在没有明确授权时解除 `product_page_collection` 的暂停状态或扩大请求；不要把 Airflow `success`、单元测试通过或 UCSD 离线覆盖当作 Amazon 页面全量采集完成。

## 11. 尚未满足及客观原因

1. 253,058 个 ASIN 未发出页面请求；覆盖率不足。
2. 有效 Amazon 页面为 0，无法验证真实页面字段提取及其到产品/电影血缘的写入。
3. 没有真实 404、429、超时、重定向或模板变化样本；目前只有构造测试覆盖这些分支。
4. 没有 Amazon 对本课程大规模自动化商品页采集的明确授权。
5. 历史站点已返回 CAPTCHA 验证页；按要求不能规避。

因此任务 2 的最终状态是：**部分完成**。工程实现和证据机制已补齐，真实页面覆盖这一核心结果未完成，且不能在缺少授权并已遇到访问限制时通过继续自动请求来伪造完成。

## 12. 真实页面/授权快照获取复核（2026-11-08）

本轮以“取得至少1份有效Amazon商品详情HTML或课程获准快照”为目标，但三类有效来源最终均为0，未修改`network_authorized=false`。

### 本地原始证据

- 已递归检查项目内HTML、MHTML、HAR、WARC、Scrapy缓存、历史快照和数据库响应记录。
- Scrapy缓存只有一份商品响应和一份`robots.txt`。商品响应以gzip保存，解压后3,781字节，SHA-256为`512ea675a0d08b0ce8cf484ed6c9f1c4e88acfeb9aae82335c84d3a1c943da69`，与既有HTML完全一致。
- 该响应目标为`0005019281`、HTTP 200，但包含`validatecaptcha`，没有页面内ASIN证据，也没有商品模板标记，仍为`access_restricted/verification_page`。
- 缓存的2026-11-03 `robots.txt`包含`User-agent: Scrapy`和`Disallow: /`。结合CAPTCHA证据，本轮没有新的Amazon请求或缓存重放。
- 逐文件复核结果保存在`data/work/product_collection/local_page_audit.json`，SHA-256为`8d63aa332d33a2d31ce359afd490358e7072261bf199bfb7438852b2e7a3435a`。

### 公共归档

- 从100条候选中选取`B000060MVP`和`B004SEUJ0A`进行有界索引检查。
- Internet Archive CDX/availability接口在当前环境无法连接，未取得归档记录或正文。
- Common Crawl对`B000060MVP`的2026-43及2018-17精确`/dp/`路径返回无捕获；其他有界`/dp/`、`/gp/product/`查询出现502/504/超时。没有下载到Amazon原始HTML。
- 搜索引擎找到了`B004SEUJ0A`的第三方商品引用，但它不是Amazon原始响应。
- Common Crawl条款允许访问其服务，同时明确底层Crawled Content可能受原内容所有者的独立条款和权利约束。因此即使索引命中，也不能仅据“可检索”视为课程获准快照。本轮归档导入为0。

### 官方或机构渠道

- `.env`、配置、文档和工作区中没有Amazon API凭据，也没有老师、课程方或机构提供的书面使用许可或原始快照。
- Amazon Creators API要求已正式接受的Associates账户、合格销售和专用凭据；许可用途以推广Amazon并导流为主，且返回结构化JSON而非原始HTML。
- SP-API需要卖家/供应商开发者注册和授权，返回结构化目录数据而非原始HTML。
- UCSD、SNAP、ESCI继续属于离线结构化数据，不作为Amazon页面或授权快照。

### 本轮结果

| 指标 | 新增数 |
|---|---:|
| 实时有效页面 | 0 |
| 从既有缓存/存档找回的有效页面 | 0 |
| 获准使用的历史快照 | 0 |
| 跨来源去重成功ASIN | 0 |
| 页面字段提取 | 0 |
| 数据库字段断言写入 | 0 |

数据库只读复核同时显示：`raw.source_record`中成功HTTP商品页为0，关联HTTP响应的`integration.field_assertion`为0。因此没有可据实写入的页面字段或血缘，本轮没有从最终值反推来源。

下一项可执行的授权措施是：**请老师或课程方提供一份目标ASIN对应的Amazon原始商品详情页HTML快照，并书面说明允许为本课程保存、解析和作为提交证据使用，以及允许分享给老师/助教的范围。** 最好同时提供原始URL、抓取时间、HTTP状态和重定向信息；缺少的元数据将按缺失保存。另一条可行路径是由Amazon或学校提供明确覆盖本课程用途、访问方式和保存范围的书面许可，之后才可将`network_authorized`设为`true`并对1个ASIN执行低频实测。
