# Amazon电影评论与多源电影信息集成

本仓库从零开始实现课程作业。作业说明是验收依据，两份课件用于架构和方法选择。阶段0—7已完成环境、全量评论/产品清单、UCSD离线商品元数据、IMDb固定快照选择性集成、电影/人物独立评价、四类各5组案例、真实合集评论归因以及血缘/业务查询验收。产品页面采集器已完成分类、证据、数据库领取、恢复和幂等缺陷修复，但 Amazon 页面仍只有历史有界尝试且真实有效页为0；不能据此宣布实时页面全量采集完成。最终审计见 [PRODUCT_COLLECTION_FINAL_AUDIT.md](PRODUCT_COLLECTION_FINAL_AUDIT.md)。

## 阶段0真实结论

- 原始文件：`D:\amazon\movies.txt.gz`，3,321,791,660 字节，SHA-256 为 `4B6EE09EA4880F3FB197B1407D45DFB08A892FBB48A9E7A2D74AA0572CDCC715`。文件保持原位且未修改。
- 有界读取成功：前5条记录均可按字段前缀和空行边界解压读取；首个 ASIN 为 `B003AI2VGA`。
- UCSD 2018 Movies and TV 元数据端点可访问，响应为 105,021,691 字节的 gzip。流式样本含 `asin`、`title`、`category`、`description`、`brand` 等字段。它与本地前50个真实 ASIN 存在交集，但字段稀疏且数据时期、产品集合与 SNAP 文件不同。
- Amazon `robots.txt` 可访问。普通 `/dp/<ASIN>` 路径未被通用规则整体禁止，但对一个真实 ASIN 的单次普通请求得到 HTTP 200 验证页，因此本次结果是“访问受限”，不是页面采集成功；未尝试绕过。
- IMDb 非商业 TSV 可访问并每日刷新。样本验证了作品、年份、类型、导演、主要演职员和人物表；这些表没有 ASIN。
- Wikidata 可访问且 P5749 表示 ASIN，但对4个本地真实 ASIN的查询返回0条；只能作为稀疏的辅助候选源。
- Windows 主机有 15.26 GiB 内存，D盘可用约625 GiB。现已安装WSL 3.0.3、WSL2 Ubuntu 26.04.1、Docker Desktop 4.97.0（Engine 29.9.2、Compose 5.6.1），并启用Ubuntu集成。Airflow与PostgreSQL已完成阶段0运行验收；本机PATH中的Windows Python仍不是项目目标Python，容器使用官方Python 3.12镜像。

详细证据见 [阶段0验收记录](docs/stage0_acceptance.md)、[要求矩阵](docs/requirements_matrix.md) 和 [数据源可行性](docs/source_feasibility.md)。

## 技术栈与固定版本

- Python 3.12
- Apache Airflow 3.3.2，官方 Python 3.12 镜像
- apache-airflow-providers-postgres 7.0.2（与Airflow 3.3.2官方Python 3.12约束一致）
- PostgreSQL 18.6
- Scrapy 2.19.0
- psycopg 3.3.5
- RapidFuzz 3.14.6

Airflow 3.3.2 官方支持 Python 3.10 至 3.14 和 PostgreSQL 14 至 18；Scrapy 2.19.0 要求 Python 3.10 以上。直接依赖固定在 `requirements/`，Airflow 镜像构建还使用官方版本约束文件固定传递依赖。

## Windows环境准备

在管理员 PowerShell 中执行：

```powershell
wsl --install -d Ubuntu
wsl --update
```

按提示重启，然后确认：

```powershell
wsl --version
wsl -l -v
```

安装 Docker Desktop，启用“Use the WSL 2 based engine”，并在 Settings > Resources > WSL Integration 中启用 Ubuntu。建议为 Docker/WSL 保留至少 6 GiB 内存；本机总内存 15.26 GiB，运行前应关闭高占用应用，使可用内存明显高于当前实测的 3.12 GiB。

确认 Docker：

```powershell
docker version
docker compose version
```

Python 业务命令可在 WSL Ubuntu 内单独建立环境，不必等待 Airflow：

```bash
sudo apt update
sudo apt install -y python3.12 python3.12-venv
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements/business.txt
PYTHONPATH=src python -m amazon_movies.cli doctor --data-file /mnt/d/amazon/movies.txt.gz
```

## 最小服务启动与恢复

```powershell
.\scripts\init_env.ps1
docker compose build airflow-init
docker compose up -d postgres
docker compose up --no-build airflow-init
docker compose up -d airflow-api-server airflow-scheduler airflow-dag-processor
docker compose ps
```

`init_env.ps1`只在 `.env` 不存在时生成本地随机密码和密钥，并且不显示其值。若Docker Hub访问需要本机代理，先在Docker Desktop的Proxies中配置实际代理；命令行构建仍超时时，可仅在当前PowerShell会话设置 `HTTP_PROXY`、`HTTPS_PROXY` 和 `NO_PROXY` 后重试，不要把个人代理配置提交Git。

可在PowerShell中生成密钥并手动填入 `.env`：

```powershell
$bytes = New-Object byte[] 32
[Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
[Convert]::ToHexString($bytes).ToLower() # API secret；重新执行一次得到JWT secret
[Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
[Convert]::ToBase64String($bytes).Replace('+','-').Replace('/','_') # Fernet key
```

Web UI：`http://localhost:8080`。本地管理员密码保存在被Git忽略的 `airflow/logs/simple_auth_manager_passwords.json.generated`；只在本机查看，不要粘贴到文档或提交。触发 `stage0_smoke` DAG 后检查：

```powershell
docker compose logs --no-log-prefix airflow-scheduler
docker compose logs airflow-api-server | Select-String "admin"
docker compose exec postgres psql -U movie_app -d amazon_movies -c "select current_database(), current_user;"
```

Simple Auth Manager中的 `admin:admin` 表示用户名和角色，不是用户名和密码。该认证方式只用于本地课程环境；生产部署必须换用正式认证管理器。

恢复原则：先解决失败原因，再清除失败任务实例或重新触发同一 `batch_id`。后续数据任务必须在同一个业务数据库事务中提交目标写入和 `meta.batch_progress`，Airflow 重试只负责重新调度，不承担幂等保证。

## 阶段1样本解析与装载

以下命令只读取真实前1000条，不验证整个gzip完整性：

```powershell
$sha = '4B6EE09EA4880F3FB197B1407D45DFB08A892FBB48A9E7A2D74AA0572CDCC715'

docker compose run --rm --no-deps airflow-init `
  python -m amazon_movies.cli parse-sample `
  --data-file /opt/airflow/data/movies.txt.gz `
  --source-sha256 $sha --limit 1000 `
  --parser-version snap-movies-review-v2

docker compose run --rm --no-deps airflow-init `
  python -m amazon_movies.cli load-sample `
  --data-file /opt/airflow/data/movies.txt.gz `
  --source-uri file:///D:/amazon/movies.txt.gz `
  --source-sha256 $sha `
  --batch-id stage1-manual-sample `
  --partition-key real-sample-1-1000 `
  --limit 1000 --batch-size 500 `
  --parser-version snap-movies-review-v2 --load-strategy copy
```

实际阶段1业务样本结果为1000条读取、993条有效、7条隔离；7条均因非法UTF-8，但都有独立可用ASIN。业务库保留1000条v1和1000条v2解析历史，共享1000条不可变原始记录；当前视图只返回v2的1000条。原字节可由Base64或定位+哈希证据恢复。`0/0` helpfulness共230条，比例保持NULL。该结果只是样本，不是源文件全量完成状态。

v2检查点包含解析版本和逻辑范围；升级规则不会被v1检查点跳过。正式默认使用COPY临时表加集合合并，逐条SQL路径可用`--load-strategy row`复验。用户ID按原始不透明值识别，显示名“latest”由评论时间、记录号和稳定ID确定，不依赖装载顺序。迁移说明见 [数据库迁移](docs/migrations.md)。

Airflow DAG为 `stage1_review_sample_ingest`，`schedule=None`、`catchup=False`、`max_active_runs=1`。默认上限1000，XCom只返回路径、批次ID和小型计数。恢复采用 `scan_from_start`：重新打开gzip并从头解析到已提交记录后继续；保存的是解压流偏移，不是可直接跳转的压缩字节偏移。

详细实证见 [阶段1验收记录](docs/stage1_acceptance.md)。

## 阶段2全量评论与产品清单

当前解析版本为`snap-movies-review-v3`。全量入口持续读取到真实gzip EOF，不以官方预计条数停止；完成后才核对原始记录、稳定身份、解析结果、定位和ASIN观察并激活版本。

最终结果：7,911,696个物理空行分隔块，其中7,849,544有效、62,152隔离，分类平衡。源内有12个缺少productId/userId/profileName的畸形块；其余7,911,684条都有可靠ASIN，与官方评论口径一致。不同ASIN 253,059、不同可靠用户ID 889,176，均与官方参考一致。全量产品采集状态全部为`not_attempted`，本阶段没有发起页面请求。

手动全量命令（正常情况下由同一业务模块的Airflow DAG调用）：

```powershell
$sha = '4B6EE09EA4880F3FB197B1407D45DFB08A892FBB48A9E7A2D74AA0572CDCC715'

docker compose run --rm --no-deps airflow-init `
  python -m amazon_movies.cli load-full `
  --data-file /opt/airflow/data/movies.txt.gz `
  --source-uri file:///D:/amazon/movies.txt.gz `
  --source-sha256 $sha `
  --batch-id stage2-full-v3 `
  --partition-key full-source-v3 `
  --batch-size 5000 --load-strategy copy `
  --monitor-path /opt/airflow/docker-disk-host `
  --min-free-bytes 32212254720 --monitor-interval-batches 10
```

Airflow DAG为`stage2_review_full_ingest`，`schedule=None`、`catchup=False`、`max_active_runs=1`。业务任务自身不自动重试，避免低磁盘停止循环写入；恢复依赖事务检查点和稳定键。失败后先检查`meta.batch_progress`、磁盘和任务日志，再复用相同`batch_id`恢复。

Docker Desktop活动磁盘已通过官方设置迁至`D:\DockerDesktopData\DockerDesktopWSL\disk\docker_data.vhdx`。Compose把该目录只读挂入Airflow作为宿主磁盘监控路径；若再次迁移Docker磁盘，必须同步修改`compose.yaml`并重建Airflow服务。迁移前整盘备份及清单位于被Git忽略的`backups/stage2-pre-migration-20261103-0840/`。

全量核对：

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/queries/stage2_full_validation.sql

docker compose exec -T airflow-scheduler airflow dags list-import-errors
docker compose ps
```

可重算查询、性能、故障恢复、Docker迁移、导出物哈希和未验证事项见[阶段2验收记录](docs/stage2_acceptance.md)。

## 阶段3首批商品与电影集成

阶段3有界配置在`config/stage3_sample.json`。实际UCSD流只读前20条即命中6/6目标ASIN；IMDb候选与支持来源确认了首条 `0005019281 -> tt0078764 -> An American Christmas Carol (1979) -> Henry Winkler/David Wayne` 链路。另有3个商品因证据不足进入开放复核队列。该100%离线覆盖率只属于选定样本，不代表全量产品。

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage3-offline-sample `
  --config /opt/airflow/project/config/stage3_sample.json `
  --batch-id stage3-manual-offline --max-records 100

docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage3-imdb-verify `
  --config /opt/airflow/project/config/stage3_sample.json `
  --batch-id stage3-manual-candidate

docker compose exec -T airflow-scheduler airflow dags trigger stage3_product_movie_sample `
  --run-id manual__stage3_sample_manual `
  --conf '{"batch_id":"stage3-airflow-manual"}'

docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run `
  stage3_product_movie_sample manual__stage3_sample_manual
```

预期：离线命中6、候选verified 1、页面 `access_restricted` 1；重跑不会增加离线原始记录、商品元数据、同版本候选、Product-Movie或Movie-Person关系。日志在`airflow/logs/dag_id=stage3_product_movie_sample/`。恢复时换一个Airflow run id，但可复用同一业务batch id；稳定键和唯一约束保证实体幂等。

UCSD全量入口已经由用户手动执行完成。固定文件SHA-256为`b7362b6c0bf92a6c47fbbb7855aea2bffd45042f6f10af73bc3afa3d8716c3e9`；实际203,766条JSONL干净到达EOF，匹配73,813/253,059个ASIN（29.1683%）。以下保留可恢复复现命令：

```powershell
$url = 'https://mcauleylab.ucsd.edu/public_datasets/data/amazon_v2/metaFiles2/meta_Movies_and_TV.json.gz'
$dest = 'D:\amazon\data\raw\ucsd\meta_Movies_and_TV.json.gz'
New-Item -ItemType Directory -Force (Split-Path $dest) | Out-Null
Invoke-WebRequest -Uri $url -OutFile $dest
$metaSha = (Get-FileHash $dest -Algorithm SHA256).Hash.ToLower()

docker compose up -d --force-recreate airflow-api-server airflow-scheduler airflow-dag-processor
docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage3-offline-full `
  --metadata-file /opt/airflow/project/data/raw/ucsd/meta_Movies_and_TV.json.gz `
  --product-list /opt/airflow/project/data/work/stage2/product_list.tsv `
  --source-sha256 $metaSha --batch-id stage3-ucsd-full-v1 `
  --batch-size 5000 --partition-key ucsd-movies-tv-full-v1
```

程序先复算文件SHA-256，再读到真实gzip EOF；预期输出包含实际`source_records`、`matched_asins`、`target_asins=253059`和`reached_eof=true`，不以UCSD网页声明的203,970作为停止条件。进度查询：

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_movies -c `
  "SELECT * FROM meta.product_metadata_progress WHERE partition_key='ucsd-movies-tv-full-v1';"
```

失败后复用完全相同的文件、SHA、partition key和命令。检查点与本批写入同事务；恢复会从gzip开头扫描到`committed_record_number`后继续，不能把压缩字节偏移当随机恢复点。真实全量完成前不得把小样本覆盖率写成全量覆盖。IMDb完整TSV仍需另行固定快照和实现批量候选入口。

### 产品页面采集

`stage3_crawler.py` 保留原模块名并已修复：HTTP 200 不再等于成功；有效页必须同时具有商品页结构和目标 ASIN 证据；验证页、404/软404、403、429、超时、网络错误、重定向 ASIN 不一致和解析失败分别记录。响应正文按 SHA-256 保存，重定向链、分类证据、缓存命中和实际请求序号写入数据库。`meta.product_harvest_batch` 与 `staging.product_harvest_work_item` 使用数据库原子领取并支持失败后恢复；成功快照不会被后续失败覆盖。

独立 DAG 为 `product_page_collection`，默认保持暂停。开发期“最多10个ASIN”的人为限制已改为可配置的候选上限1—100；空`asins`时从253,059条清单按`seeded-md5-unattempted-v1`和固定随机种子确定性选择，并排除已有HTTP尝试及其他活动计划中的ASIN。候选上限不等于请求数。选择方法、种子、排名和访问依据写入数据库；`network_authorized=false`时只登记`prepared`计划，不启动Scrapy。请求侧仍保持每域并发1、至少5秒间隔、最多3次显式请求、24小时缓存和robots遵循。原有`stage3_product_movie_sample`也默认禁止页面网络任务，只有明确授权后才可显式开启。

2026-11-07实际生成批次`product-page-sample100-plan-20261107`：100个不同且未曾请求的ASIN，随机种子20261107，100/100工作项请求次数为0；Airflow以`network_authorized=false`重放成功后已恢复暂停。该绿色运行只证明候选计划与幂等，不代表采集成功。配置快照见`config/product_collection.json`，结构迁移见`sql/022_product_collection_sampling.sql`。

截至2026-11-07，清单为253,059，`not_attempted=253,058`、`access_restricted=1`、真实有效 Amazon 页面为0。历史5次逻辑执行针对同一个ASIN：1次已保存网络响应、4次缓存重放、不是5个不同页面，也没有重试请求。HTTP 200响应是验证码页，修复后的分类器仍判为`access_restricted`。Amazon当前公开条款限制未经许可的数据挖掘/机器人采集，加之站点已返回验证页，因此未新增商品页请求、未绕过限制，也未启动253,059页采集。UCSD离线覆盖73,813/253,059（29.1683%）始终单独统计，不算实时Amazon成功。完整证据与未满足项见 [产品采集最终审计](PRODUCT_COLLECTION_FINAL_AUDIT.md)。

2026-11-08又对本地Scrapy压缩缓存、全部页面类文件、公共归档索引和官方API资格做了有界复核。本地唯一商品缓存解压后仍是同一CAPTCHA页；公共归档没有取得可导入原始HTML，且归档可检索性本身不授予课程提交权；项目没有Creators API/SP-API凭据或课程授权快照。因此新增实时页、缓存找回有效页、获准历史快照及页面字段断言均为0，网络开关保持关闭。证据见`data/work/product_collection/local_page_audit.json`和`source_acquisition_audit.json`。

后续真实单页和有界批次产生了四份经原始字节哈希、商品结构、目标ASIN及非空标题复核的有效Amazon HTML。`python -m amazon_movies.amazon_page_reprocess`可从已存快照幂等提取标题和ASIN字段候选，写入`integration.field_assertion`并链接原始`source_record_id`；首次实跑为4份页面、8项断言，重跑新增0。该入口不联网，不能把CAPTCHA或UCSD离线元数据记为Amazon有效页。学校页面采集许可由用户确认；Creators API仍需独立的Amazon Associates资格、美国站Partner Tag及专属凭据，当前项目未配置，故未调用API。

## 阶段4首批电影匹配开发样本

从阶段3的433个`movie` ASIN中按固定证据分数、明显合集/非电影排除词和规范标题去重，选出50个开发样本。IMDb标题只用于生成候选；只有标题近似精确一致并且UCSD描述/品牌与IMDb principal name形成独立交叉证据时，状态才为`rule_verified`。商品日期与电影上映年份严格分开，标题中的年份标记不能单独确认作品。

v1、v2、v2.1和v3均作为历史保留。当前规则`stage4-first50-movie-match-v3.1-evidence-extraction`修正通用人物上下文和前置冠词提取后，311个缓存候选得到36条规则确认、1条多候选歧义、9条证据不足、3条无合适候选、1条空池。学生通过聊天核查5个development商品并提交6项独立标签。统一采用逻辑优先人工决定，否则允许v3.1 `rule_verified`自动采用；当前人工采用4、规则采用34、排除1、暂缓11。最新版TSV为50行、38列，SHA-256 `15EAA38DA2113C823BA4CA5A62EDE2FF28A9E37BFBA8465FC59BBC166DB6A7E6`。

独立命令：

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage4-select `
  --batch-id stage4-first50-select-v1 --limit 50

docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage4-match `
  --batch-id stage4-first50-match-v1 --limit 50

docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage4-v3-reevaluate `
  --batch-id stage4-v3-final-cli --limit 50

docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage4-export `
  --rule-version stage4-first50-movie-match-v3.1-evidence-extraction `
  --output /opt/airflow/project/data/work/stage4/first50_movie_candidates.tsv

docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage4-import-labels `
  --input /opt/airflow/project/data/work/stage4/first50_movie_candidates.tsv `
  --import-batch stage4-v3-human-review-1

docker compose exec -T postgres psql -U postgres -d amazon_movies `
  -v ON_ERROR_STOP=1 -f /opt/project/sql/queries/stage4_first50_validation.sql
```

Airflow DAG为`stage4_first_movie_matching`，参数`match_limit`限1至50、手动触发、`catchup=False`、`max_active_runs=1`，只复用缓存而不发起网络请求。最终50条运行连续两次成功且内容摘要一致；日志位于`airflow/logs/dag_id=stage4_first_movie_matching/`。完整说明、失败修复和未验证项见[阶段4验收记录](docs/stage4_acceptance.md)。

当前v3.1的全部阈值、候选排序、确认路径、人工优先/规则自动采用边界见[阶段4匹配规则](docs/stage4_matching_rules.md)。原14条证据不足的逐项审计见[阶段4证据不足审计](docs/stage4_insufficient_audit.md)。默认导出、CLI、Airflow和验证SQL均显式使用v3.1；旧版本继续可查询。

若进入可重放的导演、演职员和人物ID核实，应由用户手动下载IMDb官方非商业TSV快照到D盘；当前第一批验收不要求执行。`curl --continue-at -`会在已有部分文件后续传：

```powershell
$snapshotDate = '2026-11-04'
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
    --output (Join-Path $imdbDir $name) `
    "https://datasets.imdbws.com/$name"
}
Get-ChildItem -LiteralPath $imdbDir -Filter '*.tsv.gz' |
  Get-FileHash -Algorithm SHA256 |
  Export-Csv -NoTypeInformation -Encoding UTF8 `
    (Join-Path $imdbDir 'sha256_manifest.csv')
Get-ChildItem -LiteralPath $imdbDir | Select-Object Name,Length,LastWriteTime
```

预期得到5个gzip文件和`sha256_manifest.csv`；下载过程中`curl`显示每个文件进度，失败后重跑同一命令会续传。只有文件齐全、哈希清单生成且逐个gzip可读后，才能登记快照版本并启动后续批处理。

阶段4的50条development现已冻结为38条采用映射、11条复核队列和1条单电影排除，不再用于继续调规则。IMDb固定快照、nconst人物集成和development之外的电影/人物独立评价集准备见[阶段5准备记录](docs/stage5_preparation.md)。新增CLI明确区分快检与完整gzip验证，并可导出不含程序预测、没有预填答案的HTML+TSV盲评材料。

IMDb 2026-11-05固定快照现已完整验证。`stage5-adopted-38-v1`真实扫描已完成：五文件均到EOF，作品38、nconst人物742、关系事实809，缺失0；与阶段4冻结的38条采用映射差集0。可重跑验收SQL为`sql/queries/stage5_adopted38_acceptance.sql`，详细口径见[阶段5验收记录](docs/stage5_acceptance.md)。开发案例与独立评价隔离材料见[阶段5案例与评价计划](docs/stage5_cases_and_evaluation_plan.md)；当时标签尚未产生，最终标签和指标见阶段7结果。

阶段6已完成development之外381个UCSD movie商品的共享IMDb标题/别名扫描：243商品有候选，缓存6,383条，shortlist为839对/837个tconst。837作品及8,780人物的五文件选择性证据已到真实EOF。修正版v2电影和人物独立评价各100对已冻结并导出盲评材料，development交集0、盲评页不显示预测；两套答案现均已在阶段7完成。详情见[阶段6验收记录](docs/stage6_acceptance.md)。

电影v2的100条学生人工标签为同一实体42、不同实体53、无法确定5。人物v2为49/49/2，XLSX按冻结编号校验100/100、首次导入100、重复导入0。两套数据集状态均为`evaluated`。正式指标与姓名样本构成见[阶段7评价结果](docs/stage7_evaluation_results.md)。

## 阶段7查询、报告与复现

阶段7新增四类血缘查询、五类业务查询、质量/孤儿核对、ER/字段字典、Airflow恢复手册、答辩演示和隔离clean Compose复现脚本；随后在人物盲评完成后导入答案并对两套冻结预测正式计分。人工`cannot_determine`和算法`needs_review`从二分类混淆矩阵中单列，不把弃权当负类。

评价与案例结果可重算：

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli evaluation-report `
  --dataset-key stage6-movie-eval-stage6-independent-candidates-v1-v2 `
  --dataset-key stage6-person-eval-stage6-independent-candidates-v1-v2 `
  --output-dir /opt/airflow/project/data/work/stage7/evaluation/results

docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage7-case-report `
  --stage4-tsv /opt/airflow/project/data/work/stage4/first50_movie_candidates.tsv `
  --stage5-case-dir /opt/airflow/project/data/work/stage5/case_materials `
  --output-dir /opt/airflow/project/data/work/stage7/case_inventory
```

电影选择性指标为TP27/FP5/FN6/TN25、Precision 0.84375、Recall 0.81818、F1 0.83077，实际计分63、覆盖明确标签63/95、算法暂缓34。暂缓视为未自动合并时为TP27/FP5/FN15/TN48、Recall 0.64286、F1 0.72973。人物为48/1/1/48、P/R/F1均0.97959，实际计分98、无法确定2。人物姓名中仅共享一个词26对（25对人工不同）、拼写相近3对（2对不同）、规范全名相同50对（只有1对不同）；因此同名不同人的结论仍受负例数量限制。四类development案例均已达到5组；跨版本查询和逐项证据见`data/work/stage7/case_inventory/`。

当前环境验证：

```powershell
Set-Location 'D:\amazon'
.\scripts\stage7_validate.ps1
```

默认自动选择一组已有movie/person/source记录，也可传`-MovieId`、`-FieldName`、`-Asin`、`-SourceReviewId`、`-SourceRecordId`和`-PersonId`。输出写入被Git忽略的`data/work/stage7/evidence/`。

隔离干净环境复现使用独立Compose项目、端口55432/18080和独立卷，不覆盖现有数据库：

```powershell
.\scripts\stage7_clean_reproduce.ps1
```

脚本验证完整迁移链、DAG导入、业务数据库连接和10条离线Airflow样本；默认只停止clean容器并保留卷，不执行`down -v`。阶段7详细状态见[阶段7验收记录](docs/stage7_acceptance.md)，课程报告见[阶段7报告](docs/stage7_report.md)，打包边界与大文件哈希见[提交清单](docs/stage7_submission_checklist.md)。

## 目录

```text
airflow/dags/        DAG，仅组织依赖、参数、重试和日志
config/              可提交的来源与规则模板
docker/              Airflow镜像与PostgreSQL初始化
docs/                要求矩阵、决策、AI使用和阶段证据
requirements/        固定的直接依赖
sql/                 分层业务数据库最小模型
src/amazon_movies/   可独立运行的Python业务模块
```

## 任务8逻辑评论去重全量验收

迁移018、保守精确去重模块、CLI、Airflow DAG和全量验收SQL已经完成。独立10,000条小样本的精确重复、跨ASIN保留、缺失键保留、票数冲突、幂等及提交前后恢复均通过；全量7,849,544条有效评论生成7,835,611条逻辑评论和7,849,544条映射，识别13,933条重复记录，版本已`full_verified/is_current`。运行与恢复命令见[任务8运行手册](docs/task8_preflight.md)，最终结果见[任务8验收](docs/task8_acceptance.md)。

## 最终字段血缘审计

迁移019、31项字段清单、可重入修复、覆盖率审计、反向影响测试和Airflow DAG均已验收。全量run `manual__final_lineage_full_20261107`覆盖253,059个商品、46部采用作品、880个人物、960条作品人物关系、51条商品作品关系、7,835,611条逻辑评论及7,849,544条来源映射；78,641,193个非空最终值/关系成员完整来源链覆盖率100%，缺来源0。2,010,354个缺值和8,420个helpfulness冲突按业务语义单列，不冒充已核实值。审计口径和真实修复见[字段血缘审计](docs/final_field_lineage_audit.md)，启动、进度、恢复与核对命令见[运行手册](docs/final_field_lineage_runbook.md)。

## 433个电影商品离线扩展

固定UCSD `movie`范围为433个去重ASIN。P2有界Airflow实跑后，41个ASIN为既有采用、6个为本轮新增采用、216个待复核、30个明确不匹配、140个无缓存候选，合计433；当前范围采用覆盖为47/433（10.8545%），对应46部去重作品。6条新增映射均有关系来源链接，活动映射冲突、孤儿及采用作品字段冲突均为0；真实合集评论仍没有复制给成员作品。还有48条规则确认项尚未应用，不应把有界结果描述成433个ASIN全部完成。完整证据、抽查案例以及逐批应用、恢复和核对命令见[P2有界验收](docs/p2_movie_expansion_acceptance.md)。

## 数据与Git边界

`movies.txt.gz`、网页响应、第三方快照、工作文件、数据库卷、导出物和密钥均被忽略。原始文件保持只读语义；后续只在数据库或 `data/work/` 保存定位、哈希和派生结果，不复制原始大文件。

