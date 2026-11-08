# 阶段0验收记录

验收时间：2026-11-02（Asia/Shanghai）。本记录只陈述实际执行结果。

## 修改内容

- 阅读1份DOCX作业说明、65页旧版PPT和49页新版PPTX。
- 两份课件均完整导出为临时PNG；对文本稀少的架构、粒度、元数据映射、血缘、幂等和恢复图页做了视觉核验。
- 建立完整要求矩阵、实体/日期/去重/匹配/来源决策、AI使用记录、来源可行性记录和最小数据模型。
- 创建固定依赖、配置模板、Git忽略规则、Docker Compose、两个逻辑隔离数据库的初始化脚本、最小业务DDL、独立Python doctor命令和Airflow smoke DAG。
- 未复制、解压或修改 `movies.txt.gz`；未进行全量评论处理、全量页面采集或第三方全量下载。

## 实际执行命令与结果

| 检查 | 执行摘要 | 真实结果 |
|---|---|---|
| 文件盘点 | `Get-ChildItem -Force` | 找到作业DOCX、两份课件和 `movies.txt.gz` |
| 作业说明读取 | 直接读取DOCX `word/document.xml`，按正文段落和表格提取 | 133个正文/表格块；验收条款已写入要求矩阵 |
| 旧课件读取 | PowerPoint COM以只读、隐藏方式打开并遍历文本框 | 65页；未修改原PPT |
| 新课件读取 | 读取PPTX中49个slide XML和备注 | 49页；未修改原PPTX |
| 原文件属性 | `Get-Item` | 3,321,791,660字节；路径 `D:\amazon\movies.txt.gz` |
| 原文件哈希 | `Get-FileHash -Algorithm SHA256` | `4B6EE09EA4880F3FB197B1407D45DFB08A892FBB48A9E7A2D74AA0572CDCC715`；6.33秒 |
| 原文件有界样本 | .NET `GZipStream`读取至5个空行记录边界 | 5条记录、45行；首个ASIN `B003AI2VGA`；字段前缀正常 |
| PowerShell/Git | 版本命令 | PowerShell 7.6.5；Git 2.56.0.windows.3 |
| Python | PATH命令及绝对路径复核 | `python`/`py`命中不可执行的WindowsApps占位符；`C:\Users\ASUS\AppData\Local\Python\bin\python.exe` 为Python 3.15.2，但不符合项目固定的3.12版本 |
| Docker | `docker version`、`docker compose version` | 命令不存在 |
| WSL | `wsl --version`、`wsl -l -v` | 退出码1，系统提示使用 `wsl.exe --install`；无可用发行版 |
| 内存 | Win32 `GlobalMemoryStatusEx` | 总15.26 GiB，可用3.12 GiB，负载79% |
| 磁盘 | `Get-PSDrive -PSProvider FileSystem` | C盘约151 GiB可用；D盘约625 GiB可用 |
| UCSD端点 | HTTP流式打开元数据gzip，读取3条 | HTTP 200，Content-Length 105,021,691；3条均含ASIN/标题/类别 |
| ASIN交叉验证 | 本地最多200,000行取前50个ASIN；远端最多20,000条匹配 | 本地19,855行取得50个ASIN；发现3个明确交集及商品标题/分类证据 |
| Amazon访问规则 | 普通GET `robots.txt` | HTTP 200；通用规则未整体禁止 `/dp/<ASIN>`，但禁止多个动态/敏感子路径 |
| Amazon商品页 | 普通GET一个真实ASIN，仅读前64 KiB | HTTP 200，实际读取3,607字节，识别为验证/CAPTCHA页；记录为访问受限 |
| IMDb样本 | 流式读取4个gzip TSV各表头和首行 | 均HTTP 200；确认作品、年份、类型、导演、主要演职员、人物字段；无ASIN |
| Wikidata样本 | 一次SPARQL查询4个真实ASIN | HTTP 200，0条结果 |
| 独立业务命令 | Python 3.15.2绝对路径运行 `amazon_movies.cli doctor --records 3` | 成功；读取3条/27行，8种字段前缀齐全，首个ASIN `B003AI2VGA` |
| Python静态语法 | `python -m compileall -q src airflow/dags` | 成功；仅证明语法可编译，不证明Airflow可导入或运行 |

### 环境补验（2026-11-02）

| 检查 | 执行摘要 | 真实结果 |
|---|---|---|
| WSL2与Ubuntu | `wsl --version`、Ubuntu内 `uname -a` | WSL 3.0.3；Ubuntu 26.04.1；Linux 6.18.54.1-microsoft-standard-WSL2；Ubuntu用户初始化完成 |
| Docker | `docker version`、`docker compose version`、Ubuntu内复核 | Docker Desktop 4.97.0；Engine 29.9.2；Compose 5.6.1；Windows与Ubuntu均能访问同一Linux Engine |
| Docker网络 | `docker run --rm hello-world`；BuildKit实际构建 | hello-world成功；设置当前会话代理后Airflow基础镜像认证、下载和构建成功 |
| 版本约束 | 读取Airflow 3.3.2 Python 3.12官方constraints并构建 | provider修正并固定为7.0.2；镜像内psycopg 3.3.5；构建退出码0 |
| 容器业务doctor | `docker compose run --rm --no-deps airflow-init python -m amazon_movies.cli doctor --records 3` | Python 3.12.14 / WSL2 Linux；只读3条、27行；文件大小3,321,791,660；首个ASIN `B003AI2VGA`；8种字段前缀齐全 |
| 本地密钥 | `scripts/init_env.ps1` | `.env`在不存在时生成；数据库密码、JWT/API密钥和Fernet key格式检查通过；值未输出且文件被Git忽略 |
| PostgreSQL | `docker compose up -d postgres`及初始化日志 | PostgreSQL 18.6健康；`amazon_movies`与`airflow`逻辑隔离；两个应用账户经TCP密码认证成功 |
| 业务DDL | 只读查询 `information_schema` | `meta/raw/staging/integration/target` 五个schema存在，当前21张核心表 |
| Airflow迁移 | `docker compose up --no-build airflow-init` | PostgreSQL迁移完成，退出码0 |
| Airflow健康 | `/api/v2/monitor/health`、`docker compose ps` | API server、scheduler、dag processor及元数据库均healthy；未部署且当前不需要triggerer |
| Airflow UI登录 | 学生在Windows浏览器访问 `http://localhost:8080`，使用轮换后的本地Simple Auth凭据登录 | 2026-11-02学生确认登录成功；密码未发送给AI、未写入文档 |
| DAG导入 | `airflow dags list`、`airflow dags list-import-errors` | `stage0_smoke`已导入；0个导入错误；实际并发限制为4 |
| 正常运行 | 手动触发 `manual__stage0_success_after_fix_20261102_2209` | DAG success；业务库任务success；日志确认batch参数、`amazon_movies`和`movie_app`；成功分支success、失败分支skipped |
| 失败与重试 | 手动触发 `manual__stage0_forced_failure_20261102_2211` | 业务库任务受控失败共3次尝试（初次+2次重试）；失败分支success；DAG按预期failed |
| 故障恢复 | 首次运行缺少Execution API地址，随后按官方Compose修复 | 首次运行真实记录3次连接失败；补入 `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` 并重建服务后下一独立运行成功 |

## 真实验收结果

阶段0的文档、环境、来源和架构目标已完成。原始文件已确认可读并取得哈希，但没有做全量解压校验或记录计数，因此不能宣布达到官方7,911,684条。无依赖业务doctor已实际成功。WSL2、Docker、Python 3.12 Airflow镜像、PostgreSQL初始化、DAG导入、成功任务日志、失败分支和重试均已有真实运行证据。

数据源方面，当前实际可用的默认组合是：SNAP评论文件作为评论/ASIN事实源；UCSD 2018元数据作为按ASIN的商品候选补充；IMDb非商业表作为作品和人物候选/核验源；Wikidata仅作稀疏辅助。Amazon实时页面在本次测试中受验证页阻挡，不能计为成功来源；TMDb和Amazon官方API均未取得可用凭据/样本。

## 未验证事项与剩余缺口

- DOCX以OOXML正文/表格完整读取但未渲染页面；两份课件已导出全部幻灯片PNG，并视觉检查了关键图片型页面，未逐页放大核对版式细节。阶段0要求矩阵以作业正文为准，不依赖课件版式。
- Windows主机没有单独安装项目目标Python 3.12；当前目标运行时由Airflow官方Python 3.12容器提供。业务CLI在宿主Python 3.15.2做过有界doctor，但尚未在独立WSL Python 3.12虚拟环境中安装全部业务依赖。
- Simple Auth Manager仅适用于本地开发；本地管理员密码已在首次日志暴露后立即轮换，新值没有写入文档。正式环境不能使用此认证方式。
- 未做全量评论统计、全量产品清单、全量UCSD覆盖率、任何批量Amazon页面采集。
- 未建立人工电影/人物评价集；未训练或选择阈值；没有任何准确率指标。
- 原始文件的下载URL与原始下载时间不能从文件本身恢复，需要学生确认。

## 需要学生完成的具体操作

1. 确认 `movies.txt.gz` 的原始下载URL和取得日期；若无法确认，明确记录“未知”，不要编造。
2. 阶段1开始前决定是否需要独立WSL Python 3.12业务虚拟环境；容器化业务命令已不受宿主Windows Python影响。
3. 从阶段1起亲自准备电影和人物人工评价集的证据与标签；AI只能生成候选，不能代替人工标注。

