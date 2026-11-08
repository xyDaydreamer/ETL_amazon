# 最终提交包说明

最终提交目录由`scripts/build_submission.ps1`按显式白名单生成，不依赖Git历史。包内只保留课程验收所需的代码、DDL、文档、派生统计、盲评材料、规范人工答案、冻结预测及可重算证据。

## 包含内容

- `README.md`、`docs/`：报告、架构、规则、阶段验收、演示与恢复说明。
- `src/`、`airflow/dags/`、`sql/`、`tests/`：业务实现、DAG、迁移、查询与测试。
- Compose、Docker、依赖锁定、配置模板和可重放PowerShell脚本。
- `evidence/stage2/`：完整产品清单及异常汇总，不含评论原文。
- `evidence/evaluation/`：电影/人物各100对盲评材料、规范人工答案、冻结预测与标签逐对表、指标、姓名构成及错误案例。
- `evidence/cases/`：四类development案例、跨商品版本核对结果。
- `evidence/collection/`：真实合集`B001PO54OM`的4部作品关系与383条商品评论不复制验收结果。
- `evidence/task8/`：全量逻辑评论去重状态、完整性、评分前后口径、合集归因及小样本恢复证据。
- `evidence/lineage/`：31项最终字段全量覆盖率、0缺口明细、修复历史、影响分析和四类血缘/五类业务查询复验输出。
- `evidence/reproduction/`：九类查询、质量核对、DAG导入及隔离clean复现结果。

## 明确排除

- `.env`、密码、令牌、私钥、本机凭据。
- `movies.txt.gz`、IMDb/UCSD原始压缩文件、Amazon响应正文。
- PostgreSQL数据、Docker VHDX、数据库dump、WAL、备份和Airflow日志。
- 原始课件及第三方数据本体；提交包只给出版本、哈希、来源条件、记录定位和必要短证据。

## 覆盖口径

- Amazon实时页面：有界尝试5次、成功0次，保留失败证据；没有声称完成页面采集。
- UCSD离线元数据：203,766条扫描到EOF，覆盖73,813/253,059个产品（29.1683%）。该覆盖率不等同于实时页面成功率。
- 合集：`B001PO54OM`映射4部不同电影；383条评论保留在商品层，`collection_member`归因总数为0，不复制为1,532条电影评论。
- 逻辑评论：7,849,544条有效来源映射为7,835,611条逻辑评论，识别13,933条精确重复；合集商品383条源评论去重为382条商品逻辑评论，成员作品归因仍为0。

包根目录的`MANIFEST.sha256.csv`可用于逐文件校验；`PACKAGE_AUDIT.json`记录禁入文件和敏感信息扫描结果。

当前最终包为`submission/amazon_movies_etl_final_20261107_lineage_final/`，包含187个文件。`MANIFEST.sha256.csv`覆盖其余186个文件；独立复算不一致0，禁入文件0，疑似嵌入密钥0。任务8证据位于`evidence/task8/`，最终字段全量审计的8份证据位于`evidence/lineage/`。旧的`amazon_movies_etl_final_20261107/`和首次血缘构建目录仅作历史保留，不作为最终提交版本。
