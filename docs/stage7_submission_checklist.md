# 阶段7提交清单与大文件边界

最终白名单提交目录：`submission/amazon_movies_etl_final_20261107/`。包级审计为171个文件、19,342,735字节、禁入文件0、疑似密钥0；`MANIFEST.sha256.csv`覆盖其余170个文件并逐项复算，哈希不一致0。2026-11-06目录保留为历史包，但已由本目录替代。

## 建议提交

- `README.md`、`docs/*.md`
- `src/amazon_movies/*.py`、`airflow/dags/*.py`
- `sql/001_core_schema.sql`至`sql/017_collection_review_semantics.sql`及`sql/queries/*.sql`
- `requirements/*`、`pyproject.toml`、`compose.yaml`、`compose.stage7-clean.yaml`
- `docker/`初始化与镜像定义、`scripts/`可复现脚本、可公开配置模板

## 不提交

- `.env`、Airflow本地密码文件、任何令牌/密钥
- `movies.txt.gz`、`data/raw/`、`data/work/`、`data/snapshots/`
- PostgreSQL/Docker VHDX、`backups/`、Airflow日志、数据库dump
- IMDb/UCSD原始第三方文件和Amazon响应正文

静态审计实测发现9个大文件，均属于上述排除范围；2个“疑似秘密”命中均为初始化脚本读取环境变量的变量名，不是值。当前目录没有`.git`元数据，最终打包应按本清单显式选择文件，不能假定`git status`已保护边界。

## 大文件登记

| 文件/位置 | 大小 | SHA-256/清单 | 恢复 |
|---|---:|---|---|
| `D:\amazon\movies.txt.gz` | 3,321,791,660 | `4b6ee09ea4880f3fb197b1407d45dfb08a892fbb48a9e7a2d74aa0572cdcc715` | 原文件只读；丢失时由课程提供渠道重新取得并复核哈希 |
| `data/raw/ucsd/meta_Movies_and_TV.json.gz` | 105,021,691 | `b7362b6c0bf92a6c47fbbb7855aea2bffd45042f6f10af73bc3afa3d8716c3e9` | 按README官方学术数据URL下载；同SHA才可复用检查点 |
| IMDb 2026-11-05五文件 | 1,936,171,501合计 | manifest SHA `431249a8c405cdf0b9a21404f66b077aa85a4751dcadf83d671507da0254cd4d` | 按`data/work/stage5/imdb-2026-11-05-full.json`逐文件核对；不重新下载现有快照 |
| Docker活动VHDX | `D:\DockerDesktopData\DockerDesktopWSL\disk\docker_data.vhdx` | 迁移清单见忽略目录备份 | 通过Docker Desktop官方Disk image location恢复，不手动拆卷/WAL |
| 迁移前备份VHDX | `backups/stage2-pre-migration-20261103-0840/` | 同目录MANIFEST | 只作灾难恢复，不提交 |

## 任务8追加材料

- 全量逻辑评论验收：`docs/task8_acceptance.md`。
- 完整状态和Airflow结果：`evidence/task8/full_status.json`。
- 可复算数据库查询输出：`evidence/task8/full_validation.txt`。
- 小样本、幂等与恢复证据：`evidence/task8/small_sample_acceptance.json`、`evidence/task8/small_sample_database_validation.txt`。
- 最终结果：7,849,544条有效源评论映射为7,835,611条逻辑评论；重复记录13,933条、重复组12,281组；缺失、重复和孤儿映射均为0。

## 最终字段血缘追加材料

- 全量状态：`evidence/lineage/full_status.json`（run10，31项，`completed/full_scope=true`）。
- 覆盖率与缺口：`coverage_summary.csv`、`gap_detail.csv`；78,641,193个非空值完整链同数，缺来源0，gap明细0。
- 修复与影响：`repair_history.csv`、`impact_demo.json`；相同修复批次重跑新增0，原/派生断言均保留。
- 可复算输出：`full_validation.txt`、`lineage_queries_revalidation.txt`、`business_queries_revalidation.txt`。
- 明确局限：2,010,354个值为空/pending；8,420个helpfulness冲突有来源但不强行选择；候选Movie库存不计入46部采用作品分母。

IMDb逐文件SHA：

- `title.basics.tsv.gz`: `10bcde40fee43a47adf9dec4534188f81af30b6fe3815f599da9091cae474ead`
- `title.akas.tsv.gz`: `03f8228e4f22b85bbc0cb1f1ecee63643cbd8e6388afb3696d876766d70cbd4a`
- `title.crew.tsv.gz`: `689a899de6e6f8fd1cbc514139b553a10779388c3a15d51e3879bc7ae5b60364`
- `title.principals.tsv.gz`: `ac1334fbe775606aa6bb3a5f696239a7362d7f247fb6942a6790b01180ed79f5`
- `name.basics.tsv.gz`: `eeff9e11fc02984cb52e9836bad414f0768fbff5b73476173374d6e1482a6406`

IMDb只按非商业许可用于课程研究，提交物保存链接、版本、哈希、定位和派生统计，不再分发原始TSV。UCSD和Amazon证据同样只提交来源说明、定位/哈希与必要短证据，不提交整份第三方内容。

## 证据目录

- 全量评论统计/异常/产品清单：`data/work/stage2/`
- 阶段4开发候选与历史规则：`data/work/stage4/`
- IMDb快照登记、恢复与development案例：`data/work/stage5/`
- 冻结评价集与盲评材料：`data/work/stage6/formal_evaluation/`
- 阶段7九类查询/质量/clean复现：`data/work/stage7/`

阶段7本机评价子目录还包含原始人物XLSX的同哈希归档，但原始XLSX不进入提交包。提交包保留盲评HTML/manifest、规范答案TSV、冻结预测—答案逐对表、导入验收、正式指标、姓名构成及误合并/漏合并，支持独立复算。development案例明细单独放在`evidence/cases/`，正式评价pair没有回流。

## 提交前仍须披露

- Amazon页面采集只有5次有界尝试且成功0；不得写成完成全量页面采集。
- UCSD离线元数据覆盖73,813/253,059（29.1683%），与实时页面成功率分开。
- 电影跨商品版本合并已核实5组；每组两个ASIN、同一固定快照tconst、商品/人物/介质证据及实际版本查询均已保存。The Crazies同名候选因原版/重拍版冲突明确拒绝，未用于凑数。
- 合集多对多已用`B001PO54OM`真实四片商品验证：4条`collection_member`关系、商品383条评论、成员归因0；证据JSON和迁移017应随提交材料保留。
- 目录没有`.git`元数据，但老师提交清单不要求Git历史，因此这不是提交缺口；最终包仍按显式白名单生成，不把`data/raw`、数据库、日志或密钥纳入。

这些目录用于本机答辩证据且被忽略；正式提交用文档中的汇总、哈希和重算命令，不包含大型数据本体。

