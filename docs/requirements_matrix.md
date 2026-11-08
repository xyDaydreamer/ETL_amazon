# 作业要求矩阵

本矩阵以课程作业说明为硬性验收依据。状态含义：`已验证`表示已有真实执行证据；`已设计`表示接口或模型已建立但尚未运行；`未开始`表示后续阶段工作。代码存在或单元测试通过不等同于作业完成。

| 编号 | 硬性要求与验收口径 | 实现位置 | 当前证据 | 状态与缺口 |
|---|---|---|---|---|
| R01 | 全量流式处理 `movies.txt.gz`，不得用样本冒充全量 | `review_parser_v2.py`；`meta.source_file`、版本化解析表 | 真实EOF、gzip完整性和7,911,696物理块已验证；五类覆盖关系等量 | 已验证；12个畸形块与官方评论口径分开报告 |
| R02 | 核对 7,911,684 评论、253,059 产品、889,176 用户 | `stage2_full_validation.sql`；阶段2验收 | 7,911,684可靠ASIN评论、253,059产品、889,176可靠用户均精确匹配 | 已验证；未为匹配数字删除12个畸形原始块 |
| R03 | 记录下载地址、时间、大小、哈希、解析版本和资源需求 | `meta.source_file`、阶段验收文档 | 3,321,791,660字节、SHA-256、v3、全量资源/WAL/耗时已记录 | 部分完成；现有文件原始取得时间仍需用户补充 |
| R04 | 稳定 `source_review_id`，保留记录序号、原文、异常 | `review_parser_v2.py`、`raw.source_record`、`staging.review_record`、`review_parse_result/locator` | 7,911,696原始记录/身份/v3结果/定位等量；Base64、偏移和哈希可查询 | 全量已验证 |
| R05 | 读取数 = 有效数 + 隔离数，逐项解释清洗/去重变化 | `meta.etl_run.stats`、`meta.quality_issue` | 7,911,696=7,849,544有效+62,152隔离；问题实例和受影响记录分报 | 全量已验证 |
| R06 | 为全部不同 ASIN 建立产品采集清单，以253,059为目标 | `staging.product_asin_observation`、`target.product_edition`、`staging.product_harvest` | 253,059个去重ASIN、253,059个唯一候选URL；62,140隔离评论仍贡献ASIN | 清单已验证；253,058未采集 |
| R07 | 页面清单记录原始/最终URL、状态、HTTP、次数、时间、快照和哈希 | `staging.product_harvest`、`product_harvest_attempt`、`raw.source_record`、`stage3_crawler.py` | 5条历史尝试的URL、HTTP、时间、快照和SHA均完整；1次网络响应、4次缓存重放；100个不同ASIN的无网络候选计划；2026-11-08全量本地页面类文件/压缩缓存复核未找回有效页 | 证据结构已验证；仅1个ASIN实际尝试，计划与归档索引不能冒充请求/授权快照 |
| R08 | 成功、不存在、受限、超时、解析失败、未采集等互斥；200验证页不能算成功 | 状态约束；`assess_product_response` | 真实HTTP 200验证页复判为`access_restricted`；7项分类测试通过；有效页还必须证明目标ASIN和商品模板 | 分类已验证；真实有效页0 |
| R09 | 遵守robots、限速、缓存、断点、有上限重试；不绕过封禁/登录/验证码 | `stage3_crawler.py`、`product_page_collection` DAG、迁移021/022 | 人为候选上限可配1—100并与速率限制分离；数据库原子领取、缓存、每域并发1、至少5秒延迟、显式有限重试、历史成功快照保护与幂等在隔离库通过；100条种子计划与Airflow无网络重放通过 | 采集器和候选计划已验证；因官方条款及验证页限制，新增网络请求0、未获授权 |
| R10 | 电影、非电影、合集、剧集、演唱会/其他、待确认分类并保留依据 | `classify_product`、商品元数据表、字段断言 | UCSD全量交集：collection 6,835、movie 433、non_movie 2,672、series 4,270、pending 59,603；重复ASIN分类冲突0 | 离线基线全量已验证；concert_or_other无命中且规则仍需评价 |
| R11 | 页面实时采集、离线快照、第三方补充分开统计；成功率分母为全部产品 | SourceRecord类型、产品采集最终审计 | UCSD离线覆盖73,813/253,059=29.1683%；Amazon实时不同ASIN尝试1/253,059、有效页0；5次逻辑执行中4次为缓存重放 | 分口径已验证；实时页面覆盖不足 |
| R12 | 商品ASIN、版本、电影作品、人物、原始评论、逻辑评论分别标识 | `target.*`、稳定身份与版本化解析表 | 首条ProductEdition→Movie→Person关系已实例化，未复制评论 | 评论/商品/首条作品人物已验证；逻辑评论后续 |
| R13 | 合集 Product-Movie 多对多；评论不复制到每部作品 | `target.product_movie`、`target.product_movie_review_attribution`、迁移017 | `B001PO54OM`真实四片合集映射到4个IMDb作品；商品383条评论、成员作品归因均为0；重复运行仍4条关系 | 已验证 |
| R14 | 原始值与规范值分开；NULL有缺失原因；多值可查询 | `staging.review_parse_result`、质量问题、`integration.field_assertion` | v2原始/规范字段分列；NUL/非有限/越界值实际落库测试；230条0/0比例为NULL | 评论样本已验证；集成字段后续 |
| R15 | 日期区分作品上映、地区上映、商品发售；年份不得补1月1日 | `docs/decisions.md`、Movie/ProductEdition字段 | 语义规则已记录 | 已设计 |
| R16 | “主演”不得等同全部演员或任意前三名 | `target.movie_person`、`movie_person_source_evidence` | 38作品范围形成cast 380/director 39/writer 74/other 316项事实；未把principals写成starring，重复来源只作supporting | 38条范围已验证 |
| R17 | 电影候选用标题/年份/导演/外部ID，合并需多项事实；保存阈值、版本和证据 | `stage4_v3.py`、`movie_match_candidate_result`、统一采用视图 | v3.1评估311个候选：36规则确认、1歧义、9证据不足、3无合适、1空池；原14条逐项审计，5条因通用上下文漏提取修复升级，旧v3保留 | 50条开发批次已验证；尚无独立评价集，未核实IMDb官方TSV |
| R18 | 检查传递一致性，保留合并历史和可纠错性 | `integration.merge_decision` | 决策历史接口已建模 | 已设计 |
| R19 | 展示至少5组跨版本合并、5组易混淆不合并 | `stage5_case_prep.py`、`stage7_cases.py`、阶段4缓存证据 | 跨版本5组、易混淆不合并5组均已核实；10个跨版本商品决定与1个Crazies拒绝决定幂等，版本查询每部返回2个ASIN、孤儿0 | 已验证；均为development案例，不进入正式指标 |
| R20 | 人物别名/同名消歧，保留外部ID、作品、职业和年代证据 | Person、Alias、MoviePerson、MergeDecision；nconst外部ID与人物候选 | 38作品建立742个nconst、809关系事实；姓名/角色/作品均有定位；5组合并和5组不合并development案例保留作品/职业/年代证据 | 已验证有界范围；IMDb principals仍不是完整演员表 |
| R21 | 展示至少5组人物别名合并、5组同名/近名不合并 | `person_match_candidate`、MergeDecision、`stage7_cases.py` | 2组已有规则合并+3组UCSD/IMDb署名变体；5组近名/同作品双人不合并，均有SourceRecord和理由 | 已完成5+5开发案例；不计入正式评价集 |
| R22 | IMDb等先确认作品匹配再补全；基础表不冒充完整日期或全部演员 | 固定快照、关系来源证据、字段断言 | tt0078764先确认tconst再补19个主要署名；明确principals非完整演员、primaryName非credited-as | 真实小样本已验证 |
| R23 | 最早评论只作条件性上界，推断值不得覆盖核实日期 | `docs/decisions.md`、断言依赖 | 规则已记录 | 已设计 |
| R24 | 来源冲突保留所有候选、采用/拒绝/待确认及选择理由 | FieldAssertion、FieldSelection、ReviewQueue | 作品标题/年份候选、依赖和选择已实例化；3条分类待确认及1条resolved历史可查 | 首批已验证 |
| R25 | 原始评论全部保留；逻辑评论通过映射去重；跨版本相似文本不自动删除 | `logical_review_dedupe.py`、迁移018、LogicalReview、ReviewMapping | 全量7,849,544条有效输入→7,835,611条逻辑评论，7,849,544条映射，13,933条重复；跨ASIN仍独立；完整性/孤儿均0 | 已验证；62,152条隔离记录保留但不进入逻辑层 |
| R26 | Airflow提交DAG依赖、参数、日志、失败分支和重试 | 各阶段DAG；`stage5_imdb_selective_integration.py` | Stage5真实证据模式连续3次success；38条scope由同一业务入口完成，run75五文件EOF | 阶段5入口及长scope已验证；长扫描本次由CLI执行 |
| R27 | DAG导入不读大文件、不联网、不查库；XCom仅小型元数据 | 所有DAG约束；阶段2 DAG | 最终导入错误0；I/O仅在任务函数；全量XCom仅小型完成统计 | 评论DAG已验证 |
| R28 | 原始、暂存、集成、目标层均保留源和批次 | PostgreSQL分层schema及IMDb选择性scope | run75保存2,960条范围内IMDb原始记录、38作品、742人物、921关系证据/809事实及894字段选择 | 38条选择性范围已验证 |
| R29 | 目标写入和恢复进度同事务；稳定键+冲突检查保证幂等 | 评论装载器、`imdb_selective_loader.py`、版本化检查点、唯一约束 | IMDb构造库实测提交前检查点0、提交后检查点2、内容冲突阻止、重复运行幂等 | 评论全量及IMDb选择性小样本已验证 |
| R30 | 同输入运行两次目标数不变；中断恢复不漏不重 | `stage1_acceptance.md`、`stage2_acceptance.md`、`task8_acceptance.md` | 评论装载全量恢复已通过；逻辑去重10,000条实测重跑新增0、提交前/后检查点2,000/3,000恢复一致，全量检查点7,911,696 | 已验证 |
| R31 | 规则/来源变化可定位受影响字段并局部重算 | 解析版本、候选规则版本、AssertionDependency、`final_source_record_impact` | source_record_id 8002472的隔离shadow重算定位7项影响；原断言88691与派生断言190460及依赖边并存，重复执行ID不变 | 已验证 |
| R32 | 字段级血缘含原始定位、候选、父子转换、最终选择及未采用候选 | SourceRecord、FieldAssertion、Selection、关系来源证据、ReviewQueue、迁移019 | 全量31项、78,641,193个非空值/关系成员完整链78,641,193，缺来源0；候选883部与采用46部分母分开 | 全量已验证 |
| R33 | 演示4类血缘查询：movie字段、ASIN/原始评论、源记录影响、person别名过程 | `stage7_lineage_queries.sql`、`source_record_impact`视图 | 四类实际返回；Resident Evil展示采用/人工拒绝候选，另有评论版本映射、UCSD反向影响、Eric Till姓名/导演来源 | 已验证 |
| R34 | 每个非空最终字段可追溯到来源或明确人工修订 | `lineage_field_catalog`、`lineage_audit_result/gap`、`relationship_source_link` | 全量Airflow审计run10完成；78,641,193/78,641,193非空值完整，gap 0；47条规则+4条人工ProductMovie均有来源 | 全量已验证；2,010,354项为空/pending而非缺血缘 |
| R35 | 电影/人物各至少100对独立人工评价，开发与评价隔离 | `stage6_evaluation.py`、`evaluation.*`、排除清单、盲评HTML/TSV | 两套v2各100条均导入；人物首次100/重复0，电影/人物均100 label+100 prediction、无冲突 | 已验证；两套状态均`evaluated` |
| R36 | 报告 Precision/Recall/F1、分母零处理、错误案例、复核率、待确认率 | `evaluation_reporting.py`、`stage7_evaluation_results.md` | 电影TP27/FP5/FN6/TN25、F1 0.83077、计分63；人物48/1/1/48、F1 0.97959、计分98；无法确定/暂缓单列，13条错误可重算 | 已验证；电影选择性覆盖仅66.32%需一同解释 |
| R37 | 固定离线验收样本含跨版本、别名、非电影、合集、缺失日期、冲突、采集失败 | IMDb快照、阶段6分层候选与后续评价清单 | 5文件快照、跨版本、别名、非电影、真实四片合集、缺失日期、冲突及5次采集失败均有固定示例和查询入口 | 已验证；页面示例均失败/受限，不代表成功覆盖 |
| R38 | 至少5个可执行业务查询并解释评论统计口径 | `stage7_business_queries.sql`、`task8_full_validation.sql`、product/movie review summary视图 | 五类均实际返回；激活v3基础上再使用唯一激活逻辑去重版本；同时返回source/logical计数与评分，合集成员归因0 | 已验证 |
| R39 | 提交ER图、DDL、约束、索引、字段字典 | `stage7_data_dictionary.md`、SQL迁移001—017 | Mermaid ER图/字典已整理；隔离新卷实际自动创建014—017对象，包括合集评论归因视图 | 已验证；完整列级字典仍以DDL为准 |
| R40 | 提交报告、实现、全量统计/清单/异常/决策/血缘/评价集、复现与恢复证据 | `stage7_report.md`、`stage7_acceptance.md`、`task8_acceptance.md`、`final_field_lineage_audit.md` | 报告、Airflow/恢复、逻辑评论与字段血缘全量验收、双评价逐对复算材料和四类各5组案例已整理并进入白名单包 | 页面全量采集仍未完成并已明确披露；Git历史不是老师提交项 |
| R41 | 不提交密码、令牌、大型数据和数据库；第三方内容遵守条件 | `.gitignore`、`.env.example`、`stage7_submission_audit.ps1`、`build_submission.ps1` | 血缘版白名单包187文件；禁入文件0、疑似密钥0，186项清单哈希复算不一致0 | 已验证；第三方只提交定位、哈希、派生统计及必要短证据 |
| R42 | AI只辅助编程/建议，不作事实源或冒充人工标签 | `docs/ai_usage.md` | 使用与人工复核方式已记录 | 持续要求 |
| R43 | 扩大433个已识别电影商品的离线作品集成覆盖，逐ASIN互斥对账并保留来源链 | `p2_movie_expansion.py`、迁移020、P2 DAG及验收SQL | 固定433范围实际对账为既有41、新增6、待复核216、明确不匹配30、无候选140；采用47 ASIN/46作品，6条新增均有关系来源，冲突和孤儿0，合集评论复制0 | 有界验收通过；48条规则确认项仍未应用，未启动新IMDb扫描，提交包尚未据此重建 |

## 评分风险优先级

字段级血缘、评论全量、产品清单、电影/人物独立评价和四类案例均已形成可复算证据。剩余主要局限是Amazon实时页面仅有5次有界尝试且成功0、UCSD离线覆盖29.1683%、商品版本字段大量未采集；真实失败与缺值必须继续披露，不能由离线来源或推断值冒充。

