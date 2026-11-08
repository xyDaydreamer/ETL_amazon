# 数据集成决策记录

## D001 实体边界

- `ProductEdition` 表示一个 Amazon ASIN 对应的商品/发行版本。
- `Movie` 表示创作作品，不能使用 ASIN 作为 movie_id。
- `Person` 表示可消歧的人物。
- `staging.review_record` 只保存稳定源评论身份；不可变原始载荷在 `raw.source_record`，版本化解析输出在 `staging.review_parse_result`。`LogicalReview` 仅表示经规则确认后的逻辑评论，两者通过 `ReviewMapping` 多对多关联。
- 合集商品通过 `ProductMovie` 关联多部作品。合集评论默认只属于商品；除非另有可定位证据，不把一条评论复制到每部作品。

## D002 稳定键与幂等

`source_review_id = SHA256(source_file_sha256 + ':' + record_number)`。源文件注册以 SHA-256 唯一。目标实体同时使用数据库代理键和唯一 `stable_key`；所有关系具有业务唯一约束。遇到同一稳定键但负载不同必须失败并记录冲突，不能 `DO NOTHING` 掩盖差异。

目标写入与 `meta.batch_progress` 更新必须处于同一 PostgreSQL 事务。Airflow 重试只重新执行同一逻辑批次，不提供数据层“恰好一次”。

## D003 评论解析与去重

按空行识别记录边界，按第一个字段前缀后的冒号切分，正文中的冒号和分隔符不再解释。UTF-8异常使用可定位的替换/隔离策略，原始字节定位不丢失。

去重先生成候选：同一 ASIN、用户、Unix时间、评分和规范化文本哈希相同。自动合并前比较全部原始字段；跨 ASIN 的相似文本默认不去重。所有原始记录永久保留。

阶段1实测helpfulness格式为 `helpful_votes/total_votes`（例如 `7/7`），解析器同时接受明确测试过的方括号变体。`0/0`不制造比例，保存为NULL。非法UTF-8记录进入隔离区，替换字符只用于可读展示；原字节通过记录定位、内容哈希及异常记录的Base64证据保留。

v2规则中ASIN是独立字段决策：可靠UTF-8、恰好10位大写字母/数字时可建立商品，即使评论因其他字段隔离；重复相同值告警但可用，重复不同值标记冲突且不选择。解析结果主键为`(source_review_id, parser_version)`；当前统计只通过`meta.parser_release.is_current`视图选择一个版本。

用户ID作为不透明字符串，不执行NFKC、大小写或空白折叠。显示名逐条保存在`staging.reviewer_profile_observation`；“latest”按评论UTC时间最大、再按记录号最大、最后按`source_review_id`最大确定，NULL时间最旧，因而重放顺序不改变结果。

## D010 gzip恢复与样本范围

- 阶段1只处理可配置有界样本，默认且最大1000条；`sample_complete`只表示该样本范围完成，不表示源文件全量完成。
- 恢复策略固定为 `scan_from_start`：从gzip开头重新解压、识别记录边界并跳过 `committed_record_number`。
- `field_locator`中的偏移是解压后流偏移。它可用于还原和校验原始记录，但不能当作压缩字节偏移直接随机恢复；恢复成本随已提交位置增长。
- 每批源记录、暂存评论、产品/用户关系、质量问题和检查点在同一事务提交。提交前中断整批回滚；提交后中断由相同稳定键和检查点安全重试。
- v2检查点唯一身份包含输入哈希、解析版本、分区键及固定逻辑范围。解析器升级使用新版本重算，不复制`raw.source_record`。
- v2 `end_line`表示最后一个内容行，不含空行分隔符；版本化定位保存在`staging.review_parse_locator`。字节范围为半开区间且同样不含分隔空行。

## D011 装载策略

正式v2默认先用PostgreSQL `COPY`写临时表，再做集合冲突检查和集合合并。1000/10000条首次装载均显著快于逐条SQL；逐条路径保留为诊断基线。正常恢复使用同一版本检查点跳过已提交前缀；人为使用新分区重复全量输入时，COPY仍保持幂等，但集合冲突检查成本不代表正常重试成本。

## D012 全量完成、发布与资源停止

- 全量入口不接受预计记录数作为停止条件，只在gzip流干净到达真实EOF后设置`reached_eof=true`。
- `verified_complete`还要求原始记录号从1连续到EOF，并且原始记录、稳定身份、指定解析版本、定位和ASIN字段观察逐项等量，无孤儿或缺口。
- 解析版本激活是独立事务；部分范围版本保持非当前。查询只通过显式单一`is_current`版本视图统计，不混合多个版本补洞。
- PostgreSQL卷位于Docker Desktop的WSL虚拟磁盘。阶段2运行中按官方设置把活动VHDX从Windows C盘迁至`D:\DockerDesktopData\DockerDesktopWSL\disk`，Airflow同步监控该宿主目录；低于配置阈值时在批次边界安全停止且不自动重试。以后改变磁盘位置必须同步更新Compose只读监控挂载。
- gzip恢复仍为从头解压并逐条核验至已提交检查点。宿主休眠造成Airflow心跳丢失时，已提交批次保持有效；恢复运行不得把Airflow任务状态当作业务完成证据。
- 重复且冲突的`reviewer_id`字段不选择第一个值：规范用户ID置NULL、保留原始字段并记录冲突。重复相同值可保留并告警。

## D004 日期语义

- `movie.release_date` 表示作品首次公开上映或明确地区/类型的上映事件；地区和上映类型必须伴随日期。
- `product_edition.product_release_date` 表示 DVD、Blu-ray、VHS 等商品发行日期。
- 仅有年份时保存 `release_year` 且 `release_precision='year'`，不补成1月1日。
- `earliest_review_time` 是评论事实。只有作品映射、非预售和时间可信均成立时，才可生成单独的 `inferred_release_bound` 断言；它不覆盖已核实上映日期。

## D005 电影匹配

普通文本基线先做：标题/别名标准化用于候选生成；验证至少组合年份、导演、主演/演员、片长、作品类型、外部ID中的多项事实。单独标题、单独年份或缺失年份都不足以自动合并。

输出分为 `merge`、`do_not_merge`、`manual_review`，保存规则版本、阈值、置信度和证据。聚类后检查簇内所有强冲突，禁止仅凭传递闭包把 A 与 C 合并。向量检索只在文本基线用真实独立评价集测量后再决定是否加入，且只能生成候选。

## D006 人物匹配

大小写、标点、中间名、省略和音译标准化只用于候选。确认合并需人物外部ID或联合参演作品、职业、年代等多项证据。同名和缩写默认保持分离并进入人工复核。

## D007 来源与字段选择

来源记录、候选断言、断言依赖和最终选择分开保存。标量字段按语义一致性、作品匹配可靠性、来源权威性、精度和采集时间选择；多值字段可以合并，但每个成员和角色都要单独有来源。无法可靠选择时保留冲突，不制造值。

## D008 来源角色

- SNAP 文件是评论和 ASIN 清单事实源。
- Amazon 页面是商品页面候选源；验证页、登录页或错误模板均为访问失败证据。
- UCSD 2018 元数据是商品候选证据，不能假定覆盖 SNAP 全集，也不能直接作为作品合并真值。
- IMDb 非商业表用于作品/人物候选与核验，不含 ASIN。`title.principals` 是主要演职员，不等同全部演员。
- Wikidata P5749 可提供少量 ASIN 外部ID证据，但官方属性标注预期覆盖不完整；本地样本查询为0条。
- TMDb 只有在学生取得API令牌、接受条款并记录快照版本后启用。

## D009 真实、预测与人工数据

所有记录标记 `decision_origin` 或 `quality_status`。`observed` 来自可定位数据；`algorithm_prediction` 只是算法输出；`human_label` 必须由学生依据证据确认。AI建议不能写成事实或人工标签。

## D013 阶段3首批商品与作品规则（`stage3-product-movie-v2`）

- UCSD商品元数据只按可靠ASIN连接到ProductEdition；来源ETag/Last-Modified、JSONL记录号、原始JSON和内容哈希必须保留。
- 去除VHS/DVD等载体词只用于作品候选标题，不改写商品原始标题或商品版本语义。
- `Movies`分类本身不足以确认单部电影。合集、剧集和非电影使用可定位词证据；不足时保持pending并进入算法复核队列。
- 第一条作品采用要求：商品与候选规范标题一致，两个外部来源年份一致，并且人物/导演事实可定位。外部摘要不含剧情时，低描述相似度只作观测，不误作冲突。
- IMDb建议响应中的principal names只记为`starring`候选关系，不声称全部演员；未提供nconst时人物保持候选身份，不能用于同名人物自动合并。
- HTTP 200验证页属于`access_restricted`。受限结果可完成有界采集任务，但不得计入页面采集成功率。

## D010 阶段4首批电影开发样本与候选判定

- 首批50条是规则开发样本，不是独立人工评价集；`sample_role`固定为`development`。后续评价集必须从本批之外抽取，人工事实不能由算法状态回填。
- 从阶段3 `movie` 基线中排除明显合集/多标题和已知非电影标题模式；要求可定位的商品标题、描述和电影语义证据，按固定分数及ASIN稳定排序，并按规范片名去重。
- 商品发行/上架日期不等于作品上映年份。商品标题中的四位数字可能属于片名，只能作为辅助或冲突线索，不单独构成强事实。
- 标题只用于生成IMDb候选。`rule_verified`至少要求近似精确标题和一项独立人物交叉证据：IMDb principal出现在UCSD原始描述中，或UCSD品牌与principal一致。只有标题相同必须为`needs_review`。
- IMDb suggestion响应是本批有界候选证据，不是官方完整TSV快照；其principal names没有角色完备性，也可能没有人物稳定ID。批量作品/人物整合必须固定官方TSV快照并重新核实导演、年份、类型和nconst。
- 选择重跑按ASIN幂等更新，不删除已被候选证据外键引用的开发样本。候选结果按`(asin, rule_version)`覆盖同规则计算结果，原始HTTP响应仍通过内容哈希保留和复用。

## D014 固定IMDb快照、人物身份与独立评价

- 阶段4现有50条在v3.1冻结：38条采用映射和11条复核队列不再用于调规则；1条商品排除仍保留商品、评论和证据。
- IMDb下载URL每日刷新，不是不可变版本。项目以获取日期、每文件SHA-256、HTTP响应头、完整gzip EOF和记录数共同定义本地固定快照；有界表头检查不能冒充完整验证。
- `tconst`和`nconst`是固定快照内的外部身份。suggestion中的姓名字符串仍只是候选，不得按姓名直接升级或合并到nconst。
- 人物姓名、别名和规范化只用于生成候选。人物采用需外部ID，或共同作品、职业/角色、年代等多项可定位证据；同名与缩写默认不自动合并。
- 正式电影/人物评价各自使用development之外的冻结pair清单。算法预测、人工标签和最终采用分表保存；盲评材料不显示预测、不预填答案。
- 人工只填同一实体、不同实体或无法确定及理由。程序负责review_item_id、来源位置、规则版本、哈希、标注时间和导入幂等。

## D015 IMDb选择性装载与人物关系

- 固定快照很大但当前采用作品范围小，因此先按scope顺序扫描五个gzip，只保存目标tconst及由crew/principals发现的nconst；不为方便查询而默认复制IMDb全量。
- scope按tconst去重扫描，商品成员另表保存，支持多个ASIN版本指向同一作品。
- gzip恢复只使用“从头解压并丢弃到已提交记录”的可靠策略；检查点不宣称压缩字节随机定位。
- `title.crew`用于导演/编剧来源；`title.principals`是主要署名。actor/actress/self记作cast，其他类别按原始category保留；不自动生成starring或“完整演员表”。
- 同一导演/编剧同时出现在crew与principals时生成一个关系事实和多条来源证据，crew为selected、principal为supporting。
- `name.basics.primaryName`不是逐作品credited-as；因此`credited_as`保持NULL，原始姓名单独保存在人物及关系证据。
- suggestion姓名候选只有在姓名一致、通过不同来源关联同一已确认tconst、且nconst候选唯一时才规则合并；否则进入人物复核。旧候选和旧关系不删除。

### D010.1 缓存候选池v2.1

- 对缓存池每个候选先计算标题、独特作品编号、作品类型和人物位置证据，再按“证据充分性→证据分→标题相似度→IMDb rank”排序。IMDb rank只作最后的辅助排序。
- `feature`归为电影；`TV movie`归为电视电影；`video`归为直接发行或家庭视频；`TV series`/`TV mini-series`归为电视剧；`short`归为短片。纪录片由商品首段摘要的明确`documentary/non-fiction`语义识别，因为suggestion的`q`是容器类型而不是完整类型/题材。UFC、knockout、fight/sports highlight等归为体育集锦，不作为电影作品。
- 商品类型只使用标题和第一段非空商品摘要。后续编辑评论中的“像一部TV movie”等比较语句不能改变商品类型。人物位置仍检查全部描述字段。
- `single_movie`允许feature、TV movie和video候选；明确电视电影、直接发行影片或电视剧商品要求对应类型；体育集锦与电影候选类型冲突。
- 同一规范人物即使同时出现在品牌和描述中也只计一个人物事实，但保留全部字段路径、字符起止和原始片段。
- 多个候选均满足标题、兼容类型和人物证据时状态为`needs_review_ambiguous`；可疑但证据不足为`needs_review_insufficient`；候选池全部存在硬冲突为`no_suitable_candidate`；空池为`no_candidate`。

### D010.2 v3全缓存证据规则

- 规则版本`stage4-first50-movie-match-v3-evidence`从缓存响应提取全部tt条目，按IMDb ID去重后逐项评估；展示截取只在评估之后。rank是最终辅助排序，不能消除实体歧义。
- 标题保留Unicode，分别保存完整规范标题及明确分隔出的主/副标题；阈值固定为T_strong 0.95、T_close 0.85。短标题/通用主标题不能只靠副标题差异进入strong。
- 人物必须按字段上下文分为explicit、brand、mention、角色/纪录对象和其他作品引用；同一人跨字段只计一个事实。suggestion姓名不是nconst或完整演职员事实。
- 商品发行日和标题数字不当作作品上映年。可靠作品年缺失不扣分；差1年保留语义复核；大于1年为硬冲突。
- 自动确认只允许外部ID，或标题与explicit人物，或标题+可靠年份+品牌，或strong标题+两位explicit，或strong标题+可靠年份+导演这五条路径。硬冲突优先于分数。
- 人工标签与预测分开：候选错误、池中无正确作品及商品非单作品分别使用candidate、pool及product作用域；标签追加并保留历史，不覆盖算法结果。
- `approved_development_movie_mapping`仅表示人工确认子集，不是统一采用入口，也不要求每个映射都人工批准。

### D010.3 v3.1提取修正与统一采用

- v3.1用新规则版本保存通用人物上下文和前置冠词修正，不覆盖v3。不得为单个ASIN写专用规则，也不降低B—E确认路径。
- 统一采用读取`unified_development_movie_mapping_decision`：明确人工正确候选优先；人工否定的候选禁止采用；`not_single_movie`和`no_match_in_pool`排除对应单电影映射；人工决定互相冲突时暂缓。
- 没有人工决定时，v3.1 `rule_verified`可以直接采用，来源为`rule_verified`，无需逐条人工批准。否定一个候选不自动确认另一个候选；另一个候选仍需人工正确或满足规则确认。
- 歧义、证据不足、无合适候选、空候选池及人工冲突保持暂缓。当前50条为人工采用4、规则采用34、排除1、暂缓11。

## D016 独立评价计分与姓名构成

- 冻结预测只在学生完成盲评答案后读取。`same_entity`为正类；人工`cannot_determine`和算法`needs_review`都不进入二分类混淆矩阵，分别报告数量。
- Precision、Recall、F1必须同时报告实际计分样本数和对人工明确标签的覆盖率，不能把算法弃权自动算成负类。
- 人物姓名描述采用NFKD、casefold、去组合重音、Unicode词切分和空白折叠。完整规范姓名的SequenceMatcher相似度`>=0.85`且不完全相同记为“拼写相近”。
- “只共享一个姓名词”“拼写相近”“规范全名相同”用于描述样本，类别允许重叠；这些派生类别不修改人工实体标签，也不构成人物合并事实。

## D017 跨商品版本采用与双评价口径

- 跨版本development案例不得只靠标题或别名。每个ASIN都必须有可定位的UCSD记录、与固定IMDb作品兼容的标题、至少一个该作品principal人物命中，以及可描述的商品版本/介质差异；所有原始冲突继续保存。
- `stage7-cross-product-version-v1`对每个ASIN分别记录ProductMovie决定，再由同一movie_id查询多个ProductEdition。重复运行更新同规则证据，不复制关系。正式评价集中的商品不复用为development案例。
- Chocolate的地区文字相互矛盾，因此不采用地区字段作为同一作品证明；合并依赖多项演职员和共享详细规格。The Crazies的同名记录分别对应1973原版与2010重拍版，保存`do_not_merge`，不能作为版本组。
- 电影主指标沿用选择性口径：排除人工无法确定和算法暂缓，同时报告63/95覆盖。另报告自动采用口径：人工无法确定排除，算法暂缓视作“未自动合并”；这用于衡量自动覆盖，不把暂缓改写为算法负类，也不改变冻结预测。

## D018 合集成员和评论归因

- 合集成员必须由商品原始描述明确列出，并以固定IMDb快照的标题、年份、作品类型和商品中出现的principal人物共同核实；标题相同本身不够。
- 合集建立一条ProductEdition到多条Movie的`collection_member`关系。评论仍只属于ProductEdition，不生成成员作品的逻辑评论副本。

## D019 逻辑评论精确去重

- 规则`logical-review-exact-v1`只合并同一ASIN下可靠用户ID、原始时间、评分、原始摘要和原始正文全部相等的有效v3记录；哈希只用于索引，冲突检查比较实际字段。
- 关键键缺失、内容存在任何差异或ASIN不同均保持独立。helpfulness不参与身份键；组内值不同则逻辑层置NULL并保留全部来源票数与冲突状态。
- 代表记录按最小record_number、再按source_review_id选择。每个有效来源在同一规则下恰好映射一次；只有full_verified版本可成为业务查询当前版本。
- 作品评分汇总只接受`single_work`关系。`target.product_movie_review_attribution`公开每条关系的商品评论数、实际归因数和排除理由，便于验收。
- 固定案例`B001PO54OM`包含The Cowboys、Fort Apache、Rio Bravo和The Searchers；商品383条评论，四个成员的归因总数为0。

