# 数据源可行性记录

检查日期为 2026-11-02（Asia/Shanghai）。状态严格区分“可访问”“已取得样本”“已验证可用于匹配”。所有网络测试均为有界请求；没有启动全量下载或采集。

2026-11-03阶段3补充：UCSD来源实测ETag为`"64280fb-62be3295ab953"`、Last-Modified为`Fri, 17 Jan 2025 09:01:51 GMT`；有界6-ASIN样本在读取前20条后全部命中。随后固定105,021,691字节文件（SHA-256 `b7362b6c0bf92a6c47fbbb7855aea2bffd45042f6f10af73bc3afa3d8716c3e9`）真实读到gzip EOF：203,766条JSONL，与本地253,059个ASIN交集73,813，覆盖率29.1683%。IMDb `tt0078764` 建议JSON可访问并返回标题、1979、TV movie及principal names；IMDb普通作品页直连返回HTTP 202空体，因此没有假定页面HTML可采。Wikipedia支持摘要提供同年和导演Eric Till。Amazon单页Scrapy验证为HTTP 200验证页，状态`access_restricted`。

| 来源 | 实际访问 | 格式与规模 | 样本字段与真实结果 | 使用条件 | 当前结论 |
|---|---|---|---|---|---|
| Stanford SNAP Amazon movie reviews | 页面可访问；本地文件已完整读到gzip EOF | 自定义字段文本的 gzip；官方 7,911,684 评论、253,059 产品、889,176 用户 | v3全量得到7,911,684个可靠ASIN评论、253,059产品、889,176可靠用户；另保留12个缺少ASIN/用户的源内畸形块 | 引用 McAuley/Leskovec；原文件保持只读 | 评论与ASIN事实源；全量结果与物理畸形块分开统计 |
| Amazon 商品页 | `robots.txt` HTTP 200；真实 `/dp/B003AI2VGA` HTTP 200 | HTML；全量目标253,059 ASIN | 读取前3,607字节识别为验证/CAPTCHA页，不是商品页 | 遵守robots和站点规则；不得绕过验证、登录或封禁 | 可连接但本次页面访问受限；未取得可用商品样本 |
| UCSD Amazon Review Data 2018 Movies and TV metadata | HTTP 200 | gzip逐行JSON；响应 105,021,691 字节；页面声明203,970产品 | 前3条均含真实 `asin/title/category`；有界交叉验证命中 `6300147967`（Saving Grace VHS）、`6300157423`（派对教学VHS）、`6305508569`（四片合集）。字段可含 description、brand、date、tech1/tech2，但样本中日期和技术字段常空 | 页面要求引用 Ni、Li、McAuley 2019；属于后期数据快照，提交/分享须复核其条件 | 已验证可按ASIN补充商品候选与分类证据；未验证覆盖全部SNAP ASIN，不能直接当作品真值 |
| IMDb Non-Commercial Datasets | 四个文件均 HTTP 200 | 每日刷新、UTF-8 TSV gzip；样本响应大小：title.basics 229,581,474，title.crew 83,877,657，title.principals 789,511,688，name.basics 312,306,248 字节 | 实测表头和首行：title ID、标题、类型、年份、片长、类型；导演/编剧；主要演职员；人物姓名/职业/代表作 | 仅个人与非商业使用，可保存本地副本，受IMDb非商业许可和版权条款约束 | 已验证可用于作品/人物候选和事实核验；没有ASIN，principals也不是全部演员 |
| Wikidata | SPARQL HTTP 200 | 结构化知识图谱；P5749为ASIN，结构化数据CC0 | 对本地真实 `6300147967`、`6300157423`、`6305508569`、`B003AI2VGA` 查询返回0行 | 使用明确User-Agent、低并发、处理429；P5749官方标注预期覆盖不完整 | 可访问但未验证对本地样本可匹配；仅作稀疏辅助源 |
| TMDb API v3 | 文档可访问，数据API未调用 | JSON API，电影/电视/人物/图片 | 未取得样本：当前没有API令牌 | 注册账户、取得API key、接受条款并遵守限流 | 未尝试数据访问；暂不纳入事实流程 |
| Amazon Creators API / PA API | 官方文档可访问，API未调用 | 按ASIN GetItems等 | 未取得样本 | 需Amazon Associates资格、近30天10个合格销售、凭据；政策限制未经书面许可对Product Advertising Content做聚合/分析/提取/再利用 | 不适合作为本课程默认批量元数据源，除非学生另行确认资格和书面许可 |

## ASIN到IMDb的连接策略

不存在已验证的完整 `ASIN -> IMDb tconst` 映射。连接必须分两步并保留证据：

1. 从 Amazon 页面或 UCSD 元数据取得商品标题、类别、载体、描述、品牌/发行方及可能的技术字段，形成 ProductEdition 断言。Wikidata P5749命中时只增加一条候选证据。
2. 在 IMDb 的标题/别名表中生成候选，再用年份、导演、主要演职员、片长和作品类型联合验证。确认后的 `ProductMovie` 映射记录所有候选、采用/拒绝理由、规则版本和置信度。

商品标题不能直接等于电影作品；`6305508569` 的实测元数据明确是四部电影合集，必须产生多对多映射且评论仍只归商品。

## 主要阻塞点

- Amazon真实商品页当前返回验证页，不能用实时采集完成率作为既定事实。
- UCSD 2018集合只有203,970个 Movies and TV 产品，少于SNAP的253,059个产品，且不是同一时期/抽样口径；覆盖率必须在生成全量ASIN清单后实测。
- IMDb没有ASIN，作品连接质量取决于商品证据的字段覆盖和实体匹配评价。
- TMDb缺少令牌；Amazon官方API资格与使用限制不适合默认批量分析。
- 人工评价集尚未建立，现阶段不能选择自动合并阈值或宣称准确率。

## 官方资料

- SNAP：https://snap.stanford.edu/data/web-Movies.html
- UCSD 2018：https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/
- IMDb非商业数据：https://data.imdb.com/non-commercial-datasets/
- Wikidata P5749：https://www.wikidata.org/wiki/Property:P5749
- Wikidata数据访问：https://www.wikidata.org/wiki/Wikidata:Data_access
- TMDb API：https://developer.themoviedb.org/docs/getting-started
- Amazon Creators API：https://affiliate-program.amazon.com/creatorsapi/docs/en-us/introduction
- Amazon Associates政策：https://affiliate-program.amazon.com/help/operating/policies

