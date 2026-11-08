# 阶段4电影匹配规则

实际生效规则版本：`stage4-first50-movie-match-v3.1-evidence-extraction`。旧v3和更早版本仍保存在`integration.movie_match_candidate_result`，主键为`(asin, rule_version)`。算法状态是预测，不是人工事实。

主要实现：

- `src/amazon_movies/stage4_matcher.py`：开发样本筛选、历史IMDb响应和TSV导出。
- `src/amazon_movies/stage4_v3.py`：v3/v3.1候选提取、证据分类、评分、决策、缓存重评和人工标签导入。
- `airflow/dags/stage4_first_movie_matching.py`：只读缓存的v3工作流。
- `sql/008_stage4_v3_evidence_and_labels.sql`：v3决策字段和独立人工标签表。
- `tests/test_stage4_v3.py`：v3规则测试。

## 1. 商品分类、开发样本和标题

开发样本选择仍使用`stage4_matcher.select_development_sample`和固定版本`stage4-first50-movie-match-v1`。它从阶段3 UCSD基线的433个`movie` ASIN中排除明显合集、已知非电影模式和空标题，要求描述、电影语义或品牌证据，按固定选择分和ASIN稳定排序，再按规范标题去重。共282条符合选择条件，取50条并固定为`sample_role='development'`。这50条不能作为后续独立评价集。

v3的`classify_product_type`只查看商品原始标题和第一段非空描述的前2500字符：

- 同时存在赛事/运动语境和`highlight/compilation/collection/multiple/over N/finishes/TKO`等多片段语境才为`sports_highlight`；仅有`Knockout`不成立。
- 明确合集、季/集、音乐录像、纪录片、电视电影、直接发行或当前作品film/movie措辞，依次归为`collection`、`television_series`、`music_video`、`documentary`、`television_movie`、`direct_release_film`、`movie`。
- 未发现上述标记为`unknown`，不反向补造电影类型。

`title_forms`保留原始标题，先做HTML反解和Unicode NFKC，只删除明确包装：方括号/圆括号内的DVD、Blu-ray、VHS、4K UHD、UHD、widescreen及明确special/collector edition，或标题末尾对应载体/版本词。普通词不删除。以冒号后空格或两侧有空格的横线识别主标题与副标题，保存原文和规范形式。

`_collapse_unicode`使用NFKC、`casefold`、Unicode字母数字保留及标点/空白折叠，非拉丁字符不会被删除。`SequenceMatcher(None, product_full, candidate_full).ratio()`固定比较完整规范标题并保存两个输入：

- `T_exact`：完整规范标题相等，或候选等于配置传入且有来源证明的别名。当前50条没有登记核实别名。
- `T_strong`：`T_exact`，或完整相似度`>=0.95`，或主标题完全相同且差异只在副标题。
- `T_close`：已为strong，或完整相似度`>=0.85`。
- 主标题少于8字符、只有一个词，或处于显式通用标题集合时，不能只凭主标题/副标题路径进入strong；该限制保存为`main_title_route_blocked`。完整标题本身达到0.95仍按阈值处理。

## 2. 候选池

v3不发起网络请求。`reevaluate_cached`读取v1保存的50个IMDb suggestion响应。`extract_all_candidates`遍历响应`d`数组全部项目，只保留`id`以`tt`开头的条目，按IMDb ID去重；重复ID合并全部原始变体和不同principal name，rank取已知最小值。先评估全部候选，之后才设置`evaluated_rank`和`shown_in_display_top`（当前展示上限`DISPLAY_LIMIT=10`）。数据库`candidate_pool`保存所有候选，不做评估前top-5截断。

当前50个缓存响应产生311个去重tt候选；每项保存标题、年份、类型、suggestion人物线索、原始变体、全部证据、冲突、分项得分、确认路径和排序。IMDb rank只在确认资格、硬冲突、证据分、标题等级和相似度完全不能区分时使用，最后再用IMDb ID稳定排序。

## 3. 年份、类型和编号

`year_evidence`只接受明确标记为可靠作品上映年份的商品侧证据。商品发行日期不传入该列表，标题四位数字也不自动解释为年份。缺失不扣分、不冲突；相同加10；相差1年记录`one_year_difference`但需其他来源解释；相差超过1年产生硬冲突。当前50条没有可靠商品侧作品年份，因此年份均不参与自动确认。

`type_evidence`返回`compatible`、`unknown`或`conflict`。IMDb suggestion的`feature`、`TV movie`、`video`、`TV series/mini-series`、`short`和`musicVideo`只映射为候选线索；尤其`video`是`video_unknown`，不能证明是电影。明确体育集锦、合集、电视剧或音乐视频不进入单电影确认；类型缺失或商品类型未知通常保留`unknown`。unknown不加分也不产生硬冲突。

`number_evidence`识别独立阿拉伯数字和I—X罗马数字；Death Race II与Death Race 2一致。3D、4K、8K、720p、1080p、2160p以及region/code/disc数字先排除。明确编号列表不同是硬冲突；不确定语义只应记录警告。UFC第8集与缓存第2/3/4集因此冲突。

## 4. 人物证据

IMDb suggestion的`s`只提供候选人物姓名线索，不生成nconst，也不声称完整演职员。`person_evidence`对每个候选姓名检查UCSD品牌和全部description元素：

- 使用NFKD去重音、casefold、标点/空白折叠及完整姓名边界；Ann Lee不会命中Joann Lee。保留原文、变体规则、字段路径、source_record_id、字符起止和前后最多100字符片段。
- 品牌必须完整等于候选姓名才为`brand_support`。
- `starring/stars/cast/directed by/director/主演/导演/饰演`上下文为`explicit_credit`；导演另记role。
- “角色名（演员名）”中括号前姓名为`character_or_subject`，括号内姓名为演员`explicit_credit`。
- 其他作品、比较或评论者上下文为`other_work_reference`；仅出现但语义不清为`mention_only`。
- 同一规范姓名跨品牌、正文和多次出现只算一个人物事实，但保留所有位置。品牌加描述明确署名时提升为一个explicit事实，不重复加分。
- mention_only最多只提供排序分，角色/纪录对象/其他作品引用不支持当前作品演职员。

## 5. 评分、硬冲突和排序

`evaluate_candidate`的独立证据分：T_exact 40；否则T_strong 30；否则T_close 15；不同explicit人物每位20、最多40；仅brand_support且未成为explicit的不同人物每位10、最多20；存在mention_only合计5；可靠作品年份相同10；类型compatible 5、unknown 0。总分不是概率。

硬冲突包括作品编号冲突、类型冲突和可靠年份差超过1年。`_sort_key`顺序为：自动确认资格、无硬冲突、证据总分、T_exact、T_strong、完整标题相似度、IMDb rank、IMDb ID。硬冲突不能凭高分排到无冲突候选之前，更不能进入确认。

## 6. 确认路径和最终状态

无硬冲突且满足一条路径才有自动确认资格：

- A：有可定位、已核实的商品—作品外部ID映射；当前50条没有。
- B：T_exact且至少一位explicit_credit。
- C：T_exact、可靠作品年份相同且至少一位仅brand_support人物。
- D：至少T_strong且两位不同explicit_credit。
- E：至少T_strong、可靠年份相同且有明确导演对应。

`decide`规则：

- 恰好一个候选有资格时通常为`rule_verified`。
- 多个不同IMDb ID有资格，或唯一有资格者旁存在另一个T_strong、至少一个explicit人物、无硬冲突且分差`<=10`的候选，均为`needs_review_ambiguous`。
- 无资格但有T_close且无硬冲突候选为`needs_review_insufficient`。
- 候选全是硬冲突或低于T_close为`no_suitable_candidate`。
- 无tt候选为`no_candidate`。

`review_reason_code`区分`single_eligible_candidate`、`multiple_credible_works`、`evidence_missing`、`product_category_review`和`expand_search`，不把扩大检索、商品类别确认和两个可信作品难区分混为同一人工任务。疑似IMDb重复ID保持分离。

## 7. 人工标签

人工标签独立存于`integration.movie_match_human_label`，不会覆盖算法状态。每条保存ASIN、规则版本、作用域、候选IMDb ID、标签、理由、JSON证据、标注人、标注时间、导入批次和内容哈希。

- `correct_match`：指定候选就是该商品对应作品；候选级。
- `incorrect_match`：只否定指定候选，不等于池中没有正确作品；候选级。
- `uncertain`：对指定候选仍不能判断；候选级。
- `no_match_in_pool`：检查候选池后认为没有正确作品；池级，候选ID强制为空。
- `not_single_movie`：商品本身不属于单作品流程；商品级，候选ID强制为空。

候选级导入会验证IMDb ID确实存在于该ASIN、标签规则版本保存的候选池。相同标签内容哈希幂等；新标签追加，不删除历史。TSV的`human_labels_json`跨预测规则版本导出该ASIN的完整标签历史。

截至2026-11-05，学生已通过聊天提交5个development商品的6项标签：4个`correct_match`、1个`incorrect_match`、1个`not_single_movie`。这些决定不改写预测，也不计入独立评价集。`approved_development_movie_mapping`明确只是人工确认子集，不是统一采用入口。

## 8. 版本变化、统一采用与真实案例

v3将评估范围从前5候选扩至响应全部tt候选（220→311），改为保留Unicode的标题双形式，引入明确人物语义、可靠年份边界、类型unknown、罗马数字、固定评分、确认路径和独立人工标签。v2.1的35/1/11/2/1变为v3的31/1/14/3/1。严格规则使5条旧确认降为证据不足、1条降为歧义；另有1条旧歧义和1条旧证据不足升级为确认。

v3.1只修正通用证据提取：actor/host、括号后stars、top-billed、performance/portrayal、前置演员列表及英文前置冠词变体。原14条不足中5条升级，结果为36/1/9/3/1。没有降低确认阈值，也没有ASIN专用规则。

统一采用读取`unified_development_movie_mapping_decision`：人工正确优先；人工否定候选禁止采用；商品`not_single_movie`不建立单电影映射；没有人工决定时v3.1 `rule_verified`可直接采用；其余暂缓。当前人工采用4、规则采用34、排除1、暂缓11。

- Frozen `B003L1ZWG2`：`tt1323045`为T_exact；Shawn Ashmore、Emma Bell两位explicit，80分，路径B+D。2013同名动画`tt2294629`只有标题40分，故唯一确认。
- The Red Baron `B0039USJBK`：`tt0365675`为T_exact；括号内Matthias Schweighöfer和Lena Headey为演员explicit，80分，路径B+D。Manfred von Richthofen是角色/历史人物，不计演员；1977同名候选只有标题40分。v2.1歧义在v3变为规则确认。
- Castle Rock `B000PGTNCK`：电影`tt0240400`为T_exact但无可定位候选人物，40分，证据不足；电视剧候选有类型硬冲突。
- Resident Evil: Afterlife `B0046MOVD0`：`tt1220634`80分走B+D；`tt0432021`因同系列演员交叉命中、T_strong且70分走D，分差10，因此强制歧义，不用rank消除。
- UFC: Ultimate Knockouts 8 `B0047S4UKC`：商品有明确赛事和25次终结片段语境，归体育集锦；缓存第2/3/4集同时存在编号及类型硬冲突，结果为`no_suitable_candidate`/`product_category_review`。

## 9. 已知局限

缓存suggestion不是IMDb官方完整快照；principal字符串没有nconst、角色和完整演职员。当前没有可靠商品—作品外部ID、核实别名或商品侧作品年份；人物同名仍不能仅凭字符串消歧。主标题/副标题路径可能把同系列不同副标题列为strong，需编号、人物、年份或外部ID继续阻止误合并。50条为开发样本，尚无独立人工评价集，不能报告Precision、Recall或F1。下一阶段若扩大，应固定IMDb非商业TSV快照并补充`title.basics`、`title.akas`、`title.crew`、`title.principals`和`name.basics`。
