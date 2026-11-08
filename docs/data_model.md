# 最小数据模型与实施计划

## 分层模型

```text
只读外部输入
  movies.txt.gz / HTTP响应 / 第三方快照 / 人工标签
          |
          v
raw：SourceFile + SourceRecord（原始定位、快照、哈希、获取状态）
          |
          v
staging：ReviewRecord + ProductHarvest + IMDb选择性表（结构化值、隔离原因、采集清单）
          |
          v
integration：FieldAssertion + AssertionDependency
             MergeDecision + FieldSelection
          |
          v
target：ProductEdition <-> ProductMovie <-> Movie
        LogicalReview <-> ReviewMapping <-> 原始评论
        Movie <-> MoviePerson <-> Person + PersonAlias

evaluation：冻结实体对 + 独立算法预测 + 追加式人工标签

meta：ETLRun + BatchProgress + IMDbSelectiveProgress + QualityIssue 横跨全部层
```

DDL见 `sql/001_core_schema.sql`。五个schema位于业务数据库 `amazon_movies`；Airflow元数据库独立为 `airflow`。两者可以由同一个PostgreSQL服务承载，但使用不同数据库和角色。

## 粒度

- `raw.source_record`：一个可定位的源记录或一次HTTP响应。
- `raw.source_record`：不可变的SNAP原始记录与原始证据。
- `staging.review_record`：稳定评论身份，一条原始记录一行。
- `staging.review_parse_result`：按`(source_review_id, parser_version)`保存版本化解析结果。
- `staging.product_asin_observation`：独立于整条评论状态的ASIN字段决策。
- `staging.review_parse_locator`：解析版本对应的行号和解压流字节范围语义。
- `target.product_edition`：一个ASIN商品版本。
- `target.movie`：一个创作作品。
- `target.product_movie`：商品与作品的一项有证据关系，合集允许多行。
- `target.product_movie_review_attribution`：显式评论归因策略；`single_work`可进入作品汇总，`collection_member`、剧集和不确定关系贡献0，避免复制合集评论。
- `target.logical_review`：规则确认的一个逻辑评论；来源仍由映射表保留。
- `integration.field_assertion`：某实体某字段的一个候选值。
- `integration.assertion_dependency`：一个输出候选对一个输入候选的一条转换依赖边。
- `integration.field_selection`：某规则版本对一个实体字段的最终选择或冲突/缺失决策。
- `meta.external_snapshot*`：固定外部快照版本、文件哈希、表头与完整gzip验证。
- `meta.imdb_selective_scope*`：需扫描的唯一tconst、商品成员和逐文件恢复进度。
- `integration.movie_person_source_evidence`：作品—人物事实的selected/supporting来源、角色、顺序和原始姓名。
- `evaluation.entity_pair`：development之外冻结的电影/人物评价对；预测和人工标签分表。

## 事务与恢复接口

解析器以有界批次读取。每个批次在单一业务数据库事务内：

1. 校验稳定键与已存在负载是否一致。
2. 写入源定位、暂存记录、质量问题和目标变更。
3. 更新同一 `pipeline_name + input_version + partition_key` 的 `meta.batch_progress`。
4. 提交后才允许下一个批次开始。

崩溃发生在提交前则整批回滚；发生在提交后则进度已经同步推进。重试相同批次依靠唯一约束与负载冲突检查产生相同可观察结果。

## Airflow接口

DAG只传 `batch_id`、输入版本、分区/路径和少量统计。DAG文件导入不打开3.32 GB文件、不访问网络、不连接数据库。Python模块先用CLI在固定小批次运行，再由任务调用相同入口。页面采集任务只调度Scrapy作业；响应证据和状态写业务库/快照，不放入XCom。

## 实施顺序

1. 阶段1：实现流式解析器、隔离规则、稳定键、批次事务；在固定小样本验证后完成全量评论和全部ASIN清单统计。
2. 阶段2：实现Scrapy缓存、限速、状态机和响应分类；先运行极小允许样本，再逐步推进全量清单，真实失败单独保留。
3. 阶段3：装载UCSD/IMDb等允许快照，建立普通文本匹配基线和电影/人物人工评价集。开发样本与至少100对独立评价样本隔离。
4. 阶段4：运行实体匹配、冲突选择和字段级血缘；完成难例、查询、幂等/恢复/规则变更验收。
5. 阶段5：在全量结果上生成报告、ER图、字段字典、提交清单和10至15分钟演示材料。

向量候选生成不在最小实现内。只有普通文本基线在独立评价集上的候选召回不足，且真实对比显示向量方法有净收益时，才增加pgvector或外部向量索引；最终合并规则仍要求多项可定位事实。

