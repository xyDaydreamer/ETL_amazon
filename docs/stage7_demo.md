# 10—15分钟答辩演示步骤

1. **1分钟：问题和实体粒度。** 展示ER图，说明ASIN是商品版本，Movie是作品，Person独立，评论只连商品；合集不会复制评论。
2. **2分钟：全量评论事实。** 运行阶段7质量SQL，展示7,911,696物理块、12个畸形块、7,911,684可靠ASIN评论、253,059商品、889,176可靠用户，并解释有效/隔离与字段异常不是同一分类。
3. **2分钟：真实作品链。** 展示`0005019281 → tt0078764 → An American Christmas Carol (1979)`及一项人物关系；打开SourceRecord定位、文件SHA、字段断言和选择理由。强调IMDb不含ASIN，连接来自商品证据和匹配决定。
4. **2分钟：难例、跨版本与合集。** 展示阶段4冻结development中的Frozen同名、Resident Evil续集冲突、UFC体育集锦排除；再查询The Color of Money的VHS与Blu-ray两个ASIN如何落到同一`tt0090863`。补充The Crazies为何因1973原版/2010重拍版被拒绝。最后运行B1b查询：`B001PO54OM`映射The Cowboys、Fort Apache、Rio Bravo和The Searchers四部John Wayne电影，商品383条评论仍在商品层，每个`collection_member`归因0。
5. **2分钟：四类血缘与缺失值。** 用`stage7_lineage_queries.sql`依次展示movie字段、ASIN/评论、source_record反向影响、person别名/合并/作品；再运行业务查询B3展示缺失或推断上映信息保持NULL/显式语义，指出日志不替代字段血缘。
6. **2分钟：Airflow。** 在UI展示DAG参数、依赖和一次`stage1_review_sample_ingest`日志；触发10条小批，展示XCom只有小统计。
7. **1分钟：幂等重跑。** 换Airflow run id、复用business batch，展示重复写入为0或already present、检查点不倒退。
8. **1分钟：评价与局限。** 电影选择性口径为63对、F1=0.83077、覆盖63/95；把暂缓视为未自动合并时Recall=27/42=64.29%、F1=72.97%。人物98对计分，F1=97.96%，但“全名相同且人工判不同”只有1对，不能过度外推。页面采集实际尝试5、成功0；UCSD离线覆盖29.1683%，两者分报。
9. **1分钟：恢复与资源。** 解释Docker数据盘在D盘、磁盘阈值、安全停止、事务检查点以及gzip从头扫描恢复成本。

演示前运行：

```powershell
Set-Location 'D:\amazon'
.\scripts\stage7_validate.ps1
```

证据输出到`data/work/stage7/evidence/`，该目录被Git忽略，不把数据库结果或第三方原文提交。

