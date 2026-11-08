# 阶段7真实案例库存

人物v2盲评已结束。下列案例来自与正式评价集隔离的development范围；正式评价pair没有回流为调规则或补数量的案例。

| 类型 | 已核实可用于报告 | 候选（证据不足/待人工核实） | 要求 | 当前缺口 |
|---|---:|---:|---:|---:|
| 电影跨版本/多商品合并 | 5 | 0 | 5 | 0 |
| 电影易混淆不合并 | 5 | 0 | 5 | 0 |
| 人物别名/来源署名合并 | 5 | 0 | 5 | 0 |
| 人物同名或近名不合并 | 5 | 0 | 5 | 0 |

已核实案例必须有两侧SourceRecord、字段/关系位置、规则或人工决定、采用/拒绝理由和可重放查询；同标题、同姓名、相似度或“未出现在principals”单独都不算核实。候选仍保留，但在最终报告中单列，不能为达到数量改写成事实。

人物合并包括两条已有的“同一已确认作品+唯一同名nconst”规则合并，以及三条UCSD署名到固定IMDb人物的变体：`Schweighofer/Schweighöfer`、`Smith, Sr./Smith Sr.`、`Shorheh/Shohreh`。三条都要求商品已映射到同一tconst、UCSD有明确署名位置、IMDb principals有同作品nconst关系；不是只按姓名相似度合并。

人物不合并选取五组固定快照近名记录，决定依据包括不同完整姓名、出生/去世年代、作品/职业，或同一作品中两个nconst分别署名；nconst不同本身不作为唯一理由。电影不合并包括人工否定的`Resident Evil: Afterlife/Extinction`，以及Frozen/Frozen II、Vacancy/Vacancy 2的编号硬冲突和Castle Rock/The Crazies的电影—电视剧类型冲突。

电影跨版本采用规则版本`stage7-cross-product-version-v1`，全部属于development范围，并排除电影/人物正式评价集商品。每个ASIN必须有兼容标题、至少一项可定位的固定IMDb快照principal人物支持，以及商品版本/介质证据；标题相同本身不构成合并理由。

| 组 | ASIN与版本差异 | IMDb作品 | UCSD商品SourceRecord | IMDb title SourceRecord | 合并理由 |
|---|---|---|---|---|---|
| Chocolate | `B001EBO90G`、`B001L67A3M`；两个独立Blu-ray商品，来源同时出现Region All与Region 2描述冲突，冲突原样保留 | `tt1183252` Chocolate (2008) | 7984193、7986031 | 8002545 | 两商品均有同一详细规格，并定位到Pongpat Wachirabunjong、Hiroshi Abe、Taphon Phopwandee、Prachya Pinkaew等固定快照演职员；不是按地区字段或同名直接合并 |
| The Color of Money | `6300276740` VHS；`B001GS7AZS` Blu-ray/25周年版 | `tt0090863` (1986) | 7915747、7985085 | 8002521 | 两边均明确Paul Newman，描述同时给出Tom Cruise、Martin Scorsese及同一Fast Eddie/Vince情节；IMDb principal关系可定位 |
| Initial D | `B000BY9VO6` DVD/DTS；`B0014HST4W` Region-All Blu-ray | `tt0439630` (2005) | 7963920、7980350 | 8021866 | 两边均明确Shawn Yue，并描述Takumi、AE86和漂移情节；介质类别相互独立 |
| Journey to the Center of the Earth | `6302098424` VHS；`B00007JMD8` 另一20th Century Fox家庭影像商品（格式未写明） | `tt0052948` (1959) | 7917260、7950338 | 8021353 | 两边均明确James Mason与Lindenbrook地心探险，VHS描述还明确1959；未把未写明格式猜成DVD |
| The Rundown | `B00016MSVM` 工厂封装VHS；`B0009NJFPO` Universal原盒商品（格式未写明） | `tt0327850` (2003) | 7954164、7960548 | 8021761 | 两边同属Universal商品路径并明确Dwayne Johnson，固定快照同作品principal关系可定位；未补造第二商品介质 |

原候选`The Crazies`经复核后明确不得合并：`B0021L8UXK`的Timothy Olyphant、Breck Eisner和Ogden Marsh指向2010重拍版`tt0455407`；`B002VRNJQ2`的Lane Carroll、George Romero和Evans City指向1973原版。数据库保存`B002VRNJQ2 -> imdb:tt0455407`的`do_not_merge`决定，未将同名当版本证据。

实际“查询作品全部商品版本”返回上述每部作品恰好两个ASIN且状态均为`verified`，孤儿ProductMovie为0。10个合并决定与1个拒绝决定重复运行后仍分别为10/1；目标ProductMovie总数由基线39变为47，因为两个ASIN此前已有关系，未复制。查询输出见`data/work/stage7/case_inventory/cross_version_verification.json`，逐项字段位置、人物命中、内容哈希与链接见同目录`case_inventory.tsv`和`case_inventory.json`。

逐组原始记录ID、字段位置、来源链接、证据和理由位于`data/work/stage7/case_inventory/case_inventory.tsv`；汇总及输入/输出SHA-256位于同目录`case_inventory.json`。原Stage5材料及哈希继续保留在`data/work/stage5/case_materials/`。

