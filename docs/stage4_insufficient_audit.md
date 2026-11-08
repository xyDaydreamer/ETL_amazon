# 阶段4原14条证据不足审计

审计日期：2026-11-05。输入仅为既有UCSD原始描述和已缓存IMDb suggestion响应，没有新增网络请求。旧规则为`stage4-first50-movie-match-v3-evidence`，修正规则为`stage4-first50-movie-match-v3.1-evidence-extraction`。旧v3结果保留，v3.1另存50条。

## 通用修正

本轮没有放宽“仅同名不能确认”的原则。只修正以下通用提取缺陷：

- 识别`actor/actress/host/presenter + 姓名`。
- 识别`姓名 + （其他作品/奖项） + stars/starring`。
- 识别多人后接`top-billed`的当前作品演员语句。
- 识别`puts in/delivers a performance`及明确当前作品`portrayal`语句。
- 识别结构化商品元数据前的前置演员列表。
- 将仅差英文前置冠词的完整标题记为`T_strong`变体，但不记作`T_exact`或核实别名。

新增测试覆盖上述规则；v3.1规则测试21项通过。

## 逐条结果

| ASIN | 商品 | v3→v3.1 | 审计结论与依据 |
|---|---|---|---|
| `0981458971` | Quantum Communication | insufficient→insufficient | 来源缺证据。缓存候选人物Michael Chaikin、John Gray未出现在UCSD；UCSD只明确Producer David Sereda、James Law。标题相同不足以确认。 |
| `B00005NC64` | Luminous Motion | insufficient→insufficient | 来源缺证据。缓存人物Patrick Fitzgerald、Conn Horgan与UCSD中的Bette Gordon、Eric Lloyd等不交叉。 |
| `B000NA1W7W` | Starbucking | insufficient→insufficient | 来源缺证据。缓存人物Frank Edward Nora、Howard Wen与UCSD的Bill Tangeman及影片对象Winter不交叉。 |
| `B000PGTNCK` | Castle Rock | insufficient→insufficient | 缓存人物Alana Austin、Roger Velasco未出现在UCSD。程序仍保持不足；学生已有`tt0240400 correct_match`人工决定，因此统一采用走人工确认。 |
| `B00190XKN4` | When I Find The Ocean | insufficient→insufficient | 需区分正片与幕后花絮。缓存排序首项`tt1327607`是Behind the Scenes；正片`tt0455853`虽标题精确，但缓存principal未与商品人物形成明确交叉。 |
| `B001GS7AZS` | The Color of Money | insufficient→rule_verified | 通用漏提取修复。UCSD原文`legendary actor Paul Newman`为明确演员署名；T_exact，路径B，60分。 |
| `B001LIK8M0` | Amusement | insufficient→insufficient | 来源缺证据。UCSD有screenwriter Jake Wade Wall、director John Simpson，但缓存principal Katheryn Winnick、Laura Breckenridge未出现在商品描述。不能用商品导演替代缺失的候选侧人物事实。 |
| `B001PPLJO0` | The World's Biggest and Baddest Bugs | insufficient→insufficient | 已有证据此前漏提取：仅差前置`The`现为T_strong，`follows host Ruud Kleinpaste`现为explicit。仍只有一位人物且无可靠作品年份，不满足B—E路径，继续暂缓。 |
| `B002PB4I4Q` | The Open Road | insufficient→rule_verified | 通用漏提取修复。结构化元数据前`Jeff Bridges, Justin Timberlake`是演员列表；Justin同时为品牌。T_exact，路径B，60分。 |
| `B002QW7AJY` | Pandorum | insufficient→rule_verified | 通用漏提取修复。UCSD原文称Ben Foster、Dennis Quaid为`top-billed`；两位explicit，T_exact，路径B+D，80分。 |
| `B0035ECHJ0` | Disgrace | insufficient→rule_verified | 通用漏提取修复。`John Malkovich (In the Line of Fire, 1993) stars as...`明确为当前作品演员；T_exact，路径B，60分。 |
| `B003CJXJ5E` | Stolen | insufficient→insufficient | 候选源缺正确作品。缓存8个tt候选包括2023和2012同名片，但没有UCSD所述Jon Hamm作品；不能在池内强选。 |
| `B0047KP58Q` | Paa | insufficient→rule_verified | 通用漏提取修复。Amitabh Bachchan的`portrayal of Auro`和Abhishek Bachchan的`puts in a ... performance`为当前作品署名；两位explicit，路径B+D，80分。 |
| `B004B93RH4` | The John Cena Experience | insufficient→insufficient | 来源/类别仍不足。缓存候选人物Hazem Ali、Kurt Angle未出现在商品描述；John Cena是商品品牌和作品对象，但不是该候选返回的principal，不能据此补造候选人物事实。 |

结果：5条因通用提取缺陷修复升级为`rule_verified`；9条不升级。其中7条为缓存人物或正确作品证据缺失，1条Castle Rock由已有人工决定解决，1条Bugs已有证据已识别但仍不满足既定自动路径。

## 统一采用结果

统一视图`integration.unified_development_movie_mapping_decision`以人工决定优先，否则采用v3.1 `rule_verified`：

- 人工确认采用：4个商品。
- 规则自动采用：34个商品。
- 商品级排除：1个（UFC体育集锦）。另有1个候选`tt0432021`被人工否定，但其商品通过另一个人工正确候选采用，因此不重复计入商品桶。
- 暂缓：11个商品。

四类商品互斥合计50。`integration.adopted_development_movie_mapping`实际输出38条映射。development人工标签不计入独立评价集，也不计算正式Precision、Recall或F1。
