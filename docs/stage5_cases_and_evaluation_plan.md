# 阶段5开发案例与独立评价集计划

日期：2026-11-05。规则版本：`stage5-development-case-prep-v1`。

## 已生成的开发材料

运行入口：

```powershell
docker compose exec -T airflow-scheduler python -m amazon_movies.cli stage5-prepare-cases `
  --output-dir /opt/airflow/project/data/work/stage5/case_materials `
  --scope-key stage5-adopted-38-v1
```

该命令只查询PostgreSQL，不读取IMDb大文件、不联网、不修改作品或人物采用关系。输出包括人物近名候选、电影跨版本候选、HTML证据页、development排除清单和文件哈希清单。重复运行结果哈希稳定；人工答案保持空白。

人物候选从38部作品的742个nconst中按规范姓名相似度排序。规范化只用于候选生成，绝不用于身份合并。每项展示双方nconst、原始姓名、出生/死亡年、职业、known-for、当前scope作品、源文件行号/哈希及IMDb链接。现有2条pilot规则合并另行保留，未混入这10个空白候选的人工答案。

电影跨版本候选从阶段4的50 ASIN之外、UCSD分类为movie的商品中，与已物化38部作品的primary title和akas比较。阈值为SequenceMatcher 0.85；当前只有2项达到阈值且均为标题精确一致。标题一致只产生候选，不足以自动建立新的ProductMovie关系。

## 距离“各5组案例”的缺口

| 案例类型 | 已核实 | 已准备但未核实 | 仍需达到的核实数 |
|---|---:|---:|---:|
| 人物别名/候选合并 | 2 | 0 | 3 |
| 人物同名/近名不合并 | 0 | 10 | 5 |
| 电影跨版本合并 | 0 | 2 | 5 |
| 电影易混淆不合并 | 1 | 可复用阶段4冲突池 | 4 |

“已准备”不等于事实结论。新增决定需保存双方来源、采用/拒绝理由和可纠错历史。

## 独立评价集最短路径

1. 冻结`development_exclusions.json`：排除50个阶段4 ASIN、310个缓存候选tconst、761个已用于pilot/scope的人物nconst以及本轮开发对。
2. 从阶段4外商品建立新的电影候选范围，分层保留高相似、低相似、类型/年份冲突和无候选项；不得复用规则开发案例。
3. 只选择新电影范围关联的nconst形成人物候选，排除development清单两侧全部实体；按姓名难度、共同作品、职业/年代冲突分层。
4. 分别冻结至少100个movie pair和100个person pair。先保存抽样规则、snapshot_id、pair payload和清单SHA，再生成任何人工答案。
5. 使用`evaluation-create`写入冻结实体对；使用`evaluation-export`产生`review_material.html`、空白`answers.tsv`和manifest。HTML展示双方信息、必要证据和点击链接，不显示算法预测。
6. 学生只填写“同一实体／不同实体／无法确定”和简短理由。`evaluation-import`追加人工历史，不覆盖算法预测；标签冻结后才计算指标。

当前没有足够的development之外IMDb作品/人物入库，因此不能诚实地产生两套100对真实评价集。下一次扩大范围会扫描大型固定快照，必须先实现并有界验证新范围选择，再把长命令交给用户手动运行；本轮没有启动该扫描。
