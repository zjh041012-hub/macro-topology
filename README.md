# MACRO TOPOLOGY — 真实数据管道

把 75 个宏观节点从模拟数据切换到真实数据。**不爬虫、不按名字搜索**:每个节点在
`pipeline/data_registry.yaml` 中一次性绑定权威序列(FRED / Yahoo / AkShare / 纽约联储 ACM /
财政部 FiscalData / GPR 学术数据 / 派生公式),管道定时拉取并计算状态。

## 结构
```
pipeline/
  data_registry.yaml   # 75节点 → 数据源 (核心, 换数据商只改这里)
  nodes_meta.yaml      # 节点名称/类别/优先级/描述/失效条件 (与前端一致)
  edges.yaml           # 84条传导边 + 预期方向 (状态由规则引擎实时算)
  build_topology.py    # 主脚本: 拉取→变化/分位/z分→状态/背离/路径→JSON
  notify_failure.py    # 失败邮件告警
  requirements.txt
.github/workflows/macro-topology-data.yml   # 每日两次定时 (美东收盘后 + 北京17:30)
frontend/loaders.ts    # 替换 Codex 项目同名文件, 视觉层零改动
output/                # nodes/edges/paths/divergences/market_state 五个JSON
```

## 快速开始
```bash
pip install -r pipeline/requirements.txt
export FRED_API_KEY=你的key      # https://fred.stlouisfed.org/docs/api/api_key.html 免费
python pipeline/build_topology.py --out output
```
GitHub Actions 部署:仓库 Secrets 添加 `FRED_API_KEY`(可选 `GMAIL_USER`/`GMAIL_APP_PASSWORD`
用于失败告警),推送即自动按日刷新并 commit `output/*.json`。

## 规则引擎 (build_topology.py 内 RULES 区, 阈值集中可单测)
- 节点状态: 3年滚动分位 ≥95/≤5 或 20D动量|z|≥2.0 → EXTREME; ≥85/≤15 或 |z|≥1.2 → ELEVATED
- 边状态: 两端 |z|≥1.0 且方向符合 relation → ACTIVE; 违背 → DIVERGENCE; 两端|z|>1.8仍违背 → INVALIDATED
- 背离: 5对观察清单 (10Y vs DXY / 实际利率 vs 黄金 / SPX vs HY利差 / 油 vs 盈亏平衡 / 成长 vs 价值),
  附持续性检验 (5日前是否已背离)
- 激活路径: ACTIVE 边贪心连链, 取前3条
- 单源失败不阻塞: 节点标 `stale: true` 并沿用上次值

## 已知注意事项
- AkShare 接口名偶有变更 → 全部隔离在 `fetch_akshare()` 一个函数里
- `cnpminew`(PMI新订单分项)/ `mlf` / `northbound` 暂为手工维护, registry 中有说明
- Yahoo/FRED 数据用于个人项目没问题; 公开商用部署前需复核各源条款
