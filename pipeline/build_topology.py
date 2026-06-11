#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACRO TOPOLOGY 数据管道
======================
读取 data_registry.yaml(75节点→数据源映射)+ nodes_meta.yaml(节点元信息)
+ edges.yaml(84条传导边),拉取真实数据,计算变化/分位/状态/背离,
输出前端 loaders.ts 直接消费的 5 个 JSON 文件。

用法:
    export FRED_API_KEY=xxxx          # https://fred.stlouisfed.org/docs/api/api_key.html 免费申请
    python build_topology.py --out ../output

设计原则:
    1. 任何单一数据源失败不阻塞整体:失败节点标记 stale 并沿用上次值(若有缓存);
    2. 全部判定规则(状态/激活/背离)集中在本文件 RULES 区,可单测、可审计;
    3. 输出 JSON 的字段与前端 types/topology.ts 完全一致。
"""
from __future__ import annotations
import argparse, json, math, os, sys, time, datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yaml

HERE = Path(__file__).parent
TODAY = dt.date.today()

# ------------------------------------------------------------------
# 1. Fetchers —— 每个数据源一个函数, 返回 pd.Series(DatetimeIndex, float)
# ------------------------------------------------------------------

FRED_KEY = os.environ.get("FRED_API_KEY", "")
SESSION = requests.Session()
SESSION.headers["User-Agent"] = "macro-topology-pipeline/1.0"


def fetch_fred(series: str, days: int) -> pd.Series:
    start = (TODAY - dt.timedelta(days=days + 400)).isoformat()
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series}&api_key={FRED_KEY}&file_type=json&observation_start={start}")
    obs = SESSION.get(url, timeout=30).json()["observations"]
    s = pd.Series({o["date"]: o["value"] for o in obs})
    s.index = pd.to_datetime(s.index)
    return pd.to_numeric(s, errors="coerce").dropna()


def fetch_yahoo(ticker: str, days: int) -> pd.Series:
    import yfinance as yf
    df = yf.download(ticker, period=f"{max(days, 365)}d", interval="1d",
                     progress=False, auto_adjust=True)
    col = df["Close"]
    if isinstance(col, pd.DataFrame):  # yfinance 多级列兼容
        col = col.iloc[:, 0]
    return col.dropna()


def fetch_akshare(cfg: dict, days: int) -> pd.Series:
    """akshare 接口名偶有变更——所有兼容逻辑只写在这里。"""
    import akshare as ak
    fn, code, column = cfg["fn"], cfg.get("code"), cfg.get("column")
    if fn == "bond_zh_us_rate":
        df = ak.bond_zh_us_rate(start_date=(TODAY - dt.timedelta(days=days)).strftime("%Y%m%d"))
        s = df.set_index("日期")[column]
    elif fn == "repo_rate_hist":
        df = ak.repo_rate_hist(start_date=(TODAY - dt.timedelta(days=days)).strftime("%Y%m%d"),
                               end_date=TODAY.strftime("%Y%m%d"))
        df = df[df["回购代码"].str.contains(code, na=False)] if "回购代码" in df else df
        s = df.set_index("日期")["加权平均利率" if "加权平均利率" in df else df.columns[-1]]
    elif fn == "macro_china_lpr":
        df = ak.macro_china_lpr()
        s = df.set_index("TRADE_DATE")["LPR1Y"]
    elif fn == "macro_china_money_supply":
        df = ak.macro_china_money_supply()
        s = df.set_index("月份")[column]
    elif fn == "stock_zh_index_daily":
        df = ak.stock_zh_index_daily(symbol=code)
        s = df.set_index("date")["close"]
    elif fn == "fund_etf_hist_em":
        df = ak.fund_etf_hist_em(symbol=code, period="daily", adjust="qfq")
        s = df.set_index("日期")["收盘"]
    elif fn == "futures_main_sina":
        df = ak.futures_main_sina(symbol=code)
        s = df.set_index("日期")["收盘价"]
    elif fn == "futures_nh_price_index":
        df = ak.futures_nh_price_index(symbol=code)
        s = df.set_index("date")["value"]
    elif fn == "macro_china_omo":
        df = ak.macro_china_omo()  # 若接口名变更, 此处是唯一改动点
        s = df.set_index("日期")["净投放"]
    else:  # 通用单值月度宏观接口: macro_china_cpi_yearly / macro_usa_ism_pmi 等
        df = getattr(ak, fn)()
        date_col = next(c for c in df.columns if "日期" in str(c) or "时间" in str(c) or "date" in str(c).lower() or "月份" in str(c))
        val_col = column or next(c for c in df.columns if c != date_col and pd.api.types.is_numeric_dtype(pd.to_numeric(df[c], errors="coerce")))
        s = df.set_index(date_col)[val_col]
    s.index = pd.to_datetime(s.index, errors="coerce")
    return pd.to_numeric(s, errors="coerce").dropna().sort_index()


def fetch_nyfed_acm(cfg: dict, days: int) -> pd.Series:
    df = pd.read_excel(cfg["url"], sheet_name="ACM Daily")
    s = df.set_index(pd.to_datetime(df["DATE"]))[cfg["series"]]
    return pd.to_numeric(s, errors="coerce").dropna()


def fetch_treasury_auctions(days: int) -> pd.Series:
    """财政部拍卖规模 → 4周滚动发行额(十亿美元), 衡量财政供给压力。"""
    url = ("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
           "v1/accounting/od/auctions_query"
           f"?fields=auction_date,offering_amt&filter=auction_date:gte:{(TODAY - dt.timedelta(days=days)).isoformat()}"
           "&page[size]=10000")
    rows = SESSION.get(url, timeout=60).json()["data"]
    df = pd.DataFrame(rows)
    df["auction_date"] = pd.to_datetime(df["auction_date"])
    df["offering_amt"] = pd.to_numeric(df["offering_amt"], errors="coerce")
    daily = df.groupby("auction_date")["offering_amt"].sum() / 1e9
    return daily.resample("D").sum().rolling(28).sum().dropna()


def fetch_csv_url(cfg: dict, days: int) -> pd.Series:
    df = pd.read_excel(cfg["url"]) if cfg["url"].endswith((".xls", ".xlsx")) else pd.read_csv(cfg["url"])
    date_col = next(c for c in df.columns if "date" in str(c).lower() or "day" in str(c).lower())
    s = df.set_index(pd.to_datetime(df[date_col]))[cfg["column"]]
    return pd.to_numeric(s, errors="coerce").dropna()


# ------------------------------------------------------------------
# 2. Compute —— 统一日频化、变化、分位、z 分
# ------------------------------------------------------------------

def to_daily(s: pd.Series) -> pd.Series:
    idx = pd.bdate_range(s.index.min(), max(s.index.max(), pd.Timestamp(TODAY)))
    return s.reindex(idx).ffill().dropna()


def transform_series(s: pd.Series, cfg: dict) -> pd.Series:
    t = cfg.get("transform", "level")
    if t == "yoy":
        s = (s / s.shift(12) - 1) * 100  # 月度指数同比
    elif t == "mom_diff":
        s = s.diff()
    elif t == "rolling_net_7d":
        s = s.rolling(7, min_periods=1).sum()
    if cfg.get("scale"):
        s = s * cfg["scale"]
    return s.dropna()


def change(s: pd.Series, n: int, unit: str) -> float:
    if len(s) <= n:
        return 0.0
    a, b = s.iloc[-1], s.iloc[-1 - n]
    if unit == "pct":
        return round((a / b - 1) * 100, 2) if b else 0.0
    if unit == "bp":
        return round((a - b) * (100 if abs(a) < 50 else 1), 1)  # %口径×100, 已是bp口径不再放大
    return round(a - b, 2)  # pp / idx


def pct_rank(s: pd.Series, window: int) -> int:
    w = s.iloc[-window:] if len(s) > window else s
    return int(round((w <= s.iloc[-1]).mean() * 100))


def zscore_20d_momentum(s: pd.Series) -> float:
    mom = s.pct_change(20) if (s.abs().max() > 30) else s.diff(20)
    m = mom.dropna()
    if len(m) < 60 or m.iloc[-60:].std() == 0:
        return 0.0
    win = m.iloc[-756:] if len(m) > 756 else m
    return float((m.iloc[-1] - win.mean()) / (win.std() + 1e-9))


# ------------------------------------------------------------------
# 3. RULES —— 状态 / 边激活 / 背离判定 (全部阈值集中于此, 可单测)
# ------------------------------------------------------------------

TH = dict(
    extreme_pct_hi=95, extreme_pct_lo=5,      # 水平分位极端
    elevated_pct_hi=85, elevated_pct_lo=15,
    extreme_z=2.0, elevated_z=1.2,            # 20D动量z分
    edge_active_z=1.0,                        # 两端同向动量 → ACTIVE
    div_z=1.0,                                # 背离判定动量门槛
)

# 背离观察清单: (id, 标题, 节点A, 节点B, 预期符号 +1同向/-1反向, 预期关系描述, 实际口径)
DIVERGENCE_WATCHLIST = [
    ("d1", "美债收益率上行 vs 美元走弱", "us10y", "dxy", +1,
     "利差逻辑下, 美债收益率上行应支撑美元同步走强。",
     ["财政供给与期限溢价主导, 而非增长/政策利差", "海外资金对美债的边际需求转弱"]),
    ("d2", "实际利率上行 vs 黄金上涨", "us10yreal", "gold", -1,
     "实际利率是黄金的机会成本, 实际利率快速上行时黄金通常承压下跌。",
     ["财政信用对冲需求压倒利率定价", "央行购金等非价格敏感型买盘"]),
    ("d3", "股票下跌 vs 信用利差不扩", "spx", "hyspread", -1,
     "风险资产普跌时, 高收益利差通常同步走扩定价违约风险。",
     ["下跌主因是估值/久期压缩, 而非衰退与违约定价"]),
    ("d4", "原油上涨 vs 盈亏平衡通胀不动", "oil", "us10ybe", +1,
     "油价上行通常推升市场通胀补偿(盈亏平衡)。",
     ["市场认为油价驱动来自供给暂时因素, 不改变通胀趋势"]),
    ("d5", "成长/价值风格剧烈分化", "growthidx", "valueidx", +1,
     "多数时期成长与价值同涨同跌, 仅相对强弱不同。",
     ["利率久期冲击下的风格再定价"]),
]


def node_status(pct: int, z: float) -> str:
    if pct >= TH["extreme_pct_hi"] or pct <= TH["extreme_pct_lo"] or abs(z) >= TH["extreme_z"]:
        return "EXTREME"
    if pct >= TH["elevated_pct_hi"] or pct <= TH["elevated_pct_lo"] or abs(z) >= TH["elevated_z"]:
        return "ELEVATED"
    return "NORMAL"


def edge_status(e: dict, z: dict) -> str:
    zs, zt = z.get(e["source"], 0.0), z.get(e["target"], 0.0)
    if abs(zs) < TH["edge_active_z"] or abs(zt) < TH["edge_active_z"]:
        return "INACTIVE"
    sign = 1 if e["relation"] == "positive" else (-1 if e["relation"] == "negative" else 0)
    if sign == 0:  # conditional: 只标记活跃, 不判背离
        return "ACTIVE"
    consistent = (zs * zt * sign) > 0
    if consistent:
        return "ACTIVE"
    # 两端都强动量但方向违背传导关系 → 该边背离; 极强且持续(简化: |z|>1.8)视作失效候选
    if abs(zs) > 1.8 and abs(zt) > 1.8:
        return "INVALIDATED"
    return "DIVERGENCE"


def detect_divergences(z: dict, series: dict) -> list[dict]:
    out = []
    for did, title, a, b, sign, expected, alts in DIVERGENCE_WATCHLIST:
        za, zb = z.get(a, 0.0), z.get(b, 0.0)
        if abs(za) < TH["div_z"] or abs(zb) < TH["div_z"]:
            continue
        if (za * zb * sign) > 0:
            continue  # 关系成立, 无背离
        strength = "HIGH" if min(abs(za), abs(zb)) > 1.8 else ("MEDIUM" if min(abs(za), abs(zb)) > 1.3 else "LOW")
        # 持续性: 5日前是否同样背离
        persistent = False
        try:
            za5 = zscore_20d_momentum(series[a].iloc[:-5]); zb5 = zscore_20d_momentum(series[b].iloc[:-5])
            persistent = (za5 * zb5 * sign) < 0 and abs(za5) > 0.8 and abs(zb5) > 0.8
        except Exception:
            pass
        out.append(dict(
            id=did, title=title, relatedNodeIds=[a, b], relatedEdgeIds=[],
            expectedRelation=expected,
            observedRelation=f"{a} 20D动量z={za:+.1f}, {b} 20D动量z={zb:+.1f}, 与预期关系相反。",
            window="20D", strength=strength,
            persistence="PERSISTENT" if persistent else "TRANSIENT",
            alternativeExplanations=alts,
            invalidationRisk="HIGH" if strength == "HIGH" and persistent else ("MEDIUM" if strength != "LOW" else "LOW"),
        ))
    return out


# ------------------------------------------------------------------
# 4. Derived nodes —— 解析 registry 中 formula 字段
# ------------------------------------------------------------------

def eval_derived(formula: str, z: dict) -> float:
    """支持: a*z20(node) ± ... ; z60(spread(X,Y)) 由 extra_fred 在主流程单独处理。"""
    import re as _re
    val, expr = 0.0, formula.replace(" ", "")
    for m in _re.finditer(r"([+-]?[\d.]+)\*z20\((\w+)\)", expr):
        val += float(m[1]) * z.get(m[2], 0.0)
    return round(val, 2)


# ------------------------------------------------------------------
# 5. Main
# ------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(HERE.parent / "output"))
    ap.add_argument("--cache", default=str(HERE.parent / "output" / "nodes.json"))
    args = ap.parse_args()
    outdir = Path(args.out); outdir.mkdir(parents=True, exist_ok=True)

    reg = yaml.safe_load(open(HERE / "data_registry.yaml"))
    meta = yaml.safe_load(open(HERE / "nodes_meta.yaml"))["nodes"]
    edges = yaml.safe_load(open(HERE / "edges.yaml"))["edges"]
    days = reg["defaults"]["history_days"]
    pwin = reg["defaults"]["percentile_window"]

    prev = {}
    if Path(args.cache).exists():  # 上次成功值, 用于失败回退
        prev = {n["id"]: n for n in json.load(open(args.cache))}

    series: dict[str, pd.Series] = {}
    stale: list[str] = []

    # ---- 第一遍: 拉取所有非派生节点 ----
    for nid, cfg in reg["nodes"].items():
        src = cfg["source"]
        if src in ("derived", "manual"):
            continue
        try:
            if src == "fred":
                s = fetch_fred(cfg["series"], days)
            elif src == "yahoo":
                s = fetch_yahoo(cfg["ticker"], days)
            elif src == "akshare":
                s = fetch_akshare(cfg, days)
            elif src == "nyfed_acm":
                s = fetch_nyfed_acm(cfg, days)
            elif src == "treasury":
                s = fetch_treasury_auctions(days)
            elif src == "csv_url":
                s = fetch_csv_url(cfg, days)
            else:
                raise ValueError(f"unknown source {src}")
            series[nid] = to_daily(transform_series(s, cfg))
            time.sleep(0.4)  # 温和限速
        except Exception as exc:
            print(f"[WARN] {nid} fetch failed: {exc}", file=sys.stderr)
            stale.append(nid)

    # debtceiling 的 spread 特例
    try:
        a, b = fetch_fred("DTB4WK", days), fetch_fred("DTB3", days)
        series["_dc_spread"] = to_daily((a - b).dropna())
    except Exception:
        pass

    z = {nid: zscore_20d_momentum(s) for nid, s in series.items() if not nid.startswith("_")}

    # ---- 第二遍: 派生与手工节点 ----
    nodes_out = []
    for nid, cfg in reg["nodes"].items():
        m = meta[nid]
        unit_ch = cfg.get("unit_changes", "pct")
        if cfg["source"] == "derived":
            if nid == "debtceiling" and "_dc_spread" in series:
                zz = zscore_20d_momentum(series["_dc_spread"]); value = round(zz, 2)
            else:
                zz = eval_derived(cfg["formula"], z); value = zz
            z[nid] = zz
            node = dict(value=value, change1d=0.0, change5d=round(zz * 0.3, 2),
                        change20d=round(zz, 2), change60d=round(zz * 1.2, 2),
                        percentile=int(np.clip(50 + zz * 20, 1, 99)))
        elif cfg["source"] == "manual":
            node = dict(value=cfg["value"], change1d=0.0, change5d=0.0,
                        change20d=0.0, change60d=0.0, percentile=50)
            z[nid] = 0.0
        elif nid in series:
            s = series[nid]
            node = dict(
                value=round(float(s.iloc[-1]), 2 if abs(s.iloc[-1]) < 1000 else 0),
                change1d=change(s, 1, unit_ch), change5d=change(s, 5, unit_ch),
                change20d=change(s, 20, unit_ch), change60d=change(s, 60, unit_ch),
                percentile=pct_rank(s, pwin))
        else:  # 失败回退
            p = prev.get(nid, {})
            node = dict(value=p.get("value", 0), change1d=p.get("change1d", 0),
                        change5d=p.get("change5d", 0), change20d=p.get("change20d", 0),
                        change60d=p.get("change60d", 0), percentile=p.get("percentile", 50))
            z.setdefault(nid, 0.0)
        node.update(id=nid, name=m["name"], category=m["category"], priority=m["priority"],
                    unit=m.get("unit", ""), description=m["description"],
                    status=node_status(node["percentile"], z.get(nid, 0.0)),
                    stale=nid in stale)
        if "invalidation" in m:
            node["invalidation"] = m["invalidation"]
        # 历史序列 (前端迷你图): 最近60个交易日
        if nid in series:
            tail = series[nid].iloc[-60:]
            node["history"] = [{"date": d.strftime("%m-%d"), "value": round(float(v), 3)}
                               for d, v in tail.items()]
        nodes_out.append(node)

    # ---- 边状态 / 背离 / 失效 ----
    edges_out = []
    for e in edges:
        st = edge_status(e, z)
        edges_out.append({**e, "status": st})
    divs = detect_divergences(z, series)
    # 把背离涉及的边和节点同步标记
    div_pairs = {frozenset(d["relatedNodeIds"]) for d in divs}
    for e in edges_out:
        if frozenset((e["source"], e["target"])) in div_pairs and e["status"] != "INVALIDATED":
            e["status"] = "DIVERGENCE"
    nmap = {n["id"]: n for n in nodes_out}
    for d in divs:
        d["relatedEdgeIds"] = [e["id"] for e in edges_out
                               if frozenset((e["source"], e["target"])) == frozenset(d["relatedNodeIds"])]
        for nid in d["relatedNodeIds"]:
            if nmap[nid]["status"] in ("NORMAL", "ELEVATED"):
                nmap[nid]["status"] = "DIVERGENCE"
    for e in edges_out:
        if e["status"] == "INVALIDATED":
            for nid in (e["source"], e["target"]):
                pass  # 节点不直接因边失效降级, 由人工复核

    # ---- 激活路径: ACTIVE 边的连通链 (按strength取前3条最长链, 简化贪心) ----
    active = [e for e in edges_out if e["status"] == "ACTIVE"]
    adj = {}
    for e in active:
        adj.setdefault(e["source"], []).append(e)
    paths, used = [], set()
    for e in sorted(active, key=lambda x: -x["strength"]):
        if e["id"] in used:
            continue
        chain, node_chain, cur = [e], [e["source"], e["target"]], e["target"]
        used.add(e["id"])
        while True:
            nxt = [x for x in adj.get(cur, []) if x["id"] not in used]
            if not nxt:
                break
            x = max(nxt, key=lambda y: y["strength"])
            chain.append(x); used.add(x["id"]); node_chain.append(x["target"]); cur = x["target"]
        if len(chain) >= 1:
            paths.append(dict(
                id=f"p_auto_{len(paths)+1}",
                title=" → ".join(nmap[n]["name"] for n in node_chain),
                nodeIds=node_chain, edgeIds=[c["id"] for c in chain],
                note="由规则引擎根据20日动量一致性自动识别的激活传导链。"))
        if len(paths) == 3:
            break

    # ---- Market State ----
    movers = sorted((n for n in nodes_out if n["priority"] in ("P0", "P1") and "history" in n),
                    key=lambda n: -abs(z.get(n["id"], 0)))
    top = movers[0] if movers else nodes_out[0]
    high_risk = sum(n["status"] in ("EXTREME", "DIVERGENCE", "INVALIDATED") for n in nodes_out)
    regime = "EXTREME" if high_risk >= 10 else ("ELEVATED" if high_risk >= 4 else "NORMAL")
    market_state = dict(
        topMover=dict(nodeId=top["id"],
                      text=f"{top['name']} {top['change5d']:+g}{'bp' if 'b' in '' else ''}/5D · {top['percentile']}分位"),
        dominantPathId=paths[0]["id"] if paths else "",
        divergenceCount=len(divs), highRiskCount=high_risk, regime=regime,
        regimeNote=f"自动识别: {len(divs)}条背离, {high_risk}个高风险节点。"
                   + ("背离集中指向供给/久期因素而非政策预期, 建议人工复核主导逻辑。" if len(divs) >= 3 else ""),
        updatedAt=dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"))

    # ---- 写出 ----
    dump = lambda name, obj: json.dump(obj, open(outdir / name, "w"), ensure_ascii=False, indent=1)
    dump("nodes.json", nodes_out)
    dump("edges.json", edges_out)
    dump("paths.json", paths)
    dump("divergences.json", divs)
    dump("market_state.json", market_state)
    print(f"OK: {len(nodes_out)} nodes ({len(stale)} stale), {len(edges_out)} edges, "
          f"{len(divs)} divergences, {len(paths)} paths → {outdir}")


if __name__ == "__main__":
    main()
