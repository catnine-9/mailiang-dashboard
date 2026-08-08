#!/usr/bin/env python3
"""Sync 3387 MCP data to mailiang-dashboard data.json.

Data source: mailiang-mcp fx_3387youxi_event topic (3387 单游戏)
Output: data.json with structure matching dashboard.html's expectations.

Usage:
    python scripts/sync_mcp_3387.py [--output PATH]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

MCP_URL = os.getenv("MAILIANG_MCP_URL", "https://demo.4399dev.com/mailiang-mcp/mcp")
TOPIC = "fx_3387youxi_event"
BEIJING_TZ = timezone(timedelta(hours=8))

# Core 18 measures that always work. tgActiveUserCountRate triggers ClickHouse join
# failure (>18 measures) so it's queried separately when needed.
CORE_MEASURES = [
    "tgRealCost", "tgNewUserCount", "tgStartCount",
    "tgPayCount", "tgPayAmount", "tgMfPayAmount",
    "tgPayCountPrice", "tgPayRate", "tgRoi0", "tgMfRoi0",
    "tgMfRechargeTotalAmount0d", "tgRechargeTotalAmount0d",
    "tgMfLtv0", "tgLtv0",
    "tgArpu", "tgLtv1", "tgMfLtv1", "tgStartPrice",
]


def _now_bj() -> datetime:
    return datetime.now(BEIJING_TZ)


def _fmt_yyyymmdd(d: datetime) -> str:
    return d.strftime("%Y%m%d")


def _init_session() -> str:
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "mailiang-dashboard-sync", "version": "1.0"},
        },
    }
    req = urllib.request.Request(
        MCP_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json, text/event-stream"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        sid = r.headers.get("Mcp-Session-Id")
    if not sid:
        raise RuntimeError("MCP init: no Mcp-Session-Id")
    # notifications/initialized
    init = urllib.request.Request(
        MCP_URL,
        data=json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json, text/event-stream", "Mcp-Session-Id": sid},
    )
    urllib.request.urlopen(init, timeout=30).read()
    return sid


def _call(sid: str, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": name, "arguments": arguments}}
    req = urllib.request.Request(
        MCP_URL,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json, text/event-stream", "Mcp-Session-Id": sid},
    )
    raw = urllib.request.urlopen(req, timeout=60).read().decode("utf-8")
    m = re.search(r"data:(\{.*\})", raw, re.DOTALL)
    if not m:
        return {"error": "no SSE data: " + raw[:200]}
    d = json.loads(m.group(1))
    if "error" in d:
        return {"error": d["error"]}
    content = json.loads(d["result"]["content"][0]["text"])
    return {"data": content}


def _query(sid: str, dimensions: List[str], measures: List[str],
           date_start: str, date_end: str, limit: int = 500) -> List[Dict[str, Any]]:
    args = {
        "topic": TOPIC,
        "reportType": "ad",
        "dimensions": dimensions,
        "measures": measures,
        "dateStart": int(date_start),
        "dateEnd": int(date_end),
        "limit": limit,
    }
    res = _call(sid, "queryReport", args)
    if "error" in res:
        raise RuntimeError(f"queryReport error: {res['error']}")
    rows = res["data"].get("rows", [])
    return rows


def _parse_num(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "").replace("%", "")
    if s in ("", "NaN", "null", "None"):
        return None
    try:
        n = float(s)
    except Exception:
        return None
    # MCP returns "15.59%" -> 15.59; dashboard shows as percent, so divide by 100
    if isinstance(v, str) and "%" in v:
        return n / 100.0
    return n


def _date_ranges() -> Dict[str, Tuple[str, str]]:
    now = _now_bj()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return {
        "today": (_fmt_yyyymmdd(today), _fmt_yyyymmdd(today)),
        "last7d": (_fmt_yyyymmdd(today - timedelta(days=6)), _fmt_yyyymmdd(today)),
        "last30d": (_fmt_yyyymmdd(today - timedelta(days=29)), _fmt_yyyymmdd(today)),
    }


def _fmt_yyyy_mm_dd(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def _aggregate_detail(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    """Aggregate per-gameName rows for today/period."""
    cost = 0.0
    new = 0.0
    start = 0.0
    pay_cnt = 0.0
    pay_amt = 0.0
    mf_pay_amt = 0.0
    mf_recharge = 0.0
    recharge = 0.0
    arpu_sum = 0.0
    for r in rows:
        cost += _parse_num(r.get("tgRealCost")) or 0
        new += _parse_num(r.get("tgNewUserCount")) or 0
        start += _parse_num(r.get("tgStartCount")) or 0
        pay_cnt += _parse_num(r.get("tgPayCount")) or 0
        pay_amt += _parse_num(r.get("tgPayAmount")) or 0
        mf_pay_amt += _parse_num(r.get("tgMfPayAmount")) or 0
        mf_recharge += _parse_num(r.get("tgMfRechargeTotalAmount0d")) or 0
        recharge += _parse_num(r.get("tgRechargeTotalAmount0d")) or 0
        arpu_sum += _parse_num(r.get("tgArpu")) or 0
    # D1 ROI = total_mf_recharge / total_cost (weighted)
    d1_roi = (mf_recharge / cost) if cost > 0 else 0.0
    d1_pay_rate = (pay_cnt / new) if new > 0 else 0.0
    d1_price = (cost / new) if new > 0 else 0.0
    d1_arpu = arpu_sum
    return {
        "totalCost": round(cost, 2),
        "totalNew": round(new, 0),
        "totalStart": round(start, 0),
        "totalPayCount": round(pay_cnt, 0),
        "totalPayAmount": round(pay_amt, 2),
        "totalMfPayAmount": round(mf_pay_amt, 2),
        "totalMfRecharge": round(mf_recharge, 2),
        "totalRecharge": round(recharge, 2),
        "d1Roi": round(d1_roi, 6),
        "d1PayRate": round(d1_pay_rate, 6),
        "d1Price": round(d1_price, 2),
        "d1Arpu": round(d1_arpu, 2),
    }


def _build_top6(detail_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Top 6 games by new user count for the period."""
    agg: Dict[str, Dict[str, float]] = {}
    for r in detail_rows:
        gn = r.get("gameName") or "?"
        if gn not in agg:
            agg[gn] = {"new": 0.0, "cost": 0.0, "pay_cnt": 0.0, "mf_pay_amt": 0.0, "mf_recharge": 0.0}
        a = agg[gn]
        a["new"] += _parse_num(r.get("tgNewUserCount")) or 0
        a["cost"] += _parse_num(r.get("tgRealCost")) or 0
        a["pay_cnt"] += _parse_num(r.get("tgPayCount")) or 0
        a["mf_pay_amt"] += _parse_num(r.get("tgMfPayAmount")) or 0
        a["mf_recharge"] += _parse_num(r.get("tgMfRechargeTotalAmount0d")) or 0
    top = sorted(agg.items(), key=lambda kv: kv[1]["new"], reverse=True)[:6]
    out = []
    for gn, a in top:
        pay_rate = a["pay_cnt"] / a["new"] if a["new"] > 0 else 0.0
        ltv1 = (a["mf_pay_amt"] / a["new"]) if a["new"] > 0 else 0.0
        out.append({
            "gameName": gn,
            "newUsers": round(a["new"], 0),
            "payRate": round(pay_rate, 6),
            "ltv1": round(ltv1, 2),
            "cost": round(a["cost"], 2),
        })
    return out


def _build_trend(rows: List[Dict[str, Any]], days: List[str]) -> Dict[str, List]:
    """Build trend arrays indexed by day. rows come from queryReport with dimensions=[] + timeGranularity=DAY."""
    by_day: Dict[str, Dict[str, float]] = {}
    for r in rows:
        dk = str(r.get("datekey") or r.get("dateKey") or "")
        if not dk:
            continue
        # Format: 20260808 -> 2026-08-08
        if len(dk) == 8:
            dk_fmt = f"{dk[:4]}-{dk[4:6]}-{dk[6:]}"
        else:
            dk_fmt = dk
        a = by_day.setdefault(dk_fmt, {"cost": 0.0, "new": 0.0, "pay_cnt": 0.0,
                                        "mf_recharge": 0.0, "mf_pay_amt": 0.0})
        a["cost"] += _parse_num(r.get("tgRealCost")) or 0
        a["new"] += _parse_num(r.get("tgNewUserCount")) or 0
        a["pay_cnt"] += _parse_num(r.get("tgPayCount")) or 0
        a["mf_recharge"] += _parse_num(r.get("tgMfRechargeTotalAmount0d")) or 0
        a["mf_pay_amt"] += _parse_num(r.get("tgMfPayAmount")) or 0
    out_days = days
    d1r: List[Optional[float]] = []
    cost: List[Optional[float]] = []
    ltv: List[Optional[float]] = []
    payr: List[Optional[float]] = []
    for d in out_days:
        a = by_day.get(d)
        if not a:
            d1r.append(None)
            cost.append(None)
            ltv.append(None)
            payr.append(None)
            continue
        d1r_v = (a["mf_recharge"] / a["cost"]) if a["cost"] > 0 else 0.0
        ltv_v = (a["mf_pay_amt"] / a["new"]) if a["new"] > 0 else 0.0
        payr_v = (a["pay_cnt"] / a["new"]) if a["new"] > 0 else 0.0
        d1r.append(round(d1r_v * 100, 2))  # displayed as %
        cost.append(round(a["cost"] / 10000.0, 4))  # displayed as 万
        ltv.append(round(ltv_v, 2))
        payr.append(round(payr_v * 100, 2))
    return {"days": out_days, "d1r": d1r, "cost": cost, "ltv": ltv, "payr": payr}


def _build_pie(slot_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Aggregate slot rows by csite for pie chart."""
    agg: Dict[str, float] = {}
    for r in slot_rows:
        cs = r.get("csite") or r.get("platformCsite") or ""
        if not cs:
            continue
        agg[cs] = agg.get(cs, 0.0) + (_parse_num(r.get("tgNewUserCount")) or 0)
    items = sorted(agg.items(), key=lambda kv: kv[1], reverse=True)
    return [{"name": n, "value": round(v, 0)} for n, v in items if v > 0]


def _build_summary(detail_rows: List[Dict[str, Any]]) -> Dict[str, float]:
    """Build month-to-yesterday and week-to-yesterday summary."""
    agg = _aggregate_detail(detail_rows)
    return {"d1Roi": agg["d1Roi"], "d1PayRate": agg["d1PayRate"], "d1Price": agg["d1Price"]}


def sync(output_path: str = "data.json") -> Dict[str, Any]:
    sid = _init_session()
    ranges = _date_ranges()
    today_str, _ = ranges["today"]
    d7_start, d7_end = ranges["last7d"]
    d30_start, d30_end = ranges["last30d"]

    # Today detail (per game)
    today_rows = _query(sid, ["gameName"], CORE_MEASURES, today_str, today_str)
    # 7d detail
    d7_rows = _query(sid, ["gameName"], CORE_MEASURES, d7_start, d7_end)
    # 30d detail
    d30_rows = _query(sid, ["gameName"], CORE_MEASURES, d30_start, d30_end)
    # Chart (daily aggregates) - 30d
    d30_chart = _query(sid, [], CORE_MEASURES, d30_start, d30_end)
    d7_chart = _query(sid, [], CORE_MEASURES, d7_start, d7_end)
    # Slot (csite)
    slot_rows = _query(sid, ["csite"], ["tgNewUserCount", "tgRealCost"], today_str, today_str)

    # Aggregate
    today_agg = _aggregate_detail(today_rows)
    d7_agg = _aggregate_detail(d7_rows)
    d30_agg = _aggregate_detail(d30_rows)

    top6 = _build_top6(d30_rows)

    # Trend arrays
    now = _now_bj()
    today_d = now.replace(hour=0, minute=0, second=0, microsecond=0)
    days_30 = [_fmt_yyyy_mm_dd(today_d - timedelta(days=i)) for i in range(29, -1, -1)]
    days_7 = [_fmt_yyyy_mm_dd(today_d - timedelta(days=i)) for i in range(6, -1, -1)]
    trend30 = _build_trend(d30_chart, days_30)
    trend7 = _build_trend(d7_chart, days_7)
    pie = _build_pie(slot_rows)

    # Month-to-yesterday (exclude today) and week-to-yesterday
    yesterday = today_d - timedelta(days=1)
    yesterday_str = _fmt_yyyymmdd(yesterday)
    if yesterday_str >= d7_start:
        week_rows = [r for r in d7_rows if str(r.get("datekey", "")) == yesterday_str]
        week_agg = _aggregate_detail(week_rows) if week_rows else d7_agg
    else:
        week_agg = d7_agg
    if yesterday_str >= d30_start:
        month_rows = [r for r in d30_rows if str(r.get("datekey", "")) == yesterday_str]
        month_agg = _aggregate_detail(month_rows) if month_rows else d30_agg
    else:
        month_agg = d30_agg

    payload = {
        "topic": TOPIC,
        "updatedAt": now.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
        "today": today_agg,
        "week": week_agg,
        "month": month_agg,
        "last7d": d7_agg,
        "last30d": d30_agg,
        "top6": top6,
        "trend30": trend30,
        "trend7": trend7,
        "pie": pie,
        "games_count": {
            "today": len({r.get("gameName") for r in today_rows if r.get("gameName")}),
            "last7d": len({r.get("gameName") for r in d7_rows if r.get("gameName")}),
            "last30d": len({r.get("gameName") for r in d30_rows if r.get("gameName")}),
        },
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--output", default="data.json")
    args = p.parse_args()
    try:
        payload = sync(args.output)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1
    t = payload["today"]
    print(f"[today] cost={t['totalCost']:.2f} new={t['totalNew']:.0f} d1Roi={t['d1Roi']*100:.2f}%")
    print(f"[7d] cost={payload['last7d']['totalCost']:.2f} new={payload['last7d']['totalNew']:.0f}")
    print(f"[30d] cost={payload['last30d']['totalCost']:.2f} new={payload['last30d']['totalNew']:.0f}")
    print(f"top6: {len(payload['top6'])}, pie: {len(payload['pie'])}")
    print(f"written {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())