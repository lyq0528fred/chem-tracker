#!/usr/bin/env python3
"""
化工利润历史分位追踪 — 数据更新脚本
用法: python update_tracker.py <excel1.xlsx> [excel2.xlsx] --template <template.html> --output <output.html>

支持：
- 单文件（化工利润 or 能源毛利）
- 双文件（同时传入两个）
- 自动识别文件类型（按列数 / 指标名称判断）
"""

import sys
import json
import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta


# ── 常量 ──────────────────────────────────────────────────────────────
UNIT_WHITELIST = {"元/吨", "美元/吨"}
BASE_DATE = datetime(2000, 1, 1)
ROLL_WINDOWS = {"3Y": 1095, "5Y": 1825, "10Y": 3650}
CHUNK_SIZE = 38
MAX_CHUNKS = 3


# ── 工具函数 ──────────────────────────────────────────────────────────
def days_since_base(dt: datetime) -> int:
    return (dt - BASE_DATE).days


def percentile_of(arr, val):
    if len(arr) == 0:
        return None
    return float(np.sum(arr <= val) / len(arr) * 100)


def rolling_percentile(series: pd.Series, window_days: int) -> pd.Series:
    dates = series.index
    values = series.values
    result = {}
    for i, (dt, val) in enumerate(zip(dates, values)):
        cutoff = dt - timedelta(days=window_days)
        mask = (dates >= cutoff) & (dates <= dt)
        window_vals = values[mask]
        if len(window_vals) >= 10:
            result[dt] = percentile_of(window_vals, val)
        else:
            result[dt] = None
    return pd.Series(result)


def downsample(series: pd.Series, cutoff_years: int = 3) -> list:
    now = series.index.max()
    cutoff = now - timedelta(days=cutoff_years * 365)
    recent = series[series.index >= cutoff].resample("W").last().dropna()
    older = series[series.index < cutoff].resample("ME").last().dropna()
    combined = pd.concat([older, recent]).sort_index()
    out = []
    for dt, val in combined.items():
        if val is not None and not np.isnan(val):
            out.append([days_since_base(dt.to_pydatetime()), round(float(val), 1)])
    return out


# ── Excel 解析 ────────────────────────────────────────────────────────
def parse_excel(path: str) -> tuple[str, pd.DataFrame]:
    df_raw = pd.read_excel(path, header=None, engine="openpyxl")

    # 按文件名识别来源
    SOURCE_MAP = [
        ("能源", "能源毛利"),
        ("塑料", "塑料"),
        ("化纤", "化纤"),
        ("合成橡胶", "合成橡胶"),
        ("聚氨酯", "聚氨酯"),
        ("煤化工", "煤化工"),
        ("盐化工", "盐化工"),
        ("化工", "化工利润"),
    ]
    fname = Path(path).stem
    source = "化工利润"
    matched = False
    for keyword, label in SOURCE_MAP:
        if keyword in fname:
            source = label
            matched = True
            break
    if not matched:
        names_row_tmp = df_raw.iloc[1].fillna("").astype(str)
        first_name_tmp = names_row_tmp[1] if len(names_row_tmp) > 1 else ""
        if any(k in first_name_tmp for k in ["燃料油", "沥青", "石油焦", "MTBE", "石蜡"]):
            source = "能源毛利"

    data_start = 10
    for i in range(8, min(15, len(df_raw))):
        try:
            pd.to_datetime(df_raw.iloc[i, 0])
            data_start = i
            break
        except Exception:
            continue

    data = df_raw.iloc[data_start:].copy()
    data.columns = range(len(data.columns))
    data[0] = pd.to_datetime(data[0], errors="coerce")
    data = data.dropna(subset=[0]).set_index(0).sort_index()

    series_list = {}
    for col_idx in range(1, df_raw.shape[1]):
        full_name = str(df_raw.iloc[1, col_idx])
        unit = str(df_raw.iloc[2, col_idx])
        if full_name in ("", "nan") or unit not in UNIT_WHITELIST:
            continue
        if col_idx not in data.columns:
            continue
        col_data = pd.to_numeric(data[col_idx], errors="coerce").dropna()
        if len(col_data) < 50:
            continue
        series_list[(full_name, unit)] = col_data

    return source, series_list


def derive_short_name(full_name: str):
    parts = full_name.split("：")
    if len(parts) >= 2:
        product = parts[0]
        method = parts[1] if len(parts) > 1 else ""
        region = parts[-1] if len(parts) > 2 else ""
        region_clean = region.split("（")[0] if "（" in region else region
        short = product + "·" + method if method and method != product else product
        return short, product, method, region_clean
    return full_name, full_name, "", ""


# ── 核心计算 ──────────────────────────────────────────────────────────
def process_series(full_name: str, unit: str, source: str, col_data: pd.Series) -> dict | None:
    col_data = col_data.sort_index()
    latest_val = float(col_data.iloc[-1])
    latest_date = col_data.index[-1]

    month_ago = latest_date - timedelta(days=30)
    past = col_data[col_data.index <= month_ago]
    change = float(latest_val - past.iloc[-1]) if len(past) > 0 else 0.0

    arr = col_data.values
    now_ts = col_data.index.max()

    def pct_window(days):
        cutoff = now_ts - timedelta(days=days)
        w = col_data[col_data.index >= cutoff].values
        if len(w) < 10:
            return None
        return round(percentile_of(w, latest_val), 1)

    pct_all = round(percentile_of(arr, latest_val), 1)
    pct_5y = pct_window(1825)
    pct_3y = pct_window(1095)
    data_years = round((col_data.index[-1] - col_data.index[0]).days / 365.25, 1)

    chart = {}
    for key, days in ROLL_WINDOWS.items():
        roll = rolling_percentile(col_data, days)
        chart[key] = downsample(roll, cutoff_years=3)

    short, product, method, region = derive_short_name(full_name)

    return {
        "meta": {
            "name": short,
            "full_name": full_name,
            "unit": unit,
            "source": source,
            "latest": round(latest_val, 1),
            "change": round(change, 1),
            "pct_3y": pct_3y,
            "pct_5y": pct_5y,
            "pct_all": pct_all,
            "data_years": data_years,
        },
        "chart": chart,
    }


# ── 重复短名去重 ──────────────────────────────────────────────────────
def deduplicate_names(results: list[dict]) -> list[dict]:
    name_count: dict[str, int] = {}
    for r in results:
        n = r["meta"]["name"]
        name_count[n] = name_count.get(n, 0) + 1

    for r in results:
        n = r["meta"]["name"]
        if name_count[n] > 1:
            parts = r["meta"]["full_name"].split("：")
            region = parts[-1] if len(parts) > 1 else ""
            r["meta"]["name"] = f"{n}({region})"
    return results


# ── 分 chunk ─────────────────────────────────────────────────────────
def build_chunks(results: list[dict]):
    chunks = [{} for _ in range(MAX_CHUNKS)]
    cidx = {}
    for i, r in enumerate(results):
        ci = min(i // CHUNK_SIZE, MAX_CHUNKS - 1)
        name = r["meta"]["name"]
        chunks[ci][name] = r["chart"]
        cidx[name] = ci
    return cidx, chunks


# ── 组装 HTML ─────────────────────────────────────────────────────────
def build_html(template_path: str, results: list[dict], output_path: str, latest_date: str):
    cidx, chunks = build_chunks(results)
    raw = [r["meta"] for r in results]

    c0 = json.dumps(chunks[0], ensure_ascii=False, separators=(',', ':'))
    c1 = json.dumps(chunks[1], ensure_ascii=False, separators=(',', ':'))
    c2 = json.dumps(chunks[2], ensure_ascii=False, separators=(',', ':'))
    cidx_json = json.dumps(cidx, ensure_ascii=False, separators=(',', ':'))
    raw_json = json.dumps(raw, ensure_ascii=False, separators=(',', ':'))

    with open(template_path, 'r', encoding='utf-8') as f:
        tmpl = f.read()

    html = tmpl
    html = html.replace('__CIDX__', cidx_json)
    html = html.replace('__C0__', c0)
    html = html.replace('__C1__', c1)
    html = html.replace('__C2__', c2)
    html = html.replace('__RAW__', raw_json)
    html = html.replace('截至 2026-03-20', f'截至 {latest_date}')
    html = html.replace('截至2026-03-20', f'截至{latest_date}')

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"✅ 输出: {output_path}  ({len(html.encode())//1024}KB)")


# ── 主入口 ────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("excels", nargs="+", help="Excel 文件路径")
    ap.add_argument("--template", required=True, help="HTML 模板路径")
    ap.add_argument("--output", default="化工利润历史分位追踪.html", help="输出 HTML 路径")
    args = ap.parse_args()

    all_series = []
    latest_date = "未知"

    for xls_path in args.excels:
        print(f"📂 读取: {xls_path}")
        source, series_dict = parse_excel(xls_path)
        print(f"   来源: {source}，有效品种: {len(series_dict)}")
        for (full_name, unit), col_data in series_dict.items():
            all_series.append((full_name, unit, source, col_data))
            d = col_data.index.max()
            if hasattr(d, 'strftime'):
                latest_date = d.strftime("%Y-%m-%d")

    print(f"⏳ 计算 {len(all_series)} 个品种的历史分位和滚动分位…")
    results = []
    for i, (full_name, unit, source, col_data) in enumerate(all_series):
        r = process_series(full_name, unit, source, col_data)
        if r:
            results.append(r)
        if (i + 1) % 20 == 0:
            print(f"   进度: {i+1}/{len(all_series)}")

    results = deduplicate_names(results)
    print(f"✅ 有效品种: {len(results)}，最新数据日期: {latest_date}")

    build_html(args.template, results, args.output, latest_date)


if __name__ == "__main__":
    main()
