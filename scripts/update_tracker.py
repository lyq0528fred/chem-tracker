#!/usr/bin/env python3
"""
化工利润历史分位追踪 — 数据更新脚本
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
MAX_CHUNKS = 7

# ── 品类分类表 ─────────────────────────────────────────────────────────
CAT = {
    "船用燃料油·180CST":"油品调和","沥青·生产毛利":"油品调和","石油焦·延迟焦化":"油品调和",
    "煅烧焦·低硫":"油品调和","煅烧焦·中硫焦":"油品调和","再生油·生产毛利":"油品调和",
    "加氢尾油·生产毛利":"油品调和","烷基化油·生产毛利":"油品调和","MTBE·气分醚化":"油品调和",
    "MTBE·异构醚化":"油品调和","石蜡·生产毛利":"油品调和",
    "芳烃溶剂油·高沸点":"芳烃","裂解C5·生产毛利":"烯烃",
    "乙烯·MTO":"烯烃","乙烯·石脑油裂解":"烯烃","乙烯·轻烃裂解":"烯烃",
    "丙烯·MTO":"烯烃","丙烯·PDH制":"烯烃","丙烯·石脑油裂解":"烯烃",
    "丁二烯·丁烯氧化脱氢":"烯烃","丁二烯·碳四抽提法":"烯烃",
    "纯苯·生产毛利":"芳烃","邻二甲苯·生产毛利":"芳烃","二甲苯·生产毛利":"芳烃",
    "苯乙烯·非一体化装置":"芳烃","苯乙烯·一体化装置":"芳烃","PX·生产毛利":"芳烃",
    "甲苯·生产毛利":"芳烃","加氢苯·生产毛利":"芳烃","酚酮·生产毛利":"芳烃",
    "双酚A·生产毛利":"芳烃","苯酐·工业萘氧化法":"芳烃","苯酐·进口":"芳烃",
    "苯酐·邻二甲苯氧化法":"芳烃","苯胺·生产毛利":"芳烃",
    "乙醇·木薯普级":"醇醚酮","乙醇·糖蜜普级":"醇醚酮","异丙醇·丙酮加氢法":"醇醚酮",
    "异丙醇·丙烯水合法":"醇醚酮","正丁醇·生产毛利":"醇醚酮","辛醇·生产毛利":"醇醚酮",
    "异丁醇·生产毛利":"醇醚酮","异丁醛·生产毛利":"醇醚酮","新戊二醇·生产毛利":"醇醚酮",
    "丁酮·生产毛利":"醇醚酮","环己酮·生产毛利":"醇醚酮","MIBK·生产毛利":"醇醚酮",
    "碳酸二甲酯·生产毛利":"醇醚酮","二甲醚·生产毛利":"醇醚酮","甲醇·焦炉气制":"醇醚酮",
    "甲醇·煤制":"醇醚酮","甲醇·天然气制甲醇":"醇醚酮","甲醛·生产毛利":"醇醚酮",
    "甲烷氯化物·生产毛利":"醇醚酮","二甲基甲酰胺·生产毛利":"醇醚酮",
    "GBL·生产毛利":"醇醚酮","NMP·生产毛利":"醇醚酮",
    "冰醋酸·生产毛利":"有机酸酯","醋酸仲丁酯·生产毛利":"有机酸酯",
    "醋酸乙烯·电石法":"有机酸酯","醋酸乙烯·乙烯法":"有机酸酯",
    "聚乙烯醇·生产毛利":"有机酸酯","乙酸酐·合成":"有机酸酯","乙酸酐·裂解":"有机酸酯",
    "醋酸丁酯·生产毛利":"有机酸酯","乙酸乙酯·生产毛利":"有机酸酯",
    "己二酸·生产毛利":"有机酸酯","己二酸·环己烷法":"有机酸酯","己二酸·环己烯法":"有机酸酯",
    "丙烯酸·生产毛利":"丙烯酸酯","丙烯酸甲酯·生产毛利":"丙烯酸酯",
    "丙烯酸乙酯·生产毛利":"丙烯酸酯","丙烯酸丁酯·生产毛利":"丙烯酸酯",
    "丙烯酸异辛酯·生产毛利":"丙烯酸酯","MMA·ACH法":"丙烯酸酯","MMA·C4法":"丙烯酸酯",
    "甲基丙烯酸·生产毛利":"丙烯酸酯","PMMA·生产毛利":"丙烯酸酯",
    "DOP·生产毛利":"增塑剂","DBP·生产毛利":"增塑剂","邻苯二甲酸二壬酯·生产毛利":"增塑剂",
    "环氧丙烷·氯醇法":"环氧产业链","环氧丙烷·异丁烷共氧化法":"环氧产业链",
    "环氧丙烷·直接氧化法":"环氧产业链","环氧乙烷·甲醇制烯烃法":"环氧产业链",
    "环氧乙烷·外采进口乙烯":"环氧产业链","环氧树脂·E-12":"环氧产业链",
    "环氧树脂·E-51":"环氧产业链","环氧氯丙烷·甘油法":"环氧产业链",
    "环氧氯丙烷·高温丙烯氯化法":"环氧产业链",
    "MEG·甲醇制":"乙二醇","MEG·煤基合成气制":"乙二醇","MEG·石脑油一体化制":"乙二醇",
    "MEG·乙烯制":"乙二醇","碳酸乙烯酯·电池级":"乙二醇","碳酸甲乙酯·生产毛利":"乙二醇",
    "乙二醇丁醚·生产毛利":"乙二醇","乙二醇乙醚醋酸酯·生产毛利":"乙二醇",
    "聚羧酸减水剂单体·生产毛利":"乙二醇","一乙醇胺·生产毛利":"乙二醇",
    "二乙醇胺·生产毛利":"乙二醇","三乙醇胺·生产毛利":"乙二醇",
    "丙二醇甲醚·生产毛利":"乙二醇","丙二醇甲醚醋酸酯·生产毛利":"乙二醇",
    "1,4-丁二醇·生产毛利":"乙二醇","四氢呋喃·生产毛利":"乙二醇",
    "聚四亚甲基醚二醇·生产毛利":"乙二醇",
    "乙腈·生产毛利":"化纤原料","丙烯腈·生产毛利":"化纤原料","PTA·生产毛利":"化纤原料",
    "PTA·加工费":"化纤原料","己内酰胺·生产毛利":"化纤原料",
    "BOPET·生产毛利":"化纤成品","涤纶工业丝·生产毛利":"化纤成品",
    "聚酯纤维短纤·生产毛利":"化纤成品","聚酯纤维长丝·DTY":"化纤成品",
    "聚酯纤维长丝·FDY":"化纤成品","聚酯纤维长丝·POY":"化纤成品",
    "PET切片·纤维":"化纤成品","PET瓶片·生产毛利":"化纤成品","PA6·常规纺有光":"化纤成品",
    "锦纶长丝·生产毛利":"化纤成品","腈纶·生产毛利":"化纤成品",
    "再生中空·生产毛利":"化纤成品","再生普纤·瓶片熔融纺":"化纤成品","粘胶短纤·湿纺法":"化纤成品",
    "硫酸·生产毛利":"无机化工","硝酸·生产毛利":"无机化工","重质纯碱·氨碱法":"无机化工",
    "重质纯碱·联产法":"无机化工","纯碱·联碱法":"无机化工","轻质纯碱·联产法":"无机化工",
    "碳化钙·生产毛利":"无机化工","双氧水·生产毛利":"无机化工","钛白粉·生产毛利":"无机化工",
    "氯碱·生产毛利":"无机化工","硫酸铵·生产毛利":"无机化工",
    "顺酐·正丁烷氧化法":"溶剂涂料","UPR·生产毛利":"溶剂涂料",
    "天然胶乳·生产毛利":"合成橡胶","干胶·RSS3":"合成橡胶","干胶·STR20":"合成橡胶",
    "干胶·TSR9710":"合成橡胶","丁苯橡胶·生产毛利":"合成橡胶","顺丁橡胶·生产毛利":"合成橡胶",
    "SBS·791-H":"合成橡胶","SBS·F875":"合成橡胶","丁腈橡胶·贸易毛利":"合成橡胶",
    "三元乙丙橡胶·生产毛利":"合成橡胶","炭黑·N330":"合成橡胶",
    "促进剂·生产毛利":"合成橡胶","防老剂·生产毛利":"合成橡胶",
    "软泡聚醚·生产毛利":"聚氨酯","POP聚醚·生产毛利":"聚氨酯","弹性体聚醚·生产毛利":"聚氨酯",
    "高回弹聚醚·生产毛利":"聚氨酯","硬泡聚醚·生产毛利":"聚氨酯",
    "甲苯二异氰酸酯·生产毛利":"聚氨酯","聚合MDI·生产毛利":"聚氨酯","纯MDI·生产毛利":"聚氨酯",
    "三羟甲基丙烷·生产毛利":"聚氨酯","聚氨酯弹性体·生产毛利":"聚氨酯",
    "聚对苯二甲酸丁二酯·生产毛利":"聚氨酯",
    "中温煤焦油·生产毛利":"煤化工","兰炭·生产毛利":"煤化工","高温煤焦油·生产毛利":"煤化工",
    "煤沥青·深加工":"煤化工","甲醇制烯烃·生产毛利":"煤化工",
    "LDPE·油制":"塑料","LLDPE·甲醇制":"塑料","LLDPE·煤制":"塑料","LLDPE·乙烯制":"塑料",
    "PE·轻烃制":"塑料","HDPE·油制":"塑料","PP粉·MTO":"塑料","PP粉·PDH制":"塑料",
    "PS·生产毛利":"塑料","ABS·生产毛利":"塑料","EPS·生产毛利":"塑料","下游行业·EVA":"塑料",
    "PP·MTO":"塑料","PP·PDH制":"塑料","PP·煤制":"塑料","PP·外采丙烯制":"塑料","PP·油制":"塑料",
    "PVC·电石法":"塑料","PVC·乙烯法":"塑料","PVC糊树脂·生产毛利":"塑料",
    "PC·非光气法":"塑料","PC·界面缩聚光气法":"塑料","PP无纺布·生产毛利":"塑料",
    "胶带母卷·生产毛利":"塑料","CPP·生产毛利":"塑料","BOPP·生产毛利":"塑料",
    "聚己二酸/对苯二甲酸丁二醇酯·生产毛利":"塑料","聚乳酸·生产毛利":"塑料",
}


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
            "cat": CAT.get(short, ""),
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
            new_name = f"{n}({region})"
            r["meta"]["name"] = new_name
            r["meta"]["cat"] = CAT.get(new_name, CAT.get(n, ""))
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

    cidx_json = json.dumps(cidx, ensure_ascii=False, separators=(',', ':'))
    raw_json = json.dumps(raw, ensure_ascii=False, separators=(',', ':'))

    with open(template_path, 'r', encoding='utf-8') as f:
        tmpl = f.read()

    html = tmpl
    html = html.replace('__CIDX__', cidx_json)
    for i, chunk in enumerate(chunks):
        html = html.replace(f'__C{i}__', json.dumps(chunk, ensure_ascii=False, separators=(',', ':')))

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
