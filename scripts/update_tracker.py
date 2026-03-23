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
    return pd.Series(resu
