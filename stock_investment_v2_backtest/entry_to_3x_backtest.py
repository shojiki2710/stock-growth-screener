#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
「約定価格から3倍」バックテスト(正しい定義版)
=====================================================================

前提条件(確認済み):
  - 仮想約定価格 = 52週新高値を更新した日の終値
  - 損切り閾値 = 約定価格から -8%(日中安値ベースで判定)
  - 損切りルールは保有期間中ずっと有効(常に-8%を下回ったら機械的に損切り)
  - 「約定価格から3倍」の達成期限 = 約定から1年以内(365日)

raw_cache_3x_v2/ にある63銘柄全ての日次データ(2022年以降)を使い、
各銘柄の全ての52週新高値更新イベントについて、約定日翌日から365日以内に
- 先に高値が約定価格の3倍に到達すれば SUCCESS
- 先に安値が約定価格の-8%を下回れば STOPPED
- どちらも起きずに365日経過(またはデータ終了)すれば TIMEOUT
のいずれかを判定する。

ネットワーク不要・完全ローカルで動作します。

使い方:
  python3 entry_to_3x_backtest.py

出力: entry_to_3x_results_YYYYMMDD.csv (全イベントの判定結果)
"""

import csv
import glob
import json
import os
from datetime import date, datetime, timedelta

CACHE_DIR = "raw_cache_3x_v2"
STOP_LOSS_PCT = -8.0
TARGET_MULTIPLE = 3.0
DEADLINE_DAYS = 365


def get_field(row, *keys):
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def normalize(rows):
    out = []
    for r in rows:
        d_raw = get_field(r, "Date", "date")
        h_raw = get_field(r, "H", "High", "high")
        l_raw = get_field(r, "L", "Low", "low")
        c_raw = get_field(r, "C", "Close", "close")
        if d_raw is None or h_raw is None or l_raw is None or c_raw is None:
            continue
        try:
            d = datetime.strptime(str(d_raw)[:10], "%Y-%m-%d").date()
            h, lo, c = float(h_raw), float(l_raw), float(c_raw)
        except (ValueError, TypeError):
            continue
        out.append((d, h, lo, c))
    out.sort(key=lambda x: x[0])
    return out


def find_new_high_events(norm):
    """52週新高値更新イベントを全て検出する(jquants_screening.pyと同一ロジック)"""
    if not norm:
        return []
    data_start = norm[0][0]
    scan_start = data_start + timedelta(days=365)
    events = []
    for (d, h, lo, c) in norm:
        if d < scan_start:
            continue
        window_start = d - timedelta(days=365)
        prior_highs = [ph for (pd, ph, pl, pc) in norm if window_start <= pd < d]
        if not prior_highs:
            continue
        if h > max(prior_highs):
            events.append((d, c))  # 約定価格 = その日の終値
    return events


def simulate(norm, entry_idx, entry_price):
    deadline = norm[entry_idx][0] + timedelta(days=DEADLINE_DAYS)
    stop_price = entry_price * (1 + STOP_LOSS_PCT / 100)
    target_price = entry_price * TARGET_MULTIPLE

    for (d, h, lo, c) in norm[entry_idx + 1:]:
        if d > deadline:
            return "TIMEOUT", None, None
        hit_stop = lo <= stop_price
        hit_target = h >= target_price
        if hit_stop and hit_target:
            # 同日に両方触れた場合は保守的に損切り優先とみなす
            return "STOPPED", d.isoformat(), round((lo / entry_price - 1) * 100, 2)
        if hit_stop:
            return "STOPPED", d.isoformat(), round((lo / entry_price - 1) * 100, 2)
        if hit_target:
            return "SUCCESS", d.isoformat(), round((h / entry_price - 1) * 100, 2)
    return "TIMEOUT", None, None


def main():
    files = sorted(glob.glob(os.path.join(CACHE_DIR, "*.json")))
    all_results = []

    for path in files:
        code = os.path.basename(path).replace(".json", "")
        with open(path, encoding="utf-8") as f:
            rows = json.load(f)
        norm = normalize(rows)
        events = find_new_high_events(norm)

        date_to_idx = {d: i for i, (d, h, lo, c) in enumerate(norm)}

        for (entry_date, entry_price) in events:
            entry_idx = date_to_idx[entry_date]
            outcome, event_date, pct = simulate(norm, entry_idx, entry_price)
            hold_days = (datetime.strptime(event_date, "%Y-%m-%d").date() - entry_date).days if event_date else ""
            all_results.append({
                "code": code, "entry_date": entry_date.isoformat(), "entry_price": entry_price,
                "outcome": outcome, "outcome_date": event_date or "", "outcome_pct": pct if pct is not None else "",
                "hold_days": hold_days,
            })

    today = date.today()
    output_file = f"entry_to_3x_results_{today:%Y%m%d}.csv"
    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["code", "entry_date", "entry_price", "outcome", "outcome_date", "outcome_pct", "hold_days"])
        writer.writeheader()
        for r in all_results:
            writer.writerow(r)

    total = len(all_results)
    n_success = sum(1 for r in all_results if r["outcome"] == "SUCCESS")
    n_stopped = sum(1 for r in all_results if r["outcome"] == "STOPPED")
    n_timeout = sum(1 for r in all_results if r["outcome"] == "TIMEOUT")
    codes_with_success = sorted(set(r["code"] for r in all_results if r["outcome"] == "SUCCESS"))

    print(f"対象銘柄数: {len(files)}")
    print(f"全新高値更新イベント数: {total}")
    print(f"  SUCCESS(1年以内に約定価格の3倍到達): {n_success} ({n_success/total*100:.1f}%)")
    print(f"  STOPPED(-8%で損切り): {n_stopped} ({n_stopped/total*100:.1f}%)")
    print(f"  TIMEOUT(1年経過・未達): {n_timeout} ({n_timeout/total*100:.1f}%)")
    print(f"SUCCESSが1回以上あった銘柄数: {len(codes_with_success)} → {codes_with_success}")
    print(f"出力: {output_file}")


if __name__ == "__main__":
    main()
