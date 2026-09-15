#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
フェーズ3(損切り)スクリーニング(v3): 「最初の新高値更新日」だけでなく、
上昇過程で何度も起きる新高値更新イベントすべてについて、
「そこで買って3倍到達日まで持っていたら、-7%の損切りラインに
一度も触れずに済んだか」を機械的にチェックする。

ネットワーク不要・完全ローカルで動作します。

前提ファイル(同じフォルダに置いてあるはず):
  - triple_events_true_20260906.csv (各銘柄の真の安値日・3倍到達日)
  - phase1_newhighs_v2_20260906.csv (各銘柄の新高値更新イベント一覧・全seq)
  - raw_cache_3x_v2/{code}.json (日次株価キャッシュ)

使い方:
  python3 phase3_all_events_screen.py

出力: phase3_survivors_YYYYMMDD.csv
  損切りに一度も触れずに3倍到達日まで生き残れたイベントのみを出力する。
  (stop_loss_pct列で閾値を変えて再確認できるよう、実際の最大ドローダウンも出力)
"""

import csv
import json
import os
from datetime import date, datetime, timedelta

CACHE_DIR = "raw_cache_3x_v2"
TRIPLE_TRUE_FILE = "triple_events_true_20260906.csv"
NEWHIGHS_FILE = "phase1_newhighs_v2_20260906.csv"
STOP_LOSS_PCT = -7.0  # この%以下に一度でも触れたら損切りとみなす


def get_field(row, *keys):
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


_cache_mem = {}


def load_cache(code):
    if code in _cache_mem:
        return _cache_mem[code]
    path = os.path.join(CACHE_DIR, f"{code}.json")
    if not os.path.exists(path):
        _cache_mem[code] = None
        return None
    with open(path, encoding="utf-8") as f:
        rows = json.load(f)
    norm = []
    for r in rows:
        d_raw = get_field(r, "Date", "date")
        c_raw = get_field(r, "C", "Close", "close", "AdjC")
        l_raw = get_field(r, "L", "Low", "low", "AdjL")
        if d_raw is None or c_raw is None:
            continue
        try:
            d = datetime.strptime(str(d_raw)[:10], "%Y-%m-%d").date()
            c = float(c_raw)
            lo = float(l_raw) if l_raw is not None else c
        except (ValueError, TypeError):
            continue
        norm.append((d, c, lo))
    norm.sort(key=lambda x: x[0])
    _cache_mem[code] = norm
    return norm


def main():
    with open(TRIPLE_TRUE_FILE, encoding="utf-8-sig") as f:
        true_events = {r["code"].strip(): r for r in csv.DictReader(f)}

    with open(NEWHIGHS_FILE, encoding="utf-8-sig") as f:
        events = list(csv.DictReader(f))

    survivors = []
    total_checked = 0

    for e in events:
        code = e["code"].strip()
        name = e["name"].strip()
        t = true_events.get(code)
        if not t:
            continue
        true_high_date = datetime.strptime(t["high_date"], "%Y-%m-%d").date()
        true_high_price = float(t["high_price"])

        entry_date = datetime.strptime(e["new_high_date"], "%Y-%m-%d").date()
        if entry_date >= true_high_date:
            continue

        norm = load_cache(code)
        if not norm:
            continue

        # エントリー日の終値を取得(完全一致がなければ直後の営業日)
        entry_price = None
        entry_idx = None
        for i, (d, c, lo) in enumerate(norm):
            if d >= entry_date:
                entry_price = c
                entry_idx = i
                break
        if entry_price is None:
            continue

        total_checked += 1

        worst_pct = 0.0
        worst_date = ""
        triggered = False
        for (d, c, lo) in norm[entry_idx + 1:]:
            if d > true_high_date:
                break
            pct = (lo / entry_price - 1) * 100  # 日中安値ベースで判定(厳しめ)
            if pct < worst_pct:
                worst_pct = pct
                worst_date = d.isoformat()
            if pct <= STOP_LOSS_PCT:
                triggered = True
                break

        if not triggered:
            final_return = round((true_high_price / entry_price - 1) * 100, 2)
            hold_days = (true_high_date - entry_date).days
            survivors.append({
                "code": code, "name": name, "seq": e["seq"],
                "entry_date": entry_date.isoformat(), "entry_price": entry_price,
                "true_high_date": true_high_date.isoformat(),
                "true_high_price": true_high_price,
                "hold_days": hold_days,
                "worst_drawdown_pct": round(worst_pct, 2),
                "worst_drawdown_date": worst_date,
                "final_return_pct": final_return,
            })

    today = date.today()
    output_file = f"phase3_survivors_{today:%Y%m%d}.csv"
    fieldnames = ["code", "name", "seq", "entry_date", "entry_price",
                  "true_high_date", "true_high_price", "hold_days",
                  "worst_drawdown_pct", "worst_drawdown_date", "final_return_pct"]
    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in sorted(survivors, key=lambda x: (x["code"], int(x["seq"]))):
            writer.writerow(r)

    codes_survived = sorted(set(r["code"] for r in survivors))
    print(f"チェックしたイベント数: {total_checked}")
    print(f"損切り(-7%)に一度も触れずに3倍到達日まで生き残れたイベント数: {len(survivors)}")
    print(f"該当銘柄数: {len(codes_survived)} → {codes_survived}")
    print(f"出力: {output_file}")


if __name__ == "__main__":
    main()
