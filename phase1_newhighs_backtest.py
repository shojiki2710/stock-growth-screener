#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3倍株バックテスト: フェーズ1(52週新高値更新)の時点評価
=====================================================================

目的:
  triple_events_*.csv に列挙した「過去1年で3倍になった銘柄」それぞれについて、
  安値(low_date)から高値(high_date)に至る値上がり過程で、実際に52週新高値を
  更新した日をすべて洗い出す。判定ロジックは jquants_screening.py の
  compute_new_highs() と完全に同一(その日のHighが直近365日以内のHighの最大値を
  実際に上回ったか)。

  最も早い新高値更新日が「このファネルが最速で拾えたはずのタイミング」となる。

前提:
  find_3x_stocks.py 実行時に raw_cache_3x/ に保存済みの日次四本値キャッシュを
  そのまま再利用する。新たなAPI呼び出しは行わない(ネットワーク不要)。

出力: phase1_newhighs_YYYYMMDD.csv
  (code, name, seq, new_high_date, high, prior_52w_high, triple_low_date,
   triple_high_date, triple_multiple)
  seq=1がその銘柄における最速の新高値更新日。
"""

import csv
import json
import os
import sys
from datetime import date, datetime, timedelta

CACHE_DIR = "raw_cache_3x"
TRIPLE_EVENTS_FILE = sys.argv[1] if len(sys.argv) > 1 else None


def get_field(row, *keys):
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def load_cache(code):
    path = os.path.join(CACHE_DIR, f"{code}.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return None


def compute_new_highs_until(rows, until_date: date):
    """d < until_date+1 の範囲で、実際に52週新高値を更新した日をすべて返す(古い順)"""
    normalized = []
    for r in rows:
        d_raw = get_field(r, "Date", "date")
        h_raw = get_field(r, "H", "High", "high", "AdjH")
        if d_raw is None or h_raw is None:
            continue
        try:
            d = datetime.strptime(str(d_raw)[:10], "%Y-%m-%d").date()
            h = float(h_raw)
        except (ValueError, TypeError):
            continue
        normalized.append((d, h))
    normalized.sort(key=lambda x: x[0])

    if not normalized:
        return []

    data_start = normalized[0][0]
    scan_start = data_start + timedelta(days=365)

    events = []
    for (d, h) in normalized:
        if d < scan_start or d > until_date:
            continue
        window_start = d - timedelta(days=365)
        prior_highs = [ph for (pd, ph) in normalized if window_start <= pd < d]
        if not prior_highs:
            continue
        prior_52w_high = max(prior_highs)
        if h > prior_52w_high:
            events.append({"date": d.isoformat(), "high": h, "prior_52w_high": prior_52w_high})
    return events


def main():
    if not TRIPLE_EVENTS_FILE:
        print("使い方: python3 phase1_newhighs_backtest.py triple_events_YYYYMMDD.csv")
        sys.exit(1)

    with open(TRIPLE_EVENTS_FILE, encoding="utf-8-sig") as f:
        triples = list(csv.DictReader(f))

    out_rows = []
    no_event_codes = []

    for t in triples:
        code = t["code"].strip()
        name = t["name"].strip()
        high_date = datetime.strptime(t["high_date"], "%Y-%m-%d").date()

        bars = load_cache(code)
        if not bars:
            no_event_codes.append((code, name, "キャッシュなし"))
            continue

        events = compute_new_highs_until(bars, high_date)
        if not events:
            no_event_codes.append((code, name, "52週新高値更新イベントなし"))
            continue

        for seq, e in enumerate(events, 1):
            out_rows.append({
                "code": code,
                "name": name,
                "seq": seq,
                "new_high_date": e["date"],
                "high": e["high"],
                "prior_52w_high": e["prior_52w_high"],
                "triple_low_date": t["low_date"],
                "triple_high_date": t["high_date"],
                "triple_multiple": t["multiple"],
            })

    today = date.today()
    output_file = f"phase1_newhighs_{today:%Y%m%d}.csv"
    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "code", "name", "seq", "new_high_date", "high", "prior_52w_high",
            "triple_low_date", "triple_high_date", "triple_multiple"
        ])
        writer.writeheader()
        for r in out_rows:
            writer.writerow(r)

    print(f"対象3倍株: {len(triples)}件")
    print(f"新高値更新イベント合計: {len(out_rows)}件 → {output_file}")
    print(f"最速更新日(seq=1)の件数: {sum(1 for r in out_rows if r['seq'] == 1)}件")
    if no_event_codes:
        print(f"\n[注意] 以下の{len(no_event_codes)}銘柄は新高値更新イベントが検出できませんでした:")
        for code, name, reason in no_event_codes:
            print(f"  {code} {name}: {reason}")


if __name__ == "__main__":
    main()
