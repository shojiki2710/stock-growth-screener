#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
フェーズ2/3 検証用メトリクス計算(v2・修正後エントリー日ベース)
=====================================================================

フェーズ1(ファンダメンタルズ)に合格した10銘柄について、
raw_cache_3x_v2/{code}.json のキャッシュ済み日次データだけを使い、
以下をエントリー日(=52週新高値更新日)時点で計算する。
ネットワーク不要・完全ローカルで動作します。

- 出来高が直近20営業日平均の何倍か(薄商いでない/異常な急騰でないかの確認)
- SMA50・SMA150・SMA200とその並び(上昇トレンドか)
- エントリー後5・10・15営業日のリターン
- エントリー後10営業日以内に-7~8%の損切りラインに触れたか

使い方:
  python3 phase2_metrics_v2.py

出力: phase2_metrics_v2_YYYYMMDD.csv
"""

import csv
import json
import os
from datetime import date, datetime, timedelta

CACHE_DIR = "raw_cache_3x_v2"

CANDIDATES = [
    ("36920", "株式会社ＦＦＲＩセキュリティ", "2024-09-27"),
    ("95560", "ＩＮＴＬＯＯＰ株式会社", "2024-10-16"),
    ("55920", "株式会社くすりの窓口", "2025-02-14"),
    ("56210", "株式会社ヒューマンテクノロジーズ", "2025-03-27"),
    ("76940", "株式会社いつも", "2025-05-08"),
    ("58920", "株式会社ｙｕｔｏｒｉ", "2025-05-13"),
    ("73180", "セレンディップ・ホールディングス株式会社", "2025-05-13"),
    ("92400", "株式会社デリバリーコンサルティング", "2025-06-25"),
    ("55320", "株式会社リアルゲイト", "2025-08-15"),
    ("92710", "株式会社和心", "2025-08-18"),
]


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


def normalize(rows):
    out = []
    for r in rows:
        d_raw = get_field(r, "Date", "date")
        c_raw = get_field(r, "C", "Close", "close", "AdjC")
        vo_raw = get_field(r, "Vo", "Volume", "volume", "AdjVo")
        if d_raw is None or c_raw is None:
            continue
        try:
            d = datetime.strptime(str(d_raw)[:10], "%Y-%m-%d").date()
            c = float(c_raw)
            vo = float(vo_raw) if vo_raw is not None else None
        except (ValueError, TypeError):
            continue
        out.append((d, c, vo))
    out.sort(key=lambda x: x[0])
    return out


def sma(values, n):
    if len(values) < n:
        return None
    return sum(values[-n:]) / n


def main():
    results = []
    for code, name, entry_str in CANDIDATES:
        entry_date = datetime.strptime(entry_str, "%Y-%m-%d").date()
        bars = load_cache(code)
        if not bars:
            print(f"[警告] {code} {name}: キャッシュなし")
            continue
        norm = normalize(bars)

        idx = None
        for i, (d, c, vo) in enumerate(norm):
            if d == entry_date:
                idx = i
                break
        if idx is None:
            # 完全一致がなければ直近の営業日を使う
            for i, (d, c, vo) in enumerate(norm):
                if d >= entry_date:
                    idx = i
                    break
        if idx is None:
            print(f"[警告] {code} {name}: エントリー日{entry_date}のデータなし")
            continue

        entry_price = norm[idx][1]
        entry_vol = norm[idx][2]

        closes_upto = [c for (d, c, vo) in norm[:idx + 1]]
        vols_upto = [vo for (d, c, vo) in norm[:idx + 1] if vo is not None]

        sma50 = sma(closes_upto, 50)
        sma150 = sma(closes_upto, 150)
        sma200 = sma(closes_upto, 200)
        sma_order_ok = (sma50 is not None and sma150 is not None and sma200 is not None
                         and sma50 > sma150 > sma200)

        avg_vol_20 = None
        vol_ratio = None
        if len(vols_upto) >= 21:
            avg_vol_20 = sum(vols_upto[-21:-1]) / 20  # エントリー日を除く直近20日平均
            if avg_vol_20 and entry_vol is not None:
                vol_ratio = entry_vol / avg_vol_20

        def ret_after(n):
            j = idx + n
            if j < len(norm):
                return round((norm[j][1] / entry_price - 1) * 100, 2)
            return None

        ret5 = ret_after(5)
        ret10 = ret_after(10)
        ret15 = ret_after(15)

        stop_triggered = False
        stop_date = ""
        stop_pct = ""
        for j in range(idx + 1, min(idx + 11, len(norm))):
            pct = (norm[j][1] / entry_price - 1) * 100
            if pct <= -7.0:
                stop_triggered = True
                stop_date = norm[j][0].isoformat()
                stop_pct = round(pct, 2)
                break

        results.append({
            "code": code, "name": name, "entry_date": entry_date.isoformat(),
            "entry_price": entry_price,
            "vol_ratio_20d": round(vol_ratio, 2) if vol_ratio is not None else "",
            "sma50": round(sma50, 1) if sma50 else "",
            "sma150": round(sma150, 1) if sma150 else "",
            "sma200": round(sma200, 1) if sma200 else "",
            "sma_order_ok": sma_order_ok,
            "stop_loss_triggered_10d": stop_triggered,
            "stop_loss_date": stop_date,
            "stop_loss_pct": stop_pct,
            "return_5d_pct": ret5,
            "return_10d_pct": ret10,
            "return_15d_pct": ret15,
        })

    today = date.today()
    output_file = f"phase2_metrics_v2_{today:%Y%m%d}.csv"
    fieldnames = ["code", "name", "entry_date", "entry_price", "vol_ratio_20d",
                  "sma50", "sma150", "sma200", "sma_order_ok",
                  "stop_loss_triggered_10d", "stop_loss_date", "stop_loss_pct",
                  "return_5d_pct", "return_10d_pct", "return_15d_pct"]
    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    print(f"{len(results)}銘柄分を {output_file} に出力しました。")
    for r in results:
        print(r)


if __name__ == "__main__":
    main()
