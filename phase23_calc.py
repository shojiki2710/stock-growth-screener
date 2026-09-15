import json, statistics
from datetime import datetime

stocks = [
    ("36920", "FFRIセキュリティ", "2025-07-28"),
    ("73180", "セレンディップHD", "2025-07-28"),
    ("92710", "和心", "2025-08-18"),
    ("92400", "デリバリーコンサルティング", "2025-10-07"),
    ("45700", "免疫生物研究所", "2025-11-06"),
    ("76850", "BuySell Technologies", "2026-01-05"),
]

base = "/Users/CozyPlace/Documents/kabu_screening/raw_cache_3x/"
# but we're running via device_bash which mounts as $HOME/mnt/Documents
import os
base = os.path.expanduser("~/mnt/Documents/kabu_screening/raw_cache_3x/")

for code, name, hd in stocks:
    path = base + code + ".json"
    with open(path) as f:
        data = json.load(f)
    for d in data:
        d["_dt"] = datetime.strptime(d["Date"], "%Y-%m-%d")
    data.sort(key=lambda d: d["_dt"])
    hd_dt = datetime.strptime(hd, "%Y-%m-%d")
    # find index of high date (or nearest available date <= hd, but should be exact)
    idx = None
    for i, d in enumerate(data):
        if d["Date"] == hd:
            idx = i
            break
    print(f"=== {code} {name} (new_high_date={hd}) ===")
    if idx is None:
        print("  !! date not found in cache, closest dates:", [d["Date"] for d in data if abs((d["_dt"]-hd_dt).days) <= 5])
        continue

    day = data[idx]
    close_hd = day["C"]
    vol_hd = day["Vo"]
    print(f"  Close={close_hd}, Vol={vol_hd}, High={day['H']}, Low={day['L']}")

    # volume check: trailing 20d avg volume BEFORE idx (not including idx)
    for window in (20, 60):
        prior = data[max(0, idx-window):idx]
        if prior:
            avg_vol = statistics.mean([d["Vo"] for d in prior])
            ratio = vol_hd / avg_vol if avg_vol else float('nan')
            print(f"  Vol vs trailing {window}d avg: avg={avg_vol:.0f}, ratio={ratio:.2f}x")
        else:
            print(f"  Vol vs trailing {window}d avg: insufficient data (only {len(prior)} bars)")

    # SMA check: 50/150/200 using close, ending at idx (inclusive)
    smas = {}
    for period in (50, 150, 200):
        window = data[max(0, idx-period+1):idx+1]
        if len(window) >= period:
            smas[period] = statistics.mean([d["C"] for d in window])
        else:
            smas[period] = None
    print(f"  Data available: {idx+1} bars before/incl high date (need 200 for full SMA200)")
    for p in (50,150,200):
        v = smas[p]
        print(f"  SMA{p} = {v:.2f}" if v is not None else f"  SMA{p} = N/A (insufficient data, only {idx+1} bars)")
    if all(smas[p] is not None for p in (50,150,200)):
        cond = smas[50] > smas[150] > smas[200]
        print(f"  SMA50>SMA150>SMA200 : {'OK' if cond else 'NG'} ({smas[50]:.1f} vs {smas[150]:.1f} vs {smas[200]:.1f})")
    else:
        # partial check with whatever available
        avail = [p for p in (50,150,200) if smas[p] is not None]
        print(f"  Partial SMA check only ({avail} available)")

    # Phase 3: simulate entry at close_hd, track next 7-10 trading days
    window3 = data[idx+1: idx+1+10]
    print(f"  Phase3 window: {len(window3)} trading days available after high date")
    entry = close_hd
    min_ret = None
    min_ret_date = None
    stop_triggered = False
    stop_date = None
    stop_ret = None
    for d in window3:
        ret_low = (d["L"] - entry) / entry
        if min_ret is None or ret_low < min_ret:
            min_ret = ret_low
            min_ret_date = d["Date"]
        if ret_low <= -0.07 and not stop_triggered:
            stop_triggered = True
            stop_date = d["Date"]
            stop_ret = ret_low
    if window3:
        last = window3[-1]
        end_ret = (last["C"] - entry) / entry
    else:
        end_ret = None
    print(f"  Entry(Close@HD)={entry}")
    print(f"  Min return (using Low) over window = {min_ret*100:.2f}% on {min_ret_date}" if min_ret is not None else "  No forward data")
    if stop_triggered:
        print(f"  STOP-LOSS triggered on {stop_date}: {stop_ret*100:.2f}%")
    else:
        print(f"  No stop-loss triggered. End-of-window return (Close) = {end_ret*100:.2f}%" if end_ret is not None else "  N/A")
    print()
