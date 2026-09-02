import time

import requests

BASE = "http://127.0.0.1:8033"
START, END = "2026-08-03", "2026-08-31"  # 约一个月，21 个交易日


def run(mode):
    t0 = time.time()
    r = requests.post(
        BASE + "/api/analyze_range",
        json={"start": START, "end": END, "mode": mode},
        timeout=600,
    )
    dt = time.time() - t0
    j = r.json()
    days = len(j.get("trade_days", []))
    if j.get("ok"):
        total = {k: res["count"] for k, res in j["results"].items()}
    else:
        total = j.get("error")
    print(f"[{mode}] 状态 {r.status_code} 交易日 {days} 耗时 {dt:.1f}s 各策略命中 {total}")
    return total


t_s = run("single")
t_m = run("multi")
print("结果一致:", t_s == t_m)
