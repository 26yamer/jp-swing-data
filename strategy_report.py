#!/usr/bin/env python3
"""新戦略（長期保有・配分管理）の計算。

このリポジトリは公開なので、**保有銘柄と金額はここに置かない**。動かし方は2つ:

  python strategy_report.py --fetch
      GitHub Actions（strategy.yml）が実行する。strategy_watchlist.json の銘柄と
      小型割安株の候補の価格を取り data/strategy_px/ に保存し、ビットコインの
      買い増し判定を data/strategy_market.json に書く。保有には一切触れない。

  python strategy_report.py --portfolio 台帳.json --history 履歴.csv
      非公開の台帳（Claude 側で保管）を渡して、手元でレポートを作る。
      結果は標準出力に出すだけで、リポジトリには何も書かない。

レポートの中身（すべて機械計算。予測はしない）:
  1. 配分のずれ   … 目標(株55/金10/BTC25/小型10)と現在の比率
  2. BTCの判定    … 直近の完了月の平均 > その前10か月の平均 なら「買い増し可」
  3. 高値からの下落 … 履歴ファイルに総額を毎日記録し、入金の影響を除いて計算
  4. 今日やること … 上の3つと移行計画から、動く日だけ金額つきで出す
"""
import argparse, csv, json, os, re, sys, datetime as dt

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "data")
OHLCV = os.path.join(DATA, "ohlcv")
PX = os.path.join(DATA, "strategy_px")
JST = dt.timezone(dt.timedelta(hours=9))
SLEEVES = [("stock", "株の指数"), ("gold", "金"), ("btc", "ビットコイン"), ("smallcap", "小型割安株")]
BTC = "BTC-JPY"


def fname(code):
    return re.sub(r"[^0-9A-Za-z.]", "_", code) + ".csv"


def load_series(code):
    """[(date, close, adj)] を古い順で返す。無ければ []。"""
    best = []
    for d in (PX, OHLCV):
        s = _read(os.path.join(d, fname(code)))
        if s and (not best or s[-1][0] > best[-1][0]):
            best = s
    return best


def _read(p):
    if not os.path.exists(p):
        return []
    out = []
    with open(p, newline="") as f:
        for r in csv.DictReader(f):
            try:
                c = float(r["Close"])
                a = float(r.get("Adj Close") or c)
            except (TypeError, ValueError, KeyError):
                continue
            if c > 0:
                out.append((r["Date"][:10], c, a))
    return out


def fetch_prices(codes):
    """必要な銘柄を yfinance で2年ぶん取り data/strategy_px/ に保存する。失敗しても止めない。"""
    errs = {}
    try:
        import yfinance as yf
    except Exception as e:  # noqa: BLE001
        return {c: f"yfinance が使えない: {e}" for c in codes}
    os.makedirs(PX, exist_ok=True)
    for c in codes:
        try:
            df = yf.Ticker(c).history(period="2y", interval="1d", auto_adjust=False)
            if df is None or df.empty:
                errs[c] = "データが空"
                continue
            with open(os.path.join(PX, fname(c)), "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["Date", "Open", "High", "Low", "Close", "Volume", "Adj Close"])
                for idx, r in df.iterrows():
                    if r["Close"] != r["Close"]:  # NaN（取引の無い日）
                        continue
                    w.writerow([str(idx)[:10], r["Open"], r["High"], r["Low"], r["Close"],
                                r["Volume"], r.get("Adj Close", r["Close"])])
        except Exception as e:  # noqa: BLE001
            errs[c] = str(e)[:120]
    return errs


def month_add(ym, n):
    y, m = int(ym[:4]), int(ym[5:7])
    k = y * 12 + (m - 1) + n
    return f"{k // 12:04d}-{k % 12 + 1:02d}"


def months_between(a, b):
    """a, b は 'YYYY-MM'。a から b まで（両端含む）の月数。b<a なら 0。"""
    n = (int(b[:4]) - int(a[:4])) * 12 + int(b[5:7]) - int(a[5:7]) + 1
    return max(0, n)


def btc_trend(series, today):
    """直近の完了月の平均 と その前10か月の平均 を比べる。データ不足なら None。"""
    by = {}
    for d, c, _ in series:
        by.setdefault(d[:7], []).append(c)
    cur = today.strftime("%Y-%m")
    last = month_add(cur, -1)
    prev = [month_add(last, -i) for i in range(1, 11)]
    if last not in by or any(m not in by for m in prev):
        return None
    lastavg = sum(by[last]) / len(by[last])
    base = sum(sum(by[m]) / len(by[m]) for m in prev) / 10
    return {"month": last, "last_avg": lastavg, "base_avg": base, "ok": lastavg > base}


def man(x):
    """円 → '約N万円'（1万円単位）。"""
    return f"約{round(x / 10000):,}万円"


def yen(x):
    return f"¥{round(x):,}"


def fetch_main():
    """公開側の処理。価格の取得とビットコインの判定だけ。保有は読まない。"""
    now = dt.datetime.now(JST)
    codes = list(json.load(open(os.path.join(ROOT, "strategy_watchlist.json"), encoding="utf-8"))["codes"])
    kp = os.path.join(DATA, "kiyohara.json")
    if os.path.exists(kp):
        try:
            codes += [r["code"] + ".T" for r in json.load(open(kp, encoding="utf-8")).get("rows", [])]
        except Exception:  # noqa: BLE001
            pass
    codes = sorted(set(codes))
    errs = fetch_prices(codes)
    s = _read(os.path.join(PX, fname(BTC)))
    out = {"generated_at_jst": now.isoformat(), "codes": len(codes), "errors": errs,
           "btc_last": {"date": s[-1][0], "close": s[-1][1]} if s else None,
           "btc_trend": btc_trend(s, now.date()) if s else None}
    json.dump(out, open(os.path.join(DATA, "strategy_market.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps(out, ensure_ascii=False, indent=1))
    # 半分以上取れなければ失敗として知らせる（コミットはしない）
    return 1 if len(errs) > len(codes) / 2 or not s else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--portfolio")
    ap.add_argument("--history")
    args = ap.parse_args()
    if args.fetch:
        return fetch_main()
    if not args.portfolio or not args.history:
        ap.error("--fetch か、--portfolio と --history の両方を指定する")
    now = dt.datetime.now(JST)
    today = now.date()
    pf = json.load(open(args.portfolio, encoding="utf-8"))
    rules, tg = pf["rules"], pf["targets"]
    assert abs(sum(tg.values()) - 1) < 1e-6, "targets の合計が100%になっていない"

    # --- 積立の自動計上（有効なときだけ。毎月1日に買ったものとして足す）
    funds = list(pf.get("funds", []))
    ts = pf.get("tsumitate", {})
    if ts.get("enabled"):
        ym = ts["start"]
        while ym <= today.strftime("%Y-%m"):
            funds.append({"name": ts["name"], "date": ym + "-01", "yen": ts["yen"],
                          "proxy": ts["proxy"], "auto": True})
            ym = month_add(ym, 1)

    codes = sorted({h["code"] for h in pf["holdings"]} | {f["proxy"] for f in funds} | {BTC})
    errs = {}
    px = {c: load_series(c) for c in codes}
    warn = [f"{c}: 価格が取れない（{errs.get(c, 'CSVなし')}）" for c in codes if not px[c]]
    for c in codes:
        if px[c] and (today - dt.date.fromisoformat(px[c][-1][0])).days > 5:
            warn.append(f"{c}: 価格が古い（最終 {px[c][-1][0]}）")

    # --- 評価額
    val = {k: 0.0 for k, _ in SLEEVES}
    val["legacy"] = 0.0
    rows = []
    for h in pf["holdings"]:
        s = px[h["code"]]
        if not s:
            continue
        v = h["shares"] * s[-1][1]
        rows.append({**h, "price": s[-1][1], "asof": s[-1][0], "value": v})
        if h["sleeve"] != "out":
            val[h["sleeve"]] += v
    fund_val = fund_cost = 0.0
    for f in funds:
        s = px[f["proxy"]]
        base = next((a for d, _, a in s if d >= f["date"]), None)
        v = f["yen"] * (s[-1][2] / base) if s and base else f["yen"]
        fund_val += v
        fund_cost += f["yen"]
    val["stock"] += fund_val
    b = pf.get("btc", {"qty": 0, "cost": 0})
    btc_px = px[BTC][-1][1] if px[BTC] else None
    val["btc"] = (b["qty"] * btc_px) if btc_px else float(b.get("cost", 0))
    # 自動計上した積立は新しい入金なので現金からは引かない（下落率の計算では入金として除く）
    cash = float(pf["cash"])
    total = cash + sum(val.values())

    # --- 高値からの下落（入金の影響を除いた指数で測る）
    #     index_t = index_{t-1} × (総額_t − その間の入金) ÷ 総額_{t-1}
    contrib = float(pf.get("deposited", 0)) + sum(f["yen"] for f in funds if f.get("auto"))
    hp = args.history
    hist = []
    if os.path.exists(hp):
        hist = [r for r in csv.DictReader(open(hp)) if r["date"] < today.isoformat()]
    if hist:
        p = hist[-1]
        index = float(p["index"]) * (total - (contrib - float(p["contrib"]))) / float(p["total"])
    else:
        index = 100.0
    hist.append({"date": today.isoformat(), "total": round(total), "contrib": round(contrib), "index": round(index, 4)})
    with open(hp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["date", "total", "contrib", "index"])
        w.writeheader()
        w.writerows(hist)
    peak = max(hist, key=lambda r: float(r["index"]))
    peak_d = peak["date"]
    dd = index / float(peak["index"]) - 1

    # --- BTC判定
    tr = btc_trend(px[BTC], today) if px[BTC] else None

    # --- 今日やること
    acts, notes = [], []
    share = {k: val[k] / total for k, _ in SLEEVES}
    gap = {k: tg[k] * total - val[k] for k, _ in SLEEVES}  # 正＝足りない
    drift = rules["drift_pt"] / 100
    is_reb = today.month == rules["rebalance_month"]
    cur_ym = today.strftime("%Y-%m")

    legacy = [r for r in rows if r["sleeve"] == "legacy"]
    if legacy:
        acts.append("**既存株の売却**（新しい配分に入らない分）: " +
                    "、".join(f"{r['code'][:4]} {r['name']} {r['shares']}株（{man(r['value'])}）" for r in legacy))

    # BTC：計画どおりの分割買い → 終わったら配分のずれで判断
    bp = pf["btc_plan"]
    sched = min(bp["total"], bp["per_month"] * months_between(bp["start"], cur_ym))
    btc_buy = 0.0
    if b.get("cost", 0) < bp["total"]:
        btc_buy = max(0.0, sched - b.get("cost", 0))
    elif share["btc"] < tg["btc"] - drift:
        btc_buy = gap["btc"]
    if btc_buy >= 10000:
        if tr is None:
            notes.append("BTCの判定に必要な価格履歴（11か月ぶん）が無いため、買い増しの可否を出せない。")
        elif tr["ok"]:
            acts.append(f"**ビットコインを{man(btc_buy)}買う**（SBI VCトレードの取引所・板で。販売所は使わない）")
        else:
            notes.append(f"BTCは買い増し見送り（{tr['month']}の平均が、その前10か月の平均を下回っている）。予定分 {man(btc_buy)} は翌月以降に繰り越す。")
    if share["btc"] > rules["btc_cap"]:
        acts.append(f"**ビットコインが全体の{share['btc']*100:.0f}%**（上限{rules['btc_cap']*100:.0f}%超）。"
                    f"{man(val['btc'] - tg['btc'] * total)}売って目標に戻す（税金がかかっても行う例外）")
    elif is_reb and share["btc"] > tg["btc"] + drift:
        if today.isoformat() < rules["no_btc_sell_until"]:
            notes.append(f"BTCは目標より{(share['btc']-tg['btc'])*100:.0f}ポイント多いが、"
                         f"{rules['no_btc_sell_until'][:4]}年より前は売らない（最大55%課税のため）。")
        else:
            acts.append(f"**ビットコインを{man(-gap['btc'])}売る**（年1回の配分戻し）")

    # 株・金・小型：足りない分を買う。年1回（配分戻しの月）は多い分も売る
    ng = pf.get("nisa_growth", {})
    waiting = ng and today.isoformat() < ng.get("wait_until", "")
    spendable = cash - max(0.0, btc_buy if (tr and tr["ok"]) else 0.0)
    for k, label in [("stock", "株の指数"), ("gold", "金"), ("smallcap", "小型割安株")]:
        if gap[k] > drift * total or (is_reb and gap[k] > 0.01 * total):
            amt = min(gap[k], max(0.0, spendable))
            if k == "stock" and waiting:
                hold = min(amt, ng["yen"])
                amt -= hold
                notes.append(f"株の指数のうち{man(hold)}は、{ng['wait_until']}以降のNISA成長投資枠で買うために現金で待機。")
            if amt >= 10000:
                acts.append(f"**{label}を{man(amt)}買う**" + ("（下の候補から。1業種1社まで）" if k == "smallcap" else ""))
                spendable -= amt
        elif is_reb and -gap[k] > drift * total:
            acts.append(f"**{label}を{man(-gap[k])}売る**（年1回の配分戻し）")

    for lv in sorted(rules["dd_alert"], reverse=True):
        if dd <= lv:
            alert = lv
    if dd <= max(rules["dd_alert"]):
        acts.insert(0, f"**⚠ 高値から{dd*100:.0f}%下落**（警戒線{alert*100:.0f}%を超えた）。新規の買い増しを止め、配分を見直す。")

    # --- 小型割安株の候補（既存のスクリーニング結果を読むだけ）
    cands = []
    kp = os.path.join(DATA, "kiyohara.json")
    if os.path.exists(kp):
        try:
            for r in json.load(open(kp, encoding="utf-8")).get("rows", []):
                asof = r.get("nc_asof") or ""
                fresh = asof and (today - dt.date.fromisoformat(asof)).days <= 550
                if r.get("nc_real") and r["nc_real"] >= 1 and fresh:
                    cands.append(r)
                if len(cands) >= 6:
                    break
        except Exception as e:  # noqa: BLE001
            warn.append(f"小型割安株の候補が読めない: {e}")

    # --- 出力
    L = [f"# 昼のレポート  {now:%Y-%m-%d %H:%M} JST", ""]
    L.append("## 今日やること")
    L += ([f"{i}. {a}" for i, a in enumerate(acts, 1)] if acts else ["**変更なし。** そのまま持つ。"])
    L.append("")
    if notes:
        L += ["**メモ**", ""] + [f"- {n}" for n in notes] + [""]
    L += ["## 配分", "", f"総額 **{yen(total)}**（現金 {yen(cash)}）／高値（{peak_d}）から **{dd*100:+.1f}%**（上限 −50%）", "",
          "| 枠 | 目標 | 現在 | ずれ | 評価額 |", "|---|--:|--:|--:|--:|"]
    for k, label in SLEEVES:
        L.append(f"| {label} | {tg[k]*100:.0f}% | {share[k]*100:.1f}% | {(share[k]-tg[k])*100:+.1f} | {yen(val[k])} |")
    if val["legacy"] > 0:
        L.append(f"| 既存株（売却予定） | 0% | {val['legacy']/total*100:.1f}% | — | {yen(val['legacy'])} |")
    L.append(f"| 現金 | — | {cash/total*100:.1f}% | — | {yen(cash)} |")
    L.append("")
    L.append("## ビットコインの判定")
    if tr:
        L.append(f"{tr['month']}の平均 {yen(tr['last_avg'])} ／ その前10か月の平均 {yen(tr['base_avg'])} → "
                 f"**{'買い増し可' if tr['ok'] else '買い増し見送り'}**")
    else:
        L.append("価格履歴が足りず判定できない。")
    if btc_px:
        L.append(f"現在値 {yen(btc_px)}（{px[BTC][-1][0]}）／保有 {b['qty']} BTC／買った合計 {yen(b.get('cost', 0))}（計画 {yen(bp['total'])}）")
    L.append("")
    if fund_cost:
        L += [f"投資信託: 買った合計 {yen(fund_cost)} → 評価 {yen(fund_val)}（値動きは対応するETFで代用した概算）", ""]
    if cands and gap["smallcap"] > 0.01 * total:
        L += ["## 小型割安株の候補（条件を機械的に通過したもの。買う前に決算資料の確認が必要）", "",
              "| コード | 会社 | 株価 | ネットキャッシュ比率（実測） | 株主還元 |", "|---|---|--:|--:|---|"]
        for r in cands:
            L.append(f"| {r['code']} | {r['name']} | {r['price']:,.0f} | {r['nc_real']:.2f}（{r['nc_asof']}） | {r.get('ret','')} |")
        L.append("")
    if warn:
        L += ["## データの注意", ""] + [f"- {w}" for w in warn] + [""]
    L.append("> 配分のルールを機械的に当てた結果で、値動きの予測ではない。下落率は入金の影響を除いて計算している。")
    print("\n".join(L))


if __name__ == "__main__":
    sys.exit(main())
