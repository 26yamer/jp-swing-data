#!/usr/bin/env python3
"""
後場スイング用 データ取得スクリプト（GitHub Actions 上で実行する前提）

なぜこの形か:
  Claude が動くクラウドコンテナは外向き通信がパッケージレジストリと GitHub に
  制限されており、Yahoo Finance 等へ直接アクセスできない。
  一方 raw.githubusercontent.com は読める。
  そこで「GitHub Actions が取りに行って、リポジトリにコミットする」ことで
  Claude 側は raw.githubusercontent.com から正確な数値を読めるようにする。

出力:
  data/latest.json        … 最新スナップショット（前場終値・マクロ）
  data/ohlcv/<code>.csv   … 日足OHLCV（累積・追記マージ）
                            Close=実際の取引価格（発注の指値に使う）
                            Adj Close=分割・配当調整済み（指標の計算に使う）
  data/intraday/<code>.csv… 5分足（当日のみ）
  data/meta.json          … 取得時刻・成否・データソース・健全性
  data/jgb.csv            … 日本国債利回り（財務省・全年限）
  data/events.json        … 分配金の実績と利回り、決算発表予定日

方針:
  一部の銘柄が取れなくてもスクリプトは異常終了しない。取れた分は必ずコミットし、
  何が取れなかったかを meta.json に残す。失敗を握りつぶすのではなく、
  「部分的な成功を捨てない」ための設計。
"""
import os, sys, re, time, json, datetime as dt
import pandas as pd

JST = dt.timezone(dt.timedelta(hours=9))
NOW = dt.datetime.now(JST)

# ── 監視ユニバース ────────────────────────────────────────────────
HOLDINGS = ["1306.T","1343.T","1540.T","1615.T","2559.T","2563.T","2621.T","4755.T","9432.T"]
# スイング候補の拡張ユニバース（値動きがあり流動性の高い日本株/ETF）
# レバレッジ/インバース型（1357・1459・1568）は日々減価する設計でスイングの
# 保有候補にならず、株式併合が頻繁でデータも荒れるため監視対象から外した。
WATCH    = ["1321.T","1699.T","2510.T","1343.T","2559.T","2621.T",
            "8306.T","8316.T","8411.T","7203.T","6758.T","6501.T",
            "9984.T","6857.T","8035.T","4063.T","9433.T","9434.T"]
# 業種別ETF（TOPIX-17）。値上がり/値下がり銘柄数をスクレイピングする代わりに、
# セクターの物色動向を「価格データ」として取る。壊れにくく、遅延もない。
SECTOR = {"1617.T":"食品","1618.T":"エネルギー資源","1619.T":"建設・資材","1620.T":"素材・化学",
          "1621.T":"医薬品","1622.T":"自動車・輸送機","1623.T":"鉄鋼・非鉄","1624.T":"機械",
          "1625.T":"電機・精密","1626.T":"情報通信・サービスその他","1627.T":"電気・ガス",
          "1628.T":"運輸・物流","1629.T":"商社・卸売","1630.T":"小売","1631.T":"銀行",
          "1632.T":"金融（除く銀行）","1633.T":"不動産"}
MACRO    = ["^N225","^GSPC","^IXIC","^VIX","^TNX","^TYX",
            "JPY=X","CL=F","GC=F","SI=F","^SOX"]
ALL = HOLDINGS + WATCH + list(SECTOR) + MACRO

OUT = "data"
os.makedirs(f"{OUT}/ohlcv", exist_ok=True)
os.makedirs(f"{OUT}/intraday", exist_ok=True)
meta = {"fetched_at_jst": NOW.isoformat(), "sources": {}, "errors": []}

def slug(code):
    return code.replace("^", "_").replace("=", "_")

def norm_dates(s):
    """タイムゾーンの有無に関わらず、日付だけの naive な列に揃える。
       pandas 2系では tz-naive に tz_localize(None) を呼ぶと TypeError になるため、
       tz を持っているときだけ外す。utc=True は使わない（日付が1日ずれるため）。"""
    d = pd.to_datetime(s)
    if getattr(d.dt, "tz", None) is not None:
        d = d.dt.tz_localize(None)
    return d.dt.normalize()

def retry(fn, *a, **k):
    for i in range(3):
        try:
            return fn(*a, **k)
        except Exception as e:
            if i == 2:
                meta["errors"].append(f"{getattr(fn,'__name__','fn')}{a}: {type(e).__name__}: {e}")
                return None
            time.sleep(2 * (i + 1))

# ── 1) 日足: yfinance（主）→ stooq（副） ─────────────────────────
def fetch_daily_all(tickers):
    import yfinance as yf
    return yf.download(tickers, period="2y", interval="1d",
                       auto_adjust=False, group_by="ticker",
                       threads=True, progress=False)

def fetch_daily_stooq(code):
    """yfinance が落ちた時のフォールバック。1306.T -> 1306.jp"""
    import io, requests
    s = code.replace(".T", ".jp").lower()
    r = requests.get(f"https://stooq.com/q/d/l/?s={s}&i=d", timeout=30)
    if r.status_code != 200 or len(r.text) < 100:
        raise RuntimeError(f"stooq HTTP {r.status_code}")
    return pd.read_csv(io.StringIO(r.text), parse_dates=["Date"])

def sanity(code, d):
    if not code.endswith(".T"):     # ^VIX や SI=F は実際に25%動く。誤検知になるので対象外
        return
    """理由の説明できない価格の飛びを検出して meta に残す。
       Close は実際の取引価格なので株式分割で不連続になる。指標計算には
       Adj Close（分割・配当調整済み）を使うこと。ここでは Adj Close 側に
       飛びが残っていないかを見る＝データ自体の異常の検出。"""
    col = "Adj Close" if "Adj Close" in d.columns else "Close"
    v = d[col].astype(float)
    ch = (v / v.shift() - 1).abs()
    bad = d.loc[ch > 0.25, "Date"]
    if len(bad):
        meta["anomalies"] = meta.get("anomalies", [])
        meta["anomalies"].append({"code": code, "col": col,
                                  "dates": [str(x.date()) for x in bad][:5],
                                  "n": int(len(bad))})

def merge_csv(path, new):
    """既存CSVに追記マージ（重複日は新しい方で上書き）。履歴を失わないため。"""
    new = new.dropna(subset=["Close"])
    if new.empty:
        return 0
    if os.path.exists(path):
        try:
            old = pd.read_csv(path, parse_dates=["Date"])
            new = pd.concat([old, new]).drop_duplicates(subset=["Date"], keep="last")
        except Exception as e:
            meta["errors"].append(f"merge {path}: {e}")
    new = new.sort_values("Date")
    new.to_csv(path, index=False)
    return len(new)

print(f"[{NOW:%Y-%m-%d %H:%M JST}] 日足取得開始 ({len(ALL)}銘柄)")
raw = retry(fetch_daily_all, ALL)
ok = 0
for code in ALL:
    d = None
    if raw is not None:
        try:
            d = raw[code].reset_index()
            d.columns = [str(c) for c in d.columns]
            datecol = "Date" if "Date" in d.columns else d.columns[0]
            d = d.rename(columns={datecol: "Date"})
            cols = ["Date", "Open", "High", "Low", "Close", "Volume"]
            if "Adj Close" in d.columns:
                cols.append("Adj Close")
            d = d[cols]
            if d["Close"].dropna().empty:
                d = None
            else:
                meta["sources"][code] = "yfinance"
        except Exception:
            d = None
    if d is None and code.endswith(".T"):
        d = retry(fetch_daily_stooq, code)
        if d is not None:
            meta["sources"][code] = "stooq"
    if d is None or d.empty:
        meta["errors"].append(f"no-daily: {code}")
        continue
    try:
        d["Date"] = norm_dates(d["Date"])
        sanity(code, d)
        merge_csv(f"{OUT}/ohlcv/{slug(code)}.csv", d)
        ok += 1
    except Exception as e:
        meta["errors"].append(f"write {code}: {type(e).__name__}: {e}")
print(f"日足OK: {ok}/{len(ALL)}")

# ── 2) 当日5分足（前場の値動きを見るため） ───────────────────────
try:
    import yfinance as yf
    intra = yf.download(HOLDINGS + ["^N225"], period="1d", interval="5m",
                        group_by="ticker", threads=True, progress=False, prepost=False)
    for code in HOLDINGS + ["^N225"]:
        try:
            d = intra[code].reset_index().dropna(subset=["Close"])
            if not d.empty:
                d.to_csv(f"{OUT}/intraday/{slug(code)}.csv", index=False)
        except Exception:
            pass
except Exception as e:
    meta["errors"].append(f"intraday: {type(e).__name__}: {e}")

# ── 3) スナップショット ──────────────────────────────────────────
snap = {}
for code in ALL:
    p = f"{OUT}/ohlcv/{slug(code)}.csv"
    if not os.path.exists(p):
        continue
    try:
        d = pd.read_csv(p, parse_dates=["Date"]).sort_values("Date")
        if len(d) < 2:
            continue
        c, pv = float(d["Close"].iloc[-1]), float(d["Close"].iloc[-2])
        vol = d["Volume"].iloc[-1] if "Volume" in d.columns else None
        snap[code] = dict(date=str(d["Date"].iloc[-1].date()), close=c, prev=pv,
                          chg_pct=round((c / pv - 1) * 100, 3),
                          volume=(float(vol) if pd.notna(vol) else None),
                          bars=len(d))
    except Exception as e:
        meta["errors"].append(f"snapshot {code}: {e}")

# 前場のリアルタイム値（5分足の最終足）を上乗せ
for code in HOLDINGS:
    p = f"{OUT}/intraday/{slug(code)}.csv"
    if os.path.exists(p) and code in snap:
        try:
            d = pd.read_csv(p)
            if not d.empty:
                snap[code]["intraday_last"] = float(d["Close"].iloc[-1])
                snap[code]["intraday_time"] = str(d.iloc[-1, 0])
                snap[code]["intraday_high"] = float(d["High"].max())
                snap[code]["intraday_low"]  = float(d["Low"].min())
        except Exception:
            pass

json.dump({"generated_at_jst": NOW.isoformat(), "snapshot": snap},
          open(f"{OUT}/latest.json", "w"), ensure_ascii=False, indent=1)

meta["n_ok"] = ok
meta["n_total"] = len(ALL)
meta["holdings_ok"] = sum(1 for c in HOLDINGS if c in snap)


# ── 3.5) ニュース見出しの取得 ─────────────────────────────────
#    Claudeの定期実行では WebFetch が承認を要求して止まることがある。
#    ニュースもここで取り、report.md に書き込んでしまえば、
#    Claude側はレポートを1回読むだけで済み、承認の対象が減る。
# フィードごとに信頼度を持たせる。market=True のものは市場記事しかないので
# キーワード絞り込みをかけず全件採用する（日経マーケットが最も有用）。
FEEDS = [
    ("日経 マーケット",     "https://assets.wor.jp/rss/rdf/nikkei/markets.rdf", True),
    ("日経 政治・経済",     "https://assets.wor.jp/rss/rdf/nikkei/economy.rdf", False),
    ("Yahoo!ニュース 市況", "https://news.yahoo.co.jp/rss/categories/business.xml", False),
    ("Yahoo!ニュース 経済", "https://news.yahoo.co.jp/rss/topics/business.xml", False),
    ("産経 経済",          "https://assets.wor.jp/rss/rdf/sankei/economy.rdf", False),
    ("財経新聞",           "https://www.zaikei.co.jp/rss/", False),
]
KEYS = ["日経平均","TOPIX","東証","株価","長期金利","国債","利回り","日銀","利上げ","金融政策",
        "円安","円高","為替","ドル円","原油","OPEC","FRB","FOMC","CPI","物価","インフレ",
        "銀行","半導体","決算","地政学","中東","イラン","REIT","不動産","金価格","米株",
        "ナスダック","ダウ","先物","値上がり","値下がり","業種","相場","マーケット"]
news = []
def fetch_feed(name, url):
    import requests, xml.etree.ElementTree as ET, html as _h, re as _re
    r = requests.get(url, timeout=25, headers={"User-Agent": "Mozilla/5.0 (compatible; market-bot)"})
    r.raise_for_status()
    txt = r.content.decode(r.apparent_encoding or "utf-8", errors="replace")
    out = []
    try:
        root = ET.fromstring(txt.encode("utf-8"))
        for it in root.iter():
            if it.tag.split("}")[-1] != "item": continue
            g = lambda k: next((c.text for c in it if c.tag.split("}")[-1] == k and c.text), "")
            ti = _h.unescape((g("title") or "").strip())
            if ti: out.append({"src": name, "title": ti,
                               "date": (g("pubDate") or g("date") or "")[:25]})
    except Exception:                       # XMLとして壊れている場合は素朴に抜く
        for m in _re.finditer(r"<title[^>]*>(.*?)</title>", txt, _re.S)   :
            ti = _h.unescape(_re.sub(r"<[^>]+>", "", m.group(1))).strip()
            if ti: out.append({"src": name, "title": ti, "date": ""})
    return out[:40]

for nm, u, is_market in FEEDS:
    got = retry(fetch_feed, nm, u)
    if got:
        for g in got: g["market_feed"] = is_market
        news.extend(got)
seen = set(); hits = []
for n in news:
    k = n["title"]
    if k in seen: continue
    seen.add(k)
    # 市場専門フィードは無条件採用、それ以外はキーワードで絞る
    if n.get("market_feed") or any(w in k for w in KEYS): hits.append(n)
hits.sort(key=lambda x: (not x.get("market_feed"),))   # 市場フィードを先頭に
json.dump({"fetched_at_jst": NOW.isoformat(), "matched": hits[:45], "all_count": len(news)},
          open(f"{OUT}/news.json", "w"), ensure_ascii=False, indent=1)
meta["news_ok"] = len(hits)
print(f"ニュース: {len(news)}件取得 / 関連 {len(hits)}件")

# ── 4) 日本国債利回り（財務省・公式）────────────────────────────
#    米国債は ^TNX/^TYX で取れるが日本国債は Yahoo に無い。
#    いまのレジーム判断の中心変数なので財務省の公表CSVから直接取る。
#    形式: Shift-JIS / 日付が和暦(R8.9.4等) / 先頭に説明行あり、という癖がある。
def fetch_jgb():
    import io, requests
    base = "https://www.mof.go.jp/jgbs/reference/interest_rate/"
    have = 0
    try:
        if os.path.exists(f"{OUT}/jgb.csv"):
            have = sum(1 for _ in open(f"{OUT}/jgb.csv", encoding="utf-8")) - 1
    except Exception:
        pass
    # jgbcm.csv は「当年版」ではなく **当月分だけ**（9月上旬なら7行しかない）。
    # サイズで正否を判定すると、この正当なファイルを弾いてしまう。
    # 判定は中身（基準日ヘッダの有無）で行い、全履歴版を第一候補にする。
    # 全履歴版は data/ 配下に移動済み（旧URLは404のまま残っている）。
    cands = [base + "data/jgbcm_all.csv", base + "jgbcm_all.csv", base + "jgbcm.csv"]
    hdrs = {"User-Agent": "Mozilla/5.0 (compatible; market-bot)"}
    txt, url, last = None, None, None
    for u in cands:
        try:
            rr = requests.get(u, timeout=60, headers=hdrs)
            if rr.status_code != 200:
                last = f"{u} -> HTTP {rr.status_code}"; continue
            t = None
            for enc in ("cp932", "shift_jis", "utf-8-sig", "utf-8"):
                try:
                    t = rr.content.decode(enc); break
                except Exception:
                    continue
            if t and "基準日" in t:
                txt, url = t, u; break
            last = f"{u} -> 基準日ヘッダなし ({len(rr.content)}bytes)"
        except Exception as e:
            last = f"{u} -> {type(e).__name__}"
    if txt is None:
        raise RuntimeError(last or "JGB: 取得先なし")
    print(f"JGB: 手元{have}行 → {url.rsplit('/', 1)[-1]} ({len(txt):,}文字)")
    lines = txt.splitlines()
    hdr = next(i for i, l in enumerate(lines) if l.startswith("基準日"))
    df = pd.read_csv(io.StringIO("\n".join(lines[hdr:])))
    df = df.rename(columns={df.columns[0]: "Date"})

    ERA = {"S": 1925, "H": 1988, "R": 2018}   # 昭和/平成/令和 の加算基準年
    def wareki(x):
        x = str(x).strip()
        try:
            e = x[0]
            if e in ERA:
                y, m, d = x[1:].split(".")
                return pd.Timestamp(ERA[e] + int(y), int(m), int(d))
            return pd.to_datetime(x)               # 西暦表記に変わっていた場合
        except Exception:
            return pd.NaT
    df["Date"] = df["Date"].map(wareki)
    df = df.dropna(subset=["Date"])
    for c in df.columns[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

try:
    jgb = retry(fetch_jgb)
    if jgb is None or not len(jgb):
        # 当日取れなくても、蓄積済みの jgb.csv があれば金利は読める。
        # 「取得できず」で金利を丸ごと落とすと、レジーム判断の主材料が消える。
        if os.path.exists(f"{OUT}/jgb.csv"):
            meta["jgb_stale"] = True
            print("JGB: 当日の取得に失敗。蓄積済みの jgb.csv を使う")
    if jgb is not None and len(jgb):
        merge_csv(f"{OUT}/jgb.csv", jgb.assign(Close=jgb.get("10年")))
        last = jgb.iloc[-1]
        meta["jgb"] = {"date": str(last["Date"].date()),
                       "y2":  (float(last["2年"])  if pd.notna(last.get("2年"))  else None),
                       "y10": (float(last["10年"]) if pd.notna(last.get("10年")) else None),
                       "y30": (float(last["30年"]) if pd.notna(last.get("30年")) else None)}
        print(f"JGB 10年: {meta['jgb']['y10']}%  ({meta['jgb']['date']})")
except Exception as e:
    meta["errors"].append(f"jgb: {type(e).__name__}: {e}")

# ── 5) 分配金・決算発表予定日（損切りと建玉の可否に直結）──────────
#    ETFの権利落ちは機械的な下落なので、知らないと損切りが誤発動する。
#    決算をまたぐ建玉は避ける（4755は2026-08-12の決算で-13.8%）。
events = {}
try:
    import yfinance as yf
    for code in HOLDINGS:
        ev = {}
        try:
            tk = yf.Ticker(code)
            dv = tk.dividends
            if dv is not None and len(dv):
                dv = dv.tail(8)
                ev["dividends"] = [{"date": str(pd.Timestamp(i).date()), "amount": float(v)}
                                   for i, v in dv.items()]
                # 窓の起点は「最後の支払日」ではなく「本日」。境界は含まない。
                # 起点を最後の支払日にすると年1回払いで2回分を合算してしまう。
                _t = pd.Timestamp(NOW.date())
                _idx = pd.to_datetime(dv.index).tz_localize(None)
                ttm = float(dv[(_idx > _t - pd.Timedelta(days=365)) & (_idx <= _t)].sum())
                ev["ttm_dividend"] = ttm
                if code in snap and snap[code]["close"]:
                    ev["ttm_yield_pct"] = round(ttm / snap[code]["close"] * 100, 2)
        except Exception as e:
            ev["dividends_error"] = str(e)[:120]
        try:
            ed = tk.get_earnings_dates(limit=8)
            if ed is not None and len(ed):
                ev["earnings_dates"] = [str(pd.Timestamp(i).date()) for i in ed.index]
        except Exception:
            pass
        if ev:
            events[code] = ev
    json.dump(events, open(f"{OUT}/events.json", "w"), ensure_ascii=False, indent=1)
except Exception as e:
    meta["errors"].append(f"events: {type(e).__name__}: {e}")
meta["events_ok"] = len(events)

meta["n_anomalies"] = sum(a["n"] for a in meta.get("anomalies", []))
meta["health"] = ("ok" if meta["holdings_ok"] == len(HOLDINGS)
                  else "partial" if meta["holdings_ok"] >= 6 else "bad")


# ══════════════════════════════════════════════════════════════════════
#  7) 全銘柄スクリーニング
#     これまでは保有銘柄しかスコアリングしておらず、東証約4,000のうち
#     0.2%しか俎上に載っていなかった。「何を新しく買うか」を出せる状態にする。
#
#     設計上の要点:
#       ・生データはコミットしない（4,000銘柄×2年をgitに置くと破綻する）。
#         ジョブ内で取得→計算→**上位候補の表だけ**を残す。
#       ・銘柄リストは初回に総当たりで作って universe.json に保存し、
#         以降は再利用する（30日で作り直す）。
#       ・時間予算を持ち、超えたらそこまでの分で結果を出す。止まらない。
# ══════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════
#  指標ヘルパ（repair / _rsi / _atr）
#  ★ここに置く理由: 全銘柄スクリーニングがこれらを使う。以前は分析セクション
#    （もっと下）で定義していたため、スクリーニング実行時には未定義で
#    NameError が全銘柄で発生し、except で握りつぶされて通過0件になっていた。
#    定義は必ず最初の呼び出しより前に置くこと。
# ══════════════════════════════════════════════════════════════════════
ROUND = [1/100, 1/50, 1/25, 1/20, 1/10, 1/5, 1/4, 1/3, 1/2,
         2, 3, 4, 5, 10, 20, 25, 50, 100]
def _near(r, tol=0.12):
    if r <= 0: return None
    import math
    c = min(ROUND, key=lambda x: abs(math.log(x) - math.log(r)))
    return c if abs(math.log(c) - math.log(r)) < tol else None

def repair(s, thr=0.20, maxrun=5):
    """株価系列の異常値と株式分割を直す。これを飛ばすと相関も指標も壊れる。

    パス1（異常値）: 段差の後、数日以内に元の水準へ戻る区間は Yahoo 側の異常値。
      **倍率がいくつであっても**前後を線形補間した値で置き換える。
      （1306は3/30〜31に1/10、1629は同じ日に約1/500になっていた。
        1/500は「丸い倍率」ではないため、倍率を見る方式では直せなかった。）
    パス2（分割）: 水準が戻らない段差は株式分割とみなし、丸い倍率で遡及調整する。
    """
    import numpy as np
    v = np.asarray(s.values, float).copy(); n = len(v); log = []

    i = 1
    while i < n:
        if v[i-1] > 0 and v[i] > 0 and abs(v[i]/v[i-1] - 1) > thr:
            fixed = False
            for k in range(1, maxrun+1):
                j = i + k
                if j < n and v[j] > 0 and abs(v[j]/v[i-1] - 1) < 0.15:
                    before, after = v[i-1], v[j]
                    for kk in range(i, j):                    # 前後をなめらかにつなぐ
                        f = (kk - i + 1) / (j - i + 1)
                        v[kk] = before + (after - before) * f
                    log.append(f"異常値 {s.index[i].date()}〜{s.index[j-1].date()} を前後の水準で補間")
                    fixed = True; break
            if fixed:
                i = i + 1; continue
        i += 1

    for i in range(1, n):
        if v[i-1] > 0 and v[i] > 0 and abs(v[i]/v[i-1] - 1) > thr:
            r = _near(v[i]/v[i-1])
            if r:
                v[:i] *= r
                log.append(f"分割 {s.index[i].date()} " + (f"1:{1/r:.0f}" if r < 1 else f"{r:.0f}:1"))
    return pd.Series(v, index=s.index), log

def _rsi(c, n=14):
    import numpy as np
    d = np.diff(c); u = np.where(d > 0, d, 0.); w = np.where(d < 0, -d, 0.)
    if len(d) < n: return float("nan")
    a, b = u[:n].mean(), w[:n].mean()
    for i in range(n, len(d)): a = (a*(n-1)+u[i])/n; b = (b*(n-1)+w[i])/n
    return 100.0 if b == 0 else 100 - 100/(1 + a/b)

def _atr(d, n=14):
    import numpy as np
    h, l, c = d["High"].values, d["Low"].values, d["Close"].values
    tr = [h[0]-l[0]] + [max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])) for i in range(1, len(c))]
    tr = np.array(tr)
    if len(tr) < n+1: return float("nan")
    a = tr[1:n+1].mean()
    for i in range(n+1, len(tr)): a = (a*(n-1)+tr[i])/n
    return a


# ══════════════════════════════════════════════════════════════════════
#  5a) 銘柄マスタ（JPX 東証上場銘柄一覧）と 適時開示（TDnet）
#      狙い: スクリーニング結果に「銘柄名」「業種」「当日の開示」を付ける。
#            コードだけの表は人間が検証できず、なぜ動いているかの裏も取れない。
#      注意: どちらも取得に失敗してもジョブは止めない。マスタは前回の
#            キャッシュにフォールバックし、開示は空で先に進む。
# ══════════════════════════════════════════════════════════════════════
JPX_URLS = [
    "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xlsx",
    "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls",
]
MASTER_MIN_ROWS     = 1000        # これ未満しか取れなければ壊れたファイルとみなす
MASTER_MAX_AGE_DAYS = 25          # 月初の第3営業日に更新されるので25日で取り直す
EXCLUDE_MARKET_KW   = ["pro market", "pro-market", "ｐｒｏ"]
# 2024年以降、新規コードには英字が入る（例 130A）。数字だけで判定してはいけない。
CODE_RE = re.compile(r"^[0-9][0-9A-Za-z]{3}$")

def classify_kind(mkt):
    m = mkt or ""
    if "ETF" in m or "ETN" in m: return "ETF/ETN"
    if "REIT" in m or "インフラ" in m or "ベンチャー" in m or "カントリー" in m: return "REIT等"
    if "出資証券" in m: return "出資証券"
    if "内国株式" in m or "外国株式" in m: return "株式"
    return (m[:8] or "不明")

def _master_bytes():
    import requests
    last = None
    for u in JPX_URLS:
        try:
            r = requests.get(u, timeout=120, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200 and len(r.content) > 50_000:
                return u, r.content
            last = f"{u} -> HTTP {r.status_code} / {len(r.content)}bytes"
        except Exception as e:
            last = f"{u} -> {type(e).__name__}: {e}"
    raise RuntimeError(last or "URLなし")

def _xlsx_rows(blob):
    """.xlsx を標準ライブラリだけで読む（openpyxl 等が無い環境でも動かすため）。
       xlsx は XML の zip なので、共有文字列表とシートを直接引けば足りる。
       ここが動けば「銘柄名が付かない」という静かな劣化が環境依存でなくなる。"""
    import zipfile, io
    import xml.etree.ElementTree as ET
    NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    z = zipfile.ZipFile(io.BytesIO(blob))

    shared = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")):
            # <si> の下は <t> 直下か、書式ごとに分かれた <r><t> の連なり
            shared.append("".join(t.text or "" for t in si.iter(NS + "t")))

    names = [n for n in z.namelist() if n.startswith("xl/worksheets/sheet")]
    if not names:
        raise RuntimeError("シートが見つからない")
    sheet = sorted(names)[0]

    def colnum(ref):
        n = 0
        for ch in ref:
            if not ch.isalpha(): break
            n = n * 26 + (ord(ch.upper()) - 64)
        return n - 1

    rows = []
    root = ET.fromstring(z.read(sheet))
    for r in root.iter(NS + "row"):
        cells, width = {}, 0
        for c in r.iter(NS + "c"):
            i = colnum(c.get("r") or "A")
            t = c.get("t")
            if t == "inlineStr":
                v = "".join(x.text or "" for x in c.iter(NS + "t"))
            else:
                node = c.find(NS + "v")
                v = node.text if node is not None else None
                if t == "s" and v is not None:
                    k = int(v)
                    v = shared[k] if 0 <= k < len(shared) else ""
            cells[i] = v
            width = max(width, i + 1)
        rows.append([cells.get(i) for i in range(width)])
    return rows

def _master_frame(blob):
    """まず標準ライブラリで読む。だめなら pandas のエンジンを順に試す。
       依存を1つ減らすほうが、環境差で静かに壊れる確率が下がる。"""
    errs = []
    try:
        rows = _xlsx_rows(blob)
        rows = [r for r in rows if any(x not in (None, "") for x in r)]
        if len(rows) < 2:
            raise RuntimeError(f"行が足りない: {len(rows)}")
        hdr = [str(x or "").strip() for x in rows[0]]
        w = len(hdr)
        body = [(r + [None] * w)[:w] for r in rows[1:]]
        df = pd.DataFrame(body, columns=hdr).astype("object")
        df = df.where(pd.notna(df), None)      # pandas が None を NaN にするのを戻す
        print(f"[マスタ] 標準ライブラリで読み込み: {len(df):,}行 × {w}列")
        return df
    except Exception as e:
        errs.append(f"stdlib:{type(e).__name__}:{str(e)[:60]}")

    import io
    for eng in ("openpyxl", "calamine", "xlrd"):
        try:
            df = pd.read_excel(io.BytesIO(blob), dtype=str, engine=eng)
            print(f"[マスタ] pandas({eng}) で読み込み: {len(df):,}行")
            return df
        except Exception as e:
            errs.append(f"{eng}:{type(e).__name__}")
    raise RuntimeError("読み込み失敗 " + " / ".join(errs))

def _col(df, *names):
    norm = lambda s: str(s).replace(" ", "").replace("　", "").strip()
    for n in names:
        for c in df.columns:
            if norm(c) == n: return c
    return None

def load_master():
    """コード → 銘柄名・市場区分・17業種 の対照表。"""
    p, cached = f"{OUT}/jpx_master.json", None
    try:
        if os.path.exists(p):
            cached = json.load(open(p, encoding="utf-8"))
            age = (NOW - dt.datetime.fromisoformat(cached["built_at_jst"])).days
            if age < MASTER_MAX_AGE_DAYS and len(cached.get("rows", {})) >= MASTER_MIN_ROWS:
                print(f"[マスタ] jpx_master.json を再利用 ({len(cached['rows']):,}銘柄 / {age}日前)")
                meta["master"] = {"count": len(cached["rows"]), "cached": True}
                return cached["rows"]
    except Exception as e:
        meta["errors"].append(f"master cache: {e}")
    try:
        url, blob = _master_bytes()
        df = _master_frame(blob)
        c_code = _col(df, "コード"); c_name = _col(df, "銘柄名")
        c_mkt  = _col(df, "市場・商品区分"); c_s17 = _col(df, "17業種区分")
        c_s33  = _col(df, "33業種区分");    c_sz  = _col(df, "規模区分")
        if not (c_code and c_name):
            raise RuntimeError(f"想定した列が無い: {list(df.columns)[:12]}")
        def cell(r, c):
            """読み手（標準ライブラリ / pandas）で None にも NaN にもなりうるので、
               どちらでも空文字に寄せる。"""
            if not c: return ""
            v = r[c]
            if v is None or v != v: return ""       # v != v は NaN の判定
            v = str(v).strip()
            return "" if v in ("nan", "None", "-", "－") else v

        rows = {}
        for _, r in df.iterrows():
            code = cell(r, c_code)
            if not CODE_RE.match(code): continue
            mkt = cell(r, c_mkt)
            if any(k in mkt.lower() for k in EXCLUDE_MARKET_KW): continue
            rows[code] = {"name": cell(r, c_name), "market": mkt,
                          "s17": cell(r, c_s17), "s33": cell(r, c_s33),
                          "size": cell(r, c_sz), "kind": classify_kind(mkt)}
        if len(rows) < MASTER_MIN_ROWS:
            raise RuntimeError(f"件数が少なすぎる: {len(rows)}")
        json.dump({"built_at_jst": NOW.isoformat(), "source": url, "rows": rows},
                  open(p, "w"), ensure_ascii=False)
        meta["master"] = {"count": len(rows), "source": url}
        print(f"[マスタ] JPX 東証上場銘柄一覧を取得: {len(rows):,}銘柄")
        return rows
    except Exception as e:
        meta["errors"].append(f"master: {type(e).__name__}: {e}")
        if cached and cached.get("rows"):
            print(f"[マスタ] 取得失敗。古いキャッシュで継続: {e}")
            meta["master"] = {"count": len(cached["rows"]), "stale": True, "error": str(e)[:120]}
            return cached["rows"]
        print(f"[マスタ] 取得失敗・キャッシュ無し: {e}")
        meta["master"] = {"error": str(e)[:120]}
        return {}

# 17業種区分 → 業種別ETF。セクター物色の表と候補を機械的に突き合わせるため。
S17_TO_ETF = {v: k for k, v in SECTOR.items()}

TDNET_MAX_PAGES = 12

def fetch_tdnet(days=4):
    """適時開示（TDnet）の直近4日分。月曜に金曜の開示を拾うため4日みる
       （土日祝のURLは404になるだけなので空振りは安い）。「なぜ動いているか」の裏取りに使う。
       TDnetは約31日分しか保持しないので、履歴の分析には使えない。"""
    import requests, html as _html
    out, pages, errs = {}, 0, 0
    for back in range(days):
        d = (NOW - dt.timedelta(days=back)).strftime("%Y%m%d")
        for pg in range(1, TDNET_MAX_PAGES + 1):
            u = f"https://www.release.tdnet.info/inbs/I_list_{pg:03d}_{d}.html"
            try:
                r = requests.get(u, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            except Exception as e:
                errs += 1; meta["errors"].append(f"tdnet {d}p{pg}: {type(e).__name__}"); break
            if r.status_code != 200: break
            try:
                txt = r.content.decode("utf-8")
            except UnicodeDecodeError:
                txt = r.content.decode("cp932", errors="replace")
            pages += 1
            before = sum(len(v) for v in out.values())
            for tr in re.split(r"<tr", txt, flags=re.I)[1:]:
                # 属性は " と ' の両方があり得る。要素も td/div の両方が使われてきた。
                mc = re.search(r'kjCode["\']?[^>]*>\s*([0-9][0-9A-Za-z]{3,4})', tr)
                mt = re.search(r'kjTitle["\']?[^>]*>(.*?)</(?:td|th|div)>', tr, re.S | re.I)
                mm = re.search(r'kjTime["\']?[^>]*>\s*([\d:]+)', tr)
                if not (mc and mt): continue
                title = _html.unescape(re.sub(r"<[^>]+>", " ", mt.group(1)))
                title = re.sub(r"\s+", " ", title).strip()
                if not title: continue
                out.setdefault(mc.group(1)[:4], []).append(
                    {"date": d, "time": (mm.group(1) if mm else ""), "title": title[:90]})
            if sum(len(v) for v in out.values()) == before: break   # 空ページ＝終端
    total = sum(len(v) for v in out.values())
    meta["tdnet"] = {"pages": pages, "codes": len(out), "items": total, "errors": errs}
    print(f"[適時開示] {pages}ページ / {len(out)}銘柄 / {total}件")
    return out

SCREEN_ENABLED   = True
SCREEN_BUDGET_S  = 2700      # 取得に使ってよい秒数（超えたら打ち切って結果を出す）
                             # マスタ経由だと対象が約4,000銘柄になるため 30分→45分。
                             # publicリポジトリのActionsは実行時間の上限が無い。
SCREEN_CHUNK     = 180       # 1リクエストあたりの銘柄数
MIN_TURNOVER     = 50_000_000   # 20日平均の売買代金がこの額未満は流動性不足として除外
MIN_PRICE        = 100          # 低位株を除外（呼値の粗さで往復コストが重くなる）
MIN_ATR_PCT      = 1.5          # これ未満は値幅が小さくスイングの期待値が立たない
MAX_ATR_PCT      = 6.0          # これを超えるものは一過性の材料で動いている公算が大きい。
                                # 材料の中身をこの仕組みは見られないので、順位以前に外す。

def load_universe(master=None):
    """スクリーニング対象の銘柄リスト。
       第一候補は JPX の上場銘柄一覧（正確・英字コードも拾える・数秒で済む）。
       取れなかったときだけ、従来どおり Yahoo を総当たりして作る。"""
    if master:
        keep = ("株式", "ETF/ETN", "REIT等", "出資証券")
        codes = [f"{c}.T" for c, v in sorted(master.items()) if v.get("kind") in keep]
        if len(codes) > 1000:
            from collections import Counter
            mix = Counter(v.get("kind") for v in master.values() if v.get("kind") in keep)
            print(f"[全銘柄] JPXマスタから {len(codes):,}銘柄を対象にする "
                  + " / ".join(f"{k}{n:,}" for k, n in mix.most_common()))
            meta["universe_source"] = "jpx_master"
            return codes
        meta["errors"].append(f"universe from master too small: {len(codes)}")

    p = f"{OUT}/universe.json"
    try:
        if os.path.exists(p):
            u = json.load(open(p, encoding="utf-8"))
            age = (NOW - dt.datetime.fromisoformat(u["built_at_jst"])).days
            if age < 30 and len(u.get("codes", [])) > 500:
                print(f"[全銘柄] universe.json を再利用 ({len(u['codes'])}銘柄 / {age}日前に作成)")
                return u["codes"]
    except Exception as e:
        meta["errors"].append(f"universe read: {e}")

    print("[全銘柄] JPXマスタが無いため総当たりで構築（初回のみ数分。英字コードは拾えない）")
    import yfinance as yf
    cands = [f"{c}.T" for c in range(1300, 10000)]
    found, t0, truncated = [], time.time(), False
    for i in range(0, len(cands), SCREEN_CHUNK):
        if time.time() - t0 > SCREEN_BUDGET_S:
            print("[全銘柄] 時間予算に達したため打ち切り"); truncated = True; break
        part = cands[i:i+SCREEN_CHUNK]
        try:
            d = yf.download(part, period="5d", interval="1d", auto_adjust=False,
                            group_by="ticker", threads=True, progress=False)
            for c in part:
                try:
                    if not d[c]["Close"].dropna().empty: found.append(c)
                except Exception:
                    pass
        except Exception as e:
            meta["errors"].append(f"universe chunk {i}: {type(e).__name__}")
        if (i // SCREEN_CHUNK) % 10 == 0:
            print(f"  {i+len(part)}/{len(cands)} 走査  有効 {len(found)}銘柄  {time.time()-t0:.0f}秒")
    # 途中で打ち切ったリストを保存すると、次回それを「完成品」として再利用し、
    # 欠けたユニバースのまま何日も走ることになる。完走したときだけ保存する。
    if truncated:
        meta["errors"].append(f"universe truncated at {len(found)}; not cached")
        print(f"[全銘柄] 打ち切りのため保存しない（{len(found)}銘柄／次回作り直す）")
    else:
        json.dump({"built_at_jst": NOW.isoformat(), "codes": found},
                  open(p, "w"), ensure_ascii=False)
        print(f"[全銘柄] {len(found)}銘柄を universe.json に保存")
    meta["universe_source"] = "bruteforce" + ("(truncated)" if truncated else "")
    return found

def screen_all(codes):
    """全銘柄の直近データを取得し、流動性と値幅で絞ってから指標を付ける。"""
    import yfinance as yf
    rows, t0 = [], time.time()
    done = 0
    skipped, skip_msg = {}, {}
    for i in range(0, len(codes), SCREEN_CHUNK):
        if time.time() - t0 > SCREEN_BUDGET_S:
            print(f"[全銘柄] 時間予算に達したため {done}/{len(codes)} で打ち切り"); break
        part = codes[i:i+SCREEN_CHUNK]
        try:
            d = yf.download(part, period="6mo", interval="1d", auto_adjust=False,
                            group_by="ticker", threads=True, progress=False)
        except Exception as e:
            meta["errors"].append(f"screen chunk {i}: {type(e).__name__}"); continue
        for c in part:
            done += 1
            try:
                x = d[c].dropna(subset=["Close"])
                if len(x) < 60: continue
                cl = x["Close"]; last = float(cl.iloc[-1])
                if last < MIN_PRICE: continue
                turnover = float((cl * x["Volume"]).tail(20).mean())
                if turnover < MIN_TURNOVER: continue
                a = _atr(x.reset_index())
                if not a or a != a: continue
                atr_pct = a / last * 100
                if atr_pct < MIN_ATR_PCT or atr_pct > MAX_ATR_PCT: continue
                adj, _ = repair(cl)
                w60 = x.tail(60)
                rows.append(dict(
                    code=c, close=last, turnover=turnover,
                    atr=round(a, 2), atr_pct=round(atr_pct, 2),
                    rsi=round(_rsi(adj.values), 1),
                    vs25=round((adj.iloc[-1]/adj.rolling(25).mean().iloc[-1]-1)*100, 2),
                    vs75=round((adj.iloc[-1]/adj.rolling(75).mean().iloc[-1]-1)*100, 2),
                    pos60=round((last-w60["Low"].min())/(w60["High"].max()-w60["Low"].min())*100, 1),
                    r20=round((adj.iloc[-1]/adj.iloc[-21]-1)*100, 2),
                    r60=round((adj.iloc[-1]/adj.iloc[-61]-1)*100, 2),
                    vol_ratio=round(float(x["Volume"].iloc[-1]/x["Volume"].tail(20).mean()), 2)))
            except Exception as e:
                # 握りつぶすと「全銘柄が同じ理由で落ちている」事故が見えなくなる。
                # 種類ごとに件数を数え、最初の1件はメッセージも残す。
                k = type(e).__name__
                skipped[k] = skipped.get(k, 0) + 1
                if k not in skip_msg: skip_msg[k] = str(e)[:80]
        if (i // SCREEN_CHUNK) % 5 == 0:
            print(f"  {done}/{len(codes)} 処理  通過 {len(rows)}銘柄  {time.time()-t0:.0f}秒")
    if skipped:
        meta["screen_skipped"] = {k: {"n": v, "例": skip_msg.get(k, "")}
                                  for k, v in sorted(skipped.items(), key=lambda x: -x[1])}
        print("[全銘柄] 除外の内訳:", ", ".join(f"{k}×{v}" for k, v in
              sorted(skipped.items(), key=lambda x: -x[1])[:5]))
        # フィルタ落ちではなく例外で全滅している場合は明確に警告する
        if len(rows) == 0 and done > 0:
            print(f"::warning::スクリーニングの通過が0件です。除外の内訳を data/meta.json で確認してください")
    return pd.DataFrame(rows), done

def rank_candidates(df):
    """順張りと逆張りは別の設定なので、混ぜずに分けて順位を付ける。
       単一の総合スコアにすると、性格の違う銘柄が同じ土俵で比較されて意味を失う。"""
    import numpy as np
    if df.empty: return df, df
    d = df.copy()
    heat = np.where(d["rsi"] > 75, (d["rsi"]-75)/25*20, 0)

    # 順張り: 移動平均の上に並び、60日レンジの上方にいて、20日が伸びている
    # 満点に達する水準が低すぎると、強い銘柄が全部同じ点になって順位が意味を失う。
    # 初回の実運用では上位15件が 97.1〜99.3 の 2.2点差に固まっていた（4項目が飽和）。
    # 実際の分布（25日 +9〜38% / 75日 +15〜84% / 20日 +11〜76%）に合わせて広げる。
    d["trend"] = (
        20*np.clip(d["vs25"]/12, 0, 1) +         # 25日線からの上方乖離（12%で満点）
        20*np.clip(d["vs75"]/30, 0, 1) +         # 75日線からの上方乖離（30%で満点）
        25*np.clip(d["pos60"]/100, 0, 1) +       # 60日レンジ内の位置
        20*np.clip(d["r20"]/25, 0, 1) +          # 20日リターン（25%で満点）
        15*np.clip((d["atr_pct"]-1.5)/2.5, 0, 1) # 値幅（スイング適性）
        - heat
    ).round(1)

    # 逆張り: 長期トレンドは生きている（75日線の上）が、短期で売られすぎ
    #   ★ 以前は「75日線を5%以上割ったら0」としていたが、これはほぼ効かなかった。
    #     初回の実運用では上位8件中6件が75日線の -3.7〜-5.0% に固まり、
    #     見出しの「長期は上向き」が事実と食い違っていた。
    #     下降トレンドの途中を「押し目」と呼ばないよう、75日線の上を必須にする。
    d["revert"] = (
        25*np.clip(-d["vs25"]/8, 0, 1) +         # 25日線を下回るほど高得点
        25*np.clip((45-d["rsi"])/25, 0, 1) +     # RSIが低いほど高得点
        20*np.clip((40-d["pos60"])/40, 0, 1) +   # 60日レンジの下方
        15*np.clip(d["vs75"]/10, 0, 1) +         # ただし長期は上向きであること
        15*np.clip((d["atr_pct"]-1.5)/2.5, 0, 1)
    ).round(1)
    d.loc[d["vs75"] < 0, "revert"] = 0           # 75日線の下にあるものは逆張り対象外

    return (d.sort_values("trend", ascending=False).head(20),
            d.sort_values("revert", ascending=False).head(20))

MASTER = {}
try:
    MASTER = load_master()
except Exception as e:
    meta["errors"].append(f"master outer: {type(e).__name__}: {e}")

DISC = {}
try:
    DISC = fetch_tdnet(days=4)
    json.dump({"generated_at_jst": NOW.isoformat(),
               "holdings": {c[:4]: DISC.get(c[:4], []) for c in HOLDINGS},
               "count_codes": len(DISC),
               "count_items": sum(len(v) for v in DISC.values())},
              open(f"{OUT}/disclosures.json", "w"), ensure_ascii=False, indent=1)
except Exception as e:
    meta["errors"].append(f"tdnet outer: {type(e).__name__}: {e}")
    meta["tdnet"] = {"error": str(e)[:120]}
    print("[適時開示] 失敗:", e)

def _decorate(recs):
    """候補の行に 銘柄名・業種・対応する業種ETF・当日の開示 を付ける。
       コードだけの表は人間が検証できず、材料の裏も取れないため必須。"""
    for r in recs:
        c4 = str(r["code"])[:4]
        m = MASTER.get(c4, {})
        r["name"]   = m.get("name", "")
        r["s17"]    = m.get("s17", "")
        r["kind"]   = m.get("kind", "")
        r["size"]   = m.get("size", "")
        s17 = m.get("s17", "")
        etf = S17_TO_ETF.get(s17, "")
        if s17 and not etf:
            # 表記のゆれで対応が取れないと、その業種の候補すべてでBが付かなくなる。
            # 実際に「情報通信・サービスその他」を1文字違いで書いていて全滅した。
            meta.setdefault("s17_unmapped", {})
            meta["s17_unmapped"][s17] = meta["s17_unmapped"].get(s17, 0) + 1
        r["sector_etf"] = etf[:4]
        r["news"]   = DISC.get(c4, [])[:3]
    return recs

try:
    if SCREEN_ENABLED:
        uni = load_universe(MASTER)
        if uni:
            sc, scanned = screen_all(uni)
            meta["screen"] = {"universe": len(uni), "scanned": scanned, "passed": len(sc),
                              "named": bool(MASTER),
                              "source": meta.get("universe_source", "?")}
            if not sc.empty:
                trend, revert = rank_candidates(sc)
                tr = _decorate(trend.to_dict("records"))
                rv = _decorate(revert.to_dict("records"))
            else:
                tr, rv = [], []
            # 通過0件でも必ず書く。書かないと report 側が「未実行」と表示してしまい、
            # 「走らせたが0件だった」という事故が「まだ動かしていない」に見える。
            json.dump({"generated_at_jst": NOW.isoformat(),
                       "universe": len(uni), "scanned": scanned, "passed": len(sc),
                       "master_count": len(MASTER),
                       "universe_source": meta.get("universe_source", "?"),
                       "tdnet": meta.get("tdnet", {}),
                       "skipped": meta.get("screen_skipped", {}),
                       "filters": {"min_turnover": MIN_TURNOVER, "min_price": MIN_PRICE,
                                   "min_atr_pct": MIN_ATR_PCT, "max_atr_pct": MAX_ATR_PCT},
                       "s17_unmapped": meta.get("s17_unmapped", {}),
                       "trend": tr, "revert": rv},
                      open(f"{OUT}/candidates.json", "w"), ensure_ascii=False,
                      indent=1, default=str)
            named = sum(1 for r in tr + rv if r["name"])
            print(f"[全銘柄] 走査{scanned} / 通過{len(sc)} / "
                  f"銘柄名あり {named}/{max(len(tr)+len(rv),1)} / candidates.json を生成")
except Exception as e:
    import traceback
    meta["errors"].append(f"screen: {type(e).__name__}: {e}")
    meta["screen"] = {"error": str(e)[:120]}
    print("[全銘柄] 失敗:", e); traceback.print_exc()

# ══════════════════════════════════════════════════════════════════════
#  6) 分析まで GitHub Actions 側でやりきる
#     狙い: Claudeのセッションが承認待ちや障害で止まっても、
#           リポジトリに完成したレポートが残る状態にする。
#           算術はコードが担い、モデルは「相場観」だけを担当する。
# ══════════════════════════════════════════════════════════════════════
print("\n[分析] 開始")

def load_positions():
    """positions.json があればそれを使う（保有変更時はこのファイルだけ直せばよい）。"""
    p = f"{OUT}/../positions.json"
    default = {"cash": 1144125, "holdings": {
        "1306.T": {"name": "NF TOPIX",        "acct": "NISA", "shares": 2870, "cost": 434.0},
        "1540.T": {"name": "純金信託",          "acct": "特定", "shares": 16,   "cost": 21399.0},
        "1615.T": {"name": "NF銀行業",         "acct": "特定", "shares": 190,  "cost": 767.0},
        "2563.T": {"name": "iS S&P500ヘッジ",  "acct": "特定", "shares": 1140, "cost": 412.0},
        "4755.T": {"name": "楽天グループ",       "acct": "特定", "shares": 100,  "cost": 917.0},
        "9432.T": {"name": "NTT",             "acct": "特定", "shares": 100,  "cost": 149.0}}}
    try:
        if os.path.exists(p):
            return json.load(open(p, encoding="utf-8"))
    except Exception as e:
        meta["errors"].append(f"positions.json: {e}")
    return default

def ttm_div(code, price, today):
    """events.json の集計値は使わない（窓の起点が最後の支払日で回数を誤る）。
       本日起点の過去365日・境界を含まないで数え直す。"""
    d = events.get(code, {}).get("dividends", [])
    if not d: return 0.0, 0.0
    lo = today - dt.timedelta(days=365)
    tot = sum(x["amount"] for x in d
              if lo < dt.date.fromisoformat(x["date"]) <= today)
    return tot, (tot/price*100 if price else 0.0)

try:
    import numpy as np
    POS = load_positions(); HOLD = POS["holdings"]; CASH = float(POS.get("cash", 0))
    BENCH = "1306.T"          # ベンチマークはTOPIX連動ETF。日経平均は値がさ株に歪められる
    today = NOW.date()
    px = {}; rep_log = {}
    for c in list(HOLD) + [BENCH, "_N225"]:
        p = f"{OUT}/ohlcv/{slug(c)}.csv"
        if not os.path.exists(p): continue
        d = pd.read_csv(p, parse_dates=["Date"]).sort_values("Date")
        d = d[d["Close"].notna()].reset_index(drop=True)
        col = "Adj Close" if "Adj Close" in d.columns and d["Adj Close"].notna().any() else "Close"
        adj, lg = repair(d.set_index("Date")[col])
        if lg: rep_log[c] = lg
        px[c] = {"raw": d, "adj": adj}

    bench = px[BENCH]["adj"]; n225 = px.get("_N225", {}).get("adj")
    rows = []; rets = {}
    for c, h in HOLD.items():
        if c not in px: continue
        d = px[c]["raw"]; adj = px[c]["adj"]; rets[c] = adj.pct_change()
        last = float(d["Close"].iloc[-1]); a = _atr(d)
        m = adj.ewm(span=12, adjust=False).mean() - adj.ewm(span=26, adjust=False).mean()
        hist = m - m.ewm(span=9, adjust=False).mean()
        j = pd.concat([adj, bench], axis=1, join="inner").dropna()
        def rs(k):
            return ((j.iloc[-1,0]/j.iloc[-1-k,0]-1) - (j.iloc[-1,1]/j.iloc[-1-k,1]-1))*100 if len(j) > k else float("nan")
        w60 = d.tail(60)
        dv, dy = ttm_div(c, last, today)
        rows.append(dict(code=c, name=h["name"], acct=h["acct"], shares=int(h["shares"]),
            cost=float(h["cost"]), close=last, mkt=round(last*h["shares"]),
            pl_pct=round((last/h["cost"]-1)*100, 2), bars=len(d),
            rsi=round(_rsi(adj.values), 1), atr=round(a, 2), atr_pct=round(a/last*100, 2),
            macd_hist=round(float(hist.iloc[-1]), 3),
            macd_dir="up" if hist.iloc[-1] > hist.iloc[-2] else "down",
            vs25=round((adj.iloc[-1]/adj.rolling(25).mean().iloc[-1]-1)*100, 2),
            vs75=round((adj.iloc[-1]/adj.rolling(75).mean().iloc[-1]-1)*100, 2),
            vs200=round((adj.iloc[-1]/adj.rolling(200).mean().iloc[-1]-1)*100, 2),
            pos60=round((last-w60["Low"].min())/(w60["High"].max()-w60["Low"].min())*100, 1),
            rs1=round(rs(1), 2), rs5=round(rs(5), 2), rs20=round(rs(20), 2),
            vol_ratio=round(float(d["Volume"].iloc[-1]/d["Volume"].tail(20).mean()), 2),
            div_ttm=round(dv, 2), div_yield=round(dy, 2),
            next_earnings=next((x for x in sorted(events.get(c, {}).get("earnings_dates", []))
                                if x >= str(today)), "")))
    t = pd.DataFrame(rows)
    TOT = float(t["mkt"].sum() + CASH)

    R = pd.DataFrame(rets).dropna(how="any").tail(120)
    C = R.corr()
    t["avg_corr"] = [round(float(C[c].drop(c).abs().mean()), 3) for c in t["code"]]
    t["A"] = (25*np.clip((t["atr_pct"]-0.5)/3.0, 0, 1)).round(1)
    heat = np.where(t["rsi"] > 75, (t["rsi"]-75)/25, 0)
    t["C"] = (25*np.clip(0.5*np.clip(t["pos60"]/100, 0, 1)
                         + 0.5*np.clip((t["rs20"]+8)/16, 0, 1) - heat, 0, 1)).round(1)
    t["D"] = (25*np.clip(1-(t["avg_corr"]-0.1)/0.5, 0, 1)).round(1)
    t["ACD"] = (t["A"]+t["C"]+t["D"]).round(1)   # B（レジーム適合）はモデルが判断して足す

    # サイジング: ATR基準と1銘柄15%上限の、小さいほう
    def size(r):
        stop_w = 2*r["atr"]
        n_atr = int(TOT*0.01 // stop_w) if stop_w > 0 else 0
        n_cap = int(max(TOT*0.15 - r["mkt"], 0) // r["close"]) if r["close"] > 0 else 0
        n = min(n_atr, n_cap); n = int(round(n, -1))
        return pd.Series([n, round(r["close"]-stop_w, 1), round(r["close"]+3*r["atr"], 1),
                          round(r["close"]+4*r["atr"], 1), round(n*r["close"]),
                          "ATR" if n_atr <= n_cap else "15%上限"])
    t[["add_shares","stop","tp3","tp4","add_cost","binding"]] = t.apply(size, axis=1)
    t = t.sort_values("ACD", ascending=False).reset_index(drop=True)

    nk = ""
    if n225 is not None:
        jj = pd.concat([bench.pct_change(), n225.pct_change()], axis=1, join="inner").dropna()
        gap = (jj.iloc[-1,1]-jj.iloc[-1,0])*100
        corr = float(jj.tail(120).corr().iloc[0,1])
        nk = (f"日経-TOPIX騰落差 {gap:+.2f}pt" + ("（**指数が歪んでいる。物色の偏りに注意**）" if abs(gap) > 1.0 else "（正常）")
              + f" / 健全性チェック: 1306と日経の120日相関 {corr:.3f}"
              + ("" if corr >= 0.80 else " ← **0.80未満。データを疑うこと**"))

    jgbline = "取得できず"
    if meta.get("jgb"):
        g = meta["jgb"]
        jgbline = f"{g['date']} 時点  2年 {g.get('y2')}% / 10年 {g.get('y10')}% / 30年 {g.get('y30')}%"

    L = []
    L.append(f"# 後場スイング 事前分析  {NOW:%Y-%m-%d %H:%M JST}\n")
    L.append(f"データ健全性: **{meta['health']}**  保有{meta['holdings_ok']}/{len(HOLDINGS)}  "
             f"全体{meta['n_ok']}/{meta['n_total']}  エラー{len(meta['errors'])}件\n")
    L.append(f"日本国債（財務省）: {jgbline}\n")
    if nk: L.append(f"{nk}\n")
    if rep_log:
        L.append("補正した系列: " + " / ".join(f"{k}: {'; '.join(v)}" for k, v in rep_log.items()) + "\n")
    L.append(f"\n総額 **¥{TOT:,.0f}**（保有 ¥{t['mkt'].sum():,.0f} + 現金 ¥{CASH:,.0f} = 現金比率 {CASH/TOT*100:.1f}%）")
    L.append(f"／ 1トレード許容損失(1.0%) ¥{TOT*0.01:,.0f}\n")

    L.append("\n## テクニカル（ベンチマーク=TOPIX/1306）\n")
    L.append("| コード | 銘柄 | 終値 | 損益% | RSI | MACD | 25日 | 75日 | 200日 | 60日位置 | ATR% | 対TOPIX 1d/5d/20d | 出来高 |")
    L.append("|---|---|--:|--:|--:|:-:|--:|--:|--:|--:|--:|--:|--:|")
    for _, r in t.iterrows():
        L.append(f"| {r['code'][:4]} | {r['name']} | {r['close']:,.1f} | {r['pl_pct']:+.2f} | {r['rsi']:.1f} | "
                 f"{'＋' if r['macd_hist']>0 else '−'}{'↑' if r['macd_dir']=='up' else '↓'} | {r['vs25']:+.1f}% | {r['vs75']:+.1f}% | "
                 f"{r['vs200']:+.1f}% | {r['pos60']:.0f}% | {r['atr_pct']:.2f} | "
                 f"{r['rs1']:+.1f} / {r['rs5']:+.1f} / {r['rs20']:+.1f} | {r['vol_ratio']:.2f}x |")

    L.append("\n## スコア（A/C/D は計算済み。**B＝レジーム適合はモデルが判断して加算する**）\n")
    L.append("| コード | 銘柄 | 評価額 | 比率 | A 適性 | C 位置 | D 分散 | A+C+D | B込み満点 |")
    L.append("|---|---|--:|--:|--:|--:|--:|--:|:-:|")
    for _, r in t.iterrows():
        L.append(f"| {r['code'][:4]} | {r['name']} | {r['mkt']:,.0f} | {r['mkt']/TOT*100:.1f}% | "
                 f"{r['A']:.1f} | {r['C']:.1f} | {r['D']:.1f} | **{r['ACD']:.1f}** | +B(0〜25) |")
    L.append(f"\n判定: 62以上=買い増し / 48以上=維持 / 34以上=縮小 / 34未満=売却")
    L.append("拒否ルール: C<5→損切り管理 ／ RSI>78→買い増し不可 ／ B≤2.5→維持以上にしない\n")

    L.append("\n## サイジング（ATR基準と1銘柄15%上限の小さいほう）\n")
    L.append("| コード | 銘柄 | 追加可能株数 | 必要資金 | 損切り | 利確3ATR | 利確4ATR | 制約 |")
    L.append("|---|---|--:|--:|--:|--:|--:|:-:|")
    for _, r in t.iterrows():
        L.append(f"| {r['code'][:4]} | {r['name']} | {r['add_shares']:,} | {r['add_cost']:,.0f} | "
                 f"{r['stop']:,.1f} | {r['tp3']:,.1f} | {r['tp4']:,.1f} | {r['binding']} |")


    # ── サイジング早見表（総額に依存しない。表を引くだけで株数が出る）──
    L.append("\n## サイジング早見表（許容損失いくらなら何株か）\n")
    L.append("**株数 = 許容損失 ÷ 2ATR幅**。許容損失は総額の0.75〜1.0%。損切り・利確は総額に依存しないのでそのまま使える。\n")
    L.append("| コード | 銘柄 | 現値 | 2ATR幅 | 損切り | 利確3ATR | 利確4ATR | 損失2万 | 2.5万 | 3万 | 3.5万 | 4万 |")
    L.append("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
    for _, r in t.iterrows():
        w = 2*r["atr"]
        cells = " | ".join(f"{int(x//w):,}" for x in (20000, 25000, 30000, 35000, 40000))
        L.append(f"| {r['code'][:4]} | {r['name']} | {r['close']:,.1f} | {w:,.1f} | "
                 f"{r['stop']:,.1f} | {r['tp3']:,.1f} | {r['tp4']:,.1f} | {cells} |")
    L.append("\n**1銘柄の上限は総額の15%**。上の株数と、(総額×15% − その銘柄の既存評価額) ÷ 現値 を比べて**小さいほう**を採る。")
    L.append("\n## 分配金と決算予定\n")
    L.append("| コード | 銘柄 | 年間分配 | 利回り | 次回決算発表 |")
    L.append("|---|---|--:|--:|:-:|")
    for _, r in t.iterrows():
        L.append(f"| {r['code'][:4]} | {r['name']} | {r['div_ttm']:,.2f} | {r['div_yield']:.2f}% | {r['next_earnings'] if isinstance(r['next_earnings'],str) and r['next_earnings'] else '—'} |")


    # ── セクター物色（業種別ETFの騰落＝値上がり/値下がり銘柄数の代わり）──
    sec = []
    for code, jname in SECTOR.items():
        sp = f"{OUT}/ohlcv/{slug(code)}.csv"
        if not os.path.exists(sp): continue
        try:
            sd = pd.read_csv(sp, parse_dates=["Date"]).sort_values("Date")
            sd = sd[sd["Close"].notna()]
            if len(sd) < 22: continue
            c0 = float(sd["Close"].iloc[-1])
            sec.append({"name": jname,
                        "d1": (c0/float(sd["Close"].iloc[-2])-1)*100,
                        "d5": (c0/float(sd["Close"].iloc[-6])-1)*100,
                        "d20": (c0/float(sd["Close"].iloc[-21])-1)*100})
        except Exception: pass
    if sec:
        sec.sort(key=lambda x: x["d1"], reverse=True)
        L.append("\n## セクター物色（業種別ETF・当日騰落順）\n")
        L.append("| 順 | 業種 | 当日 | 5日 | 20日 |")
        L.append("|--:|---|--:|--:|--:|")
        for i, x in enumerate(sec):
            mark = " ←保有" if x["name"] == "銀行" else ""
            L.append(f"| {i+1} | {x['name']}{mark} | {x['d1']:+.2f}% | {x['d5']:+.2f}% | {x['d20']:+.2f}% |")
        up = sum(1 for x in sec if x["d1"] > 0)
        L.append(f"\n上昇 {up}/{len(sec)} 業種。**業種の広がりが狭い日は指数が一部銘柄に歪められている**と判断する。")

    # ── ニュース見出し（Claude側のWebFetchを不要にするため、ここに載せる）──
    try:
        nj = json.load(open(f"{OUT}/news.json", encoding="utf-8"))
        if nj.get("matched"):
            L.append(f"\n## 市場関連の見出し（{len(nj['matched'])}件／取得 {nj['fetched_at_jst'][:16]}）\n")
            for n in nj["matched"][:35]:
                L.append(f"- [{n['src']}] {n['title']}")
            L.append("\n※ 見出しのみ。判断に必要なら本文を確認すること。")
    except Exception:
        L.append("\n## 市場関連の見出し\n\n取得できませんでした。")

    # ── 保有銘柄の適時開示（TDnet）──────────────────────────────
    #    決算・上方下方修正・自社株買い・increase/decrease of 配当 など、
    #    価格が動いた理由がここに出る。ニュース見出しより一次情報に近い。
    try:
        dj = json.load(open(f"{OUT}/disclosures.json", encoding="utf-8"))
        hd = {k: v for k, v in dj.get("holdings", {}).items() if v}
        n_items = sum(len(v) for v in hd.values())
        L.append(f"\n## 保有銘柄の適時開示（TDnet 直近4日／全体 {dj.get('count_items',0)}件）\n")
        if hd:
            L.append("| コード | 日付 | 時刻 | 表題 |")
            L.append("|---|:-:|:-:|---|")
            for c, items in sorted(hd.items()):
                for x in items[:4]:
                    L.append(f"| {c} | {x['date'][4:6]}/{x['date'][6:]} | {x.get('time','')} | {x['title']} |")
            L.append(f"\n※ 保有 {len(hd)}銘柄に {n_items}件。**開示が出た銘柄は指標より開示を優先して判断すること。**")
        else:
            L.append("保有銘柄に直近4日の適時開示はなし。")
            L.append("\n※ 「開示なし」は「材料なし」ではない。報道・需給・指数入替はTDnetには載らない。")
    except FileNotFoundError:
        L.append("\n## 保有銘柄の適時開示\n\n取得していません。")
    except Exception as e:
        L.append(f"\n## 保有銘柄の適時開示\n\n生成に失敗: {e}")

    # ── 全銘柄スクリーニングの結果 ─────────────────────────────
    try:
        cj = json.load(open(f"{OUT}/candidates.json", encoding="utf-8"))
        def _nm(r):
            n = (r.get("name") or "").strip()
            return n[:14] if n else "*(名称取得できず)*"
        def _disc(r):
            ns = r.get("news") or []
            if not ns: return "—"
            return " / ".join(f"{x['title'][:26]}" for x in ns[:2])

        L.append(f"\n## 新規候補（全銘柄スクリーニング）\n")
        _uni, _sc = cj.get("universe", 0), cj["scanned"]
        L.append(f"対象 {_uni:,}銘柄 → 走査 {_sc:,} → フィルタ通過 **{cj['passed']:,}銘柄**"
                 f"（売買代金20日平均 {cj['filters']['min_turnover']/1e8:.1f}億円以上／"
                 f"株価{cj['filters']['min_price']}円以上／"
                 f"ATR {cj['filters']['min_atr_pct']}〜{cj['filters'].get('max_atr_pct', 99)}%）")
        _un = cj.get("s17_unmapped") or {}
        if _un:
            L.append(f"\n> **注意: 業種名の対応が取れない値がある** → "
                     + " / ".join(f"「{k}」{v}件" for k, v in list(_un.items())[:5])
                     + "。該当銘柄は業種ETFが空欄になり、Bを機械的に付けられない。\n")
        L.append(f"銘柄マスタ {cj.get('master_count', 0):,}件（JPX上場銘柄一覧／"
                 f"ユニバースの出所: {cj.get('universe_source', '?')}）／"
                 f"適時開示 {cj.get('tdnet', {}).get('items', 0)}件・{cj.get('tdnet', {}).get('codes', 0)}銘柄\n")
        if _uni and _sc < _uni:
            L.append(f"\n> **注意: 時間切れで {_sc:,}/{_uni:,} までしか走査していない。** "
                     f"残り {_uni-_sc:,}銘柄は本日の候補に含まれていない。"
                     f"上位に見えるものが「全体の上位」とは限らないので、採用のハードルを上げること。\n")
        if cj.get("master_count", 0) == 0:
            L.append("\n> **注意: 銘柄マスタが取得できていない。** 候補に銘柄名と業種が付かず、"
                     "候補のBを機械的に付けられない。`openpyxl` が入っているか確認すること。\n")
        if not cj.get("trend") and not cj.get("revert"):
            L.append(f"\n> **本日の通過は0件。** これはフィルタが厳しすぎるか、走査側で例外が出ているかのどちらか。")
            sk = cj.get("skipped") or {}
            if sk:
                L.append("> 除外の内訳（例外の種類ごと）: "
                         + " / ".join(f"`{k}`×{v['n']}（例: {v['例']}）" for k, v in list(sk.items())[:4]))
                L.append("> **例外が大半を占める場合は、フィルタの結果ではなく不具合。** "
                         "この日の候補は信用しないこと。")
            else:
                L.append("> 例外は記録されていないので、フィルタ（売買代金・価格・ATR）で全件が落ちたということ。")
            L.append("")

        L.append("\n### 順張り候補（移動平均の上・レンジ上方・20日が伸びている）\n")
        L.append("| 順 | コード | 銘柄名 | 17業種 | 業種ETF | スコア | 終値 | RSI | 25日 | 75日 | 60日位置 | 20日 | ATR% | 売買代金(億) | 出来高比 | 当日の開示 |")
        L.append("|--:|---|---|---|:-:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|")
        for i, r in enumerate(cj["trend"][:15]):
            L.append(f"| {i+1} | {r['code'][:4]} | {_nm(r)} | {r.get('s17') or '—'} | {r.get('sector_etf') or '—'} | "
                     f"**{r['trend']:.1f}** | {r['close']:,.1f} | {r['rsi']:.0f} | "
                     f"{r['vs25']:+.1f}% | {r['vs75']:+.1f}% | {r['pos60']:.0f}% | {r['r20']:+.1f}% | "
                     f"{r['atr_pct']:.2f} | {r['turnover']/1e8:.1f} | {r['vol_ratio']:.2f}x | {_disc(r)} |")

        L.append("\n### 逆張り候補（長期は上向きだが短期で売られすぎ）\n")
        L.append("| 順 | コード | 銘柄名 | 17業種 | 業種ETF | スコア | 終値 | RSI | 25日 | 75日 | 60日位置 | 20日 | ATR% | 売買代金(億) | 当日の開示 |")
        L.append("|--:|---|---|---|:-:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|")
        for i, r in enumerate(cj["revert"][:15]):
            if r["revert"] <= 0: continue
            L.append(f"| {i+1} | {r['code'][:4]} | {_nm(r)} | {r.get('s17') or '—'} | {r.get('sector_etf') or '—'} | "
                     f"**{r['revert']:.1f}** | {r['close']:,.1f} | {r['rsi']:.0f} | "
                     f"{r['vs25']:+.1f}% | {r['vs75']:+.1f}% | {r['pos60']:.0f}% | {r['r20']:+.1f}% | "
                     f"{r['atr_pct']:.2f} | {r['turnover']/1e8:.1f} | {_disc(r)} |")

        L.append("\n**この表の読み方と限界**")
        L.append("- スコアは機械的な順位付けにすぎず、推奨ではない。順張りと逆張りは別の尺度なので**混ぜて比較しない**。")
        L.append("- 「業種ETF」列は業種別ETF17本の表と突き合わせるための対応コード。**候補のBはこの列の業種の位置から機械的に付けられる。**")
        L.append("- 「当日の開示」は TDnet の直近4日分（新しい順）。**空欄(—)は「開示が無い」であって「材料が無い」ではない**（報道・需給・指数入替は載らない）。")
        L.append("- **決算発表日の照合は入っていない。** 発注前に必ず個別に確認すること。")
        L.append("- ATR%が突出して高いものは一過性の材料で動いている可能性が高い。順位が上でも採用しない。")
    except FileNotFoundError:
        L.append("\n## 新規候補\n\nスクリーニング未実行。")
    except Exception as e:
        L.append(f"\n## 新規候補\n\n生成に失敗: {e}")
    L.append("\n## マクロ\n")
    L.append("| 指標 | 日付 | 値 | 前日比 |")
    L.append("|---|:-:|--:|--:|")
    for k, lbl in [("^N225","日経225"),("1306.T","TOPIX(1306)"),("^GSPC","S&P500"),("^VIX","VIX"),
                   ("^TNX","米10年"),("^TYX","米30年"),("JPY=X","ドル円"),("CL=F","WTI"),("GC=F","金")]:
        s = snap.get(k)
        if s: L.append(f"| {lbl} | {s['date']} | {s['close']:,.2f} | {s['chg_pct']:+.2f}% |")

    L.append(f"\n---\n本ファイルは GitHub Actions が自動生成（承認不要・完全無人）。"
             f"数値はすべてコードが計算しており、モデルによる算術は介在していない。\n"
             f"モデル側の担当は B（レジーム適合）の判断、ニュース・マクロの解釈、スコアの上書き判断、文章化。\n"
             f"発注前にSBI証券の板・気配で価格を確認すること。")

    open(f"{OUT}/report.md", "w", encoding="utf-8").write("\n".join(L))
    json.dump({"generated_at_jst": NOW.isoformat(), "total": TOT, "cash": CASH,
               "benchmark": BENCH, "jgb": meta.get("jgb"), "repairs": rep_log,
               "rows": t.to_dict("records")},
              open(f"{OUT}/analysis.json", "w"), ensure_ascii=False, indent=1, default=str)
    meta["analysis"] = "ok"
    print(f"[分析] 完了 {len(t)}銘柄 / 総額 ¥{TOT:,.0f} / report.md と analysis.json を生成")
except Exception as e:
    import traceback
    meta["errors"].append(f"analysis: {type(e).__name__}: {e}")
    meta["analysis"] = "failed"
    print("[分析] 失敗:", e); traceback.print_exc()

json.dump(meta, open(f"{OUT}/meta.json", "w"), ensure_ascii=False, indent=1)

print(f"保有銘柄: {meta['holdings_ok']}/{len(HOLDINGS)}  健全性: {meta['health']}")
print(f"エラー件数: {len(meta['errors'])}  価格の飛び: {meta['n_anomalies']}件  イベント: {meta['events_ok']}銘柄  ニュース: {meta.get('news_ok',0)}件")
for e in meta["errors"][:10]:
    print("  -", e)
if meta["health"] == "bad":
    print("::warning::保有銘柄の取得が不足しています。data/meta.json を確認してください")
# 部分的にでも取れていればコミットさせるため、常に正常終了する
sys.exit(0)
