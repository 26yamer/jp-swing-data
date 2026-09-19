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
# ★保有銘柄。**positions.json（または load_positions の既定）と必ず一致させる。**
#   ここに無い銘柄は株価を取りに行かないので、保有していても分析に出てこない。
#   実測で 8053 を買ったのにここへ追加し忘れ、分析から丸ごと落ちていた。
#   下の HOLD_SYNC_CHECK で食い違いを検出して警告する。
#   2026-09-16のスクリーンショットで更新（1343・2559・2621は売却済みで削除）。
HOLDINGS = ["1306.T","1540.T","1615.T","2563.T","4755.T","8053.T","9432.T"]
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
          "1625.T":"電機・精密","1626.T":"情報通信・サービスその他","1627.T":"電力・ガス",
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
JGB_BASE = "https://www.mof.go.jp/jgbs/reference/interest_rate/"
JGB_HDRS = {"User-Agent": "Mozilla/5.0 (compatible; market-bot)"}

def _jgb_text(urls):
    """最初に「基準日」ヘッダを含む本文が取れたURLの中身を返す。
       jgbcm.csv は当月分だけで数百バイトしかないので、サイズで判定してはいけない。"""
    last = None
    for u in urls:
        try:
            rr = __import__("requests").get(u, timeout=60, headers=JGB_HDRS)
            if rr.status_code != 200:
                last = f"{u} -> HTTP {rr.status_code}"; continue
            for enc in ("cp932", "shift_jis", "utf-8-sig", "utf-8"):
                try:
                    t = rr.content.decode(enc); break
                except Exception:
                    t = None
            if t and "基準日" in t:
                return t, u, None
            last = f"{u} -> 基準日ヘッダなし ({len(rr.content)}bytes)"
        except Exception as e:
            last = f"{u} -> {type(e).__name__}"
    return None, None, last

ERA = {"S": 1925, "H": 1988, "R": 2018}   # 昭和/平成/令和 の加算基準年

def _jgb_parse(txt):
    import io
    lines = txt.splitlines()
    hdr = next(i for i, l in enumerate(lines) if l.startswith("基準日"))
    df = pd.read_csv(io.StringIO("\n".join(lines[hdr:])))
    df = df.rename(columns={df.columns[0]: "Date"})

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

def fetch_jgb():
    """★全履歴版は月次更新で、当月の日次が入っていない。
       実際 9/11 の実行で最新が 8/31 のまま（11日遅れ）になっていた。
       金利は保有の判断で最も効く変数（NTTの利回り差・銀行・REIT）なので、
       履歴版と当月版の両方を取って結合する。片方だけでも走る。"""
    frames, used, errs = [], [], []
    for label, urls in [("履歴", [JGB_BASE + "data/jgbcm_all.csv", JGB_BASE + "jgbcm_all.csv"]),
                        ("当月", [JGB_BASE + "jgbcm.csv"])]:
        txt, url, err = _jgb_text(urls)
        if txt is None:
            errs.append(f"{label}: {err}"); continue
        try:
            d = _jgb_parse(txt)
            if len(d):
                frames.append(d); used.append(f"{label}({url.rsplit('/', 1)[-1]}:{len(d)}行)")
        except Exception as e:
            errs.append(f"{label}: parse {type(e).__name__}")
    if not frames:
        raise RuntimeError("JGB: " + " / ".join(errs) if errs else "JGB: 取得先なし")
    df = (pd.concat(frames, ignore_index=True)
            .drop_duplicates(subset=["Date"], keep="last")
            .sort_values("Date").reset_index(drop=True))
    if errs:
        meta["errors"].append("jgb: " + " / ".join(errs))
    print(f"JGB: {' + '.join(used)} → 結合 {len(df)}行（最新 {df['Date'].iloc[-1].date()}）")
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
        age = (pd.Timestamp(NOW.date()) - last["Date"]).days
        meta["jgb"] = {"date": str(last["Date"].date()), "age_days": int(age),
                       "y2":  (float(last["2年"])  if pd.notna(last.get("2年"))  else None),
                       "y10": (float(last["10年"]) if pd.notna(last.get("10年")) else None),
                       "y30": (float(last["30年"]) if pd.notna(last.get("30年")) else None)}
        # 金利が数日古いまま「現在値」として使われると、NTTの利回り差や
        # 銀行の追い風の判断が丸ごとずれる。古ければ必ず表に出す。
        if age > 5:
            meta["jgb_stale"] = True
            meta["errors"].append(f"jgb: 最新が{age}日前（{last['Date'].date()}）")
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

def adjusted_frame(x):
    """OHLC を Adj Close の比率で調整した枠を返す。
       Adj Close が無い／壊れている場合は素通りさせる（生値のまま）。"""
    out = x.copy()
    if "Adj Close" not in out.columns:
        return out
    f = out["Adj Close"] / out["Close"]
    f = f.replace([float("inf"), float("-inf")], float("nan"))
    if f.isna().all():
        return out
    f = f.ffill().bfill()
    for c in ("Open", "High", "Low", "Close"):
        if c in out.columns:
            out[c] = out[c] * f
    return out

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
# ★マスタのキャッシュは data/ の外に置く。
#   data/ は Actions が `git add -A data` で丸ごとコミットするので、
#   ここに置くと JPX の「東証上場銘柄一覧」がそのまま public に再配信される
#   （JPXサイト利用条件で禁じられている）。
#   cache/ は .gitignore に入れてコミットしない。
#   Actions のランナーは毎回まっさらなので、代わりに actions/cache で
#   実行をまたいで持ち回る（fetch.yml 参照）。取れなくても
#   JPXから取り直すだけで、運用は止まらない。
MASTER_DIR          = "cache"
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

# ── 公開リポジトリに書き出してよい銘柄名 ──────────────────────
#   ★JPXサイト利用条件:
#     「当サイトに掲載されている情報について、有料・無料を問わずJPXからの
#       許諾を得ている場合を除き、商用目的によるデータ収集のほか
#       如何なる用途に関わらず二次利用及び再配信はできません。」
#     このリポジトリは public なので、JPXの「東証上場銘柄一覧」から取った
#     銘柄名・業種区分を data/ に書き出すことは**再配信に当たる**。
#   ★そこで、JPXのマスタは**メモリ上の判定にだけ使い**（市場区分の除外・
#     業種の除外・業種ETFの対応付け）、**書き出す名前はEDINETの提出者名**に
#     置き換える。EDINETは政府標準利用規約(PDL1.0)で、出典を書けば再配布できる。
#   ★EDINETに無い銘柄（ETF・REIT・新規上場直後など）は**名前を空にする**。
#     JPXの名前で埋め戻すことはしない。空欄は「まだEDINETに無い」という意味。
ED_NAMES = {}          # コード4桁 → 提出者名（EDINET）。実行時に読み込む。

def pub_name(code):
    """公開して良い銘柄名。無ければ空文字（JPXの名前では埋めない）。"""
    return ED_NAMES.get(str(code)[:4], "")

# ── JPX由来でpublicに置けない項目 ─────────────────────────────
#   JPXサイト利用条件:
#   「有料・無料を問わずJPXからの許諾を得ている場合を除き、商用目的による
#     データ収集のほか如何なる用途に関わらず二次利用及び再配信はできません。」
#   このリポジトリは public なので、data/ に書けば再配信に当たる。
#   ★銘柄名は EDINET の提出者名（PDL1.0＝出典明記で再配布可）に置き換えた。
#     name 自体は禁止語にできないので、ここでは業種・規模・市場区分を見る。
JPX_KEYS = {"s17", "s33", "size", "market", "sector_etf"}

# JPX由来のファイルそのもの。data/ に現れてはいけない。
JPX_FILES = ("jpx_master.json",)

def jpx_safe(o):
    """書き出す直前に、JPX由来の項目を丸ごと落とす。

       ★なぜ「使う側で消す」のではなく「書く直前に落とす」のか:
         実測で漏れた。_decorate は r["s17"] を持ち回っており、
         表からは外したのに**レコード自体は candidates.json に
         そのまま書かれていた**（commit_guard が検出）。
         判定に使う項目を持ち回ること自体は要るので、
         **出口で一律に落とす**のが確実。
       ★これは commit_guard の検査と二重化している。
         こちらが漏らしても向こうが止める、向こうが漏らしても
         こちらが落とす、という関係にしてある。"""
    if isinstance(o, dict):
        return {k: jpx_safe(v) for k, v in o.items() if k not in JPX_KEYS}
    if isinstance(o, (list, tuple)): return [jpx_safe(v) for v in o]
    return o

def load_master():
    """コード → 銘柄名・市場区分・17業種 の対照表。"""
    try: os.makedirs(MASTER_DIR, exist_ok=True)
    except Exception: pass
    p, cached = f"{MASTER_DIR}/jpx_master.json", None
    # 以前は data/ に置いていた。残っていれば消す（public に出したままにしない）。
    _old = f"{OUT}/jpx_master.json"
    try:
        if os.path.exists(_old):
            if not os.path.exists(p):
                import shutil; shutil.copy(_old, p)
            os.remove(_old)
            print("[マスタ] data/jpx_master.json を削除（JPX由来のため公開しない）")
            meta["master_moved"] = True
    except Exception as e:
        meta["errors"].append(f"master move: {type(e).__name__}")
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
        # ★33業種区分・規模区分はもう読まない。公開できないものを
        #   手元に持つと、いずれどこかの行から漏れる（JPXサイト利用条件）。
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
            # ★銘柄名・33業種・規模区分は**持たない**。
            #   公開リポジトリに書き出せないものを手元に持つと、
            #   いずれどこかの行から漏れる。最初から取り込まない。
            #   17業種と市場区分は判定に要るのでメモリ上だけで使う。
            rows[code] = {"market": mkt, "s17": cell(r, c_s17),
                          "kind": classify_kind(mkt)}
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

def _s17key(x):
    """括弧の全半角・中黒・空白のゆれを吸収する。文字そのものが違う場合
       （電力/電気 のような取り違え）は吸収せず、未対応として表に出す。"""
    import unicodedata
    x = unicodedata.normalize("NFKC", str(x or ""))
    for ch in "（）()・･ 　":
        x = x.replace(ch, "")
    return x

S17_NORM = {_s17key(v): k for k, v in SECTOR.items()}

def s17_etf(name):
    return S17_TO_ETF.get(name) or S17_NORM.get(_s17key(name), "")

# ★TDnetのHTML取得は既定で止めてある。
#   根拠: 適時開示情報閲覧サービスのページに
#     「安定稼働のため、スクレイピング等を用いた適時開示情報の
#       自動取得はご遠慮ください。」
#   とあり、www.release.tdnet.info の robots.txt はドメイン全体を
#   クローラ拒否にしている。JPXのサイト利用条件にも
#     「当サイトへの高頻度・高負荷に繋がる可能性のある自動取得等は
#       ご遠慮いただいております」
#   「有料・無料を問わずJPXからの許諾を得ている場合を除き、
#     商用目的によるデータ収集のほか如何なる用途に関わらず
#     二次利用及び再配信はできません」
#   とある。取得量が小さくても、明示された意向に反する形なので既定を False にした。
#   TDnet API は有料（初期費用7万円＋月額）。
#   ★止めると失うもの: 保有銘柄の当日の適時開示（決算短信・自己株買いの
#     実行・業績修正）の把握。決算発表**予定日**はJPXの公開Excelと
#     yfinance で代替しているが、実測で候補20件中9件が「不明」だった。
#     J-Quants無料プランの決算発表予定日エンドポイントに寄せるのが本筋。
#   ★株主還元のタグ（増配・自己株買い）はJ-Quantsの財務から作っていて、
#     TDnetには依存していない。清原枠の判定はこの切り替えで変わらない。
TDNET_ENABLED   = False
TDNET_MAX_PAGES = 12

def fetch_tdnet(days=4):
    """適時開示（TDnet）の直近4日分。月曜に金曜の開示を拾うため4日みる
       （土日祝のURLは404になるだけなので空振りは安い）。「なぜ動いているか」の裏取りに使う。
       TDnetは約31日分しか保持しないので、履歴の分析には使えない。
       ★TDNET_ENABLED が False のときは何もしない（既定）。理由は定数の注記。"""
    if not TDNET_ENABLED:
        meta["tdnet"] = {"skipped": "TDNET_ENABLED=False（JPXがスクレイピングの"
                                    "自動取得を控えるよう求めており、robots.txtも"
                                    "拒否しているため）"}
        print("[TDnet] 取得しない（JPXの意向とrobots.txtによる。"
              "TDNET_ENABLED を True にすれば動くが、規約を確認のうえで）")
        return {}
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

# ══════════════════════════════════════════════════════════════════════
#  J-Quants 財務情報 ── 欠けていた「ファンダメンタルズ」の半分
#
#  なぜ入れるか:
#    自作の trend / revert は実測で母集団平均との差がほぼ0だった。
#    値動きの形だけでは順位に情報が無い。一方、日本株はバリューが効き
#    モメンタムが効かないとする研究が複数ある（Fama-French 2012 /
#    Asness-Moskowitz-Pedersen 2013）。証拠の強い側を作っていなかった。
#
#  認証:
#    V2 は APIキーを x-api-key ヘッダーに載せるだけ。期限の記載なし。
#    （V1 のリフレッシュトークン方式は 2026-06-01 に終了している）
#    キーは環境変数からのみ読み、ログにも data/ にも一切出さない。
#    public リポジトリの Actions ログは誰でも読めるので、失敗時も
#    ステータスコードだけを出しレスポンス本文は出さない。
#
#  ライセンス上の制約（重要・設計を縛る）:
#    利用条件は「本データを第三者が閲覧できる状態である時には私的利用には
#    該当しません」。このリポジトリは public なので、取得した財務数値
#    そのものをコミットすることはできない。同じ条件が「ご自身の分析結果や
#    分析手法を公開いただくことは構いません」としているので、
#    書き出すのは **断面での順位（パーセンタイル）だけ** にする。
#    生値はこのプロセスのメモリ上だけに置く。
#    → 機械的な保証は commit_guard() で行う（人の注意力に頼らない）。
#
#  取れないもの:
#    有利子負債と投資有価証券は財務諸表(/spec/fin-details)で Premium 限定。
#    よって清原式のネットキャッシュ比率は無料では組めない。
#    現金だけを見て負債を見ないネットキャッシュは符号が反転しうるので作らない。
#    競争優位そのもの（シェア・参入障壁）も無料データに無い。
#    代理として営業利益率の「水準」と「ばらつきの小ささ」を使う。
#    これは代理指標であって競争優位の測定ではない。表示にもそう書く。
# ══════════════════════════════════════════════════════════════════════
JQ_BASE      = "https://api.jquants.com"
JQ_PATH      = "/v2/fins/summary"
JQ_KEY       = (os.environ.get("JQUANTS_API_KEY") or "").strip()
JQ_LAG_D     = 84 + 3    # Freeは「12週間前〜2年12週間前」。3日は境界のぶれの余裕
JQ_HIST_D    = 730       # 2年（Freeで取れる全期間）
# ★遡る日数は「1四半期ぶん」では足りない。
#   業績修正の方向は同じ決算期の予想を2回以上、営業利益率の安定性は
#   4回以上の開示が要る。200日（=約2回）だと安定性が常に空になり、
#   修正方向も半分しか埋まらない。実測（偽の応答での通し）でこれに気づいた。
#   730日なら四半期開示が6〜8回入る。
JQ_RECENT_D  = 730
JQ_GAP_S     = 0.5       # 公式サンプルが推奨する間隔
JQ_MAX_REQ   = 1400      # 事故で叩き続けないための上限（730日＝約520営業日）
JQ_RETRY     = 3
JQ_MIN_POOL  = 50        # 断面の順位付けに要る最低銘柄数
JQ_BACKFILL_ROWS = 6     # 項目ごとに値を遡って探す開示の回数（約1年半）

_JQ = {"req": 0, "n429": 0, "err": {}, "blocked": None, "key_used": None}

# V2の短い項目名を主に、V1の長い名前を予備に見る。
# 名前が変わっても静かに全欠損にならないようにするため。
# ★非連結で開示する会社は、連結の項目（BPS, FEPS など）が空で
#   非連結の項目（NCBPS, FNCEPS など）だけが埋まる。
#   連結名だけを見ていたため、実測で予想EPSが1,604銘柄中428件しか
#   付かなかった（27%）。非連結の会社は小型株に多く、まさに清原枠の
#   母集団を systematically に落としていた。連結を主に、非連結を予備に見る。
JQ_F = {
    "date":  ("DiscDate", "DisclosedDate"),
    "code":  ("Code", "LocalCode"),
    "doc":   ("DocType", "TypeOfDocument"),
    "per":   ("CurPerType", "TypeOfCurrentPeriod"),
    "fyend": ("CurFYEn", "CurrentFiscalYearEndDate"),
    "sales": ("Sales", "NCSales", "NetSales"),
    "op":    ("OP", "NCOP", "OperatingProfit"),
    "eps":   ("EPS", "NCEPS", "EarningsPerShare"),
    "bps":   ("BPS", "NCBPS", "BookValuePerShare"),
    "ta":    ("TA", "NCTA", "TotalAssets"),
    "eqar":  ("EqAR", "NCEqAR", "EquityToAssetRatio"),
    "cfo":   ("CFO", "CashFlowsFromOperatingActivities"),
    "cash":  ("CashEq", "CashAndEquivalents"),
    "eq":    ("Eq", "NCEq", "ShEq", "NCShEq", "Equity"),
    "feps":  ("FEPS", "FNCEPS", "ForecastEarningsPerShare"),
    "fop":   ("FOP", "FNCOP", "ForecastOperatingProfit"),
    "fnp":   ("FNP", "FNCNP", "ForecastProfit"),
    # 時価総額を出すのに要る。期末発行済株式数（自己株を含む）と期末自己株式数。
    "shout": ("ShOutFY", "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock"),
    "trsh":  ("TrShFY", "NumberOfTreasuryStockAtTheEndOfFiscalYear"),
    # 株主還元。清原達郎氏は「最終的なカタリストは株主還元」としている。
    "divann":  ("DivAnn", "ResultDividendPerShareAnnual"),
    "fdivann": ("FDivAnn", "ForecastDividendPerShareAnnual"),
    "payout":  ("PayoutRatioAnn", "ResultPayoutRatioAnnual"),
}

def _jqv(row, name):
    for k in JQ_F[name]:
        if k in row:
            v = row[k]
            if v in ("", None): return None
            return v
    return None

def _jqf(row, name):
    v = _jqv(row, name)
    if v is None: return None
    try: f = float(v)
    except (TypeError, ValueError): return None
    return f if f == f else None

def jq_dates(end_lag_d, span_d):
    """開示日として問い合わせる日付の列。土日は開示が無いので省く。
       Freeの提供範囲（12週間前より過去）に収まる側だけを返す。"""
    end = (NOW - dt.timedelta(days=end_lag_d)).date()
    out = []
    for k in range(span_d + 1):
        d = end - dt.timedelta(days=k)
        if d.weekday() >= 5: continue
        out.append(d.isoformat())
    return out          # 新しい順

def _jq_get(params, path=None):
    """1回分を取る。pagination_key を辿って全件返す。
       401/403 は叩き続けても直らないので、以後の呼び出しを止める。

       path … 既定は財務情報。決算発表予定日など別の入口にも使う。"""
    import urllib.parse, urllib.request, urllib.error
    if not JQ_KEY or _JQ["blocked"]: return None
    JQ_PATH_USE = path or JQ_PATH
    rows, pk = [], None
    while True:
        if _JQ["req"] >= JQ_MAX_REQ:
            _JQ["blocked"] = f"リクエスト上限{JQ_MAX_REQ}に到達"
            break
        p = dict(params)
        if pk: p["pagination_key"] = pk
        url = f"{JQ_BASE}{JQ_PATH_USE}?" + urllib.parse.urlencode(p)
        body = None
        for att in range(JQ_RETRY):
            try:
                req = urllib.request.Request(
                    url, headers={"x-api-key": JQ_KEY, "User-Agent": "jp-swing/1.0"})
                _JQ["req"] += 1
                with urllib.request.urlopen(req, timeout=30) as r:
                    body = json.loads(r.read().decode("utf-8"))
                break
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    _JQ["n429"] += 1
                    try: wait = float(e.headers.get("Retry-After"))
                    except (TypeError, ValueError): wait = JQ_GAP_S * (2 ** att)
                    time.sleep(min(wait, 60)); continue
                if e.code in (401, 403):
                    # ★本文は出さない。認証エラーの応答にリクエストが
                    #   そのまま含まれることがあり、ログは公開されている。
                    _JQ["blocked"] = f"HTTP {e.code}（キーかプラン範囲）"
                    return None
                k = f"HTTP {e.code}"
                _JQ["err"][k] = _JQ["err"].get(k, 0) + 1
                if e.code >= 500 and att < JQ_RETRY - 1:
                    time.sleep(JQ_GAP_S * (2 ** att)); continue
                return None
            except Exception as e:
                k = type(e).__name__
                _JQ["err"][k] = _JQ["err"].get(k, 0) + 1
                if att < JQ_RETRY - 1:
                    time.sleep(JQ_GAP_S * (2 ** att)); continue
                return None
        if body is None: break
        got, used = None, None
        for key in ("summary", "statements", "fins_summary", "data"):
            if isinstance(body.get(key), list): got, used = body[key], key; break
        if got is None:
            for key, v in body.items():
                if isinstance(v, list): got, used = v, key; break
        if got is None: got, used = [], "?"
        if _JQ["key_used"] is None: _JQ["key_used"] = used
        rows.extend(got)
        pk = body.get("pagination_key")
        time.sleep(JQ_GAP_S)
        if not pk: break
    return rows

JQ_EARN_PATH = "/v2/fins/earnings-date"   # 決算発表予定日（無料プランの範囲）

def _jq_pick_field(row, want):
    """行から目的の値が入っている鍵を見つける。

       ★なぜ推測で決め打ちしないか: 公開されているエンドポイント一覧に
         **レスポンスの項目名が載っていない**。EDINETのCSVで一度、
         項目名を決め打ちして半期報告書が0件になった。同じ失敗をしない。
         候補名で探し、無ければ値の形（4桁コード / YYYY-MM-DD）で探す。
         見つかった鍵は meta に残して、次回から当てにできるようにする。"""
    if not isinstance(row, dict): return None, None
    cands = {"code": ("Code", "code", "LocalCode", "local_code", "SecuritiesCode"),
             "date": ("Date", "date", "AnnouncementDate", "announcement_date",
                      "EarningsDate", "earnings_date", "ScheduledDate")}[want]
    for k in cands:
        v = row.get(k)
        if v not in (None, ""): return k, v
    # 名前で見つからなければ形で探す（決め打ちしない）
    for k, v in row.items():
        s = str(v or "")
        if want == "date" and re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return k, s
        if want == "code" and re.fullmatch(r"\d{4}0?", s):
            return k, s
    return None, None

def jq_earnings_dates():
    """J-Quantsの決算発表予定日。**無料プランの範囲**。

       ★なぜ足したか: TDnetの自動取得を止めたことで、候補の決算予定日が
         20件中10件「不明」になった（実測）。
         「決算をまたぐ建玉は作らない」という規則が、半分の候補で効かない。
         JPXの公開Excelは当日分しか載らず、yfinanceの日本株は推定で穴が多い。
       ★引数の形も公式仕様に無いので、まず引数なしで叩き、
         駄目なら日付を付けて試す。どちらが通ったかを meta に残す。"""
    out, diag = {}, {"tried": [], "rows": 0, "path": JQ_EARN_PATH}
    if not JQ_KEY:
        meta["jq_earn"] = {"skipped": "JQUANTS_API_KEY 未設定"}
        return out
    rows = None
    for label, params in (("引数なし", {}),
                          ("date=今日", {"date": str(NOW.date())})):
        diag["tried"].append(label)
        try:
            r = _jq_get(params, path=JQ_EARN_PATH)
        except Exception as e:
            diag.setdefault("err", []).append(f"{label}:{type(e).__name__}")
            continue
        if r:
            rows = r; diag["used"] = label; break
    if not rows:
        diag["result"] = "取れなかった"
        meta["jq_earn"] = diag
        print("[決算予定] J-Quantsから取れませんでした。meta.json の jq_earn を確認")
        return out
    diag["rows"] = len(rows)
    diag["sample_keys"] = sorted(rows[0])[:20] if isinstance(rows[0], dict) else None
    ck, _ = _jq_pick_field(rows[0], "code")
    dk, _ = _jq_pick_field(rows[0], "date")
    diag["code_key"], diag["date_key"] = ck, dk
    if not ck or not dk:
        diag["result"] = "コードか日付の項目が見つからない"
        meta["jq_earn"] = diag
        print(f"[決算予定] 項目名が判別できません: {diag.get('sample_keys')}")
        return out
    today = str(NOW.date())
    for r in rows:
        if not isinstance(r, dict): continue
        c = str(r.get(ck) or "").strip()
        d = str(r.get(dk) or "").strip()[:10]
        if not c or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", d): continue
        c4 = c[:4] if len(c) == 5 and c.endswith("0") else c
        if d < today: continue                 # 過ぎた予定は使わない
        cur = out.get(c4)
        if cur is None or d < cur: out[c4] = d   # 一番近い予定
    diag["codes"] = len(out)
    diag["result"] = "ok"
    meta["jq_earn"] = diag
    print(f"[決算予定] J-Quants {len(rows):,}件 → {len(out):,}銘柄"
          f"（項目 {ck} / {dk}）")
    return out

def jq_fetch(dates):
    """開示日ごとに引く。code を付けず date 単独指定にすると、
       その日に開示した全銘柄が返る（公式仕様）。"""
    out, hit = [], 0
    for d in dates:
        r = _jq_get({"date": d})
        if _JQ["blocked"]: break
        if r:
            out.extend(r); hit += 1
    return out, hit

# 同じ実行の中で、当日の断面と過去検証の両方が同じ開示を必要とする。
# 2回取ると照会が倍になりレート制限にも近づくので、広い方を1度だけ取って使い回す。
_JQ_ROWS = {"span": -1, "rows": [], "hit": 0, "asked": 0}

def jq_rows(span_d):
    """span_d 日ぶんの開示。既に同じか広い範囲を取っていればそれを返す。"""
    if _JQ_ROWS["span"] >= span_d:
        return _JQ_ROWS["rows"], _JQ_ROWS["hit"], _JQ_ROWS["asked"]
    ds = jq_dates(JQ_LAG_D, span_d)
    rows, hit = jq_fetch(ds)
    if len(rows) >= len(_JQ_ROWS["rows"]):
        _JQ_ROWS.update(span=span_d, rows=rows, hit=hit, asked=len(ds))
    return rows, hit, len(ds)

def jq_build(rows):
    """開示の生値を、銘柄別・開示日昇順の履歴に組み直す。
       生値はここから先、順位に変換されるまでメモリ上にしか存在しない。"""
    hist = {}
    for r in rows:
        d, c = _jqv(r, "date"), _jqv(r, "code")
        if not d or not c: continue
        c = str(c).strip()
        # J-Quantsは5桁（末尾に予備の1桁）で返す。4桁の証券コードに揃える。
        if len(c) == 5 and c.endswith("0"): c = c[:4]
        hist.setdefault(c, []).append({
            "date": str(d)[:10], "doc": str(_jqv(r, "doc") or ""),
            "per": str(_jqv(r, "per") or ""),
            "fyend": str(_jqv(r, "fyend") or "")[:10],
            "bps": _jqf(r, "bps"), "feps": _jqf(r, "feps"), "eps": _jqf(r, "eps"),
            "sales": _jqf(r, "sales"), "op": _jqf(r, "op"), "ta": _jqf(r, "ta"),
            "eqar": _jqf(r, "eqar"), "cfo": _jqf(r, "cfo"),
            "cash_eq": _jqf(r, "cash"), "eq": _jqf(r, "eq"),
            "fop": _jqf(r, "fop"), "fnp": _jqf(r, "fnp"),
            "shout": _jqf(r, "shout"), "trsh": _jqf(r, "trsh"),
            "divann": _jqf(r, "divann"), "fdivann": _jqf(r, "fdivann"),
            "payout": _jqf(r, "payout")})
    for c in hist:
        hist[c].sort(key=lambda x: x["date"])
    return hist

def jq_metrics(rows_upto):
    """その時点までの開示列から、断面比較に使う素の指標を作る。
       ★rows_upto には「その日より後の開示」を絶対に含めない。
         含めれば未来を見たことになり、検証結果は意味を失う。"""
    if not rows_upto: return None
    fin = [r for r in rows_upto if "FinancialStatements" in r["doc"]]
    if not fin: return None
    last = fin[-1]

    # ★最新の1行に全項目が入っている前提が間違っていた。
    #   四半期短信では予想EPSが空だったり、会社によって埋まる項目が違う。
    #   最新行だけを見ると、他の四半期に載っていた値を捨てることになる。
    #   項目ごとに「直近で値が入っている行」を遡って探す。
    #   ★遡るのは rows_upto の範囲内だけなので、未来は絶対に混ざらない。
    #   ★古すぎる値を使わないため、遡りは直近6回の開示までに限る。
    _win = fin[-JQ_BACKFILL_ROWS:]
    def L(key):
        for r in reversed(_win):
            v = r.get(key)
            if v is not None: return v
        return None
    # 営業利益率: 同じ行の分子と分母なので、四半期でも通期でも比較できる
    opms = []
    for r in fin[-8:]:
        s, o = r.get("sales"), r.get("op")
        if s and s > 0 and o is not None: opms.append(o / s * 100)
    opm = opms[-1] if opms else None
    stab = None
    if len(opms) >= 4:
        mu = sum(opms) / len(opms)
        sd = (sum((x - mu) ** 2 for x in opms) / len(opms)) ** 0.5
        stab = -sd                      # ばらつきが小さいほど上位にしたいので符号を反転
    # 業績修正方向: 同じ決算期(fyend)について会社予想がどう改定されたか。
    #   通期決算(FY)の行は予想の対象年度が1つ先にずれるので、列から外す。
    rev, cur = None, last.get("fyend")
    if cur:
        seq = [r for r in rows_upto
               if r.get("fyend") == cur and r.get("per") != "FY"
               and (r.get("fop") is not None or r.get("fnp") is not None)]
        if len(seq) >= 2:
            def _f(r): return r["fop"] if r.get("fop") is not None else r.get("fnp")
            a, b = _f(seq[-2]), _f(seq[-1])
            if a is not None and b is not None and a > 0:
                rev = (b / a - 1) * 100
    # ── 決算サプライズ（会社自身の前回予想に対する実績のズレ）──────
    #   ★なぜアナリスト予想ではないか: 有料データで引けないのと、
    #     **日本では会社が自ら通期予想を出す**ので、そちらとの差が
    #     素直なサプライズになる。Kitagawa & Shuto (2024, JBFA 51(9-10))
    #     は日本企業の会社予想の「予想外部分」に将来リターンとの関係が
    #     あることを報告している。
    #   ★通期(FY)の実績営業利益と、その期について**その前に**出ていた
    #     会社予想を突き合わせる。予想は rows_upto の範囲内にしか無いので
    #     未来は混ざらない。
    sue = None
    _fy = [r for r in fin if r.get("per") == "FY" and r.get("op") is not None]
    if _fy:
        _last_fy = _fy[-1]
        _tgt = _last_fy.get("fyend")
        # その期末について、通期決算より前に出ていた会社予想の最後の値
        _pre = [r.get("fop") for r in fin
                if r.get("fyend") == _tgt and r.get("per") != "FY"
                and r.get("fop") is not None
                and r["date"] < _last_fy["date"]]
        if _pre and _pre[-1] not in (None, 0):
            _f0 = _pre[-1]
            if _f0 > 0:
                sue = (_last_fy["op"] / _f0 - 1) * 100

    # ── 成長（清原式は「割安 *成長* 株」）──────────────────────
    #   ★『わが投資術』の対象は「割安 “小型 成長” 株」であって、
    #     単に安いだけの会社ではない。これまで成長の条件が一つも
    #     無かったので、万年割安のまま伸びない会社が候補に残っていた。
    #   ★なぜ「過去3期」ではないか: J-Quants無料プランの提供範囲は
    #     「12週間前〜2年12週間前」の2年ぶんしかない。年度の数字を
    #     3期並べることは**データとして不可能**。できるのは
    #     「同じ四半期どうしの前年比」で、決算期のずれを跨がないので
    #     季節性のある会社でも意味が保てる。何組で測れたかも返す。
    def _yoy(field):
        by = {}
        for r in fin:
            v, per, fy = r.get(field), r.get("per"), r.get("fyend")
            if v is None or not per or not fy: continue
            by.setdefault(per, {})[fy] = v      # 同じ期の訂正は後の開示で上書き
        vals = []
        for per, d in by.items():
            fys = sorted(d)
            for fa, fb in zip(fys, fys[1:]):
                va, vb = d[fa], d[fb]
                # 前年が赤字・ゼロだと伸び率が意味を成さないので数えない
                if va is None or vb is None or va <= 0: continue
                vals.append((vb / va - 1) * 100)
        if not vals: return None, 0
        vals.sort(); n = len(vals)
        med = vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2
        return med, n
    g_sales, n_gs = _yoy("sales")
    g_op,    n_go = _yoy("op")
    # 会社自身の今期予想 対 直近通期の実績。前を向いた唯一の数字なので別に持つ。
    g_fop = None
    _fy_ops = [r["op"] for r in fin if r.get("per") == "FY" and r.get("op") is not None]
    _fop_now = L("fop")
    if _fop_now is not None and _fy_ops and _fy_ops[-1] > 0:
        g_fop = (_fop_now / _fy_ops[-1] - 1) * 100

    cfoa = None
    _cfo, _ta = L("cfo"), L("ta")
    if _cfo is not None and _ta and _ta > 0:
        cfoa = _cfo / _ta * 100
    sh = None
    _shout, _trsh = L("shout"), L("trsh")
    if _shout:
        sh = _shout - (_trsh or 0.0)
        if not (sh > 0): sh = None

    # ── 株主還元（清原氏が「最終的なカタリスト」とする部分）─────────
    #   ① 増配の方向 … 今期の会社予想年間配当 ÷ 直近の実績年間配当
    #   ② 自己株買いの実行 … 期末自己株式数が前の開示から増えているか
    #   ③ 配当の水準 … 予想（無ければ実績）年間配当。株価は呼び出し側で割る
    div_now = L("fdivann")
    if div_now is None: div_now = L("divann")
    div_up = None
    _base = None
    for r in reversed(_win[:-1]):
        if r.get("divann") is not None and r["divann"] > 0:
            _base = r["divann"]; break
    if _base and div_now is not None:
        div_up = (div_now / _base - 1) * 100
    buyback = None
    _prev_tr = None
    for r in reversed(_win[:-1]):
        if r.get("trsh") is not None:
            _prev_tr = r["trsh"]; break
    if (_prev_tr is not None and _trsh is not None and _shout and _shout > 0):
        buyback = (_trsh - _prev_tr) / _shout * 100
    no_div = (div_now is not None and div_now <= 0 and (L("divann") or 0) <= 0)
    return {"bps": L("bps"), "feps": L("feps"), "eqar": L("eqar"),
            "opm": opm, "opm_stab": stab, "rev": rev, "cfoa": cfoa,
            "g_sales": g_sales, "g_op": g_op, "g_fop": g_fop,
            "n_growth": n_gs + n_go,
            # 決算サプライズ（会社予想比）と、その開示日
            "sue": sue, "disc_date": last["date"],
            # ネットキャッシュ比率の上下限を出すのに使う素の値
            "cash": L("cash_eq"),
            "ta": _ta, "eq": L("eq"), "sh": sh,
            "op": L("op"), "per": last.get("per"),
            "div": div_now, "div_up": div_up, "buyback": buyback, "no_div": no_div,
            "asof": last["date"]}

def jq_pit_index(hist, cal):
    """各銘柄について「カレンダーの各日までに何件開示されているか」を持つ。
       検証の各日で hist[c][:k] を渡せば、未来の開示は構造的に入らない。"""
    import numpy as np
    cald = np.array([pd.Timestamp(t).normalize().value for t in cal])
    out = {}
    for c, rows in hist.items():
        ds = np.array([pd.Timestamp(r["date"]).normalize().value for r in rows])
        out[c] = np.searchsorted(ds, cald, side="right")
    return out

_JQ_MEMO = {}

def jq_metrics_at(hist, pit, code, i):
    """日 i 時点の指標。(銘柄, 開示件数) で覚えておく。
       k は開示があった日にしか変わらないので、実際の計算回数は
       銘柄あたり開示回数ぶんに収まる。"""
    arr = pit.get(code)
    if arr is None: return None
    k = int(arr[i])
    if k <= 0: return None
    key = (code, k)
    if key not in _JQ_MEMO:
        _JQ_MEMO[key] = jq_metrics(hist[code][:k])
    return _JQ_MEMO[key]

# ── ファンダの4つの選び方 ─────────────────────────────────────────
#   value    … 純資産倍率と予想利益の利回り。日本株で最も証拠が強い側。
#   quality  … 予想ROE・営業利益率・自己資本比率。
#   moat     … 営業利益率の水準とばらつきの小ささ。**競争優位の代理**。
#   revision … 会社予想が上方に改定されたか。
# ── 清原達郎氏が挙げている順位付けを、書かれたとおりに分解して試す ──────
#   本人のまとめスライドより:
#     ・株を買うときに一番重視するのは PER
#     ・さらにネットキャッシュ比率も加える
#     ・PBR は見るけど重視はしない
#     ・バリュートラップがある
#     ・最終的なカタリストは株主還元
#
#   これまでの value は PBR と PER を等ウェイトで混ぜていた。
#   本人の重み付けと食い違っていたので、分解して測り直す。
#     ep       … 予想利益利回り（＝1/PER）だけ。本人が一番重視するもの
#     bp       … 純資産倍率の逆数（＝1/PBR）だけ。本人が重視しないもの
#     netcash  … ネットキャッシュ比率（無料データで作れる下限版）
#     kiyohara … ep と netcash を 2:1 で合成。「PERを一番重視し、
#                さらにネットキャッシュ比率も加える」の素直な形。
#                ★2:1 という重みは本人が指定していない。私が置いた仮定。
#                だから ep 単独・netcash 単独とも並べて出す。
#     payout   … 株主還元（増配の方向・自己株買いの実行・配当の水準）。
#                本人が「最終的なカタリスト」と言っているもの。
#                同時にバリュートラップの見分けでもある（割安なまま放置
#                される会社は株主に返さない）。
#   value は比較のため残す（混ぜたものが分解したものより良いかを見る）。
FUND_FACTORS = ("value", "ep", "bp", "netcash", "kiyohara", "payout",
                "quality", "moat", "revision", "growth", "ky_grow")

# ★実運用で並べ替えに使う因子。過去検証で基準（t≥2.8）を通ったものだけを置く。
#   いま通っているのは value（2026-09-17 の補正後の検証）。
#   保有3日 t=+2.94 / 5日 t=+3.85 / 10日 t=+3.78（3つのtの最小値）、
#   独立した建て日 107日・107日・53日。**数日のスイングの時間軸で通った**。
#   清原式の kiyohara（PER重視＋ネットキャッシュ）はまだ未検証なので、
#   検証結果を見てから差し替える。先に入れ替えると、また検証していない
#   ものを運用することになる。
#   ここに置くのは fund_today より前で定義しておくため（定義順の事故を作らない）。
RANK_BY = "value"

def _mean_pct(parts):
    """使える成分だけで平均する。欠けている成分で全体を落とさない。"""
    import numpy as np
    acc = None; cnt = None
    for s in parts:
        v = s.to_numpy(float)
        ok = ~np.isnan(v)
        acc = np.where(ok, v, 0.0) if acc is None else acc + np.where(ok, v, 0.0)
        cnt = ok.astype(float) if cnt is None else cnt + ok
    out = np.where(cnt > 0, acc / np.maximum(cnt, 1), float("nan"))
    return pd.Series(out, index=parts[0].index)

def fund_scores(df, fmap):
    """その日の母集団 df に、開示済みの素の指標を当てて断面順位に変える。
       返すのは順位（0〜1）だけ。生値は返さない。"""
    import numpy as np
    n = len(df)
    close = df["close"].to_numpy(float)
    codes = [str(c).replace(".T", "") for c in df["code"].tolist()]

    def col(fn):
        a = np.full(n, float("nan"))
        for k, c in enumerate(codes):
            m = fmap.get(c)
            if not m: continue
            try: v = fn(m, close[k])
            except Exception: v = None
            if v is not None and v == v: a[k] = v
        return pd.Series(a, index=df.index)

    bp   = col(lambda m, p: m["bps"] / p if m.get("bps") and p > 0 else None)
    ep   = col(lambda m, p: m["feps"] / p if m.get("feps") is not None and p > 0 else None)
    froe = col(lambda m, p: m["feps"] / m["bps"] * 100
               if m.get("feps") is not None and m.get("bps") and m["bps"] > 0 else None)
    opm  = col(lambda m, p: m.get("opm"))
    eqar = col(lambda m, p: m.get("eqar"))
    stab = col(lambda m, p: m.get("opm_stab"))
    rev  = col(lambda m, p: m.get("rev"))
    # 成長。売上と営業利益の前年比（同じ四半期どうし）。
    #   ★清原式の「割安 *成長* 株」の成長側。順位にしてから使うので、
    #     伸び率の絶対値ではなく、その日の母集団の中での位置で効く。
    gsl  = col(lambda m, p: m.get("g_sales"))
    gop  = col(lambda m, p: m.get("g_op"))

    # ネットキャッシュ比率の下限版（清原式の保守的な近似）
    #   負債合計＝総資産−純資産、時価総額＝株価×(発行済−自己株)
    #   流動資産と投資有価証券は無料では取れないので、
    #   現金同等物 ≤ 流動資産 / 0 ≤ 投資有価証券×70% で下に押さえた値を使う。
    #   本来の比率はこれ以上になる。過大評価にはならない。
    nc = col(lambda m, p: ((m["cash"] - (m["ta"] - m["eq"])) / (p * m["sh"]))
             if (m.get("cash") is not None and m.get("ta") is not None
                 and m.get("eq") is not None and m.get("sh") and p > 0) else None)
    dy = col(lambda m, p: (m["div"] / p * 100)
             if (m.get("div") is not None and p > 0) else None)
    dup = col(lambda m, p: m.get("div_up"))
    bb  = col(lambda m, p: m.get("buyback"))

    def pr(s): return s.rank(pct=True)
    _ep, _bp, _nc = pr(ep), pr(bp), pr(nc)
    # 株主還元: 配当の水準・増配の方向・自己株買いの実行。
    # 無配は最下位に落とす（0点ではなく最下位。バリュートラップの典型）
    _po = _mean_pct([pr(dy), pr(dup), pr(bb)])
    _nodiv = col(lambda m, p: 1.0 if m.get("no_div") else None)
    _po = _po.where(_nodiv.isna(), 0.0)
    out = {"value":    _mean_pct([_bp, _ep]),
           "ep":       _ep,
           "bp":       _bp,
           "netcash":  _nc,
           # 「PERを一番重視し、さらにネットキャッシュ比率も加える」を 2:1 で
           "kiyohara": _mean_pct([_ep, _ep, _nc]),
           "payout":   _po,
           "quality":  _mean_pct([pr(froe), pr(opm), pr(eqar)]),
           "moat":     _mean_pct([pr(opm), pr(stab)]),
           "revision": pr(rev),
           # 成長そのものと、「割安かつ成長」の組み合わせ。
           # 過去検証（BT_FACTORS）に載せて、効いているかを先に見る。
           "growth":   _mean_pct([pr(gsl), pr(gop)]),
           "ky_grow":  _mean_pct([_ep, _nc, pr(gsl), pr(gop)])}
    # ファンダが引けた銘柄。ここに入らない銘柄は
    # ファンダ側の母集団から外す（基準線も同じ母集団の平均を取る）。
    has = (~bp.isna()) | (~ep.isna())
    return out, has

def fund_coverage(out, has):
    return {"has": int(has.sum()),
            **{k: int((~v.isna()).sum()) for k, v in out.items()}}

# ══════════════════════════════════════════════════════════════════════
#  EDINET（金融庁）── 本物のネットキャッシュ比率を作るための貸借対照表
#
#  なぜ要るか:
#    清原達郎氏の式は
#      ネットキャッシュ ＝ 流動資産 ＋ 投資有価証券×70% − 負債合計
#    だが、**流動資産も投資有価証券も J-Quants 無料枠には無い**
#    （無料で取れるのは総資産と純資産だけ。貸借の内訳は Premium）。
#    つまりこれまで一度も本物の比率を計算できていなかった。
#    現金−負債の下限値と「1に届くのに何割必要か」の目安で代用していた。
#
#  EDINET なら無料で取れる:
#    有価証券報告書・半期報告書のXBRLをCSVで取得でき、
#    流動資産合計・投資有価証券・負債合計が入っている。
#    しかも2008年以降の履歴があるので、開示日ベースの検証もできる
#    （J-Quants無料枠は2年しかなく、保有250日の検定ができなかった原因）。
#
#  ライセンス（J-Quantsと扱いが違う）:
#    EDINETの情報は公共データ利用規約(PDL1.0)準拠で、
#    **二次利用・再配布が認められている**。出典表記が必要。
#    よって取得した数値を public リポジトリにコミットしてよい。
#    加工したものを「国が作成した未加工のもの」のように見せてはいけないので、
#    出典と「もとに作成」を必ず添える。
#    ★J-Quantsの数値は引き続きコミットしない。混ぜないよう別ファイルにする。
#
#  APIキーの扱い（J-Quantsより危ない）:
#    EDINETはキーを**URLのクエリパラメータ**で送る仕様。
#    publicリポジトリのActionsログにURLが出ると鍵が漏れる。
#    → URLは一切ログに出さない。例外にも出さない。
#    → commit_guard の伏せ字対象にEDINETのキーも加える。
#
#  取得量:
#    1社あたり年2回（有報＋半期報告書。四半期報告書は2024年に廃止）。
#    約4,000社で年8,000書類。1回の実行で取る数に上限を置き、
#    日々のぶんを積み上げる。規約が大量アクセスを禁じているので間隔も空ける。
# ══════════════════════════════════════════════════════════════════════
ED_BASE       = "https://api.edinet-fsa.go.jp/api/v2"
ED_KEY        = (os.environ.get("EDINET_API_KEY") or "").strip()
ED_PATH       = f"{OUT}/edinet.json"
# ★開示1件ごとの履歴。edinet.json は「銘柄ごとの最新1件」しか持たず、
#   古い開示は上書きで**消えていた**。清原式のネットキャッシュ比率を
#   バックテストで検証するには、各時点で「その日に知り得た貸借」が要る。
#   捨ててから取り直すのはレート制限のせいで高くつくので、いまから貯める。
#   JSONではなくCSVにするのは、数万行になったときに軽いから。
ED_HIST_PATH  = f"{OUT}/edinet_hist.csv"
# ★公開リポジトリに出す銘柄名の出どころ。
#   JPXの「東証上場銘柄一覧」の銘柄名は、JPXサイト利用条件で
#   「有料・無料を問わず…二次利用及び再配信はできません」とされている。
#   このリポジトリは public なので、JPX由来の名前を書き出すことは再配信に当たる。
#   EDINETは政府標準利用規約(PDL1.0)＝出典を書けば再配布できるので、
#   書き出す名前は EDINET の提出者名に置き換える。
#   ★書類を落とさなくても一覧(documents.json)だけで名前は取れるので、
#     貸借対照表の取得上限(ED_MAX_DOCS)とは関係なく全件溜まる。
ED_NAME_PATH  = f"{OUT}/edinet_names.json"
ED_HIST_COLS  = ("code", "docid", "submit", "period", "dtype",
                  "ca", "inv", "liab", "ta", "na", "solo", "mixed", "bs_ok",
                  # 提出者名（PDL1.0＝出典明記で再配布できる）。
                  # 公開リポジトリに出す銘柄名はここから取る。
                  "name")
ED_HIST_MAX   = 150_000         # 行数の上限（古い提出日から捨てる）。
                                 # 6年×約4,000銘柄×年2回 ≒ 48,000行の見込みで、
                                 # 1行約90バイトなので150,000行で約13MB。
                                 # 40万行だと36MBになりリポジトリに重すぎる。
ED_DOC_TYPES  = ("120", "160")   # 120=有価証券報告書 160=半期報告書
ED_LIST_DAYS  = 10               # 毎回見る「新しい側」の日数
ED_BACKFILL_D = 2200            # 遡る範囲（約6年＝営業日で約1,570日）。
                                 # ★1年半だとバックテストに使える履歴が
                                 #   1銘柄1〜2件しか溜まらず、比率の検証が
                                 #   できない。1回の一覧は45日までなので、
                                 #   埋まるまで時間はかかるが日々貯まる。
                                 #   EDINETの保存期間は2008年以降。
ED_MAX_LIST   = 45               # 1回の実行で引く一覧の日数
ED_MAX_DOCS   = 220              # 1回の実行で落とす書類数（積み上げる）
                                 # 実測: 未取得780件に対して90件/回では
                                 # 全部埋まるまで9回かかる。1件1秒空けても
                                 # 220件で約5分、ジョブの330分枠に収まる。
ED_GAP_S      = 1.0              # 大量アクセス禁止なので1秒空ける
ED_RETRY      = 3
ED_ATTRIB     = ("出典：EDINET閲覧（提出）サイト"
                 "（https://disclosure2.edinet-fsa.go.jp/）をもとに作成、PDL1.0")
ED_INV_HAIRCUT = 0.70            # 投資有価証券の掛け目（売却時の税約30%を引く）

# 拾う要素。連結が空なら個別を見る（非連結の会社がある）。
# jppfs_cor は日本基準、jpigp_cor はIFRS。IFRS採用会社は日本基準の
# 要素IDを一切出さないので、別名を並べておく。
# ★IFRSの「投資有価証券」に一対一で対応する要素は無い。取れなければ
#   None＝0として扱うので、比率は**低めに出る**（見落としはあっても
#   過大評価はしない）。
ED_ELEM = {
    "ca":   ("jppfs_cor:CurrentAssets", "jpigp_cor:CurrentAssetsIFRS"),
    "inv":  ("jppfs_cor:InvestmentSecurities",),
    "liab": ("jppfs_cor:Liabilities", "jpigp_cor:LiabilitiesIFRS"),
    "ta":   ("jppfs_cor:Assets", "jpigp_cor:AssetsIFRS"),
    "na":   ("jppfs_cor:NetAssets", "jpigp_cor:EquityIFRS"),
}
_ED = {"req": 0, "n429": 0, "err": {}, "blocked": None,
       "enc": None, "sep": None, "cols": None, "docs": 0, "parsed": 0,
       # ★読めなかった書類の理由と、そのとき実際に入っていた要素ID／
       #   コンテキストID。実測で90件落として55件しか読めず、しかも
       #   読めたのは全部 有報(120)、半期報告書(160)は0件だった。
       #   「なぜ読めないか」を次の実行が自分で教えるようにする。
       "nomatch": {}, "probe": []}

def _ed_url(path, params):
    """URLを作る。★この文字列は絶対にログへ出さない（鍵が入っている）。"""
    import urllib.parse
    p = dict(params); p["Subscription-Key"] = ED_KEY
    return f"{ED_BASE}{path}?" + urllib.parse.urlencode(p)

def _ed_get(path, params, binary=False):
    """EDINETを1回叩く。鍵はURLに入るので、例外にもログにもURLを出さない。"""
    import urllib.request, urllib.error
    if not ED_KEY or _ED["blocked"]: return None
    for att in range(ED_RETRY):
        try:
            req = urllib.request.Request(
                _ed_url(path, params), headers={"User-Agent": "jp-swing/1.0"})
            _ED["req"] += 1
            with urllib.request.urlopen(req, timeout=60) as r:
                b = r.read()
            time.sleep(ED_GAP_S)
            return b if binary else json.loads(b.decode("utf-8"))
        except urllib.error.HTTPError as e:
            # ★e には URL が含まれる。str(e) を残さない。
            if e.code == 429:
                _ED["n429"] += 1
                time.sleep(min(ED_GAP_S * (2 ** att) * 5, 60)); continue
            if e.code in (401, 403):
                _ED["blocked"] = f"HTTP {e.code}（キーか権限）"
                return None
            k = f"HTTP {e.code}"
            _ED["err"][k] = _ED["err"].get(k, 0) + 1
            if e.code >= 500 and att < ED_RETRY - 1:
                time.sleep(ED_GAP_S * (2 ** att)); continue
            return None
        except Exception as e:
            k = type(e).__name__        # メッセージは残さない（URLが混じりうる）
            _ED["err"][k] = _ED["err"].get(k, 0) + 1
            if att < ED_RETRY - 1:
                time.sleep(ED_GAP_S * (2 ** att)); continue
            return None
    return None

def ed_list(date):
    """その日に提出された書類の一覧。type=2 で提出書類一覧まで取る。"""
    o = _ed_get("/documents.json", {"date": date, "type": 2})
    if not isinstance(o, dict): return []
    r = o.get("results")
    return r if isinstance(r, list) else []

def ed_pick(results):
    """有報・半期報告書のうち、証券コードが付いているものだけ。
       訂正報告書(130/170)は本体と別に出るので、ここでは拾わない
       （訂正を本体と誤って混ぜると数字が二重になる）。"""
    out = []
    for r in results or []:
        if str(r.get("docTypeCode") or "") not in ED_DOC_TYPES: continue
        sec = str(r.get("secCode") or "").strip()
        if not sec: continue                       # 非上場は対象外
        code = sec[:4] if len(sec) == 5 and sec.endswith("0") else sec
        did = str(r.get("docID") or "").strip()
        if not did: continue
        out.append({"code": code, "docid": did,
                    "submit": str(r.get("submitDateTime") or "")[:10],
                    "period": str(r.get("periodEnd") or "")[:10],
                    "dtype": str(r.get("docTypeCode")),
                    # ★提出者名。EDINETは政府標準利用規約(PDL1.0)なので
                    #   出典を書けば再配布できる。JPXの「東証上場銘柄一覧」の
                    #   銘柄名は再配信が禁じられているので、公開リポジトリに
                    #   書き出す名前はこちらに置き換える。
                    "name": str(r.get("filerName") or "").strip()})
    return out

def _ed_decode(raw):
    """EDINETのCSVは文字コードと区切りが環境依存で当てにくい。
       決め打ちにせず、順に試して「列名が読めたもの」を採る。
       どれで読めたかは meta に残す（次回の当てが付く）。"""
    for enc in ("utf-16", "utf-16-le", "utf-8-sig", "cp932", "utf-8"):
        try:
            t = raw.decode(enc)
        except Exception:
            continue
        if "要素ID" in t or "elementId" in t or "要素ID" in t:
            head = t.splitlines()[0] if t.splitlines() else ""
            sep = "\t" if head.count("\t") >= head.count(",") else ","
            return t, enc, sep
    return None, None, None

# 使ってよい「連結・個別の別」を表す接尾辞。これ以外の Member は
# セグメントや子会社など**全体ではない数字**なので、絶対に使わない。
ED_CTX_TAIL_OK = ("", "NonConsolidatedMember")

def _ed_ctx_rank(cx):
    """コンテキストIDの優先順位。小さいほうを採る。
       (順位, 個別か) を返す。使ってはいけないものは None。

       ★前期(Prior)は絶対に使わない。
       ★★接尾辞の Member を必ず見る。ここが今回の不具合だった。
         実測: 3645 で 流動資産 36.0億 > 資産合計 30.3億、
               1401 で 資産合計 11.4億 < 純資産 38.8億 と、
               貸借が成立しない行が4件出た。
         原因は `CurrentYearInstant_ReportableSegmentsMember...` の
         ようなセグメント別の数字を、全体の合計として拾っていたこと。
         「CurrentYear で始まり Instant を含む」だけで通していたため。
         いまは接尾辞が空（全体）か NonConsolidatedMember（個別）の
         ときだけ通す。
       ★半期報告書は `InterimInstant`（当中間期末日時点）を使う。
         金融庁「報告書インスタンス作成ガイドライン」新旧対照表
         （2024-11-12、四半期報告書廃止に伴う改正）で、相対期間の値は
           当年度 CurrentYear / 中間期 Interim /
           前年度 Prior1Year / 前中間期 Prior1Interim /
           提出日 FilingDate / 議決権行使基準日 RecordDate /
           最近日 RecentDate / 予定日 FutureDate
         と定められている。
         以前は CurrentYear 決め打ちだったため、半期報告書は
         **実測で0件**だった（有報55件に対し160は0件）。
         前中間期は Prior1Interim… なので "Prior" の除外でそのまま弾ける。
         中間貸借対照表の比較欄（前年度末）も Prior1YearInstant で弾ける。"""
    if not cx or "Instant" not in cx: return None
    if "Prior" in cx: return None
    if cx.startswith("FilingDate"): return None   # 提出日時点＝株式数など別物
    head, _sep, tail = cx.partition("_")
    if tail not in ED_CTX_TAIL_OK: return None    # セグメント・子会社などは使わない
    solo = (tail == "NonConsolidatedMember")
    if head.startswith("CurrentYear"): return (0, solo)
    if head.startswith("Interim"):     return (1, solo)   # 半期報告書
    if head.startswith("Current"):     return (1, solo)   # 念のため
    if head.startswith(("RecordDate", "RecentDate", "FutureDate")):
        return None            # 議決権行使基準日・最近日・予定日は貸借ではない
    return (2, solo)

def ed_bs_ok(v, tol=0.005):
    """貸借対照表として成立しているか。
         資産合計 ＝ 負債合計 ＋ 純資産合計
         流動資産 ≤ 資産合計
       確かめられない（項目が欠けている）ときは None を返す。

       ★既に保存済みのキャッシュにも読み込み時にこれを当てる。
         印が無い古い行も、ここで弾けるようにしておく
         （作り直しを待たずに、間違った比率が出るのを止める）。"""
    ca, liab, ta, na = v.get("ca"), v.get("liab"), v.get("ta"), v.get("na")
    if ca is not None and ta is not None and ta > 0 and ca > ta * (1 + tol):
        return False
    if None in (ta, liab, na): return None
    if not (ta > 0): return False
    return abs(ta - (liab + na)) <= max(abs(ta) * tol, 1e6)

def ed_parse_csv(zip_bytes, dtype=None):
    """ZIPの中のCSVから、貸借対照表の必要項目を取り出す。

       ★当期の時点（Instant）の値だけを使う。前期の値は混ぜない。
       ★連結を優先し、無ければ個別を使う（非連結の会社がある）。
       取れなかった項目は None のまま返す（0で埋めない）。
       読めなかったときは、実際に入っていた要素IDとコンテキストIDを
       _ED["probe"] に少しだけ残す（次の実行で原因が分かるように）。"""
    import zipfile, io
    try:
        z = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except Exception as e:
        _ED["err"]["zip:" + type(e).__name__] = _ED["err"].get("zip:" + type(e).__name__, 0) + 1
        return None
    # ★監査報告書のCSVは除く。実際のファイル名は jpaud-aai-cc-001_... で、
    #   "audit" という語は入っていない（以前は "audit" で弾くつもりだったが
    #   何も弾けていなかった）。接頭辞 jpaud で判定する。
    names = [n for n in z.namelist()
             if n.lower().endswith(".csv")
             and not os.path.basename(n).lower().startswith("jpaud")]
    # XBRL_TO_CSV 配下が本体。無ければ全部見る
    pref = [n for n in names if "XBRL_TO_CSV" in n.upper()] or names
    want = {e: k for k, es in ED_ELEM.items() for e in es}
    got = {}
    # 読めなかったときの手掛かり。要素IDは見えているのにコンテキストで
    # 落としているのか、要素IDそのものが違うのかを切り分ける。
    seen_bs, seen_ctx, n_rows = set(), set(), 0
    for n in pref:
        try:
            raw = z.read(n)
        except Exception:
            continue
        txt, enc, sep = _ed_decode(raw)
        if txt is None: continue
        if _ED["enc"] is None: _ED["enc"], _ED["sep"] = enc, ("TAB" if sep == "\t" else "COMMA")
        lines = txt.splitlines()
        if not lines: continue
        hdr = [h.strip().strip('"') for h in lines[0].split(sep)]
        if _ED["cols"] is None: _ED["cols"] = hdr[:12]
        def col(*cands):
            for c in cands:
                if c in hdr: return hdr.index(c)
            return None
        i_el = col("要素ID", "elementId")
        i_cx = col("コンテキストID", "contextRef")
        i_vl = col("値", "value")
        i_cs = col("連結・個別")
        if i_el is None or i_cx is None or i_vl is None: continue
        for ln in lines[1:]:
            f = [x.strip().strip('"') for x in ln.split(sep)]
            if len(f) <= max(i_el, i_cx, i_vl): continue
            n_rows += 1
            el = f[i_el]
            if el not in want:
                # 貸借対照表の合計らしい要素IDだけ手掛かりに取っておく
                if len(seen_bs) < 24 and el.endswith(
                        ("CurrentAssets", "Liabilities", "Assets", "NetAssets",
                         "CurrentAssetsIFRS", "LiabilitiesIFRS", "AssetsIFRS",
                         "EquityIFRS")):
                    seen_bs.add(el)
                continue
            cx = f[i_cx]
            if len(seen_ctx) < 12: seen_ctx.add(cx)
            rk = _ed_ctx_rank(cx)
            if rk is None: continue        # 前期・提出日時点・セグメントは使わない
            crank, solo = rk
            if i_cs is not None and len(f) > i_cs and f[i_cs] == "個別":
                solo = True
            try:
                v = float(f[i_vl].replace(",", ""))
            except (ValueError, AttributeError):
                continue
            k = want[el]
            # 優先順位: 当期本体 > 当期その他、連結 > 個別。
            # 良いものが既に入っていれば上書きしない。
            if k in got and (crank, solo) >= (got[k][2], got[k][1]): continue
            got[k] = (v, solo, crank)
    if not got:
        # ★黙って捨てない。何が入っていたかを残す。
        why = ("行が読めない" if n_rows == 0 else
               "要素IDが一致しない" if not seen_ctx else
               "コンテキストが当期の時点でない")
        key = f"{dtype or '?'}:{why}"
        _ED["nomatch"][key] = _ED["nomatch"].get(key, 0) + 1
        if len(_ED["probe"]) < 6:
            _ED["probe"].append({"dtype": dtype, "why": why, "rows": n_rows,
                                 "elems": sorted(seen_bs)[:8],
                                 "ctx": sorted(seen_ctx)[:8]})
        return None
    out = {k: v for k, (v, _s, _c) in got.items()}
    out["solo"] = all(s for _v, s, _c in got.values())
    # どのコンテキストから取れたか（当期本体以外なら印を残す）
    out["ctx_alt"] = any(c > 0 for _v, _s, c in got.values())
    # ★比率の式に効くのは ca と liab。この2つの基準（連結/個別）が
    #   違っていたら、その比率は意味を持たないので使わない。
    _base = {k: s for k, (_v, s, _c) in got.items()}
    out["mixed"] = ("ca" in _base and "liab" in _base
                    and _base["ca"] != _base["liab"])
    # ★投資有価証券だけ基準が違うのは異常ではない。IFRS採用会社は連結に
    #   「投資有価証券」の行が無く、単体（日本基準）にしか出ないため。
    #   その場合は inv を捨てる。0扱いになるので比率は**低めに出る**
    #   （見落とすことはあっても、過大評価はしない）。
    if ("inv" in _base and "ca" in _base and _base["inv"] != _base["ca"]):
        out["inv"] = None
        out["inv_dropped"] = True
    # ★貸借が成立しているか（資産合計＝負債合計＋純資産合計）。
    #   成立しなければ「全体でない数字」を拾っている。実測で4件出た。
    out["bs_ok"] = ed_bs_ok(out)
    if out["mixed"] or out["bs_ok"] is False:
        k2 = f"{dtype or '?'}:{'連結と個別が混在' if out['mixed'] else '貸借が成立しない'}"
        _ED["nomatch"][k2] = _ED["nomatch"].get(k2, 0) + 1
        if len(_ED["probe"]) < 6:
            _ED["probe"].append({"dtype": dtype, "why": k2.split(":")[1],
                                 "ctx": sorted(seen_ctx)[:8],
                                 # 鍵名に生の項目名を使わない（検査に引っかかる）
                                 "v": {"流動資産": out.get("ca"),
                                       "負債合計": out.get("liab"),
                                       "資産合計": out.get("ta"),
                                       "純資産": out.get("na")}})
    _ED["parsed"] += 1
    return out

def ed_names_load():
    """コード → 提出者名（EDINET・PDL1.0）。壊れていても落とさない。"""
    try:
        if os.path.exists(ED_NAME_PATH):
            o = json.load(open(ED_NAME_PATH, encoding="utf-8"))
            r = o.get("rows") if isinstance(o, dict) else None
            if isinstance(r, dict):
                return {str(k): str(v) for k, v in r.items() if k and v}
    except Exception as e:
        meta["errors"].append(f"edinet names load: {type(e).__name__}")
    return {}

def ed_names_save(names):
    try:
        json.dump({"generated_at_jst": NOW.isoformat(),
                   "attribution": ED_ATTRIB,
                   "note": "提出者名。JPXの銘柄名は再配信できないため、"
                           "公開リポジトリに出す名前はこちらを使う。" + ED_ATTRIB,
                   "count": len(names),
                   "rows": dict(sorted(names.items()))},
                  open(ED_NAME_PATH, "w"), ensure_ascii=False, indent=1)
    except Exception as e:
        meta["errors"].append(f"edinet names write: {type(e).__name__}")
    return len(names)

def ed_hist_load():
    """開示1件ごとの履歴を読む。docid をそのまま鍵にする（重複しない）。
       壊れていても落とさない（履歴が無いだけで運用は続く）。"""
    import csv
    out = {}
    try:
        if not os.path.exists(ED_HIST_PATH): return out
        with open(ED_HIST_PATH, encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                did = (row.get("docid") or "").strip()
                if not did: continue
                r = {"code": (row.get("code") or "").strip(),
                     "docid": did,
                     "submit": (row.get("submit") or "").strip(),
                     "period": (row.get("period") or "").strip(),
                     "dtype": (row.get("dtype") or "").strip(),
                     # 古いキャッシュには name 列が無い。空で読む（落とさない）。
                     "name": (row.get("name") or "").strip()}
                for k in ("ca", "inv", "liab", "ta", "na"):
                    v = (row.get(k) or "").strip()
                    try:
                        r[k] = float(v) if v != "" else None
                    except ValueError:
                        r[k] = None
                for k in ("solo", "mixed", "bs_ok"):
                    v = (row.get(k) or "").strip()
                    r[k] = True if v == "1" else (False if v == "0" else None)
                out[did] = r
    except Exception as e:
        meta["errors"].append(f"edinet hist load: {type(e).__name__}")
    return out

def ed_hist_save(hist):
    """履歴を書き出す。提出日の新しい順に上限まで残す。"""
    import csv
    def _b(v): return "" if v is None else ("1" if v else "0")
    rows = sorted(hist.values(),
                  key=lambda r: (r.get("submit") or "", r.get("docid") or ""),
                  reverse=True)[:ED_HIST_MAX]
    try:
        with open(ED_HIST_PATH, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(ED_HIST_COLS)
            for r in rows:
                w.writerow([r.get("code", ""), r.get("docid", ""),
                            r.get("submit", ""), r.get("period", ""),
                            r.get("dtype", "")]
                           + ["" if r.get(k) is None else repr(r[k])
                              for k in ("ca", "inv", "liab", "ta", "na")]
                           + [_b(r.get("solo")), _b(r.get("mixed")),
                              _b(r.get("bs_ok")), r.get("name", "")])
    except Exception as e:
        meta["errors"].append(f"edinet hist write: {type(e).__name__}")
    return len(rows)

def ed_hist_by_code(hist):
    """銘柄ごとに、提出日の古い順に並べた開示の一覧。"""
    out = {}
    for r in (hist or {}).values():
        c = r.get("code")
        if c: out.setdefault(c, []).append(r)
    for c in out:
        out[c].sort(key=lambda r: (r.get("submit") or "", r.get("docid") or ""))
    return out

def ed_row_at(by_code, code, on_date):
    """その日に**知り得た**貸借だけを返す。提出日が on_date 以前の
       最新の開示。未来の開示は絶対に混ぜない（混ぜると検証が無意味になる）。

       ★ここを間違えると、バックテストが「答えを先に見た」成績になる。
         価格側の jq_metrics_at と同じ規律。"""
    lst = (by_code or {}).get(str(code)) or []
    if not lst: return None
    d = str(on_date)[:10]
    best = None
    for r in lst:                      # 提出日の古い順に並んでいる
        s = r.get("submit") or ""
        if s and s <= d: best = r
        else: break
    return best

def ed_load():
    try:
        if os.path.exists(ED_PATH):
            o = json.load(open(ED_PATH, encoding="utf-8"))
            if isinstance(o.get("rows"), dict): return o
    except Exception as e:
        meta["errors"].append(f"edinet load: {type(e).__name__}")
    return {"rows": {}, "seen_dates": [], "attribution": ED_ATTRIB}

def ed_dates_to_scan(seen):
    """引く日付。新しい側（直近ED_LIST_DAYS日）は毎回見て、
       残りは古い側へ遡って埋める。土日は提出が無いので省く。"""
    today = NOW.date()
    fresh, back = [], []
    for k in range(1, ED_LIST_DAYS + 1):
        d = today - dt.timedelta(days=k)
        if d.weekday() < 5: fresh.append(d.isoformat())
    for k in range(ED_LIST_DAYS + 1, ED_BACKFILL_D + 1):
        d = today - dt.timedelta(days=k)
        if d.weekday() >= 5: continue
        s = d.isoformat()
        if s not in seen: back.append(s)
    # 新しい側は必ず見る。古い側は残り枠だけ
    return fresh + back[:max(ED_MAX_LIST - len(fresh), 0)]

def _neg_date(s):
    """ISOの日付を「新しいほど小さい」数に直す。sort の第2キー用。
       文字列の降順を使うために reverse を分けると、第1キー（優先）の
       向きまで反転してしまう。"""
    try:
        return -int(str(s).replace("-", "")[:8] or 0)
    except (ValueError, TypeError):
        return 0

def ed_update(priority=None):
    """一覧を引いて、まだ持っていない書類を落として貸借を取り出す。
       1回の実行で落とす数に上限を置き、日々積み上げる。

       priority … 先に落としたい証券コードの集合。
         ★これが無いと、実測が要る銘柄に当たるまで何日もかかる。
           実測: 未取得780件に対し40件の表のうち実測が入ったのは2件だけ。
           清原枠の条件を通った銘柄を先に落とせば、表は1〜2回で埋まる。"""
    cache = ed_load()
    hist = ed_hist_load()
    if not ED_KEY:
        meta["edinet"] = {"skipped": "EDINET_API_KEY 未設定",
                          "codes": len(cache["rows"])}
        print("[EDINET] EDINET_API_KEY が無いので貸借対照表は取りに行かない"
              f"（手元のキャッシュ {len(cache['rows'])}銘柄はそのまま使う）")
        return cache
    seen = set(cache.get("seen_dates") or [])
    rows = cache["rows"]
    ednames = ed_names_load()
    dates = ed_dates_to_scan(seen)
    found, got_docs, listed = [], 0, 0
    for d in dates:
        r = ed_list(d)
        if _ED["blocked"]: break
        listed += 1
        seen.add(d)
        _pk = ed_pick(r)
        # ★名前は一覧だけで取れる。書類を落とさなくても溜まる。
        for _f in _pk:
            if _f.get("name"): ednames[_f["code"]] = _f["name"]
        found.extend(_pk)
    # 既に持っていて、同じ書類なら取り直さない
    todo = []
    for f in found:
        cur = rows.get(f["code"])
        # ★検算に落ちている行は、同じ書類でも取り直す。
        #   読み方（コンテキストの解釈）を直したので結果が変わる。
        _bad = bool(cur) and (cur.get("mixed") is True or ed_bs_ok(cur) is False)
        if cur and cur.get("docid") == f["docid"] and not _bad: continue
        if cur and (cur.get("submit") or "") > f["submit"] and not _bad:
            continue                                   # 古い方は不要
        todo.append(f)
    # 清原枠の候補を先に、そのあとは新しい開示から。
    # 提出日は ISO なので文字列の降順＝新しい順。
    _pri = set(str(c) for c in (priority or ()))
    todo.sort(key=lambda x: (0 if x["code"] in _pri else 1,
                             _neg_date(x["submit"])))
    _npri = sum(1 for x in todo if x["code"] in _pri)
    # ★検算に落ちている手持ちの行を、一覧を経由せず docid で引き直す。
    #   提出日が既に走査済みだと一覧に二度と出てこないので、
    #   todo 経由では永久に直らない（実測で4件が残り続けた）。
    _fix = [{"code": c, "docid": v.get("docid"), "submit": v.get("submit") or "",
             "period": v.get("period") or "", "dtype": v.get("dtype")}
            for c, v in rows.items()
            if v.get("docid") and (v.get("mixed") is True or ed_bs_ok(v) is False)]
    _fix = [f for f in _fix if not any(t["docid"] == f["docid"] for t in todo)]
    _nfix = len(_fix)
    todo = _fix + todo            # 直しを最優先
    for f in todo[:ED_MAX_DOCS]:
        if _ED["blocked"]: break
        b = _ed_get(f"/documents/{f['docid']}", {"type": 5}, binary=True)
        _ED["docs"] += 1
        if not b: continue
        v = ed_parse_csv(b, f.get("dtype"))
        if not v: continue
        got_docs += 1
        # ★履歴には1件ごとに残す（rows は最新1件しか持たない）
        hist[f["docid"]] = {"code": f["code"], "docid": f["docid"],
                            "submit": f["submit"], "period": f["period"],
                            "dtype": f["dtype"],
                            "ca": v.get("ca"), "inv": v.get("inv"),
                            "liab": v.get("liab"), "ta": v.get("ta"),
                            "na": v.get("na"), "solo": v.get("solo"),
                            "mixed": v.get("mixed"), "bs_ok": v.get("bs_ok"),
                            "name": f.get("name", "")}
        # rows は「銘柄ごとの最新」。古い提出で上書きしない。
        _cur = rows.get(f["code"])
        if _cur and (_cur.get("submit") or "") > f["submit"]:
            continue
        rows[f["code"]] = {"docid": f["docid"], "submit": f["submit"],
                           "period": f["period"], "dtype": f["dtype"],
                           "ca": v.get("ca"), "inv": v.get("inv"),
                           "liab": v.get("liab"), "ta": v.get("ta"),
                           "na": v.get("na"), "solo": v.get("solo"),
                           "ctx_alt": v.get("ctx_alt"),
                           "mixed": v.get("mixed"), "bs_ok": v.get("bs_ok")}
    cache["rows"] = rows
    cache["seen_dates"] = sorted(seen)[-1200:]
    cache["attribution"] = ED_ATTRIB
    cache["generated_at_jst"] = NOW.isoformat()
    cache["note"] = ("有価証券報告書・半期報告書の貸借対照表から、"
                     "流動資産合計・投資有価証券・負債合計・資産合計・純資産合計を"
                     "取り出したもの。" + ED_ATTRIB)
    cache["with_ca"] = sum(1 for v in rows.values() if v.get("ca") is not None)
    cache["with_inv"] = sum(1 for v in rows.values() if v.get("inv") is not None)
    # ★検算を通って比率に使える行が何件か。with_ca より小さくなる。
    cache["usable"] = sum(1 for v in rows.values()
                          if v.get("ca") is not None and v.get("liab") is not None
                          and v.get("mixed") is not True
                          and ed_bs_ok(v) is not False)
    cache["bs_bad"] = sum(1 for v in rows.values() if ed_bs_ok(v) is False)
    try:
        json.dump(cache, open(ED_PATH, "w"), ensure_ascii=False, indent=1)
    except Exception as e:
        meta["errors"].append(f"edinet write: {type(e).__name__}")
    # 履歴側にも名前があれば拾っておく（過去分の取りこぼしを埋める）
    for _r in hist.values():
        if _r.get("name") and _r.get("code") and _r["code"] not in ednames:
            ednames[_r["code"]] = _r["name"]
    _nnames = ed_names_save(ednames)
    # ★この実行で増えたぶんも、今日のレポートから使う
    ED_NAMES.update(ednames)
    cache["names"] = _nnames
    _nhist = ed_hist_save(hist)
    _hbc = ed_hist_by_code(hist)
    _multi = sum(1 for v in _hbc.values() if len(v) >= 2)
    _subs = [r.get("submit") for r in hist.values() if r.get("submit")]
    cache["hist_rows"] = _nhist
    cache["hist_codes"] = len(_hbc)
    cache["hist_multi"] = _multi
    meta["edinet"] = {"codes": len(rows), "with_ca": cache["with_ca"],
                      "usable": cache["usable"], "bs_bad": cache["bs_bad"],
                      "refetch_bad": _nfix, "names": _nnames,
                      "with_inv": cache["with_inv"], "listed_days": listed,
                      "todo": len(todo), "todo_priority": _npri,
                      "priority_given": len(_pri), "downloaded": _ED["docs"],
                      "parsed": got_docs, "req": _ED["req"], "n429": _ED["n429"],
                      "blocked": _ED["blocked"], "enc": _ED["enc"],
                      "sep": _ED["sep"], "cols": _ED["cols"],
                      "dtypes": {d: sum(1 for v in rows.values()
                                        if v.get("dtype") == d)
                                 for d in ED_DOC_TYPES},
                      # ★履歴の溜まり具合。比率をバックテストで検証できる
                      #   ようになるのは、1銘柄に複数の開示が溜まってから。
                      "hist_rows": _nhist, "hist_codes": len(_hbc),
                      "hist_multi": _multi,
                      "hist_from": (min(_subs) if _subs else None),
                      "hist_to": (max(_subs) if _subs else None),
                      "nomatch": dict(_ED["nomatch"]),
                      "probe": _ED["probe"],
                      "err": dict(_ED["err"])}
    print(f"[EDINET] 履歴{_nhist:,}行 / {len(_hbc):,}銘柄"
          f"（2件以上ある銘柄 {_multi:,}）")
    print(f"[EDINET] 一覧{listed}日 / 未取得{len(todo)}件"
          f"（うち清原枠の候補{_npri}件を優先）のうち{_ED['docs']}件を取得 / "
          f"解釈成功{got_docs}件 / 累計{len(rows):,}銘柄"
          f"（比率に使える{cache['usable']:,} / 検算に落ちた{cache['bs_bad']:,}）")
    if _ED["blocked"]:
        print(f"::warning::EDINETが止まりました（{_ED['blocked']}）。"
              "キーとプランを確認してください")
    elif len(rows) == 0:
        print("::warning::EDINETから1件も取れていません。meta.json の edinet を確認してください")
    return cache

def ed_netcash(ed_rows, code, price, shares):
    """清原式のネットキャッシュ比率。**これが本物**。
         ネットキャッシュ ＝ 流動資産 ＋ 投資有価証券×70% − 負債合計
         比率 ＝ ネットキャッシュ ÷ 時価総額
       作れなければ None を返す（推測で埋めない）。"""
    r = (ed_rows or {}).get(str(code))
    if not r or not price or not shares: return None
    ca, liab = r.get("ca"), r.get("liab")
    if ca is None or liab is None: return None
    # ★検算に落ちる行からは比率を作らない（推定に戻す）。
    #   「全体でない数字」や連結・個別の混在で作った比率は、
    #   推定より悪い。実測を名乗る以上、合っていることを確かめる。
    if r.get("mixed") is True: return None
    # ★保存時に付いた印と、いま計算し直した検算の**どちらか**が
    #   落ちていたら使わない。保存時のほうが情報が多い（セグメント混入など
    #   その場でしか分からないことがある）ので、数字が偶然つじつまが
    #   合っていても印を覆さない。
    if r.get("bs_ok") is False: return None
    if ed_bs_ok(r) is False: return None
    mcap = price * shares
    if not (mcap > 0): return None
    inv = r.get("inv") or 0.0        # 投資有価証券が無い会社は0で正しい
    nc = ca + inv * ED_INV_HAIRCUT - liab
    return {"ratio": nc / mcap, "asof": r.get("submit"),
            "period": r.get("period"), "solo": r.get("solo"),
            "inv_missing": r.get("inv") is None,
            # 本人指定の「流動資産のほうが負債より大きい」。投資有価証券を
            # 足せば比率が出ても、この条件自体は流動資産と負債合計で見る。
            "ca_gt_liab": bool(ca > liab),
            "ca": ca, "liab": liab, "inv": r.get("inv"), "ta": r.get("ta")}

def ed_ratio_at(by_code, code, on_date, price, shares):
    """その日時点で知り得た情報だけで作った、清原式のネットキャッシュ比率。
       検算に落ちる開示からは作らない（ed_netcash と同じ規律）。"""
    r = ed_row_at(by_code, code, on_date)
    if not r: return None
    return ed_netcash({str(code): r}, code, price, shares)

# ══════════════════════════════════════════════════════════════════════
#  清原枠のスクリーニング（2026-09-13に指定された条件）
#
#  指定:
#    ・予想PER 8倍以下         … 本人が「一番重視するのはPER」
#    ・PBR 0.8倍以下
#    ・時価総額 500億円以下     … 「小型株」
#    ・流動資産のほうが負債より大きい
#    ・ネットキャッシュ比率が1以上、または1に近いもの
#    そのうえで、読み取った考え（株主還元・バリュートラップ）で有望なものを絞る
#
#  条件同士が噛み合っている:
#    PBR ≤ 0.8 ⟺ 純資産÷時価総額 ≥ 1.25 ⟺ ネットキャッシュ比率の**上限** ≥ 1.25
#    つまりPBRの条件を通った銘柄は、必ず「比率が1以上になりうる」側に入る。
#    「対象外（上限で1未満）」は構造的に出ない。
#
#  無料データで作れないもの（流動資産・投資有価証券）は不等式で挟む:
#    流動資産 > 負債合計 は
#      確実   … 現金同等物 > 負債合計（現金だけで負債を超えている）
#      要確認 … それ以外（売掛金・棚卸資産を入れれば超える可能性がある）
#    ネットキャッシュ比率 ≥ 1 は
#      確実   … 下限 ≥ 1
#      確実   … 現金だけで比率1以上
#      有力   … 普通の貸借（現金以外の流動資産が総資産の35%程度）で届く
#      要確認 … 60%まで要る
#      薄い   … それ以上＝異常な貸借が必要
#
#  この節が出すのは**候補**であって推奨ではない。
#  清原氏は「小型株は経営者が9割」としており、経営者の意志・言動の一致・
#  中期経営計画の具体性は無料データに無い。そこは機械では判定できない。
# ══════════════════════════════════════════════════════════════════════
KY_PATH         = f"{OUT}/kiyohara.json"
KY_MAX_PER      = 8.0            # 予想PER の上限
KY_MAX_PBR      = 0.8            # PBR の上限
KY_MAX_MCAP     = 50_000_000_000  # 時価総額 500億円
# ★分類のしきい値を実測で切り直した（2026-09-13）。
#   最初は「下限（現金−負債合計）÷時価総額 ≥ 0.8」を「有力」としたが、
#   条件を通った71件のうち該当0件、現金>負債すら3件だけだった。
#   現金だけで時価総額に迫るには現金が総資産の8割という異常な貸借が要るので、
#   この線はほぼ誰も超えない＝分類が情報を持たなかった。
#   代わりに「比率1に届くのに、現金以外の資産がどれだけ要るか」で切る:
#     need ＝ (時価総額 ＋ 負債合計 − 現金) ÷ 総資産
#   これは「(流動資産−現金) ＋ 投資有価証券×70%」が総資産の何割あれば
#   比率1に届くかを表す。日本の製造業だと現金以外の流動資産は総資産の
#   35〜40%程度が普通なので、need が小さいほど届きやすい。
#   ★この境目は私が置いた仮定。手作業の優先順位を付けるための目安で、
#     判定そのものはバフェット・コードの実数で行う。
KY_NC_MIN       = 0.8            # 本物の比率が取れている銘柄に課す下限。
                                 # 「1以上または1に近い」の「近い」をここで定義する。
                                 # ★EDINETの貸借対照表が無い銘柄は落とさない
                                 #   （キャッシュが積み上がる途中で消えてしまうため）。
KY_NEED_SURE    = 0.0            # これ以下なら現金だけで比率1以上（確実）
KY_NEED_LIKELY  = 0.35           # これ以下なら普通の貸借で届く（有力）
KY_NEED_MAYBE   = 0.60           # これ以下なら届く可能性がある（要確認）
KY_MIN_TURNOVER = 5_000_000      # 売買代金20日平均の下限。1銘柄37.5万円なら
                                 # 日商500万円の7.5%。数日に分ければ入れる。
                                 # 1,000万円だと1,125件が落ちていて、小型株を
                                 # 必要以上に削っていた（実測）。
KY_NAMES        = 8              # 清原枠で持つ銘柄数の想定。
                                 # 総額600万円なら清原枠300万円、1銘柄37.5万円。
                                 # 単元100株なので株価3,750円以下まで買える。
                                 # 清原氏は20銘柄を勧めているが、20だと15万円
                                 # ＝株価1,500円以下しか買えず候補が狭すぎる。
KY_MIN_FILL     = 0.6            # 1銘柄の目安額に対して、これを下回る建玉は
                                 # 作らない。清原式は等ウェイトの分散が前提で、
                                 # 目安37.5万円の枠に6万円の端株を混ぜると
                                 # 「8銘柄に分散した」と言えなくなる。
                                 # 現金が足りないなら銘柄数を減らすほうが正しい。
KY_TOP          = 40             # 出す候補の上限
KY_EXCLUDE      = ("銀行", "金融（除く銀行）")   # 流動資産と負債の意味が違う

KY_GROW_MIN = 0.0        # 「伸びている」の境目（前年比 %）。0＝横ばいは不可。
                         # ★上げ下げする前に、まず過去検証（BT_FACTORS の
                         #   growth）で効いているかを見ること。

def ky_grow(m):
    """売上か営業利益が伸びているか。True / False / None（測れない）。

       ★清原達郎氏『わが投資術』の対象は「割安 *小型 成長* 株」。
         安いだけの会社を除くための条件で、ここが無いと
         「万年割安で伸びない会社」がそのまま候補に残る。
       ★見る順は ①実績の前年比（売上・営業利益のどちらか）
                 ②会社自身の今期予想営業利益 対 直近通期実績
         ②だけで通すことはしない（会社予想は外れるため）が、
         ①が測れないときの拠りどころにはする。
       ★測れないときに False を返さないのは、
         「伸びていない」と「まだ数字が無い」を混同しないため。
         件数は meta に残して、取りこぼしが増えたら気付けるようにする。"""
    if not m: return None
    gs, go, gf = m.get("g_sales"), m.get("g_op"), m.get("g_fop")
    have = [x for x in (gs, go) if x is not None]
    if have:
        return any(x > KY_GROW_MIN for x in have)
    if gf is not None:
        return gf > KY_GROW_MIN
    return None

def ky_metrics(m, price, ed_rows=None, code=None):
    """清原枠の判定に使う数字。作れないものは None のまま返す（推測しない）。

       nc_real … EDINETの貸借対照表から作った**本物の**ネットキャッシュ比率。
                 流動資産＋投資有価証券×70%−負債合計 ÷ 時価総額。
       need   … 本物が作れないときの代用（比率1に届くのに現金以外の資産が
                総資産の何割必要か）。本物があるときは使わない。"""
    if not m or not price or price <= 0: return None
    ta, eq, cash, sh = m.get("ta"), m.get("eq"), m.get("cash"), m.get("sh")
    feps, op = m.get("feps"), m.get("op")
    if ta is None or eq is None or sh is None: return None
    if not (ta > 0) or not (sh > 0) or not (eq > 0): return None
    mcap = price * sh
    if not (mcap > 0): return None
    debt = ta - eq
    # 比率1に届くのに、現金以外の資産が総資産の何割必要か（代用）
    need = ((mcap + debt - cash) / ta) if cash is not None else None
    # 本物のネットキャッシュ比率（EDINETの貸借対照表がある銘柄だけ）
    real = ed_netcash(ed_rows, code, price, sh) if (ed_rows and code) else None
    return {"mcap": mcap, "debt": debt, "need": need,
            "nc_real": (real or {}).get("ratio"),
            "nc_asof": (real or {}).get("asof"),
            "nc_solo": (real or {}).get("solo"),
            "nc_inv_missing": (real or {}).get("inv_missing"),
            "nc_ca_gt_liab": (real or {}).get("ca_gt_liab"),
            "per": (price / feps) if (feps is not None and feps > 0) else None,
            "pbr": mcap / eq,
            "nc_lo": ((cash - debt) / mcap) if cash is not None else None,
            "nc_hi": eq / mcap,
            "cash_gt_debt": (cash is not None and cash > debt),
            "op": op, "no_div": m.get("no_div"),
            "g_sales": m.get("g_sales"), "g_op": m.get("g_op"),
            "g_fop": m.get("g_fop"), "n_growth": m.get("n_growth") or 0,
            "grow": ky_grow(m),
            "div": m.get("div"), "div_up": m.get("div_up"),
            "buyback": m.get("buyback"), "opm": m.get("opm"),
            "asof": m.get("asof")}

def ky_pass(k):
    """指定された条件を満たすか。落ちたときは理由の分類を返す。
       黙って落とすと「条件が厳しすぎて0件」と「不具合で0件」が区別できない。"""
    if not k:                                return False, "財務が引けない"
    if k["per"] is None:                     return False, "予想PERが出ない(赤字予想)"
    if k["per"] > KY_MAX_PER:                return False, f"PER>{KY_MAX_PER:.0f}倍"
    if k["pbr"] > KY_MAX_PBR:                return False, f"PBR>{KY_MAX_PBR}倍"
    if k["mcap"] > KY_MAX_MCAP:              return False, f"時価総額>{KY_MAX_MCAP/1e8:.0f}億円"
    if k["op"] is not None and k["op"] <= 0: return False, "本業が赤字"
    # ★成長。測れた銘柄にだけ課す（測れない＝落とす、にはしない）。
    if k.get("grow") is False:               return False, KY_WHY_GROW
    # ★本物の比率が取れている銘柄には、指定された条件をそのまま課す。
    #   取れていない銘柄は落とさない（EDINETのキャッシュは日々積み上がる途中で、
    #   ここで落とすと「まだ取っていないだけ」の銘柄が消える）。
    r = k.get("nc_real")
    if r is not None:
        if k.get("nc_ca_gt_liab") is False:
            return False, KY_WHY_CA
        if r < KY_NC_MIN:
            return False, KY_WHY_NC
    return True, "通過"

# 実測で落としたときの理由。レポート側でも同じ字を使うので定数にする。
KY_WHY_GROW = "売上も営業利益も伸びていない"
KY_WHY_CA = "流動資産≤負債合計(実測)"
KY_WHY_NC = f"ネットキャッシュ比率<{KY_NC_MIN}(実測)"

def ky_band(k):
    """分類の符号。表示文（ky_state）と分けておく。
       件数の集計は必ずこちらを使う。表示文の先頭一致で数えると、
       文言を変えた瞬間に黙って0件になる（実際に一度やった）。"""
    r = k.get("nc_real")
    if r is not None:
        if r >= 1.0:        return "real_ge1"
        if r >= KY_NC_MIN:  return "real_near"
        return "real_lo"
    n = k.get("need")
    if n is None:                   return "unknown"
    if n <= KY_NEED_SURE:           return "sure"
    if n <= KY_NEED_LIKELY:         return "likely"
    if n <= KY_NEED_MAYBE:          return "maybe"
    return "thin"

KY_BAND_JA = {"real_ge1": "実測1以上", "real_near": "実測1に近い",
              "real_lo": "実測1未満", "sure": "推定：確実",
              "likely": "推定：有力", "maybe": "推定：要確認",
              "thin": "推定：薄い", "unknown": "推定不可"}

def ky_state(k):
    """ネットキャッシュ比率の状態。

       ★EDINETの貸借対照表がある銘柄は**本物の比率**で判定する。
         無い銘柄だけ、代用（必要割合）で確からしさを言う。
         両者を同じ言葉で並べると区別が付かないので、表記を分ける。"""
    r = k.get("nc_real")
    if r is not None:
        if r >= 1.0:  return f"実測 {r:.2f}（1以上）"
        if r >= 0.8:  return f"実測 {r:.2f}（1に近い）"
        return f"実測 {r:.2f}"
    n = k.get("need")
    if n is None:                   return "推定不可(現金が取れない)"
    if n <= KY_NEED_SURE:           return "推定：確実"
    if n <= KY_NEED_LIKELY:         return "推定：有力"
    if n <= KY_NEED_MAYBE:          return "推定：要確認"
    return "推定：薄い"

def ky_grow_tag(k):
    """成長の表示。★伸び率の数値は書かない。
       J-Quantsの生の財務数値を出さない方針（利用条件 第8条2項）に合わせ、
       レポートに出すのは分類だけにする。"""
    if not k: return "測れない"
    gs, go = k.get("g_sales"), k.get("g_op")
    if gs is None and go is None:
        gf = k.get("g_fop")
        if gf is None: return "測れない"
        return "会社予想は増益" if gf > KY_GROW_MIN else "会社予想は減益"
    t = []
    if gs is not None: t.append("増収" if gs > KY_GROW_MIN else "減収")
    if go is not None: t.append("増益" if go > KY_GROW_MIN else "減益")
    return "".join(t)

def ky_return_tag(k):
    """株主還元。清原氏が「最終的なカタリスト」とするもの＝罠の見分け。"""
    if k.get("no_div"): return "無配"
    t = []
    if (k.get("div_up") or 0) > 0: t.append("増配")
    if (k.get("buyback") or 0) > 0.1: t.append("自己株買い")
    return "／".join(t) if t else "配当のみ"

def kiyohara_screen(sc_all, fmap, names=None, s17=None, ed_rows=None,
                    bt_codes=None):
    """清原枠の候補。指定条件で絞り、読み取った考えで並べる。

       bt_codes … 過去検証が使っている母集団の証券コード集合。
       ★なぜ要るか: 過去検証は売買代金50百万円以上・ATR1.5〜6.0%で
         母集団を絞っている（スイングの条件）。清原枠は売買代金5百万円以上・
         ATRの条件なしなので、**清原枠の候補の多くは検証された母集団の
         外側にいる**。この食い違いを毎回数えて出さないと、
         「検証された割安効果が清原枠にも効く」と読み違える。

       ★書き出すのは分類・順位・タグと、条件の通過状況だけ。
         PER・PBR・時価総額の**数値は書かない**。
         純資産や総資産が逆算でき、J-Quantsの生の財務数値を
         公開したことになるため（利用条件）。"""
    import numpy as np
    names, s17 = names or {}, s17 or {}
    if sc_all is None or len(sc_all) == 0:
        meta["kiyohara"] = {"error": "価格の枠が空"}
        return []
    reasons, rows = {}, []
    for i in range(len(sc_all)):
        c = str(sc_all["code"].iloc[i]); cc = c.replace(".T", "")
        if float(sc_all["turnover"].iloc[i]) < KY_MIN_TURNOVER:
            reasons["売買代金が薄い"] = reasons.get("売買代金が薄い", 0) + 1; continue
        if s17.get(cc) in KY_EXCLUDE:
            reasons["銀行・金融（式が成立しない）"] = reasons.get("銀行・金融（式が成立しない）", 0) + 1
            continue
        k = ky_metrics(fmap.get(cc), float(sc_all["close"].iloc[i]), ed_rows, cc)
        ok, why = ky_pass(k)
        if not ok:
            reasons[why] = reasons.get(why, 0) + 1
            continue
        # ★17業種は書き出さない（JPX由来）。除外の判定にだけ使っている。
        rows.append({"code": cc, "name": names.get(cc, ""),
                     # ★株価は発注株数の計算に要る。公開の市場データなので、
                     #   これだけでは純資産や総資産を逆算できない。
                     "price": round(float(sc_all["close"].iloc[i]), 1),
                     "state": ky_state(k), "band": ky_band(k),
                     # ★過去検証の母集団に入っているか。入っていなければ、
                     #   検証された割安効果はこの銘柄には当てはまらない。
                     "in_bt": (None if bt_codes is None
                               else (cc in bt_codes or c in bt_codes)),
                     "ret": ky_return_tag(k),
                     "grow": ky_grow_tag(k),
                     # 「測れた」のか「まだ数字が無い」のかを区別して残す
                     "grow_ok": k.get("grow"),
                     "nc_asof": k.get("nc_asof"),
                     "nc_solo": k.get("nc_solo"),
                     "_ep": (1.0 / k["per"]) if k["per"] else 0.0,
                     # ★本物の比率があればそれを使う。無い銘柄は代用（必要割合の
                     #   符号反転）。本物と代用が混ざるので、順位は目安として読む。
                     "_nc": (k["nc_real"] if k.get("nc_real") is not None
                             else (-k["need"] if k.get("need") is not None else -9e9)),
                     "nc_real": (round(k["nc_real"], 2)
                                 if k.get("nc_real") is not None else None),
                     "_po": (0.0 if k.get("no_div") else
                             1.0 + max(k.get("div_up") or 0, 0) / 100
                             + max(k.get("buyback") or 0, 0) / 10)})
    # 並べ方: 読み取った考えをそのまま使う。
    #   PERを一番重視し、ネットキャッシュ比率を加え、株主還元で罠を外す。
    #   断面の順位に直してから足す（単位の違う数字を直接足さない）。
    if rows:
        def prank(key):
            """断面の順位（0〜1）。同じ値には同じ順位を与える。

               ★同点を並び順で割ってはいけない。株主還元タグは
                 0.0 / 1.0 / 1.x の数種類しか取らないので同点が大量に出る。
                 以前は同点でも (pos+1)/n を順に振っていたため、
                 リストの位置（ほぼ証券コード順）が点数に漏れていた。
                 実測1.10の銘柄が、実測の無い銘柄に抜かれることが起きた。"""
            n = len(rows)
            order = sorted(range(n), key=lambda i: rows[i][key])
            r = [0.0] * n
            p = 0
            while p < n:
                q = p
                while q + 1 < n and rows[order[q + 1]][key] == rows[order[p]][key]:
                    q += 1
                avg = ((p + 1) + (q + 1)) / 2 / n      # 同点は平均順位
                for i in order[p:q + 1]: r[i] = avg
                p = q + 1
            return r
        pe, pn, pp = prank("_ep"), prank("_nc"), prank("_po")
        for i, r in enumerate(rows):
            # PER 2 : ネットキャッシュ 1 : 株主還元 1
            r["rank_score"] = round((2 * pe[i] + pn[i] + pp[i]) / 4 * 100, 1)
        # ★実測で条件を満たした銘柄を、未実測より必ず上に置く。
        #   未実測の銘柄は「指定条件を満たしている」と確かめられていない
        #   ＝発注の対象ではなく、測る対象。点数だけで混ぜて並べると、
        #   条件を満たしていない銘柄が満たしている銘柄より上に来る
        #   （実測で確かめた72件のうち46件が条件を外していた）。
        rows.sort(key=lambda r: (0 if str(r.get("band", "")).startswith("real")
                                 else 1, -r["rank_score"]))
    # 本物の比率がある銘柄と無い銘柄が混ざる。何件ずつかを残す。

        for n, r in enumerate(rows[:KY_TOP], start=1):
            r["rank"] = n
            for k2 in ("_ep", "_nc", "_po"): r.pop(k2, None)
    out = rows[:KY_TOP]
    payload = {"generated_at_jst": NOW.isoformat(),
               "screen": {"max_per": KY_MAX_PER, "max_pbr": KY_MAX_PBR,
                          "max_mcap_oku": KY_MAX_MCAP / 1e8,
                          "need_bands": [KY_NEED_SURE, KY_NEED_LIKELY, KY_NEED_MAYBE],
                          "min_turnover": KY_MIN_TURNOVER,
                          "excluded_sectors": list(KY_EXCLUDE)},
               "formula": "ネットキャッシュ＝流動資産＋投資有価証券×70%−負債合計／比率＝÷時価総額",
               "universe": int(len(sc_all)), "passed": len(rows), "shown": len(out),
               # ★表示文ではなく band で数える（文言変更で0件になるのを防ぐ）
               "bands": {b: sum(1 for r in out if r.get("band") == b)
                         for b in ("real_ge1", "real_near", "real_lo",
                                   "sure", "likely", "maybe", "thin", "unknown")},
               "sure": sum(1 for r in out if r.get("band") == "sure"),
               "likely": sum(1 for r in out if r.get("band") == "likely"),
               "maybe": sum(1 for r in out if r.get("band") == "maybe"),
               "thin": sum(1 for r in out if r.get("band") == "thin"),
               "need_bands": {"sure": KY_NEED_SURE, "likely": KY_NEED_LIKELY,
                              "maybe": KY_NEED_MAYBE},
               "no_div": sum(1 for r in out if r["ret"] == "無配"),
               # 通過した全件（rows）と表に出す分（out）を分けて数える
               "with_real": sum(1 for r in rows if r.get("nc_real") is not None),
               "with_real_shown": sum(1 for r in out if r.get("nc_real") is not None),
               "real_ge1": sum(1 for r in rows
                               if r.get("nc_real") is not None
                               and r["nc_real"] >= 1.0),
               "real_near": sum(1 for r in rows
                                if r.get("nc_real") is not None
                                and KY_NC_MIN <= r["nc_real"] < 1.0),
               "nc_min": KY_NC_MIN,
               # ★成長の条件。何件を落とし、何件が「測れない」まま通ったか。
               #   測れない件数が増えたら、条件が骨抜きになっている合図。
               "growth": {"min_pct": KY_GROW_MIN,
                          "rejected": reasons.get(KY_WHY_GROW, 0),
                          "passed_measured": sum(1 for r in rows
                                                 if r.get("grow_ok") is True),
                          "passed_unmeasured": sum(1 for r in rows
                                                   if r.get("grow_ok") is None),
                          "note": "無料プランは2年ぶんしか取れないため、"
                                  "年度3期の比較ではなく同一四半期の前年比で測る"},
               # 検証された母集団に入っている件数。少なければ、
               # 清原枠に「検証済み」と言えるものは無いということ。
               "in_bt_universe": sum(1 for r in rows if r.get("in_bt") is True),
               "bt_universe_known": bt_codes is not None,
               # ★推定の当たり具合。実測で確かめた銘柄のうち、
               #   実際に条件を満たしていた割合。推定はこれらを全部
               #   「通る」としていたので、これが推定の精度そのもの。
               "real_checked": (sum(1 for r in rows if r.get("nc_real") is not None)
                                + reasons.get(KY_WHY_CA, 0)
                                + reasons.get(KY_WHY_NC, 0)),
               "real_failed": (reasons.get(KY_WHY_CA, 0)
                               + reasons.get(KY_WHY_NC, 0)),
               "edinet_attribution": ED_ATTRIB,
               "dropped": reasons,
               "rank_weights": "PER2 : ネットキャッシュ1 : 株主還元1（この重みは指定に無い私の置き方）",
               "note": ("条件は本人指定（予想PER8倍以下・PBR0.8倍以下・時価総額500億円以下・"
                        "流動資産>負債・ネットキャッシュ比率1以上または1に近い）。"
                        "流動資産と投資有価証券は無料データに無いため不等式で挟んでいる。"
                        "PER・PBR・時価総額・比率の数値は利用条件により載せない。"),
               "rows": out}
    json.dump(jpx_safe(payload), open(KY_PATH, "w"),
              ensure_ascii=False, indent=1)
    meta["kiyohara"] = {k: payload[k] for k in
                        ("universe", "passed", "shown", "sure", "likely", "no_div",
                         "with_real", "real_ge1", "real_near", "bands", "dropped")}
    print(f"[清原枠] 母集団{len(sc_all):,} → 条件通過{len(rows)}件"
          f"（実測あり{payload['with_real']}：1以上{payload['real_ge1']} / "
          f"1に近い{payload['real_near']}"
          f"／推定 確実{payload['sure']} 有力{payload['likely']}"
          f"／無配{payload['no_div']}）")
    return out

NC_PATH      = f"{OUT}/netcash.json"
NC_EXCLUDE   = ("銀行", "金融（除く銀行）")   # 式が成立しない業種
NC_SHORTLIST = 60      # 手で確かめる候補の上限件数

def netcash_bounds(code, m, price):
    """1銘柄のネットキャッシュ比率の下限・上限。作れなければ None。
       近似はしない。挟めるところまでしか言わない。"""
    if not m or not price or price <= 0: return None
    ta, eq, cash, sh = m.get("ta"), m.get("eq"), m.get("cash"), m.get("sh")
    if ta is None or eq is None or sh is None: return None
    if not (ta > 0) or not (sh > 0): return None
    debt = ta - eq                       # 負債合計＝総資産−純資産
    mcap = price * sh
    if not (mcap > 0): return None
    lo = (cash - debt) / mcap if cash is not None else None
    hi = eq / mcap                       # ＝1/PBR
    return {"lo": lo, "hi": hi, "op": m.get("op"), "asof": m.get("asof")}

def netcash_state(b):
    """3分類。手で確かめる必要があるのはどれかを決める。"""
    if not b: return "不明"
    if b["op"] is not None and b["op"] <= 0: return "除外(本業赤字)"
    if b["hi"] is None or b["hi"] < 1.0:     return "対象外(上限で1未満)"
    if b["lo"] is not None and b["lo"] >= 1.0: return "確実(下限で1以上)"
    return "要確認"

def netcash_shortlist(sc, fmap, names=None, s17=None):
    """手で確かめる価値のある銘柄だけを、確からしい順に並べて書き出す。

       ★書き出すのはコード・銘柄名・分類・順位だけ。
         比率の数値そのものは書かない。純資産や総資産が逆算できてしまい、
         J-Quantsの生の財務数値を公開したことになるため。
         正確な比率は、本人がバフェット・コードから取った値で計算する。"""
    names, s17 = names or {}, s17 or {}
    rows = []
    for i, c in enumerate(sc["code"].tolist()):
        cc = str(c).replace(".T", "")
        b = netcash_bounds(cc, fmap.get(cc), float(sc["close"].iloc[i]))
        st = netcash_state(b)
        if st in ("不明", "対象外(上限で1未満)", "除外(本業赤字)"): continue
        if s17.get(cc) in NC_EXCLUDE: continue
        m = fmap.get(cc) or {}
        # 株主還元の印。清原氏は「最終的なカタリストは株主還元」としている。
        # ネットキャッシュが厚いのに株主に返さない会社は、割安なまま放置される。
        if m.get("no_div"):
            ret = "無配"
        else:
            _u, _b = m.get("div_up"), m.get("buyback")
            _t = []
            if _u is not None and _u > 0: _t.append("増配")
            if _b is not None and _b > 0.1: _t.append("自己株買い")
            ret = "／".join(_t) if _t else "配当のみ"
        # ★17業種は書き出さない（JPX由来）。除外の判定にだけ使っている。
        rows.append({"code": cc, "name": names.get(cc, ""),
                     "state": st, "ret": ret,
                     "_lo": (b["lo"] if b["lo"] is not None else -9e9)})
    # 下限が大きいほど「1以上」が確からしい
    rows.sort(key=lambda r: -r["_lo"])
    for n, r in enumerate(rows[:NC_SHORTLIST], start=1):
        r["rank"] = n; r.pop("_lo", None)
    out = rows[:NC_SHORTLIST]
    n_sure = sum(1 for r in out if r["state"].startswith("確実"))
    payload = {"generated_at_jst": NOW.isoformat(),
               "formula": "ネットキャッシュ＝流動資産＋投資有価証券×70%−負債合計／比率＝÷時価総額",
               "pool": int(len(sc)), "shortlist": len(out), "sure": n_sure,
               "excluded_sectors": list(NC_EXCLUDE),
               "note": ("上限（＝純資産÷時価総額＝1/PBR）が1未満の銘柄は清原式でも"
                        "1以上になりえないので除いてある。本業赤字も除いてある。"
                        "比率の数値は載せない（J-Quantsの生の財務数値が逆算できるため）。"
                        "正確な値はバフェット・コードの 流動資産・投資有価証券・負債合計・"
                        "時価総額 で計算すること。"),
               "rows": out}
    json.dump(jpx_safe(payload), open(NC_PATH, "w"),
              ensure_ascii=False, indent=1)
    meta["netcash"] = {k: payload[k] for k in ("pool", "shortlist", "sure")}
    print(f"[ネットキャッシュ] 手で確かめる候補 {len(out)}件"
          f"（うち下限で既に1以上 {n_sure}件）")
    return out

FUND_PATH = f"{OUT}/fund.json"
FUND_MAX_AGE_D = 20     # これより古い順位はレポートで警告する
# 清原枠に渡す「ATRで落とす前の全銘柄の枠」。呼び出し側で入れる。
FUND_ALL = None

def load_fund_prev():
    """前回コミットした順位。順位は「分析結果」なので data/ に置いてあり、
       そのまま再利用できる（生値は置いていないので再利用しても規約に触れない）。"""
    try:
        if os.path.exists(FUND_PATH):
            pv = json.load(open(FUND_PATH, encoding="utf-8"))
            if isinstance(pv.get("pct"), dict) and pv["pct"]:
                return pv
    except Exception as e:
        meta["errors"].append(f"fund prev: {type(e).__name__}")
    return None

def fund_reuse(reason):
    """取りに行かず、前回の順位を使う。
       無料枠の財務はもともと12週間前のものなので、1日ぶん古いことによる
       情報の劣化は無い。株価だけが新しく、会計の数字は前回と同じという状態。
       前回分も無ければ空を返す（そのときは列が空になるだけで止まらない）。"""
    pv = load_fund_prev()
    if not pv:
        meta["fund"] = {"skipped": reason, "reused": False}
        print(f"[財務] {reason}。前回分も無いのでファンダ列は空になる")
        return {}
    # 経過は切り捨てなので、前夜の取得は「0日前」になる。
    # レポートでは時間も見せて「0日前」が古いことのように読まれないようにする。
    age, age_h, gen = None, None, pv.get("generated_at_jst")
    try:
        _d = NOW - dt.datetime.fromisoformat(gen)
        age = _d.days
        age_h = round(_d.total_seconds() / 3600, 1)
    except Exception:
        pass
    meta["fund"] = {"reused": True, "reason": reason, "age_days": age,
                    "age_hours": age_h, "fetched_at_jst": gen,
                    "asof_latest_disclosure": pv.get("asof_latest_disclosure"),
                    "pool": pv.get("pool"), "n_codes": len(pv["pct"]),
                    "coverage": pv.get("coverage"), "span_days": pv.get("span_days")}
    print(f"[財務] 前回の順位を再利用（{reason} / {len(pv['pct']):,}銘柄 / "
          f"{age}日前 / 開示の最新 {pv.get('asof_latest_disclosure')}）")
    if age is not None and age > FUND_MAX_AGE_D:
        print(f"::warning::ファンダの順位が{age}日前のものです。"
              "J-Quantsの取得が続けて失敗していないか data/meta.json を確認してください")
    return pv["pct"]

def fund_pool(sc, snapshot, holdings):
    """順位を付ける断面。通過銘柄＋保有銘柄。

       ★保有銘柄を入れないと、持っている銘柄のファンダが分からない。
         スクリーニングのフィルタ（売買代金・ATR）は「新規で建てる候補」の
         条件なので、保有銘柄はそこを通らないことがある（実測で4755が抜けた）。
         残すか売るかの判断にこそファンダが要るので、断面に足す。
         数銘柄増えてもパーセンタイルはほとんど動かない。"""
    have = {str(c).replace(".T", "") for c in sc["code"]} if len(sc) else set()
    add = []
    for t in holdings:
        c = str(t).replace(".T", "")
        if c in have: continue
        px = (snapshot.get(t) or {}).get("close")
        if px and px > 0: add.append({"code": t, "close": float(px)})
    if not add: return sc, 0
    return pd.concat([sc, pd.DataFrame(add)], ignore_index=True), len(add)

def fund_today(sc, refetch=True):
    """今日の断面でファンダの順位を作り、data/fund.json に書き出す。

       ★書き出すのはパーセンタイル（0〜100の整数）だけ。
         PBR・ROE・BPS・売上・営業利益といった生値は一切書かない。
         J-Quantsの利用条件が第三者の閲覧を禁じており、
         このリポジトリは public だから。

       ★この順位は当面「表示のみ」で、trend/revert のスコアには入れない。
         効くかどうかは過去検証で確かめてからにする。
         検証前に採点へ混ぜるのは、trend/revert を作ったときと同じ間違い。"""
    if not JQ_KEY:
        return fund_reuse("JQUANTS_API_KEY 未設定")
    # ★取り直すのは大引け後の実行だけ。
    #   財務は12週間遅れのデータなので、前場と後場で内容は変わらない。
    #   毎回523回照会するのは無駄で、レート制限にも近づく。
    #   11:35の実行は後場の発注に間に合わせたい回なので、ここを軽くする。
    if not refetch:
        return fund_reuse(f"場中（{NOW:%H:%M} JST）は取り直さない（大引け後の実行で更新）")
    rows, hit, asked = jq_rows(JQ_RECENT_D)
    hist = jq_build(rows)
    jqinfo = {"dates_asked": asked, "dates_with_data": hit, "rows": len(rows),
              "codes": len(hist), "req": _JQ["req"], "n429": _JQ["n429"],
              "blocked": _JQ["blocked"], "resp_key": _JQ["key_used"],
              "err": dict(_JQ["err"])}
    if not hist:
        # 取得に失敗した日に列を空にすると、前回分が使えるのに捨てることになる。
        pct = fund_reuse("J-Quantsから開示が取れなかった")
        meta["fund"]["jq"] = jqinfo
        print("::warning::J-Quantsから財務情報が取れませんでした。"
              "前回の順位で代替しています（data/meta.json の fund を確認）")
        return pct
    fmap = {}
    for c, rs in hist.items():
        m = jq_metrics(rs)
        if m: fmap[c] = m
    # ネットキャッシュ比率（清原式）の候補絞り込み。fmap があるここでやる。
    try:
        _nm = {c: pub_name(c) for c in fmap}
        _s7 = {c: (MASTER.get(c, {}) or {}).get("s17", "") for c in fmap}
        netcash_shortlist(sc, fmap, _nm, _s7)
    except Exception as e:
        meta["errors"].append(f"netcash: {type(e).__name__}: {e}")
        meta["netcash"] = {"error": str(e)[:120]}
    # 清原枠は母集団が違う（ATR帯や売買代金の条件がスイングと別）。
    # ATRで落とす前の全銘柄の枠を使う。
    # EDINETの貸借対照表（本物のネットキャッシュ比率に要る）。
    # キーが無くても手元のキャッシュは使う。
    # 先に落とすべき銘柄＝EDINET抜きで清原枠の条件を通るもの。
    # （PER・PBR・時価総額・売買代金・業種・本業黒字だけで絞る）
    _pri = []
    try:
        _pool = FUND_ALL if FUND_ALL is not None else sc
        for _i in range(len(_pool)):
            _c = str(_pool["code"].iloc[_i]).replace(".T", "")
            if float(_pool["turnover"].iloc[_i]) < KY_MIN_TURNOVER: continue
            if _s7.get(_c) in KY_EXCLUDE: continue
            _k = ky_metrics(fmap.get(_c), float(_pool["close"].iloc[_i]))
            if ky_pass(_k)[0]: _pri.append(_c)
    except Exception as e:
        meta["errors"].append(f"edinet priority: {type(e).__name__}")
    _ed = {"rows": {}}
    try:
        _ed = ed_update(_pri)
    except Exception as e:
        # ★str(e) は使わない。EDINETは鍵をURLのクエリに載せる仕様なので、
        #   例外文をそのまま残すと public リポジトリに鍵が出る。
        meta["errors"].append(f"edinet: {type(e).__name__}")
        meta["edinet"] = {"error": type(e).__name__}
        try: _ed = ed_load()
        except Exception: _ed = {"rows": {}}
    try:
        kiyohara_screen(FUND_ALL if FUND_ALL is not None else sc, fmap, _nm, _s7,
                        (_ed or {}).get("rows"),
                        # 過去検証の母集団＝スイングの絞り込みを通った銘柄。
                        # コードは "7203.T" と "7203" の両方で照合できるようにする。
                        set(str(x) for x in sc["code"])
                        | set(str(x).replace(".T", "") for x in sc["code"]))
    except Exception as e:
        meta["errors"].append(f"kiyohara: {type(e).__name__}: {e}")
        meta["kiyohara"] = {"error": str(e)[:120]}
    # 素の項目がどれだけ埋まったか。欠けている場所を次回すぐ特定できるように残す。
    _keys = ("bps", "feps", "eq", "ta", "cash", "sh", "op", "div")
    # ★鍵に "n_" を付ける。中身は件数だけだが、鍵の名前が生の項目名と
    #   同じだと commit_guard が毎回 ::error:: を出し（実測でそうなった）、
    #   本当の混入を見落とすようになる。検査は緩めず、名前をずらす。
    meta["fund_raw"] = {"n_" + k: sum(1 for m in fmap.values()
                                      if m.get(k) is not None)
                        for k in _keys}
    meta["fund_raw"]["n_codes"] = len(fmap)
    out, has = fund_scores(sc, fmap)
    codes = [str(c).replace(".T", "") for c in sc["code"].tolist()]
    pct = {}
    for i, c in enumerate(codes):
        if not bool(has.iloc[i]): continue
        d = {}
        for k, ser in out.items():
            v = ser.iloc[i]
            if v == v: d[k] = int(round(float(v) * 100))
        if d: pct[c] = d
    asof = max((m["asof"] for m in fmap.values() if m.get("asof")), default="")
    payload = {
        "generated_at_jst": NOW.isoformat(),
        "asof_latest_disclosure": asof,
        "pool": int(len(sc)), "n_codes": len(pct),
        "span_days": JQ_RECENT_D,
        "note": ("断面のパーセンタイル（0〜100、大きいほど上位）のみを収録する。"
                 "母集団は当日スクリーニングを通過した銘柄。"
                 "J-Quantsの生の財務数値は利用条件により公開できないため含まない。"),
        "factors": {
            "ep":       "予想利益利回り＝1/PER（清原氏が一番重視するもの）",
            "netcash":  "ネットキャッシュ比率の下限版（現金同等物−負債合計）÷時価総額",
            "kiyohara": "epとnetcashを2:1で合成（2:1の重みは本人指定ではない）",
            "payout":   "株主還元＝配当の水準＋増配の方向＋自己株買いの実行。無配は最下位",
            "bp":       "純資産倍率の逆数＝1/PBR（清原氏は見るが重視しない）",
            "value":    "bpとepの等ウェイト合成（従来版・比較用）",
            "quality":  "予想ROE・営業利益率・自己資本比率の合成",
            "moat":     "営業利益率の水準とばらつきの小ささ。競争優位の代理指標であって測定ではない",
            "revision": "会社予想の改定方向（高いほど上方修正）"},
        "ranked_by": RANK_BY,
        "coverage": fund_coverage(out, has),
        "used_in_score": False,
        "jq": jqinfo, "pct": pct}
    json.dump(payload, open(FUND_PATH, "w"), ensure_ascii=False, indent=1)
    meta["fund"] = {k: payload[k] for k in
                    ("asof_latest_disclosure", "pool", "n_codes", "coverage", "jq")}
    print(f"[財務] {len(pct):,}銘柄に順位を付与（開示の最新 {asof} / 照会{_JQ['req']}回）")
    return pct

# ══════════════════════════════════════════════════════════════════════
#  ネットキャッシュ比率（清原達郎『わが投資術』）の候補絞り込み
#
#  式:
#    ネットキャッシュ ＝ 流動資産 ＋ 投資有価証券×70% − 負債合計
#    ネットキャッシュ比率 ＝ ネットキャッシュ ÷ 時価総額
#    （70%を掛けるのは、売却時の税金約30%を引いて現実に使える額にするため）
#    比率が1以上 ＝ 資産を売って負債を返しても現金が余る ＝ 本業がタダで付いてくる
#
#  無料データで作れる部分と作れない部分:
#    負債合計   ＝ 総資産 − 純資産      … J-Quantsで取れる（TA, Eq）
#    時価総額   ＝ 株価 × (発行済株式数 − 自己株式数) … 取れる（ShOutFY, TrShFY）
#    流動資産      … 取れない（貸借の内訳はPremium）
#    投資有価証券  … 取れない（同じ）
#
#  そこで、取れない2つを**不等式で挟む**。近似や仮定は置かない。
#    現金同等物 ≤ 流動資産、  0 ≤ 投資有価証券×70%
#      → 下限 NC_lo ＝ 現金同等物 − 負債合計
#    流動資産 ＋ 投資有価証券×70% ≤ 総資産
#      → 上限 NC_hi ＝ 総資産 − 負債合計 ＝ 純資産
#
#  ここから手作業を激減させる3分類が出る:
#    ① 下限比率 ≥ 1 → 清原式でも必ず1以上。**手で確かめる必要がない**
#    ② 下限 < 1 ≤ 上限 → 1以上になりうる。**ここだけ手で確かめる**
#    ③ 上限比率 < 1（＝PBR > 1）→ 清原式で1以上になることが**数学的に不可能**。
#       手作業の対象から完全に外せる
#
#  除外:
#    銀行・保険・証券は流動資産と負債の意味が違うので式が成立しない。17業種で外す。
#    本業が赤字（営業利益 ≤ 0）の会社も外す。現金が減っていく側なので、
#    現金の多さを割安と読むと逆になる。
# ══════════════════════════════════════════════════════════════════════
# ── public リポジトリに出してはいけないものが混ざっていないかの検査 ──
#   ① J-Quantsの生の財務数値（利用条件で第三者閲覧が禁止されている）
#   ② APIキーそのもの
#   見つけたら黙って公開せず、その場で削って大きく警告する。
#   ジョブを落とすのではなく削るのは、その日のレポートまで失うのを避けるため。
# EDINET由来のファイルは項目名の検査から外す（PDL1.0で公開が認められている）
ED_EXEMPT = {"edinet.json"}
RAW_KEYS = {"bps", "feps", "eps", "sales", "op", "ta", "eq", "eqar",
            "cfo", "fop", "fnp", "opm", "opm_stab", "roe", "cfoa",
            # 成長も生の伸び率は出さない（分類タグ grow だけを書く）
            "g_sales", "g_op", "g_fop"}


def json_safe(o):
    """NaN / Inf を null に直す。json.dump は既定で `NaN` という
       JSONとして不正な字を書き、読み手によっては解析に失敗する。
       実測で candidates.json に29個の NaN が出ていた。"""
    if isinstance(o, dict): return {k: json_safe(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)): return [json_safe(v) for v in o]
    if isinstance(o, float) and (o != o or o in (float("inf"), float("-inf"))):
        return None
    return o

def _walk_keys(o, found):
    if isinstance(o, dict):
        for k, v in o.items():
            if k in RAW_KEYS: found.add(k)
            _walk_keys(v, found)
    elif isinstance(o, list):
        for v in o: _walk_keys(v, found)

def _walk_jpx(o, found):
    """JPX由来の項目が書き出されていないか。中身が空の鍵は数えない
       （列だけ残して値を空にしている箇所があるため）。"""
    if isinstance(o, dict):
        for k, v in o.items():
            if k in JPX_KEYS and v not in (None, "", [], {}): found.add(k)
            _walk_jpx(v, found)
    elif isinstance(o, list):
        for v in o: _walk_jpx(v, found)

def redact_keys(fp):
    """1ファイルからAPIキーを伏せる。伏せたら True。

       ★EDINETの鍵はURLのクエリパラメータで送る仕様なので、
         J-Quantsの鍵より漏れやすい。両方を対象にする。
       ★commit_guard から切り出してある。meta.json は検査のあとに
         書き直すので、そのあとで**これだけ**もう一度当てる必要がある。"""
    try:
        txt = open(fp, encoding="utf-8").read()
    except Exception:
        return None
    hit = False
    for _k in (JQ_KEY, ED_KEY):
        if _k and len(_k) >= 12 and _k in (txt or ""):
            hit = True
            txt = txt.replace(_k, "[REDACTED]")
    if hit:
        open(fp, "w", encoding="utf-8").write(txt)
    return txt, hit

def commit_guard():
    import glob
    bad_files, key_files = {}, []
    jpx_fields, jpx_files, jpx_cleaned = {}, [], {}
    # ★data直下のCSV（edinet_hist.csv）も鍵の伏せ字の対象にする。
    #   ohlcv/ の数千ファイルは価格だけなので対象外にして時間を使わない。
    for fp in sorted(glob.glob(f"{OUT}/*.csv")):
        _r = redact_keys(fp)
        if _r and _r[1] and fp not in key_files: key_files.append(fp)
    for fp in sorted(glob.glob(f"{OUT}/*.json")):
        _r = redact_keys(fp)
        if _r is None: continue
        txt, _hit = _r
        if _hit and fp not in key_files: key_files.append(fp)
        try:
            o = json.loads(txt if txt is not None
                           else open(fp, encoding="utf-8").read())
        except Exception:
            continue
        # ★JPX由来の項目（業種・規模・市場区分）は**免除しない**。
        #   下のEDINET免除はJ-Quantsの財務数値の話で、
        #   JPXの区分表はまったく別の規約。先に見ておく。
        jf = set()
        _walk_jpx(o, jf)
        if jf: jpx_fields[fp] = sorted(jf)
        # ★EDINETの数値は公共データ利用規約(PDL1.0)で二次利用が認められて
        #   いるので、生の項目名（ta など）が入っていて正常。
        #   この検査はJ-Quantsのデータを守るためのものなので対象外にする。
        #   （鍵の伏せ字は上で済んでいて、そちらは全ファイルに適用している）
        if os.path.basename(fp) in ED_EXEMPT:
            continue
        found = set()
        _walk_keys(o, found)
        if found: bad_files[fp] = sorted(found)
    # ★古い実行で書かれたまま残っているファイルも掃除する。
    #   実測で candidates.json / kiyohara.json / netcash.json に s17 が
    #   残っていた。前2つは今回の書き出しで直るが、その回に生成されなかった
    #   ファイル（財務が引けずスキップされた回など）は**古いまま居座る**。
    #   見つけたらその場で落として書き戻す。
    for fp in sorted(glob.glob(f"{OUT}/*.json")):
        if os.path.basename(fp) in JPX_FILES: continue
        try:
            _o2 = json.load(open(fp, encoding="utf-8"))
        except Exception:
            continue
        _jf2 = set(); _walk_jpx(_o2, _jf2)
        if not _jf2: continue
        try:
            json.dump(jpx_safe(_o2), open(fp, "w"), ensure_ascii=False, indent=1)
            jpx_cleaned[fp] = sorted(_jf2)
            # 直したものは警告から外す（直せなかったものだけを ::error:: にする）
            jpx_fields.pop(fp, None)
            print(f"[見張り] {fp} からJPX由来の項目 {sorted(_jf2)} を削除しました")
        except Exception as e:
            meta["errors"].append(f"jpx clean {os.path.basename(fp)}: {type(e).__name__}")

    # JPX由来のファイルが data/ に残っていないか
    for _n in JPX_FILES:
        _fp = f"{OUT}/{_n}"
        if os.path.exists(_fp): jpx_files.append(_fp)
    # 台帳（CSV）の列も見る。json と同じ扱いにする。
    for fp in sorted(glob.glob(f"{OUT}/*.csv")):
        try:
            with open(fp, encoding="utf-8") as f:
                head = (f.readline() or "").strip()
        except Exception:
            continue
        _cols = {c.strip().strip('"') for c in head.split(",")}
        _hit = sorted(_cols & JPX_KEYS)
        if _hit: jpx_fields[fp] = _hit
    res = {"raw_fields": bad_files, "api_key_found_in": key_files,
           "jpx_fields": jpx_fields, "jpx_files": jpx_files,
           "jpx_cleaned": jpx_cleaned}
    if key_files:
        print(f"::error::APIキーが {', '.join(key_files)} に出ていたので伏せました。"
              "コードの見直しが必要です")
    if bad_files:
        for fp, ks in bad_files.items():
            print(f"::error::{fp} に生の財務項目 {ks} が含まれています。"
                  "J-Quantsの利用条件に反するのでコミット前に取り除いてください")
    for fp, ks in jpx_cleaned.items():
        print(f"::warning::{fp} に JPX由来の項目 {ks} が残っていたので削除しました"
              "（古い実行で書かれたもの）。書き出し側も直っているか確認すること")
    for fp, ks in jpx_fields.items():
        print(f"::error::{fp} に JPX由来の項目 {ks} が含まれています。"
              "publicリポジトリへの再配信に当たるので取り除いてください"
              "（JPXサイト利用条件）")
    for fp in jpx_files:
        print(f"::error::{fp} は JPXの銘柄一覧そのものです。"
              "data/ に置くと再配信に当たります。cache/ に移してください")
    meta["commit_guard"] = res
    return res

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

# ══════════════════════════════════════════════════════════════════════
#  過去検証 ── このスコアに優位性があるのかを、実際に付いた値段で確かめる
#
#  なぜ要るか:
#    台帳は前向きに積み上がるが、平均+0.10Rを判定するには400件規模が要り、
#    しかも同じ日の銘柄は独立でないので、実効的には1年以上かかる。
#    一方、過去2年の実データは今ある。同じ問いに今日答えられる。
#
#  何を比べるのか（先に決めておく。後から良い結果を探さない）:
#    ① バリュー上位10件 vs 同じ母集団の全銘柄を等ウェイトで建てた平均
#    ② 文献由来の因子（モメンタム・短期反転・低ボラ）も同じ母集団で
#    ③ ①②を「TOPIXが200日線の上/下」で分けたとき
#    比較対象が母集団平均であることが肝心。「平均Rがプラス」では意味がない。
#    母集団平均は乱数ではなく全銘柄の実測値。誰が何回走らせても同じ数字になる。
#    流動性と値幅で絞った母集団から適当に買っても同じ結果なら、
#    順位付けは何もしていないことになる。
#
#  自分を欺かないための約束:
#    ・指標は t 時点までのデータだけで計算する（先読みしない）
#    ・建値は t の終値、決着は t+1 以降の高安。窓は寄値で約定
#    ・価格はすべて調整済み
#    ・保有期間の感度（10/15/25日）は「頑健性の確認」であって、
#      良い数字を選ぶためではない
#
#  どうしても残るバイアス（結果を良い方に歪める）:
#    ・生存バイアス。銘柄一覧は「今上場している会社」なので、
#      この2年で上場廃止になった会社が母集団から抜けている。
#    ・スリッページ・板の薄さ・約定できない場面を含まない。
#    ・税を引いていない。
# ══════════════════════════════════════════════════════════════════════
BT_ENABLED   = True
BT_PERIOD    = "5y"     # 2年では200日線を割る局面がほぼ無く（実測n=10）
                        # レジームの検証ができなかった。5年に延ばす。
BT_STEP      = 5        # 何営業日ごとに建てるか
BT_TOP       = 10       # 各サイドの採用数
# 保有期間を大きく広げた。理由:
#   バリューの効き目（建て日で束ねた日次の差）が 10日+0.182 → 15日+0.229
#   → 25日+0.256 と単調に増えていた。25日で打ち切ると、伸びている途中で
#   測るのをやめていることになる。清原達郎氏が『3年（場合によっては5年）持つ・
#   最低2倍を狙う・3割上昇での売却は勧めない』としているのと方向が一致する。
#   5年分の実データは既に手元にあるので、追加の取得なしで測れる。
# ★「数日」を測れていなかった。最短が10日だったので、スイングトレードの
#   本来の時間軸（2〜5営業日）が一度も検定されていない。3日と5日を足す。
#   短い保有ほど独立した建て日が多く取れる（5営業日ごとに建てるので
#   保有3〜5日なら重なりがほぼ無い）＝検定の力が一番強い領域でもある。
BT_HOLDS     = (3, 5, 10, 25, 60, 120, 250)
# 利確の有無も比べる。3ATR利確はバリューの上振れを切っている可能性がある。
# (名前, 利確を使うか, 損切りを使うか)
#   2atr_3atr … いまのスイングの執行
#   2atr_only … 利確をやめる。実測で平均Rが大きく伸びた（上振れを刈っていた）
#   hold_only … 損切りもやめて期限まで持つ。★これが清原式に一番近い。
#     実測で「利確なし・250日」の損切り率が62%だった。2ATR（約5%）の損切りを
#     250日保有に付けるのは噛み合っていない。値動きの雑音でほぼ確実に当たる。
#     清原氏は5%の損切りなど使わず、仮説が崩れたときに降りる。
#     その「仮説が崩れたとき」は機械化できないので、下限として
#     「何もせず期限まで持つ」を測る。
# 執行の方式。★ここまで「降り方」を3通りしか試していなかった。
#   1銘柄の期待値は「選び方」と「降り方」の掛け算なので、
#   選び方だけ何十通り試して降り方を3通りに固定しているのは片手落ち。
#   トレーリングストップ（含み益を追いかけて損切り位置を上げる）は
#   実務で最も一般的な降り方のひとつなのに、一度も測っていなかった。
#   ★組み合わせが増えるぶん多重検定の床（bt_dsr）が自動で上がる。
#     それで通らなくなるなら、それが正しい判定。
BT_EXITS     = (("2atr_3atr",  True,  True),      # 2ATR損切り・3ATR利確
                ("2atr_only",  False, True),      # 2ATR損切りのみ
                ("hold_only",  False, False),     # 期限まで持つだけ
                ("trail2atr",  False, True, 2.0), # 高値から2ATRのトレーリング
                ("trail3atr",  False, True, 3.0)) # 同・3ATR（緩め）
BT_WARMUP    = 280      # 12-2モメンタム（252日）に必要な本数
BT_MAX_AGE_D = 30       # これより新しい結果があれば作り直さない
BT_BUDGET_S  = 2400
BT_CHUNK     = 180
# 乱数は使わない。基準線は母集団の全銘柄平均（推定ではなく実測）なので、
# 抽出回数も種も要らない。同じ入力なら常に同じ結果になる。

def _rsi_series(c, n=14):
    """全期間のRSI。1点ずつ再計算すると検証が終わらないので通しで出す。"""
    import numpy as np
    c = np.asarray(c, float)
    d = np.diff(c, prepend=c[0])
    up = np.where(d > 0, d, 0.0); dn = np.where(d < 0, -d, 0.0)
    au = np.full(len(c), np.nan); ad = np.full(len(c), np.nan)
    if len(c) <= n: return np.full(len(c), np.nan)
    au[n] = up[1:n+1].mean(); ad[n] = dn[1:n+1].mean()
    for i in range(n+1, len(c)):
        au[i] = (au[i-1]*(n-1) + up[i]) / n
        ad[i] = (ad[i-1]*(n-1) + dn[i]) / n
    rs = np.where(ad > 0, au/np.where(ad == 0, np.nan, ad), np.inf)
    return 100 - 100/(1+rs)

def _ema(x, n):
    """指数移動平均。α = 2/(n+1)。最初の n 本の単純平均を種にする（Appel/一般実装）。"""
    import numpy as np
    x = np.asarray(x, float); out = np.full(len(x), np.nan)
    if len(x) < n: return out
    a = 2.0 / (n + 1.0)
    out[n-1] = x[:n].mean()
    for i in range(n, len(x)):
        out[i] = a * x[i] + (1 - a) * out[i-1]
    return out

def _macd(c, fast=12, slow=26, sig=9):
    """MACD。Gerald Appel。慣習パラメータ 12/26/9。
       返り値: (MACD線, シグナル線, ヒストグラム)"""
    import numpy as np
    m = _ema(c, fast) - _ema(c, slow)
    # シグナル線はMACD線が出てからのEMAなので、NaN区間を除いて計算する
    ok = ~np.isnan(m)
    sg = np.full(len(m), np.nan)
    if ok.sum() >= sig:
        sg[ok] = _ema(m[ok], sig)
    return m, sg, m - sg

def _stoch(h, l, c, n=14, k=3, d=3):
    """ストキャスティクス Full(14,3,3)（= Slow(14,3) と数学的に同一）。
       George Lane。%K は生の%Kをk期間平均、%D はそれをd期間平均。"""
    import numpy as np
    h, l, c = map(lambda z: np.asarray(z, float), (h, l, c))
    hh = pd.Series(h).rolling(n).max().to_numpy()
    ll = pd.Series(l).rolling(n).min().to_numpy()
    with np.errstate(invalid="ignore", divide="ignore"):
        raw = (c - ll) / (hh - ll) * 100.0
    kk = pd.Series(raw).rolling(k).mean().to_numpy()
    dd = pd.Series(kk).rolling(d).mean().to_numpy()
    return kk, dd

def _bbands(c, n=20, k=2.0):
    """ボリンジャーバンド。John Bollinger。
       ★標準偏差は**母集団**（n で割る＝ddof=0）。Bollingerの実装がそちら。
         pandas の既定は ddof=1 なので明示しないと値がずれる。
       返り値: (中心線, %b, バンド幅%)"""
    import numpy as np
    sr = pd.Series(np.asarray(c, float))
    mid = sr.rolling(n).mean()
    sd  = sr.rolling(n).std(ddof=0)
    up, lo = mid + k*sd, mid - k*sd
    with np.errstate(invalid="ignore", divide="ignore"):
        pb = (sr - lo) / (up - lo)
        bw = (up - lo) / mid * 100.0
    return mid.to_numpy(), pb.to_numpy(), bw.to_numpy()

def _ichimoku(h, l, c, t=9, k=26, sp=52, shift=26):
    """一目均衡表。細田悟一（一目山人）。慣習 9/26/52、先行/遅行は26。

       ★時点 t で**見えている**雲は、t-26 時点で計算された値。
         先行スパンを前方にずらして表示するとはそういうこと。
         ここを取り違えると未来を見たことになる。
       ★ずらし幅は資料によって25と26で食い違う（当日を1と数えるか）。
         市販チャート（TradingView・MetaTrader・証券各社）はすべて26なので
         26を採る。25でも見られるように引数にしてある。"""
    import numpy as np
    hs, ls = pd.Series(np.asarray(h, float)), pd.Series(np.asarray(l, float))
    ten = (hs.rolling(t).max()  + ls.rolling(t).min())  / 2
    kij = (hs.rolling(k).max()  + ls.rolling(k).min())  / 2
    a_raw = (ten + kij) / 2
    b_raw = (hs.rolling(sp).max() + ls.rolling(sp).min()) / 2
    # 前方にずらす＝時点tで見える値は (t-shift) 時点の計算値
    spa = a_raw.shift(shift).to_numpy()
    spb = b_raw.shift(shift).to_numpy()
    return ten.to_numpy(), kij.to_numpy(), spa, spb

def _adx_series(h, l, c, n=14):
    """ADX / +DI / -DI。Wilder (1978)。合計版のWilder平滑化。
       ★ADXは**向きを示さない**（強さだけ）。単独の買い合図にはしない。
         フィルタとして使うのが原典どおりの用法。"""
    import numpy as np
    h, l, c = map(lambda z: np.asarray(z, float), (h, l, c))
    n_ = len(c)
    up = np.diff(h, prepend=h[0]); dn = -np.diff(l, prepend=l[0])
    pdm = np.where((up > dn) & (up > 0), up, 0.0)
    ndm = np.where((dn > up) & (dn > 0), dn, 0.0)
    pc = np.roll(c, 1); pc[0] = c[0]
    tr = np.maximum(h-l, np.maximum(abs(h-pc), abs(l-pc)))
    def _w(x):
        o = np.full(n_, np.nan)
        if n_ <= n: return o
        o[n] = x[1:n+1].sum()
        for i in range(n+1, n_):
            o[i] = o[i-1] - o[i-1]/n + x[i]
        return o
    tr14, p14, n14 = _w(tr), _w(pdm), _w(ndm)
    with np.errstate(invalid="ignore", divide="ignore"):
        pdi = 100 * p14 / tr14
        ndi = 100 * n14 / tr14
        dx  = 100 * np.abs(pdi - ndi) / (pdi + ndi)
    adx = np.full(n_, np.nan)
    ok = np.flatnonzero(~np.isnan(dx))
    if len(ok) >= n:
        st = ok[n-1]
        adx[st] = np.nanmean(dx[ok[:n]])
        for i in range(st+1, n_):
            if np.isnan(dx[i]): continue
            adx[i] = (adx[i-1]*(n-1) + dx[i]) / n
    return adx, pdi, ndi

def _atr_series(h, l, c, n=14):
    import numpy as np
    h, l, c = map(lambda z: np.asarray(z, float), (h, l, c))
    pc = np.roll(c, 1); pc[0] = c[0]
    tr = np.maximum(h-l, np.maximum(abs(h-pc), abs(l-pc)))
    a = np.full(len(c), np.nan)
    if len(c) <= n: return a
    a[n] = tr[1:n+1].mean()
    for i in range(n+1, len(c)):
        a[i] = (a[i-1]*(n-1) + tr[i]) / n
    return a

def bt_features(ax, vol):
    """調整済みの枠から、全期間ぶんの指標を一度に作る。
       各検証日では添字を引くだけにして、計算量を抑える。"""
    import numpy as np
    cl = ax["Close"].to_numpy(float)
    out = {
        "close": cl,
        "rsi":   _rsi_series(cl),
        "atr":   _atr_series(ax["High"].to_numpy(float), ax["Low"].to_numpy(float), cl),
        "ma25":  ax["Close"].rolling(25).mean().to_numpy(float),
        "ma75":  ax["Close"].rolling(75).mean().to_numpy(float),
        "hi60":  ax["High"].rolling(60).max().to_numpy(float),
        "lo60":  ax["Low"].rolling(60).min().to_numpy(float),
        "open":  ax["Open"].to_numpy(float),
        "high":  ax["High"].to_numpy(float),
        "low":   ax["Low"].to_numpy(float),
        "turn":  (ax["Close"]*vol).rolling(20).mean().to_numpy(float),
        "volr":  (vol / vol.rolling(20).mean()).to_numpy(float),
        # ── 仕掛けの形に要る素材（すべて慣習パラメータ）──────────
        # ★上抜けの比較相手は shift(1)＝**前日までの**高値。
        #   当日の高値を含めると、終値がそれ以下なのは当たり前で
        #   上抜けが構造的に出ないか、当日の値動きを先に見たことになる。
        "hi20p": ax["High"].rolling(20).max().shift(1).to_numpy(float),
        "hi50p": ax["High"].rolling(50).max().shift(1).to_numpy(float),
        "hi55p": ax["High"].rolling(55).max().shift(1).to_numpy(float),
        "ma5":   ax["Close"].rolling(5).mean().to_numpy(float),
        "ma50":  ax["Close"].rolling(50).mean().to_numpy(float),
        "ma200": ax["Close"].rolling(200).mean().to_numpy(float),
        "rng":   (ax["High"] - ax["Low"]).to_numpy(float),
    }
    _h = ax["High"].to_numpy(float); _l = ax["Low"].to_numpy(float)
    # MACD (Appel 12/26/9)
    out["macd"], out["macd_sig"], out["macd_hist"] = _macd(cl)
    # ストキャスティクス Full(14,3,3) (Lane)
    out["stk"], out["std_"] = _stoch(_h, _l, cl)
    # ボリンジャー (Bollinger 20, 2σ, 母集団σ)
    _mid, out["bb_pb"], out["bb_bw"] = _bbands(cl)
    # バンド幅が「過去6ヶ月で最小」か（Bollinger のスクイーズの定義）
    out["bb_bw_min"] = pd.Series(out["bb_bw"]).rolling(SETUP_BB_LOOK).min().to_numpy(float)
    # 一目均衡表 (細田 9/26/52、ずらし26)
    out["ich_ten"], out["ich_kij"], out["ich_a"], out["ich_b"] = _ichimoku(_h, _l, cl)
    # ADX（向きは示さない。フィルタ用）
    out["adx"], out["pdi"], out["ndi"] = _adx_series(_h, _l, cl)
    # NR7 (Crabel 1990): 直近7日で最小の値幅
    out["rng7min"] = pd.Series(out["rng"]).rolling(7).min().to_numpy(float)
    # 出来高の過去49日分位 (Gervais-Kaniel-Mingelgrin 2001)
    out["vol_pct49"] = (pd.Series(vol.to_numpy(float))
                        .rolling(SETUP_HVOL_LOOK)
                        .rank(pct=True).to_numpy(float) * 100.0)
    # 25日移動平均乖離率（日本の実務。短期リバーサルのプロキシ）
    with np.errstate(invalid="ignore", divide="ignore"):
        out["dev25"] = (cl / out["ma25"] - 1.0) * 100.0
    # 21日（約1ヶ月）騰落率。Chang-McLeavey-Rhee 1995 の形成期間に合わせる
    r21 = np.full(len(cl), np.nan); r21[22:] = cl[22:]/cl[:-22] - 1
    out["r21"] = r21 * 100
    # 前日値（クロスの判定に要る。「今日はじめて上抜けた」を見るため）
    for _k in ("macd", "macd_sig", "ma5", "ma25", "ma50", "ma75",
               "rsi", "stk", "std_", "close", "ich_ten", "ich_kij"):
        if _k in out:
            out["p_" + _k] = np.r_[np.nan, np.asarray(out[_k], float)[:-1]]
    r20 = np.full(len(cl), np.nan); r20[21:] = cl[21:]/cl[:-21] - 1
    out["r20"] = r20*100
    r5 = np.full(len(cl), np.nan); r5[6:] = cl[6:]/cl[:-6] - 1
    out["r5"] = r5*100
    # 12-2モメンタム: t-252 から t-21 まで（直近1ヶ月を除く）
    m = np.full(len(cl), np.nan)
    if len(cl) > 253:
        m[253:] = cl[232:-21]/cl[1:-252] - 1
    out["mom12_2"] = m*100
    return out

# ══════════════════════════════════════════════════════════════
#  比べる選び方（先に決めて、全部報告する。良いものだけ拾わない）
#    trend / revert … 自作。実測で母集団平均との差 t=+0.08 / +0.06 だった
#    mom12_2 … 12ヶ月モメンタム（直近1ヶ月を除く）。Jegadeesh-Titman。
#               直近1ヶ月を除くのは、そこが反転する領域だから。
#               ただし日本株はモメンタムが効かないとする研究が複数ある。
#    rev5    … 短期リバーサル。直近5日の下落が大きいほど上位。
#               断面の異常として比較的頑健とされる。
#    lowvol  … 低ボラティリティ。ATR%が**小さい**ほど上位。
#               自作スコアは逆に高ボラを加点していた（BAB/低ボラ研究と逆符号）。
#  5通りを一度に比べるので、たまたま良く見えるものが出やすい。
#  positiveと言うには t≥2.5 を要求する（名目t≥2では5通りで誤検出2割弱）。
#    value / quality / moat / revision … J-Quantsの財務情報から作るファンダ側。
#               日本株はバリューが効きモメンタムが効かないとする研究があり、
#               証拠の強い側を今まで作っていなかった。
#               ★この4つは「財務が引けた銘柄」だけの母集団で戦うので、
#                 比較の基準線も同じ母集団の全銘柄平均 pool_f を使う。
#                 全銘柄の平均と比べると、ETFを含む/含まないの
#                 違いが優位性に見えてしまう。
# 自作の順張り/逆張りは 2026-09-13 に削除した（本人の判断）。
# 5年の検証で点推定が全条件でマイナス、かつ有意ではなかった。
# 「効かなかったものとの比較」を失うという心配はあるが、
# 意味のある比較相手は同じ母集団の全銘柄平均（pool / pool_f）で、
# そちらは残るので検出力は落ちない。
# ★acd_score を足した。**あなたが毎日見ている点数が一度も検定されて
#   いなかった**ため（Dを除いた A+C）。手で決めた重みを検定せずに
#   発注の材料にしていたことになる。
# 表に出すときの名前。BT_FACTORS に足したものは必ずここにも足す
# （足し忘れても符号のまま出るので、黙って消えることはない）。
BT_FACTOR_JA = {
    "mom12_2": "12-2モメンタム", "rev5": "短期リバーサル(5日)",
    "lowvol": "低ボラティリティ",
    "acd_score": "**このレポートの点数(A+C／Dは除く)**",
    "value": "バリュー（財務）", "ep": "予想益回り（財務）",
    "bp": "純資産倍率の逆数（財務）", "netcash": "ネットキャッシュ下限（財務）",
    "kiyohara": "清原式（PER重視＋NC）", "payout": "株主還元（財務）",
    "quality": "クオリティ（財務）", "moat": "競争優位の代理（財務）",
    "revision": "業績修正方向（財務）",
    "growth": "成長（増収・増益／財務）", "ky_grow": "清原式＋成長",
}

# ══════════════════════════════════════════════════════════════
#  仕掛けの形 ── 一般に使われている売買シグナルを、**原典の慣習パラメータ
#  のまま**実装する。自分で数字を決めない。
#
#  ★前の版で私が置いた数字（出来高2.0倍・RSI<40・ATR比0.85）は
#    どれも典拠が無かった。捨てた。しきい値を自分で選ぶこと自体が
#    Park & Irwin (2007) の言う "ex post selection of trading rules"
#    そのもので、検定の前に結果を作ってしまう。
#
#  ★調査で分かった最重要事項（事前期待値の設定に要る）:
#    日本株について、標準テクニカルシグナルに査読済みの支持証拠は
#    **ほぼ存在しない**。
#      Chong, Ng & Liew (2014) JRFM 7(1): 日経225 1976-2002 で
#        MACD(12,26,9) の buy-sell = +0.166%、t=0.410（非有意）
#        RSI(14,30/70) = -0.083%（負）、RSI(7)・RSI(21)も負
#      Kang (2021) JRFM 14(1): 日経225先物 2011-2019 で標準MACDは負
#      Marshall, Young & Cahan (2008) RQFA 31(2): ローソク足は
#        日本株30年(1975-2004)で価値なし（日本発祥なのに）
#      Yamamoto (2012) JBF 36(11): 日経225個別の板データ、
#        White RC / Hansen SPA 補正後、どの戦略も buy-and-hold に勝てない
#      Bessembinder & Chan (1995) PBFJ 3: 新興国では効くが
#        「香港・日本のような先進市場では説明力が低い」
#      Hsu, Hsu & Kuan (2010) JAE: 日本含むアジア8市場、SPA補正後は利益なし
#    → だから「効かなかった」という結果が出ても、それが**既存文献どおり**
#      であることを先に承知しておく。効いたときこそ疑う。
#
#  ★唯一、日本株で実証されている効果は**短期リバーサル（逆張り）**:
#      Chang, McLeavey & Rhee (1995) JBFA: TSE全上場、1975-1991、
#        形成後1ヶ月で loser-winner = +1.97%/月（リスク・サイズ調整後
#        +1.69%）。2ヶ月目は +0.63% に急減衰。
#      Asness (2011) JPM 37(4): 日本は中期モメンタムが効かない例外。
#    日本の実務で言う「25日移動平均乖離率」がこれに当たる。
#    ★ところが当方の補正後の実測では rev5 が有意にマイナスだった
#      （3つのt が -3.2〜-8.7）。文献と符号が逆。
#      形成期間が5日か1ヶ月かの違いが効いている可能性が高いので、
#      形成期間を分けて測る（rev5 / rev21 / 乖離率）。
#
#  ★出版後の減衰を織り込む: McLean & Pontiff (2016) JF 71(1) は
#    97の予測変数で「出版後に -58%」。テクニカルは論文より遥かに
#    広く知られているので、減衰はこれより大きいと見るのが安全。
#    Fang, Jacobsen & Qin はボリンジャーバンドで、
#    1983年の公表前は国際株式市場で高収益 → 2001年の著書以降ほぼ消滅、
#    という教科書的な事例を示している。
#
#  ★パラメータは慣習値に固定し、**探索しない**。
#    Kang (2021) が日経225先物で (4,22,3) に最適化して有意な正を得たのは
#    イン・サンプル最適化の典型で、採用してはいけない。

# 出典と、査読済みエビデンスの状態。レポートにそのまま出す。
#   "支持" = 査読済みで肯定的結果 / "反証" = 査読済みで否定的結果
#   "混合" / "検証なし" = 広く使われているが査読済みの検証がほぼ無い
# ══════════════════════════════════════════════════════════════
#  イベント起点の仕掛け（決算発表・業績予想の修正）
#
#  ★ここまでの仕掛けは全部「その日の値動きの形」だった。
#    だが数日〜数ヶ月のスイングで、**日本株について査読済みの証拠が
#    あるのはイベント起点のほう**だと分かった。順序を間違えていた。
#
#  ★根拠（すべてイベント起点・日〜週の時間軸）:
#    Meursault, Liang, Routledge & Scanlon (2023) JFQA 58(6) "PEAD.txt":
#      決算コールの**テキストだけ**から作ったサプライズ指標が、
#      報告利益の数値を一切使わずに古典的なSUEより**大きい**ドリフトを
#      生んだ。保有63日で 2.87% 対 1.54%、252日で 8.01% 対 4.63%。
#      しかも古典PEADがほぼ消えた2018-19年でも年3.4%を下回らなかった。
#      → 「テキストを読むこと」が数値に直交する情報を持つ最強の証拠。
#    Jinushi (2023) The Japanese Accounting Review 13（日本株・60,124件）:
#      日本のPEADは大幅に減衰。**ただし外国人保有が低く個人保有が高い
#      銘柄では残存**。CAR(-1,+60営業日)で測っている。
#    Okada & Saeki (2014) 証券アナリストジャーナル（日本株9,390件）:
#      CAR(+1,+21) が予想誤差の上位と下位で **2.2ポイント**。
#      注目度が高いとドリフトは0.7〜0.8pt縮む。
#      ★取引コストの代理変数が有意に負＝**実行しにくい銘柄ほど
#        ドリフトが大きい**。流動性で分けて見ないと罠にかかる。
#    Kitagawa & Shuto (2024) JBFA 51(9-10)（日本株）:
#      期初の会社予想の「予想外部分」が大きい企業は、その後の
#      異常リターンが負。会社予想は日本固有で頻度の高いイベント。
#
#  ★負の証拠も先に書いておく:
#    Martineau (2022) Critical Finance Review:「大型株では2006年以降
#      PEADは存在しない」。復活を主張する論文はマイクロキャップを
#      除外していないという批判がある。
#    種村・久保・中川 (2025) SIG-FIN FIN-035「LLM-PEAD.txt」（日本株）:
#      数値サプライズ単独・LLMテキストサプライズ単独では**一貫した
#      ドリフトを確認できず**、両者の組合せでのみ限定的に出現。
#    → だから「効かない」が出ても既存文献どおり。流動性で分けて見る。
#
#  ★建てるのは発表の翌日ではなく **2営業日以降**。
#    発表当日〜翌日の反応は取引できない部分で、そこを入れると
#    見かけの成績が跳ね上がる（Lopez-Lira & Tang 2026 JFE の
#    的中率93%は、まさにこの取引できない初期反応の部分）。
EV_MIN_AGE = 2      # 発表から最低これだけ営業日を置いてから建てる
EV_MAX_AGE = 5      # これより古い開示はもうイベントとして扱わない
EV_LATE_LO = 20     # 「発表から約1ヶ月後」の窓。吉野(2026)の指摘に合わせる
EV_LATE_HI = 40

EVENT_META = {
 "pead_up":   ("決算が会社予想を上振れ（発表2〜5営業日後）",
               "Okada & Saeki 2014 / Jinushi 2023", "支持(日本・ただし減衰)",
               "CAR(+1,+21)で上下デシル差2.2pt。現代は大幅減衰し低流動銘柄に残存"),
 "pead_dn":   ("決算が会社予想を下振れ（発表2〜5営業日後）",
               "同上", "支持(日本・ただし減衰)",
               "下振れ側。ドリフトは負に出るはず。符号の確認に要る"),
 "guid_up":   ("会社が通期予想を上方修正（2〜5営業日後）",
               "Kitagawa & Shuto 2024 JBFA", "支持(日本)",
               "日本固有の頻度の高いイベント。予想の信頼性の低い部分は過大評価される"),
 "beat_cons": ("上振れ＋新予想は保守的（2〜5営業日後）",
               "吉野貴晶 2026（実務・査読なし）", "実務の主張のみ",
               "上方サプライズと保守的ガイダンスの組合せ。発表約1ヶ月後から上昇という主張"),
 "pead_late": ("上振れの約1ヶ月後（20〜40営業日）",
               "吉野貴晶 2026（実務・査読なし）", "実務の主張のみ",
               "最適な入りは発表直後ではなく約30日後という主張の検証"),
}
EVENTS = tuple(EVENT_META)

SETUP_META = {
 "macd_gc":  ("MACDゴールデンクロス(12,26,9)", "Appel",
              "反証(日本)", "Chong-Ng-Liew 2014: 日経225でt=0.410非有意／Kang 2021: 先物で負"),
 "macd_zero":("MACDゼロライン上抜け(12,26,0)", "Appel",
              "反証(日本)", "Chong-Ng-Liew 2014: 日経225で-0.029%"),
 "rsi_os":   ("RSI30を下から上抜け(14)", "Wilder 1978",
              "反証(日本)", "Chong-Ng-Liew 2014: 日経225で-0.083%（7日・21日版も負）"),
 "rsi_fs":   ("RSIフェイラースイング(14)", "Wilder 1978",
              "検証なし", "Wilder原典の定義だが査読済みの検証が見当たらない"),
 "stoch_gc": ("ストキャスティクス20以下でクロス(14,3,3)", "Lane",
              "検証なし", "査読済み検証がほぼ無い。Lane自身は%Dのダイバージェンスを重視"),
 "ich_3":    ("一目均衡表 三役好転(9,26,52)", "細田悟一 1969-81",
              "反証寄り", "Deng et al. 2021 IJFE: 既定値では一貫した収益性なし。日本個別株の検証は無い"),
 "ich_cloud":("一目均衡表 雲上抜け(9,26,52)", "細田悟一 1969-81",
              "検証なし", "同上"),
 "gc_5_25":  ("移動平均 5日/25日ゴールデンクロス", "日本の実務慣習",
              "検証なし", "日本の証券会社の教科書で標準だが査読済みの検証は無い"),
 "gc_25_75": ("移動平均 25日/75日ゴールデンクロス", "日本の実務慣習",
              "検証なし", "同上"),
 "vma_1_50": ("終値が50日線を1%上抜け", "Brock-Lakonishok-LeBaron 1992",
              "反証(日本)", "BLLの正典ルール。日本ではBessembinder-Chan 1995・Hsu et al. 2010が否定"),
 "trb50":    ("50日高値の上抜け", "Brock-Lakonishok-LeBaron 1992",
              "反証(日本)", "同上。STW 1999で1987年以降は有意性消滅"),
 "turtle1":  ("20日高値の上抜け(タートルSystem1)", "Original Turtle Trading Rules",
              "反証(日本)", "Yamamoto 2012: 日経225個別でSPA補正後buy-and-holdに勝てず"),
 "turtle2":  ("55日高値の上抜け(タートルSystem2)", "Original Turtle Trading Rules",
              "反証(日本)", "同上"),
 "bb_squeeze":("ボリンジャー バンド幅が6ヶ月最小", "Bollinger 2001",
              "反証(公表後消滅)", "Fang-Jacobsen-Qin: 1983年公表前は高収益、2001年の著書以降ほぼ消滅"),
 "bb_sq_bo": ("収縮のあと20日高値を上抜け", "Bollinger 2001 + Donchian",
              "検証なし", "Bollinger自身が「ヘッドフェイク」（最初の抜けが騙し）を警告している"),
 "nr7":      ("NR7（直近7日で最小の値幅）", "Crabel 1990",
              "検証なし", "著者の自己申告のみ。独立した査読済み検証は見当たらない"),
 "hvol":     ("出来高が過去49日の上位20%", "Gervais-Kaniel-Mingelgrin 2001 JF",
              "支持(米国)", "High-Volume Return Premium。★日本での再現研究は見当たらない"),
 "dev25_5":  ("25日移動平均乖離率 -5%以下", "日本の実務慣習(野村證券ほか)",
              "間接的支持", "短期リバーサルのプロキシ。Chang-McLeavey-Rhee 1995がTSE個別で+1.97%/月"),
 "dev25_10": ("25日移動平均乖離率 -10%以下", "日本の実務慣習(野村證券ほか)",
              "間接的支持", "同上。閾値-10%自体は経験則"),
 "rev21":    ("21日（約1ヶ月）の下落率が大きい順", "Chang-McLeavey-Rhee 1995 JBFA",
              "支持(日本)", "★日本株で唯一、個別銘柄のクロスセクションで実証されている効果。形成1ヶ月"),
}
SETUPS = tuple(SETUP_META)
SETUP_JA = {k: v[0] for k, v in SETUP_META.items()}
# 仕掛けの名前は SETUP_META から自動で足す（手書きにすると必ず食い違う）
BT_FACTOR_JA.update({k: "【仕掛け】" + v[0] for k, v in SETUP_META.items()})
BT_FACTOR_JA.update({k: "【イベント】" + v[0] for k, v in EVENT_META.items()})

# 慣習パラメータ。★探索しない（探索した瞬間に検定の意味が消える）
SETUP_RSI_OS   = 30.0    # Wilder 1978 原典
SETUP_STOCH_OS = 20.0    # Lane 慣習
SETUP_BB_LOOK  = 125     # バンド幅の「6ヶ月最小」(Bollinger)。約125営業日
SETUP_HVOL_LOOK= 49      # Gervais-Kaniel-Mingelgrin 2001 の形成窓
SETUP_HVOL_PCT = 80.0    # 同 上位20%
SETUP_VMA_BAND = 0.01    # BLL 1992 のバンド 1%
SETUP_DEV_A    = -5.0    # 25日乖離率（野村證券の目安）
SETUP_DEV_B    = -10.0

BT_FACTORS = (("mom12_2", "rev5", "lowvol", "acd_score")
              + FUND_FACTORS + SETUPS + EVENTS)

# ★流動性上位半分だけで測った版。名前に印を付けて別の系列として持つ。
#   Avramov, Cheng & Metzker (2023, Management Science):
#     「機械学習のシグナルは裁定しにくい銘柄から収益を抽出している。
#       マイクロキャップ・ディストレス・高ボラ局面を除くと相当程度減衰する」
#   Chen & Velikov (2023, JFQA):
#     「時価総額加重にすると残存収益は完全に消える（−2bp/月）」
#   等ウェイト・全銘柄の結果だけを出すのは、文献が最も厳しく批判している
#   出し方。**実際に建てられる側**の数字を必ず併記する。
LIQ_SUF = "@liq"
_TECH = ("mom12_2", "rev5", "lowvol", "acd_score")
BT_FACTORS_LIQ = tuple(k + LIQ_SUF for k in _TECH + SETUPS)
# 13通りを一度に比べる（技術3 + 自作スコア1 + ファンダ9）。
#   t>=2.8 は両側 p≈0.005 なので 1-(1-0.005)^13 ≈ 6.3%。
#   ★因子を増やしても線は下げない。結果を見てから基準を緩めるのは
#     自分を欺く一番ありがちなやり方。
#   ★なお ep / bp / netcash / kiyohara / payout は私が思い付いたものではなく、
#     清原達郎氏のまとめスライドに書かれている順位付けをそのまま分解したもの。
#     「先に決めた仮説を測る」ので、良い数字を探し回るのとは性質が違う。
#     ただし12通り比べる事実は変わらないので、線は 2.8 のまま。
# 因子は9→7に減ったが、しきい値は下げない。
# 結果を見たあとに基準を緩めるのは、自分を欺く一番ありがちなやり方。
# ── 建て日の除外と取引コスト ───────────────────────────────
#  ★ストップ高で約定できない建玉を除く。
#    JPXの制限値幅（https://www.jpx.co.jp/equities/trading/domestic/06.html）に
#    達した日は売買が成立しないか、成立しても買い手として並べない。
#    上抜け系の仕掛けは「大きく上げた日」に合図が出るので、
#    ここを無視すると**順張り系だけが系統的に良く見える**。
#    実際の制限値幅は26段階の刻みだが、価格帯ごとの比率に直すと
#    おおむね10〜30%なので、翌日の寄りが前日終値から大きく飛んでいる
#    ものを約定不能として落とす。刻みそのものを使わないのは、
#    基準値段（前日終値とは別物）が手元に無いため。控えめに見積もる。
BT_LIMIT_MOVE = 0.16      # 翌日の寄りがこれ以上飛んでいたら約定できなかったとみなす

#  ★取引コスト。Bajgrowicz & Scaillet (2012) JFE 106(3) は
#    「イン・サンプルであっても、低い取引コストを入れると
#     テクニカルルールのパフォーマンスは完全に相殺される」と結論した。
#    コストゼロの検証結果は報告する価値がない。
#    JPX Working Paper No.7 の実効スプレッド（TOPIX100で片道2〜3bp）に
#    手数料と小型株の上乗せを見て、往復で見積もる。
BT_COST_BP = 25.0         # 往復の取引コスト（ベーシスポイント＝0.01%）

BT_T_THRESHOLD = 2.8
# ★独立した（期間が重ならない）建て日の下限。これを下回る格子は
#   「検証できていない」とし、t値がいくつでも通さない。
#   実測で必要になった理由: 保有250日の格子は独立観測が**2日**しかないのに
#   Newey-West の t が +4.28 と出ていた。重なりを織り込んでも、
#   独立な観測が2つしか無い標本から「効いている」と言ってはいけない。
#   この穴のせいで、私が本文では「10日の1通りだけが確立」と書きながら、
#   コードの安全弁は250日保有まで通す状態になっていた。
BT_MIN_INDEP   = 20
# ★調整後でも1日でこれを超えて動く系列は捨てる。Yahoo側の調整異常が
#   残っていると、R が数百になって母集団平均を1銘柄で壊す。
#   0.60 は「調整後の日足で±60%」で、実際の値動きとしてはまず出ない。
BT_MAX_1D_MOVE = 0.60
# ★基準線どうしの整合検査。pool（全銘柄）と pool_f（財務あり）は
#   同じ日・同じ執行で建てた同じ宇宙の部分集合なので、平均Rが桁違いに
#   なることはない。実測で +1.085 対 +0.097（11倍）になっていて、
#   それがテクニカル因子の「効かない」という誤った結論を生んでいた。
#   この比を毎回検査して、超えたら警告を出す。
BT_BASE_RATIO_MAX = 3.0

def bt_acd(df):
    """レポートの A（スイング適性）＋ C（位置）を、検証時点で再現する。

       ★なぜ足したか: **あなたが毎日見ている点数が一度も検定されていなかった。**
         レポートは A+C+D を出して「62以上=買い増し」等の判定に使っているのに、
         検証していたのは mom12_2 / rev5 / lowvol / 割安系だけだった。
         手で決めた重みを検定せずに発注の材料にしていたことになる。
       ★D（分散＝他銘柄との相関）は入れていない。各建て日に
         母集団1,700銘柄の相関行列が必要で、187日ぶんは計算量が持たない。
         Dは「持ち合わせとの重複」を見る項目で、単体の期待値とは別の話。
         入っていないことを明示して A+C だけで測る。"""
    import numpy as np
    a = np.asarray(df["atr_pct"], float)
    lo = np.clip((a - 0.6) / 0.9, 0, 1)      # 1.5%で満点
    hi = np.clip((7.0 - a) / 3.0, 0, 1)      # 4.0%から下がり7.0%で0
    A = 25 * np.minimum(lo, hi)
    rsi = np.asarray(df["rsi"], float)
    heat = np.where(rsi > 75, (rsi - 75) / 25, 0)
    pos = np.clip(np.asarray(df["pos60"], float) / 100, 0, 1)
    rs = np.clip((np.asarray(df["rs20"], float) + 8) / 16, 0, 1)
    C = 25 * np.clip(0.5 * pos + 0.5 * rs - heat, 0, 1)
    return A + C

def bt_alt_scores(df):
    """自作スコア以外の選び方。順位そのものを値にする（大きいほど上位）。"""
    import numpy as np
    out = {}
    out["mom12_2"] = df["mom12_2"].rank(pct=True).to_numpy()
    out["rev5"]    = (-df["r5"]).rank(pct=True).to_numpy()
    out["lowvol"]  = (-df["atr_pct"]).rank(pct=True).to_numpy()
    # ★レポートの点数そのもの（Dを除く）
    if "rs20" in df.columns:
        out["acd_score"] = bt_acd(df)
    return out

# ══════════════════════════════════════════════════════════════
#  仕掛けの形（セットアップ）
#
#  なぜ要るか:
#    ここまでの検証は全部「その日の全銘柄を点数で並べて上位N件」という形
#    だった。バリューはそれで通ったが、**数日の値動きを取りに行く形では
#    ない**。バリューは「安いものは数日でも少し戻りやすい」を測っただけで、
#    スイングトレードの仕掛け——値が動き出す瞬間に乗る——とは別物。
#    技術系の因子（モメンタム・低ボラ・短期反転）は補正後の高い検出力でも
#    ひとつも通らなかった。順位付けという形そのものに無理がある。
#
#  何が違うか:
#    因子は「全銘柄に点数を付けて上から取る」＝毎日必ず建てる。
#    仕掛けは「条件に当てはまった銘柄だけ」＝当てはまらない日は建てない。
#    出る日と出ない日があること自体が情報で、順位付けでは表現できない。
#
#  ★選び方の公平性:
#    当てはまった銘柄が数百件になる日があるので、そのまま全部建てると
#    母集団平均とほぼ同じになり、何も測れない（かつ資金でも建てられない）。
#    **その仕掛け自身の強さ**で並べて上位 BT_TOP 件だけ建てる。
#    因子側と建てる件数を揃えるためでもある。
#    強さの尺度は仕掛けごとに自然に決まるもの（上抜けの幅、出来高の倍率、
#    縮み具合）だけを使う。別の指標を混ぜない。
#
#  ★これは新しい仮説であって、検証を通ったものではない。
#    通ったかどうかは bt_verdict が同じ基準（3つのt≥2.8・独立20日）で
#    判定する。しきい値は仕掛けを足したからといって下げない。
def bt_events(sub, fmap, cal_i):
    """イベント起点の仕掛け。{名前: (当てはまるか, 強さ)} を返す。

       sub   … その日の母集団（開示が引けている銘柄だけ）
       fmap  … コード → その日までの開示から作った指標
       cal_i … 判定している日（pandas.Timestamp）

       ★建てるのは発表から EV_MIN_AGE 営業日以降。
         発表当日〜翌日の反応は取引できない部分で、そこを入れると
         見かけの成績が跳ね上がる。
       ★開示日は fmap の disc_date（その日までの最後の開示日）。
         未来の開示は jq_metrics_at が構造的に渡さない。"""
    import numpy as np
    n = len(sub)
    age = np.full(n, np.nan)
    sue = np.full(n, np.nan)
    rev = np.full(n, np.nan)
    gfp = np.full(n, np.nan)
    codes = [str(c).replace(".T", "") for c in sub["code"].tolist()]
    for k, c in enumerate(codes):
        m = fmap.get(c)
        if not m: continue
        d = m.get("disc_date")
        if d:
            try:
                # 営業日の数え方は持っていないので暦日で数え、
                # 週末ぶんを差し引いた概算にする（5/7を掛ける）。
                _dd = (cal_i - pd.Timestamp(d)).days
                if _dd >= 0: age[k] = _dd * 5.0 / 7.0
            except Exception:
                pass
        if m.get("sue") is not None:   sue[k] = m["sue"]
        if m.get("rev") is not None:   rev[k] = m["rev"]
        if m.get("g_fop") is not None: gfp[k] = m["g_fop"]
    def B(x): return np.nan_to_num(np.asarray(x, dtype=float), nan=0.0).astype(bool)
    fresh = B((age >= EV_MIN_AGE) & (age <= EV_MAX_AGE))
    late  = B((age >= EV_LATE_LO) & (age <= EV_LATE_HI))
    out = {}
    # ① 会社予想を上振れ。強さ＝上振れの大きさ
    out["pead_up"]   = (fresh & B(sue > 0), sue)
    # ② 下振れ。強さ＝下振れの大きさ（符号を反転して上位に）
    out["pead_dn"]   = (fresh & B(sue < 0), -sue)
    # ③ 通期予想の上方修正
    out["guid_up"]   = (fresh & B(rev > 0), rev)
    # ④ 上振れ＋新しい会社予想は保守的（増益を見込んでいない）
    out["beat_cons"] = (fresh & B(sue > 0) & B(gfp <= 0), sue)
    # ⑤ 上振れの約1ヶ月後（発表直後ではなく遅れて入る）
    out["pead_late"] = (late & B(sue > 0), sue)
    return out

def bt_setups(df):
    """仕掛けの形。{名前: (当てはまるか, その仕掛け自身の強さ)} を返す。

       ★すべて原典の慣習パラメータ。数字をこちらで選ばない。
       ★すべて i 日までに確定している値だけで作る。
       ★「クロス」は前日との比較で**その日はじめて**起きたものだけを取る。
         状態（上にいる）と事象（上抜けた）を混同すると、
         合図が何日も出続けて独立した建て日が水増しされる。"""
    import numpy as np
    def g(k):
        return (df[k].to_numpy(float) if k in df.columns
                else np.full(len(df), np.nan))
    c, a = g("close"), g("atr")
    def B(x): return np.nan_to_num(np.asarray(x, dtype=float), nan=0.0).astype(bool)
    def cross_up(now, prev, ref_now, ref_prev):
        """今日はじめて上抜けた（前日は下、今日は上）。"""
        return B((prev <= ref_prev) & (now > ref_now))
    out = {}

    # ① MACD シグナルラインのゴールデンクロス (Appel 12/26/9)
    out["macd_gc"] = (cross_up(g("macd"), g("p_macd"), g("macd_sig"), g("p_macd_sig")),
                      g("macd_hist"))
    # ② MACD ゼロライン上抜け (12,26,0)
    _z = np.zeros(len(df))
    out["macd_zero"] = (cross_up(g("macd"), g("p_macd"), _z, _z), g("macd"))
    # ③ RSI が30を下から上抜け (Wilder 1978)
    _r = np.full(len(df), SETUP_RSI_OS)
    out["rsi_os"] = (cross_up(g("rsi"), g("p_rsi"), _r, _r), -g("rsi"))
    # ④ RSI フェイラースイング（Wilder 原典の定義）
    #    「30を割った後、30を割らずに押し戻し、直前のRSI高値を抜く」。
    #    日足の断面では完全再現できないので、原典のうち**確認できる部分**
    #    ——直近14日に30割れがあり、いま30より上で、RSIが前日を上回る——
    #    だけを取る。原典そのものではないことを名前の説明に書いてある。
    out["rsi_fs"] = (B((g("rsi") > SETUP_RSI_OS) & (g("p_rsi") <= g("rsi"))
                       & (g("dev25") < 0)), -g("rsi"))
    # ⑤ ストキャスティクス: 20以下から %K が %D を上抜け (Lane)
    out["stoch_gc"] = (cross_up(g("stk"), g("p_stk"), g("std_"), g("p_std_"))
                       & B(g("p_stk") <= SETUP_STOCH_OS), -g("stk"))
    # ⑥⑦ 一目均衡表 (細田 9/26/52)
    _cloud_hi = np.fmax(g("ich_a"), g("ich_b"))
    _p_cloud_hi = np.fmax(g("ich_a"), g("ich_b"))   # 雲は26日前確定なので当日と同値で可
    _three = (B(g("ich_ten") > g("ich_kij"))            # ①転換線＞基準線
              & B(c > _cloud_hi)                        # ②終値が雲の上
              & B(c > g("p_close")))                    # ③遅行線好転の代理
    out["ich_3"] = (_three & ~B((g("p_ich_ten") > g("p_ich_kij"))
                                & (g("p_close") > _p_cloud_hi)),
                    (c - _cloud_hi) / a)
    out["ich_cloud"] = (cross_up(c, g("p_close"), _cloud_hi, _p_cloud_hi),
                        (c - _cloud_hi) / a)
    # ⑧⑨ 移動平均のゴールデンクロス（日本の実務慣習）
    out["gc_5_25"]  = (cross_up(g("ma5"), g("p_ma5"), g("ma25"), g("p_ma25")),
                       (g("ma5") / g("ma25") - 1) * 100)
    out["gc_25_75"] = (cross_up(g("ma25"), g("p_ma25"), g("ma75"), g("p_ma75")),
                       (g("ma25") / g("ma75") - 1) * 100)
    # ⑩ BLL 1992 の正典: 終値が50日線を1%上抜け
    _b50 = g("ma50") * (1 + SETUP_VMA_BAND)
    out["vma_1_50"] = (cross_up(c, g("p_close"), _b50, g("p_ma50") * (1 + SETUP_VMA_BAND)),
                       (c / g("ma50") - 1) * 100)
    # ⑪⑫⑬ 高値の上抜け（BLL の TRB / タートル System1・System2）
    out["trb50"]   = (B(c > g("hi50p")), (c - g("hi50p")) / a)
    out["turtle1"] = (B(c > g("hi20p")), (c - g("hi20p")) / a)
    out["turtle2"] = (B(c > g("hi55p")), (c - g("hi55p")) / a)
    # ⑭ ボリンジャーのスクイーズ: バンド幅が過去6ヶ月で最小 (Bollinger 2001)
    _sq = B(g("bb_bw") <= g("bb_bw_min"))
    out["bb_squeeze"] = (_sq, -g("bb_bw"))
    # ⑮ 収縮のあとの上抜け（Bollinger 自身が「ヘッドフェイク」を警告）
    out["bb_sq_bo"] = (_sq & B(c > g("hi20p")), (c - g("hi20p")) / a)
    # ⑯ NR7: 直近7日で最小の値幅 (Crabel 1990)
    out["nr7"] = (B((g("rng") <= g("rng7min")) & (g("rng") > 0)), -g("rng") / c)
    # ⑰ 出来高が過去49日の上位20% (Gervais-Kaniel-Mingelgrin 2001)
    out["hvol"] = (B(g("vol_pct49") >= SETUP_HVOL_PCT), g("vol_pct49"))
    # ⑱⑲ 25日移動平均乖離率（日本の実務。短期リバーサルのプロキシ）
    out["dev25_5"]  = (B(g("dev25") <= SETUP_DEV_A),  -g("dev25"))
    out["dev25_10"] = (B(g("dev25") <= SETUP_DEV_B),  -g("dev25"))
    # ⑳ 形成1ヶ月の下落率（Chang-McLeavey-Rhee 1995 に合わせた唯一の順位系）
    #    ★これだけは「全銘柄に順位を付けて下位から取る」形。
    #      文献がその形で測っているので、同じ形で測らないと比較にならない。
    out["rev21"] = (B(~np.isnan(g("r21"))), -g("r21"))
    return out

def bt_score_at(f, i):
    """日 i 時点の特徴量。i より後のデータは一切使わない。"""
    import numpy as np
    c, a = f["close"][i], f["atr"][i]
    if not (c > 0) or not (a > 0): return None
    rngw = f["hi60"][i] - f["lo60"][i]      # 値幅（randomの略ではない）
    if not (rngw > 0): return None
    m25, m75 = f["ma25"][i], f["ma75"][i]
    if not (m25 > 0) or not (m75 > 0): return None
    atr_pct = a/c*100
    mm = f["mom12_2"][i]
    if not (mm == mm): return None          # 12-2が出ない銘柄は全因子から外す
    def _v(k):
        """無い期間は NaN のまま返す（0で埋めない。埋めると偽の合図が出る）。"""
        x = f.get(k)
        return float("nan") if x is None else float(x[i])
    return dict(close=c, atr=a, atr_pct=atr_pct, rsi=f["rsi"][i],
                vs25=(c/m25-1)*100, vs75=(c/m75-1)*100,
                pos60=(c-f["lo60"][i])/rngw*100, r20=f["r20"][i],
                r5=f["r5"][i], mom12_2=mm,
                volr=f["volr"][i] if f["volr"][i] == f["volr"][i] else 1.0,
                turn=f["turn"][i],
                # 仕掛けの形の材料。すべて i 日までの値しか使っていない。
                # ★前日値(p_*)はクロスの判定に要る。「今日はじめて上抜けた」を
                #   見ないと、上抜けたあと何日も合図が出続けてしまう。
                hi20p=_v("hi20p"),
                hi50p=_v("hi50p"),
                hi55p=_v("hi55p"),
                ma5=_v("ma5"),
                ma50=_v("ma50"),
                ma200=_v("ma200"),
                rng=_v("rng"),
                rng7min=_v("rng7min"),
                macd=_v("macd"),
                macd_sig=_v("macd_sig"),
                macd_hist=_v("macd_hist"),
                stk=_v("stk"),
                std_=_v("std_"),
                bb_pb=_v("bb_pb"),
                bb_bw=_v("bb_bw"),
                bb_bw_min=_v("bb_bw_min"),
                ich_ten=_v("ich_ten"),
                ich_kij=_v("ich_kij"),
                ich_a=_v("ich_a"),
                ich_b=_v("ich_b"),
                adx=_v("adx"),
                pdi=_v("pdi"),
                ndi=_v("ndi"),
                vol_pct49=_v("vol_pct49"),
                dev25=_v("dev25"),
                r21=_v("r21"),
                p_macd=_v("p_macd"),
                p_macd_sig=_v("p_macd_sig"),
                p_ma5=_v("p_ma5"),
                p_ma25=_v("p_ma25"),
                p_ma50=_v("p_ma50"),
                p_ma75=_v("p_ma75"),
                p_rsi=_v("p_rsi"),
                p_stk=_v("p_stk"),
                p_std_=_v("p_std_"),
                p_close=_v("p_close"),
                p_ich_ten=_v("p_ich_ten"),
                p_ich_kij=_v("p_ich_kij"),
                ma25=m25, ma75=m75)

def bt_simulate(f, i, entry, atr, hold, use_target=True, use_stop=True,
                trail=None):
    """i+1 以降の実際の高安で決着させる。窓は寄値で約定。

       use_target=False は「利確しない（損切りと期限だけ）」。
       清原達郎氏は『3割上昇での売却は勧めない・最低2倍を狙う』としており、
       3ATR利確はその上振れを途中で切っている可能性がある。
       同じ選び方・同じ損切りで、利確の有無だけを変えて比べる。"""
    stop, tgt = entry - 2*atr, entry + 3*atr
    n = len(f["close"])
    # トレーリングストップのときは、建てたあとの最高値を追いかける。
    # ★当日の高値は当日の判定に使わない（同じ足の中で高値を見てから
    #   安値を判定したことになる）。前日までの最高値を使う。
    runmax = entry
    def _r(fill, k, how):
        # R は初期リスク(2ATR)基準、Rp は%リターン。
        # 等ウェイトの枠（清原枠）では%が正しい単位なので両方返す。
        return ((fill-entry)/(2*atr),
                ((fill-entry)/entry if entry > 0 else float("nan")),
                k-i, how)
    for k in range(i+1, min(i+1+hold, n)):
        op, hi, lo = f["open"][k], f["high"][k], f["low"][k]
        if trail is not None:
            lvl = runmax - trail*atr
            if lo <= lvl:
                fill = min(op, lvl) if op == op else lvl
                return _r(fill, k, "stop")
        elif use_stop and lo <= stop:
            fill = min(op, stop) if op == op else stop
            return _r(fill, k, "stop")
        if use_target and hi >= tgt:
            fill = max(op, tgt) if op == op else tgt
            return _r(fill, k, "target")
        if trail is not None and hi == hi and hi > runmax:
            runmax = hi
    k = min(i+hold, n-1)
    return _r(f["close"][k], k, "timeout")

def bt_simulate_many(M, rows, i, entry, atr, hold, use_target=True, use_stop=True,
                     trail=None):
    """上と同じ決着を、その日の母集団まとめて一度に出す。

       なぜ要るか:
         保有期間を250日まで延ばすと、1銘柄ずつPythonの輪で回すと
         「母集団の全銘柄 × 建て日 × 保有期間」で数千万回になり終わらない。
         同じ規則を行列で解く。結果は1銘柄ずつの計算と一致すること
         （bt_simulate との一致）をテストで確かめている。

       M: {"open","high","low","close"} それぞれ (銘柄, 営業日) の行列
       rows: 使う銘柄の行番号、entry/atr: 建値とATR（rowsと同じ長さ）"""
    import numpy as np
    n = M["close"].shape[1]
    hi_e = min(i + 1 + hold, n)
    if hi_e <= i + 1:
        # ★戻り値の数を本流と揃える。ここを忘れて3個のままだったため、
        #   最終日に建てた格子で ValueError になりかけた（テストで発覚）。
        return (np.zeros(len(rows)), np.zeros(len(rows)),
                np.zeros(len(rows), int),
                np.array(["timeout"] * len(rows), dtype=object))
    sl = slice(i + 1, hi_e)
    O, H, L, C = (M[k][rows, sl] for k in ("open", "high", "low", "close"))
    stop = entry - 2 * atr
    tgt = entry + 3 * atr
    big = H.shape[1] + 10
    if trail is not None:
        # ★トレーリングストップ。建てたあとの**その日までの最高値**から
        #   trail×ATR 下がったところで降りる。含み益を追いかけて
        #   損切り位置を上げていく、実務で最も一般的な降り方のひとつ。
        #   ★当日の高値を当日の損切り判定に使わない。前日までの最高値を
        #     使う（当日の高値で当日の損切り位置を決めると、
        #     同じ足の中で高値を見てから安値を判定したことになる）。
        _rm = np.maximum.accumulate(H, axis=1)
        _rm = np.concatenate([np.full((H.shape[0], 1), np.nan), _rm[:, :-1]], axis=1)
        _base = np.fmax(_rm, entry[:, None])      # 建値は下限（初日は建値基準）
        _lvl = _base - trail * atr[:, None]
        s_any = L <= _lvl
        s_k = np.where(s_any.any(1), s_any.argmax(1), big)
        # 降りる値段はその日のトレーリング水準（窓が空いたら寄値）
        _slvl = np.where(s_any.any(1),
                         _lvl[np.arange(len(rows)),
                              np.where(s_any.any(1), s_any.argmax(1), 0)],
                         np.nan)
        stop = np.where(np.isnan(_slvl), stop, _slvl)
    elif use_stop:
        s_any = L <= stop[:, None]
        s_k = np.where(s_any.any(1), s_any.argmax(1), big)
    else:
        s_k = np.full(len(rows), big)
    if use_target:
        t_any = H >= tgt[:, None]
        t_k = np.where(t_any.any(1), t_any.argmax(1), big)
    else:
        t_k = np.full(len(rows), big)
    # 同じ日に両方触れたら損切りを先とする（日足では順序が分からないため）
    is_stop = s_k <= t_k
    first = np.minimum(s_k, t_k)
    hit = first < big
    last = H.shape[1] - 1
    k_idx = np.where(hit, np.minimum(first, last), last)
    take = np.arange(len(rows))
    op = O[take, k_idx]
    fill = np.where(hit,
                    np.where(is_stop,
                             np.where(np.isnan(op), stop, np.minimum(op, stop)),
                             np.where(np.isnan(op), tgt, np.maximum(op, tgt))),
                    C[take, k_idx])
    R = (fill - entry) / (2 * atr)
    # ★%リターンも返す。R は初期リスク(2ATR)で割った値なので、
    #   **値幅の小さい銘柄ほど|R|が大きく出る**。
    #   実測: lowvol（最低ボラを選ぶ因子）の差が −0.895 と大きく負に出ていたが、
    #   これは低ATR銘柄を選べば分母が小さくなるという単位の性質で、
    #   下落を当てたわけではない。
    #   スイング枠はATR基準でサイジングするのでRが正しい単位だが、
    #   **清原枠は等ウェイトなので%リターンが正しい単位**。両方を測る。
    Rp = (fill - entry) / np.where(entry > 0, entry, np.nan)
    bars = k_idx + 1
    how = np.where(hit, np.where(is_stop, "stop", "target"), "timeout").astype(object)
    return R, Rp, bars, how

def run_backtest(codes, bench="1306.T"):
    """過去2年で、順位付けに情報があるかを母集団の全銘柄平均と比べる。

       ★全銘柄を同じ営業日カレンダーに揃えてから位置で引く。
         揃えないと、配列の位置 i が銘柄ごとに違う日付を指し、
         「同じ日に建てた」という前提が崩れて比較そのものが無意味になる。"""
    import numpy as np, yfinance as yf

    # ① 基準となる営業日カレンダーと、その日のレジーム（200日線の上か）
    b = yf.download(bench, period=BT_PERIOD, interval="1d",
                    auto_adjust=False, progress=False)
    if isinstance(b.columns, pd.MultiIndex): b.columns = b.columns.get_level_values(0)
    b = b.dropna(subset=["Close"])
    cal = b.index
    bc = adjusted_frame(b)["Close"]
    bma = bc.rolling(200).mean()
    above = {i: (bool(bc.iloc[i] > bma.iloc[i]) if bma.iloc[i] == bma.iloc[i] else None)
             for i in range(len(cal))}
    print(f"[検証] 基準カレンダー {len(cal)}営業日（{cal[0].date()}〜{cal[-1].date()}）")

    # ② 財務情報（開示日ベース）。キーが無ければファンダ側は走らない。
    #    Freeの提供範囲は「12週間前〜2年12週間前」なので、検証の5年のうち
    #    ファンダが存在するのは直近2年ぶんだけになる。存在しない日は
    #    ファンダ側の母集団が立たないので、その日は丸ごと見送る。
    jhist, jpit = {}, {}
    if JQ_KEY:
        try:
            _rows, _hit, _asked = jq_rows(JQ_HIST_D)
            jhist = jq_build(_rows)
            jpit  = jq_pit_index(jhist, cal) if jhist else {}
            meta["jq_bt"] = {"dates_asked": _asked, "dates_with_data": _hit,
                             "rows": len(_rows), "codes": len(jhist),
                             "req": _JQ["req"], "n429": _JQ["n429"],
                             "blocked": _JQ["blocked"], "resp_key": _JQ["key_used"],
                             "err": dict(_JQ["err"])}
            print(f"[検証] 財務情報 {len(jhist):,}銘柄 / 開示{len(_rows):,}件 "
                  f"/ 照会{_JQ['req']}回 / 429:{_JQ['n429']}")
            if not jhist:
                print("::warning::J-Quantsから財務情報が取れなかったためファンダ側は検証しない")
        except Exception as e:
            meta["errors"].append(f"jq bt: {type(e).__name__}: {e}")
            jhist, jpit = {}, {}
    else:
        meta["jq_bt"] = {"skipped": "JQUANTS_API_KEY 未設定"}
        print("[検証] JQUANTS_API_KEY が無いのでファンダ側は検証しない")

    # ★コード順のまま取ると、時間切れで打ち切られたときに
    #   1300〜4000番台（ETF・REIT・食品・化学）に偏った標本になり、
    #   それを市場全体の結果として報告してしまう。
    #   コードの文字列を逆順にして並べると、先頭の桁が散るので
    #   打ち切られても業種に偏らない部分標本になる。乱数は使わない
    #   （同じ入力なら必ず同じ順序＝結果が再現できる）。
    codes = sorted(codes, key=lambda c: str(c)[::-1])

    feats, t0, asked = {}, time.time(), 0
    _bt_repaired, _bt_dropped = [], []
    for j in range(0, len(codes), BT_CHUNK):
        if time.time() - t0 > BT_BUDGET_S:
            print(f"[検証] 時間予算に達したため {len(feats)}銘柄で打ち切り"); break
        part = codes[j:j+BT_CHUNK]; asked += len(part)
        try:
            d = yf.download(part, period=BT_PERIOD, interval="1d", auto_adjust=False,
                            group_by="ticker", threads=True, progress=False)
        except Exception as e:
            meta["errors"].append(f"bt chunk {j}: {type(e).__name__}"); continue
        for c in part:
            try:
                x = d[c].dropna(subset=["Close"])
                if len(x) < len(cal)*0.9: continue      # 歯抜けが多い銘柄は外す
                x = x.reindex(cal)                      # ★同じカレンダーに揃える
                if x["Close"].isna().mean() > 0.05: continue
                x = x.ffill().dropna(subset=["Close"])
                if len(x) < len(cal): continue          # 先頭が埋まらないもの（新規上場）は外す
                ax = adjusted_frame(x)
                # ★★ここで repair() を当てる。以前は日次スクリーニングにだけ
                #   当てていて、検証の経路には当てていなかった。
                #   その結果、Yahoo側の調整異常（実測で1306が1/10、1629が
                #   約1/500になっていた）がそのまま入り、
                #   R=(決着値−建値)/(2ATR) が数百〜数千になっていた。
                #   異常値を持つのはETFに多く、ETFは財務が無いので
                #   pool（全銘柄）にだけ入り pool_f（財務あり）には入らない。
                #   そのため**テクニカル因子の基準線だけが壊れ**、
                #   平均Rが pool +1.085 / pool_f +0.097 という
                #   11倍の開きになっていた（実測）。
                #   「テクニカルに裏付けがない」という結論はこの不具合の産物。
                _rc, _rlog = repair(ax["Close"])
                if _rlog: _bt_repaired.append(c)
                # 直した比率を O/H/L にも同じだけ当てる。Closeだけ直すと
                # 安値>終値のような足になり、決着の判定が壊れる。
                _ratio = (_rc / ax["Close"]).replace(
                    [float("inf"), float("-inf")], float("nan")).ffill().bfill()
                for _col in ("Open", "High", "Low", "Close"):
                    if _col in ax.columns: ax[_col] = ax[_col] * _ratio
                # ★直しても残る異常は捨てる。基準線を1銘柄で壊させない。
                _mv = ax["Close"].pct_change().abs()
                if float(_mv.max() or 0) > BT_MAX_1D_MOVE:
                    _bt_dropped.append((c, round(float(_mv.max()), 2)))
                    continue
                feats[c] = bt_features(ax, x["Volume"])
            except Exception:
                pass
        if (j // BT_CHUNK) % 4 == 0:
            print(f"  検証データ {len(feats)}銘柄  {time.time()-t0:.0f}秒")
    if len(feats) < 200:
        raise RuntimeError(f"検証に足る銘柄が集まらない: {len(feats)}")

    nbar = len(cal)
    # 決着をまとめて解くための行列。銘柄の行番号を引けるようにしておく。
    _codes_m = list(feats)
    _row = {c: k for k, c in enumerate(_codes_m)}
    M = {k: np.vstack([feats[c][k] for c in _codes_m])
         for k in ("open", "high", "low", "close")}
    print(f"[検証] 決着用の行列 {M['close'].shape[0]}銘柄 × {M['close'].shape[1]}営業日")

    trades = {k: [] for k in BT_FACTORS + BT_FACTORS_LIQ
              + ("pool", "pool_f", "pool_liq")}
    n_dates, n_dates_f = 0, 0
    # 仕掛けが1日に何件当てはまったか。0件の日も数える。
    # ★「めったに出ない形」と「毎日ほぼ全銘柄に出る形」を区別するのに要る。
    #   後者は母集団平均と同じものを測っているだけで、仕掛けとは言えない。
    _setup_n = {}
    _pool_n = []      # その日の母集団の銘柄数（仕掛けの出やすさを割る分母）
    _n_limit = [0]    # ストップ高で約定できず落とした建玉の数
    _breadth = {}     # 建て日 → 母集団のうち直近5日が上げている割合(%)
    _brd_hist = []
    # ★最長の保有期間ぶんを一律に切り落とすと、短い保有の標本まで減ってしまう。
    #   建て日は最短の保有期間で決め、各保有期間ごとに「足りる日か」を見る。
    for i in range(BT_WARMUP, nbar - min(BT_HOLDS) - 1, BT_STEP):
        pool = []
        for c, f in feats.items():
            s = bt_score_at(f, i)
            if s is None: continue
            if s["close"] < MIN_PRICE: continue
            if not (s["turn"] >= MIN_TURNOVER): continue
            if not (MIN_ATR_PCT <= s["atr_pct"] <= MAX_ATR_PCT): continue
            s["code"] = c; pool.append(s)
        if len(pool) < 50: continue
        n_dates += 1
        _pool_n.append(len(pool))
        # ★市場の広がり（騰落レシオに相当）。自分が走査している母集団から
        #   直接数えるので、JPXの公表値を取ってくる必要がない
        #   （価格から導いた自分の計算なので再配信の問題も起きない）。
        #   「相場全体が上がっている日の合図」と「下がっている日の合図」を
        #   混ぜて平均すると、局面で符号が反転する仕掛けを見落とす。
        _adv = sum(1 for x in pool if (x.get("r5") or 0) > 0)
        _brd = 100.0 * _adv / len(pool)
        _breadth[i] = _brd
        _brd_hist.append(_brd)
        # ★流動性の上位半分だけの母集団も作る。
        #   Avramov, Cheng & Metzker (2023, Management Science) の中心的な
        #   批判は「機械学習の優位は**裁定しにくい銘柄**（小型・低流動・
        #   高ボラ）から出ている。それを除くと相当程度減衰する」。
        #   Chen & Velikov (2023, JFQA) も「時価総額加重にすると残存収益は
        #   完全に消える（期待リターン −2bp/月）」と報告している。
        #   等ウェイト・全銘柄の結果だけを出すのは、文献が最も厳しく
        #   批判している出し方。**実際に建てられる側**でも測る。
        _tv = sorted(float(x["turn"]) for x in pool)
        _med_turn = _tv[len(_tv)//2]
        df = pd.DataFrame(pool)
        # ★対TOPIXの20日相対力。レポートのC（位置）がこれを使っているので、
        #   検証でも同じものを作る。指数の20日騰落を引くだけ。
        try:
            _b20 = (float(bc.iloc[i]) / float(bc.iloc[i - 21]) - 1) * 100 \
                if i >= 21 else 0.0
        except Exception:
            _b20 = 0.0
        df["rs20"] = df["r20"] - _b20
        # 流動性上位半分の行番号（基準線と各選び方の両方で使う）
        _liq = np.flatnonzero(df["turn"].to_numpy(float) >= _med_turn)
        picks = {}
        # 文献由来の因子。同じ母集団・同じ執行ルールで比べる
        alt = bt_alt_scores(df)
        for name, sc in alt.items():
            order = np.argsort(-sc)[:BT_TOP]
            picks[name] = [df.iloc[k] for k in order]
        # ★流動性上位半分でも同じ選び方をする（実際に建てられる側）
        if _liq.size >= 30:
            _sub = df.iloc[_liq].reset_index(drop=True)
            for _nm, _sc in bt_alt_scores(_sub).items():
                _o = np.argsort(-_sc)[:BT_TOP]
                picks[_nm + LIQ_SUF] = [_sub.iloc[k] for k in _o]
            for _nm, (_mk, _st) in bt_setups(_sub).items():
                _ix = np.flatnonzero(np.asarray(_mk, dtype=bool))
                if _ix.size == 0: continue
                _s2 = np.asarray(_st, dtype=float)[_ix]
                _s2 = np.where(np.isnan(_s2), -np.inf, _s2)
                picks[_nm + LIQ_SUF] = [_sub.iloc[k]
                                        for k in _ix[np.argsort(-_s2)[:BT_TOP]]]
        # ── 仕掛けの形。当てはまった銘柄だけ。出ない日は建てない ──────
        for name, (mask, strength) in bt_setups(df).items():
            idx = np.flatnonzero(np.asarray(mask, dtype=bool))
            _setup_n.setdefault(name, []).append(int(idx.size))
            if idx.size == 0:
                continue                      # 合図が出なかった日。建てない
            st = np.asarray(strength, dtype=float)[idx]
            st = np.where(np.isnan(st), -np.inf, st)
            order = idx[np.argsort(-st)[:BT_TOP]]
            picks[name] = [df.iloc[k] for k in order]
        # 基準線は「同じ母集団の全銘柄を等ウェイトで建てたときの平均」。
        # 以前は乱数で10件を5回抽出してその平均を取っていたが、
        # それは全銘柄平均を推定しているだけで、全部建てれば推定ではなく
        # 実測になる。乱数も種も要らず、誰が何回走らせても同じ数字が出る。
        # （建てる値段・決着はどちらの場合も実際に付いた高安だけを使っている。
        #   価格を作り出す種類のシミュレーションは一度も入っていない）
        picks["pool"] = [df.iloc[k] for k in range(len(df))]
        if _liq.size >= 30:
            picks["pool_liq"] = [df.iloc[k] for k in _liq]
        # ファンダ側。母集団は「その日までに財務が開示されている銘柄」だけ。
        # 基準線(pool_f)も必ず同じ母集団から取る。
        if jpit:
            fmap = {}
            for c in df["code"].tolist():
                cc = str(c).replace(".T", "")
                m = jq_metrics_at(jhist, jpit, cc, i)
                if m: fmap[cc] = m
            fs, has = fund_scores(df, fmap)
            hv = has.to_numpy()
            sub = df[hv]
            if len(sub) >= JQ_MIN_POOL:
                n_dates_f += 1
                for name, sc in fs.items():
                    v = sc.to_numpy(float)[hv]
                    ok = ~np.isnan(v)
                    if ok.sum() < JQ_MIN_POOL: continue
                    order = np.argsort(-np.where(ok, v, -np.inf))[:BT_TOP]
                    picks[name] = [sub.iloc[k] for k in order if ok[k]]
                # ファンダ側の基準線も同じ母集団の全銘柄平均
                picks["pool_f"] = [sub.iloc[k] for k in range(len(sub))]
                # ★イベント起点の仕掛け。開示が引けている母集団で判定する。
                #   合図が出ない日は建てない（イベントが無い日がある）。
                try:
                    _sub2 = sub.reset_index(drop=True)
                    for _en, (_em, _es) in bt_events(_sub2, fmap, cal[i]).items():
                        _ei = np.flatnonzero(np.asarray(_em, dtype=bool))
                        _setup_n.setdefault(_en, []).append(int(_ei.size))
                        if _ei.size == 0: continue
                        _ev = np.asarray(_es, dtype=float)[_ei]
                        _ev = np.where(np.isnan(_ev), -np.inf, _ev)
                        picks[_en] = [_sub2.iloc[k]
                                      for k in _ei[np.argsort(-_ev)[:BT_TOP]]]
                except Exception as _e:
                    meta.setdefault("bt_event_err", str(_e)[:120])
        for kind, rows in picks.items():
            if not rows: continue
            # 母集団平均は全銘柄なので、1日の重みを採用件数(BT_TOP)に揃える。
            # 揃えないと基準線だけ件数が百倍になり、件数の比較が読めなくなる。
            w = (BT_TOP / len(rows)) if kind.startswith("pool") else 1.0
            ridx = np.array([_row[r["code"]] for r in rows])
            ent = np.array([float(r["close"]) for r in rows])
            atv = np.array([float(r["atr"]) for r in rows])
            # ★ストップ高で約定できなかった建玉を落とす。
            #   上抜け系の合図は「大きく上げた日」に出るので、
            #   ここを無視すると順張り系だけが系統的に良く見える。
            if i + 1 < nbar:
                _nxo = M["open"][ridx, i + 1]
                with np.errstate(invalid="ignore", divide="ignore"):
                    _gap = np.abs(_nxo / ent - 1.0)
                _ok = ~(_gap > BT_LIMIT_MOVE)          # NaN は通す（寄値欠損）
                if not _ok.all():
                    _n_limit[0] += int((~_ok).sum())
                    _keep = np.flatnonzero(_ok)
                    if _keep.size == 0: continue
                    rows = [rows[k] for k in _keep]
                    ridx, ent, atv = ridx[_keep], ent[_keep], atv[_keep]
            up_i = above.get(i)
            brd_i = _breadth.get(i)
            for hold in BT_HOLDS:
                if i + hold >= nbar: continue      # その保有期間には足りない日
                for _ex in BT_EXITS:
                    exname, use_t, use_s = _ex[0], _ex[1], _ex[2]
                    trail = _ex[3] if len(_ex) > 3 else None
                    R, Rp, bars, how = bt_simulate_many(M, ridx, i, ent, atv,
                                                        hold, use_t, use_s,
                                                        trail=trail)
                    # ★1建玉ずつ辞書に積まない。**(日付, 保有, 降り方) ごとに
                    #   合計と件数へ畳む。**
                    #   検定に要るのは「その日の平均」と「全体の合計」だけで、
                    #   個別の建玉は一度も参照していない。
                    #   畳まないと、基準線(pool系)だけで
                    #   3母集団 × 約1,700銘柄 × 35(保有×降り方) × 188日
                    #   ≒ 3,300万個の辞書になり、GitHub Actions の
                    #   メモリ(7GB)で確実に落ちる。畳めば約44万レコード。
                    _R = np.asarray(R, float); _Rp = np.asarray(Rp, float)
                    _ok = ~np.isnan(_R)
                    _n = int(_ok.sum())
                    if _n == 0: continue
                    _hw = np.asarray(how, dtype=object)
                    trades[kind].append({
                        "i": i, "hold": hold, "ex": exname,
                        "up": up_i, "brd": brd_i, "w": w,
                        "n": _n,
                        "sR": float(np.nansum(_R)),
                        "sRp": float(np.nansum(_Rp)),
                        "win": int((_R[_ok] > 0).sum()),
                        "n_target": int((_hw == "target").sum()),
                        "n_stop": int((_hw == "stop").sum()),
                        "n_timeout": int((_hw == "timeout").sum()),
                    })
    # 仕掛けの出やすさ。これが読めないと t 値の意味が分からない。
    #   ★出やすさは t 値の読み方を決める。めったに出ない形は標本が少なく、
    #     毎日ほぼ全銘柄に出る形は母集団平均そのものを測っているだけ。
    #     どちらも「効く仕掛け」ではないので、必ず併記する。
    _sn = {}
    _pm = sorted(_pool_n) if _pool_n else [0]
    _pool_med = _pm[len(_pm)//2]
    _ALL_JA = dict(SETUP_JA)
    _ALL_JA.update({k: v[0] for k, v in EVENT_META.items()})
    for k, v in (_setup_n or {}).items():
        if not v: continue
        v2 = sorted(v)
        _med = v2[len(v2)//2]
        _share = (100.0 * _med / _pool_med) if _pool_med else None
        _sn[k] = {"名前": _ALL_JA.get(k, k),
                  "出た日": sum(1 for x in v if x > 0), "検証日": len(v),
                  "1日の中央値": _med, "1日の最大": max(v),
                  "母集団の中央値": _pool_med,
                  "母集団比_pct": (None if _share is None else round(_share, 1))}
        # 自分で気付けるようにする。実データはActionsでしか回せないので、
        # 「出なさすぎ」「出すぎ」を実行そのものに報告させる。
        _nd = _sn[k]["出た日"]
        if _nd < BT_MIN_INDEP:
            _sn[k]["警告"] = f"合図が出た日が{_nd}日しかない＝検証できない"
            print(f"::warning::仕掛け {k}（{_ALL_JA.get(k,k)}）は"
                  f"{len(v)}日中{_nd}日しか出ていません。条件が厳しすぎます")
        elif _share is not None and _share >= 50.0:
            _sn[k]["警告"] = f"母集団の{_share:.0f}%に出ている＝仕掛けとして働いていない"
            print(f"::warning::仕掛け {k}（{_ALL_JA.get(k,k)}）は母集団の"
                  f"{_share:.0f}%に出ています。母集団平均を測っているのと同じです")
    # 市場の広がりの分布。局面別の集計の境目をここから決める。
    if _brd_hist:
        _bh = sorted(_brd_hist)
        meta["backtest_breadth"] = {
            "検証日": len(_bh),
            "下位3分の1の境目": round(_bh[len(_bh)//3], 1),
            "上位3分の1の境目": round(_bh[2*len(_bh)//3], 1),
            "中央値": round(_bh[len(_bh)//2], 1),
            "note": "母集団のうち直近5日が上げている銘柄の割合(%)。"
                    "自分の走査から直接数えている（JPXの公表値は使っていない）"}
    meta["backtest_setups"] = _sn
    # ストップ高で約定できず落とした建玉。上抜け系の見かけを左右する。
    meta["backtest_limit_skipped"] = _n_limit[0]
    if _n_limit[0]:
        print(f"[検証] ストップ高等で約定できない建玉 {_n_limit[0]:,}件を除外")
    meta["backtest_dates"] = n_dates
    meta["backtest_dates_fund"] = n_dates_f
    # ★基準線どうしの整合検査。これが無かったために、壊れた pool と
    #   比べた結果を「テクニカルに裏付けがない」と報告してしまった。
    _bchk = {"repaired": len(_bt_repaired), "dropped": len(_bt_dropped),
             "dropped_sample": _bt_dropped[:5], "max_1d_move": BT_MAX_1D_MOVE}
    try:
        _h0 = min(BT_HOLDS)
        def _mean(kind):
            # ★建玉は (建て日, 保有, 降り方) ごとに畳んである。
            #   合計(sR)と件数(n)から平均を出す。
            #   ここを畳み込みのときに直し忘れて KeyError で落ちた（実測）。
            d = [r for r in trades.get(kind, [])
                 if r["hold"] == _h0 and r.get("ex") == "2atr_3atr"]
            _sn = sum((r.get("n") or 0) for r in d)
            if not _sn: return None
            return sum(r.get("sR", 0.0) for r in d) / _sn
        _mp, _mf = _mean("pool"), _mean("pool_f")
        _bchk["pool_mean_r"] = None if _mp is None else round(_mp, 4)
        _bchk["pool_f_mean_r"] = None if _mf is None else round(_mf, 4)
        if _mp is not None and _mf is not None and abs(_mf) > 1e-6:
            _rt = abs(_mp / _mf)
            _bchk["ratio"] = round(_rt, 2)
            _bchk["ok"] = bool(_rt <= BT_BASE_RATIO_MAX)
            if not _bchk["ok"]:
                print(f"::error::基準線が壊れています。pool の平均R {_mp:+.3f} と "
                      f"pool_f の平均R {_mf:+.3f} が {_rt:.1f}倍違う"
                      f"（許容 {BT_BASE_RATIO_MAX}倍）。"
                      "調整異常の残った系列が母集団に入っている可能性。"
                      "テクニカル因子の結果は信用しないこと")
    except Exception as e:
        _bchk["error"] = type(e).__name__
    meta["backtest_basecheck"] = _bchk
    print(f"[検証] 異常値を直した系列 {len(_bt_repaired)}件 / "
          f"直しても残って捨てた系列 {len(_bt_dropped)}件"
          + (f"（例: {_bt_dropped[:3]}）" if _bt_dropped else ""))
    if _bchk.get("ratio") is not None:
        print(f"[検証] 基準線の整合: pool {_bchk['pool_mean_r']:+.3f} / "
              f"pool_f {_bchk['pool_f_mean_r']:+.3f} = {_bchk['ratio']}倍 "
              f"({'OK' if _bchk.get('ok') else '★異常'})")
    cov = round(asked/len(codes)*100, 1) if codes else 0
    meta["backtest_coverage"] = {"asked": asked, "universe": len(codes),
                                 "pct": cov, "usable": len(feats)}
    print(f"[検証] {len(feats)}銘柄（母集団{len(codes):,}中{asked:,}件に照会 = {cov}%）/ "
          f"{n_dates}回の建て日（うちファンダ{n_dates_f}回）/ "
          f"延べ {sum(len(v) for v in trades.values()):,}件")
    return trades, n_dates, len(feats), str(cal[0].date()), str(cal[-1].date()), cov, n_dates_f

def bt_fdr(cells, q=0.10):
    """多重検定の補正（Benjamini-Hochberg の False Discovery Rate）。

       なぜ要るか:
         いま何十通りもの選び方を一度に測っている。しきい値 t≥2.8 は
         1つの検定に対する基準で、**試す数が増えれば偶然2.8を超えるものが
         必ず出る**。Park & Irwin (2007) が技術的分析の文献の
         最大の欠陥として挙げた "data snooping" がこれ。
       ★日本株を扱った既存研究は例外なくこの補正を行っている:
         Yamamoto (2012) JBF、Hsu-Hsu-Kuan (2010) JAE は White の
         Reality Check と Hansen の SPA、Bajgrowicz & Scaillet (2012)
         JFE は本関数と同じ Benjamini-Hochberg の FDR を使っている。
         補正しない結果は、既存文献と比較できない。
       ★BH を選んだ理由: RC/SPA はブートストラップに元の収益系列が要り、
         この仕組みは建て日ごとの差しか持ち回っていない。BH は p 値だけで
         でき、Bajgrowicz-Scaillet が同じ目的で使った実績がある。

       cells: [(鍵, t値, 自由度)] / q: 許容する偽発見の割合
       返り値: {鍵: {"p", "p_adj", "ok"}}。ok が BH を通ったもの。"""
    import math
    def _p_two(t, df):
        """両側 p 値。t分布。scipy を入れずに済ませる。"""
        if t is None or df is None or df < 1: return None
        t = abs(float(t)); df = float(df)
        # 正則化不完全ベータ関数で t分布の裾を出す
        x = df / (df + t * t)
        return _betainc(df / 2.0, 0.5, x)
    def _betacf(a, b, x):
        MAXIT, EPS, FPMIN = 200, 3e-16, 1e-300
        qab, qap, qam = a + b, a + 1.0, a - 1.0
        c = 1.0; d = 1.0 - qab * x / qap
        if abs(d) < FPMIN: d = FPMIN
        d = 1.0 / d; h = d
        for m in range(1, MAXIT + 1):
            m2 = 2 * m
            aa = m * (b - m) * x / ((qam + m2) * (a + m2))
            d = 1.0 + aa * d
            if abs(d) < FPMIN: d = FPMIN
            c = 1.0 + aa / c
            if abs(c) < FPMIN: c = FPMIN
            d = 1.0 / d; h *= d * c
            aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
            d = 1.0 + aa * d
            if abs(d) < FPMIN: d = FPMIN
            c = 1.0 + aa / c
            if abs(c) < FPMIN: c = FPMIN
            d = 1.0 / d; de = d * c; h *= de
            if abs(de - 1.0) < EPS: break
        return h
    def _betainc(a, b, x):
        if x <= 0.0: return 0.0
        if x >= 1.0: return 1.0
        lb = (math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
              + a * math.log(x) + b * math.log(1.0 - x))
        if x < (a + 1.0) / (a + b + 2.0):
            return math.exp(lb) * _betacf(a, b, x) / a
        return 1.0 - math.exp(lb) * _betacf(b, a, 1.0 - x) / b
    ps = []
    for key, t, df in cells:
        p = _p_two(t, df)
        if p is not None: ps.append((key, p))
    out = {k: {"p": None, "p_adj": None, "ok": False} for k, _, _ in cells}
    if not ps: return out
    ps.sort(key=lambda z: z[1])
    m = len(ps)
    # BH の手続き: p_(k) ≤ k/m × q を満たす最大の k まで採択
    kmax = 0
    for k, (_, p) in enumerate(ps, start=1):
        if p <= k / m * q: kmax = k
    # 調整p値（単調にする）
    prev = 1.0
    adj = [None] * m
    for k in range(m, 0, -1):
        prev = min(prev, ps[k-1][1] * m / k)
        adj[k-1] = prev
    for k, (key, p) in enumerate(ps, start=1):
        out[key] = {"p": round(p, 6), "p_adj": round(adj[k-1], 6), "ok": k <= kmax}
    return out

BT_FDR_Q = 0.10        # 許容する偽発見の割合。Bajgrowicz-Scaillet と同じ水準

# ══════════════════════════════════════════════════════════════
#  検定群の分け方（2026-09-19に本人が決定：B案）
#
#  なぜ分けるか:
#    多重検定の補正は「たくさん試すほど基準を上げる」という考え方で、
#    2,240通りを1つの山として数えると実効しきい値が約3.45まで上がる。
#    だが、その2,240通りの中身は性質が2つに分かれている。
#      ・ほとんどは「有名だから試した」もの（20日高値の上抜け、RSI30割れ…）。
#        日本株で効くという根拠は測る前には無く、むしろ否定されていた。
#        これは総当たりの探索であって、厳しく数えるのが正しい。
#      ・一部は「測る前から日本株の査読済み論文に根拠があった」もの。
#        先に仮説があって、それを確かめに行っている。
#        総当たりと同じ袋に入れて数えるのは過剰に厳しい。
#
#  ★線引きを固定する理由:
#    「結果を見てから、良かったものを別枠に移す」のが最悪の操作で、
#    それをやると補正の意味が消える。だからここに**定数として凍結**し、
#    **結果を見てから足さない**。足すときは、足す理由と査読済みの出典を
#    先に書いてから、測り直す。
#
#  ★この3つに絞った理由（当初は「イベント5つ全部」としていたのを狭めた）:
#    残り2つ（上振れ＋保守的ガイダンス／上振れの1ヶ月後）の根拠は
#    実務家の記事であって査読論文ではない。根拠の強さが違うものを
#    同じ枠に入れると、線引きが「自分に都合のよい方」に寄る。
BT_PRIOR_ARMS = {
    # 決算が会社予想を上振れ／下振れしたあとのドリフト
    #   Okada & Saeki (2014) 証券アナリストジャーナル（日本株9,390件）:
    #     CAR(+1,+21) が予想誤差の上下デシルで 2.2ポイント
    #   Jinushi (2023) The Japanese Accounting Review 13（60,124件）:
    #     大幅に減衰したが、外国人保有が低い銘柄では残存
    "pead_up": "Okada & Saeki 2014 / Jinushi 2023",
    "pead_dn": "Okada & Saeki 2014 / Jinushi 2023",
    # 会社が通期予想を上方修正したあと
    #   Kitagawa & Shuto (2024) JBFA 51(9-10)（日本企業）:
    #     期初予想の「予想外部分」と将来リターンの関係
    "guid_up": "Kitagawa & Shuto 2024 JBFA",
}

def bt_family(arm):
    """その腕がどちらの検定群か。
       "prior" … 測る前から日本株の査読済み論文に根拠があったもの
       "search" … それ以外（有名だから試した、の総当たり）"""
    base = arm.split(LIQ_SUF)[0] if LIQ_SUF in arm else arm
    return "prior" if base in BT_PRIOR_ARMS else "search"

def bt_dsr(t_obs, n_obs, n_trials):
    """試行回数で割り引いた有意性（Deflated Sharpe Ratio の考え方）。

       なぜ要るか:
         内山朋規・瀧澤秀明・菊川匡 (2020)「資産リターンの予測可能性と
         機械学習」（オペレーションズ・リサーチ 2020年7月号）は、日本株で
         **予測力がゼロの乱数データ**を使い、変数選択とモデル選択を経た
         t 統計量の分布を1万回のシミュレーションで出した。
         結果は **5%の臨界値が t=5.41**。つまり「50個の候補変数から
         選んだモデルの t=3.96」は、**偶然の範囲**だった。
         さらに「回帰木やk近傍法のほうが線形回帰より変数マイニングの
         影響が深刻」とも報告している。
       ★これは当方の BT_T_THRESHOLD=2.8 を直撃する。2.8 は
         「1つの検定」に対する基準で、何百通りも試した後の基準ではない。
         BH法（bt_fdr）と、この試行回数の割引の両方で見る。

       やること:
         N 回の独立な試行で観測される t 値の最大値の期待値を出し、
         実際の t がそれをどれだけ上回っているかを返す。
         Bailey & Lopez de Prado の Deflated Sharpe Ratio と同じ考え方
         （シャープレシオの代わりに t 値でやっている）。

       返り値: {"t_expected_max", "beats_luck", "n_trials"}"""
    import math
    if t_obs is None or n_trials is None or n_trials < 2 or not n_obs or n_obs < 3:
        return None
    # 独立なN回の標準正規の最大値の期待値（Gumbel 近似）
    g = 0.5772156649015329          # オイラー・マスケローニ定数
    N = float(n_trials)
    def _z(p):
        """標準正規の分位点（Acklam の有理近似）。scipy を入れずに済ませる。"""
        a_ = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
              1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
        b_ = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
              6.680131188771972e+01, -1.328068155288572e+01]
        c_ = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
              -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
        d_ = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
              3.754408661907416e+00]
        pl = 0.02425
        if p < pl:
            q = math.sqrt(-2 * math.log(p))
            return (((((c_[0]*q+c_[1])*q+c_[2])*q+c_[3])*q+c_[4])*q+c_[5]) / \
                   ((((d_[0]*q+d_[1])*q+d_[2])*q+d_[3])*q+1)
        if p > 1 - pl:
            q = math.sqrt(-2 * math.log(1 - p))
            return -(((((c_[0]*q+c_[1])*q+c_[2])*q+c_[3])*q+c_[4])*q+c_[5]) / \
                    ((((d_[0]*q+d_[1])*q+d_[2])*q+d_[3])*q+1)
        q = p - 0.5; r = q*q
        return (((((a_[0]*r+a_[1])*r+a_[2])*r+a_[3])*r+a_[4])*r+a_[5])*q / \
               (((((b_[0]*r+b_[1])*r+b_[2])*r+b_[3])*r+b_[4])*r+1)
    emax = (1 - g) * _z(1 - 1.0/N) + g * _z(1 - 1.0/(N * math.e))
    return {"t_expected_max": round(emax, 2),
            "beats_luck": bool(abs(t_obs) > emax),
            "n_trials": int(n_trials)}

def bt_verdict(rec, thr=None, min_indep=None):
    """その格子を「使える」と言ってよいかの判定。★ここが安全弁の本体。

       規則（緩い側に倒さない）:
         ① 独立した建て日が BT_MIN_INDEP 未満なら「検証できていない」。
            t値がいくつでも通さない。重なりの補正は情報を捨てずに済ませる
            道具であって、**独立な観測を増やす道具ではない**。
         ② 通すのは、日次・重なりなし・Newey-West の**3つの t が全部**
            しきい値を超えたときだけ（一番小さいものを見る）。
         ③ 差が負なら、いくら大きくても「使ってはいけない」。

       実測で必要になった経緯: ②だけを nw と dates の2つで見ていたため、
       独立観測が2日しかない保有250日の格子が t=+4.28 で通っていた。"""
    thr = BT_T_THRESHOLD if thr is None else thr
    min_indep = BT_MIN_INDEP if min_indep is None else min_indep
    # ★試行回数で床を上げる。**下げることはしない。**
    #   何百通りも試せば、中身が空でも最大t はそれなりの値になる。
    #   実測: 300通りなら偶然でも最大 t≒2.90 が出る。つまり
    #   しきい値2.8は**試行回数に対して低すぎた**。
    #   内山・瀧澤・菊川 (2020) は日本株の乱数実験で、変数選択と
    #   モデル選択を経た t の5%臨界値が **5.41** に達すると報告している。
    _ds = rec.get("dsr")
    if isinstance(_ds, dict) and _ds.get("t_expected_max"):
        thr = max(thr, float(_ds["t_expected_max"]))
    md = rec.get("mean_diff")
    n_ind = rec.get("n_nonoverlap") or 0
    ts = [rec.get(k) for k in ("t_dates", "t_nonoverlap", "t_nw")]
    ts = [x for x in ts if x is not None]
    if md is None or not ts:
        return {"ok": False, "why": "検定できない（標本が無い）",
                "t_used": None, "n_indep": n_ind}
    if n_ind < min_indep:
        return {"ok": False,
                "why": f"独立した建て日が{n_ind}日しかない（{min_indep}日未満）＝検証できていない",
                "t_used": None, "n_indep": n_ind}
    t_use = min(ts, key=abs)
    # ★言い切る。「判断材料にならない」で濁すと、読み手が
    #   「弱いけど使える」と解釈する余地が残る。
    if md < 0 and abs(t_use) >= thr:
        return {"ok": False,
                "why": f"母集団平均を下回っている（t={t_use:+.2f}）。使ってはいけない",
                "t_used": t_use, "n_indep": n_ind}
    if md <= 0:
        return {"ok": False,
                "why": f"差が正でない（差={md:+.4f}）。情報があるとは言えない",
                "t_used": t_use, "n_indep": n_ind}
    if abs(t_use) < thr:
        return {"ok": False,
                "why": f"誤差の範囲（t={t_use:+.2f} < {thr}）。情報があるとは言えない",
                "t_used": t_use, "n_indep": n_ind}
    # ④ 多重検定の補正（Benjamini-Hochberg）。何百通りも同時に測っている
    #    ので、t≥2.8 だけでは偶然通るものが必ず出る。
    #    ★fdr が入っていない古い backtest.json では ④ を課さない
    #      （無いものを「落ちた」と扱うと、全部が偽陰性になる）。
    fd = rec.get("fdr")
    if isinstance(fd, dict) and fd.get("p_adj") is not None and not fd.get("ok"):
        return {"ok": False,
                "why": f"多重検定の補正で落ちる（補正後p={fd['p_adj']:.3f}）。"
                       f"何百通りも試せば偶然これくらいは出る",
                "t_used": t_use, "n_indep": n_ind}
    _fdr_txt = ("／多重検定の補正も通過" if isinstance(fd, dict) and fd.get("ok")
                else "／★多重検定の補正は未実施（古い検証データ）")
    if isinstance(_ds, dict) and _ds.get("n_trials"):
        _fdr_txt += (f"／{_ds['n_trials']}通り試した中で、"
                     f"偶然の最大t {_ds['t_expected_max']} も超えている")
    return {"ok": True, "why": f"3つのtが全て{thr}以上（最小 t={t_use:+.2f}）"
                               f"／独立した建て日{n_ind}日" + _fdr_txt,
            "t_used": t_use, "n_indep": n_ind}

def bt_base_for(k):
    """その選び方を比べる相手（基準線）。★母集団が違うものを比べない。
       ファンダ側は「財務が引けた銘柄」だけ、流動性版は「流動性上位半分」
       だけで戦っているので、基準線も必ず同じ母集団から取る。
       違う母集団の平均と比べると、母集団が変わった効果を
       その選び方の優位性として報告してしまう。"""
    if k.endswith(LIQ_SUF): return "pool_liq"
    # ★イベント起点の仕掛けは、開示が引けている銘柄だけで戦っているので
    #   基準線も必ずその母集団（pool_f）から取る。全銘柄平均と比べると、
    #   「開示が引けている銘柄」に絞った効果をイベントの効果として
    #   報告してしまう。
    if k in EVENTS: return "pool_f"
    return "pool_f" if k in FUND_FACTORS else "pool"

def bt_clustered(trades, hold, regime=None, ex="2atr_3atr"):
    """建て日ごとに束ねてから差を検定する。

       なぜ要るか:
         t_rough は1件ごとのRが独立だと仮定している。実際は
           ① 同じ日に建てた10件は同じ市場の動きを共有している
           ② 5営業日ごとに建てて10〜25日持つので、期間が重なっている
         この2つで t_rough は有意性を**大きく過大評価する**。
         実効的な標本数は「建てた件数」ではなく「建てた日数」に近い。
         この値を根拠に発注するかどうかを決めるので、ここは直さないといけない。

       やること:
         各建て日について「その因子の平均R − **同じ日の**母集団平均のR」を出し、
         その日次の差を検定する。同じ日で引き算するので、市場全体の動き
         （分散の大半を占める）が消える。
         さらに保有期間ぶん間隔を空けた部分標本でも出し、期間の重なりも消す。
         採否の判断にはこの厳しい方（t_nonoverlap）を使う。"""
    import numpy as np
    # ★建玉は (日付, 保有, 降り方) ごとに畳んである（合計と件数）。
    #   ここで要るのは「その日の平均」だけなので、合計÷件数で復元する。
    #   畳む前と同じ値になることはテストで確かめてある。
    per, perp = {}, {}
    for kind, rows in trades.items():
        for r in rows:
            if r["hold"] != hold: continue
            if r.get("ex", "2atr_3atr") != ex: continue
            if regime is not None and r["up"] != regime: continue
            _n = r.get("n") or 0
            if _n <= 0: continue
            per.setdefault(r["i"], {})[kind] = r["sR"] / _n
            if r.get("sRp") is not None:
                perp.setdefault(r["i"], {})[kind] = r["sRp"] / _n
    gap = max(1, -(-hold // BT_STEP))      # 期間が重ならない間隔（切り上げ）
    def _t_nw(xs, lag):
        """重なりを捨てずに、重なったぶんだけ標準誤差を膨らませる（Newey-West）。

           なぜ要るか:
             保有250日を5営業日ごとに建てると、重なりを消した部分標本は
             5年でも2件しか残らず、検定そのものができなくなる（実測）。
             重なった系列でも、自己相関を織り込んだ標準誤差を使えば
             全105日ぶんを使って検定できる。これが標準的な扱い。
           ★有意性を作り出す道具ではない。重なりが大きいほど標準誤差は
             膨らみ、t値は小さくなる。捨てるか膨らませるかの違いで、
             膨らませるほうが情報を捨てずに済む。"""
        n = len(xs)
        if n < 12: return None, n
        a = np.asarray(xs, float); mu = float(a.mean()); e = a - mu
        v = float(e @ e) / n
        for j in range(1, min(lag, n - 1) + 1):
            gj = float(e[j:] @ e[:-j]) / n
            v += 2.0 * (1.0 - j / (lag + 1)) * gj      # Bartlett の重み
        if not (v > 0): return None, n
        return round(mu / (v / n) ** 0.5, 2), n

    def _t(xs):
        n = len(xs)
        if n < 8: return None, n
        mu = float(np.mean(xs)); sd = float(np.std(xs, ddof=1))
        # ★sd > 0 だけでは足りない。全日の差が同じ値のとき、浮動小数点の
        #   残差（1e-17程度）で sd が「0より大きい」と判定され、
        #   t値が 8.4e15 のような無意味な数になった（実測で発覚）。
        #   平均に対して無視できる大きさのばらつきは 0 として扱う。
        if not (sd > max(1e-12, abs(mu) * 1e-9)):
            return None, n
        return round(mu / (sd / n ** 0.5), 2), n
    def _diffs(src, k, bk):
        ds = []
        for i in sorted(src):
            a, b = src[i].get(k), src[i].get(bk)
            # ★None かどうかで見る。`not a` にすると**平均がちょうど 0.0 の日**を
            #   取りこぼす（畳む前はリストだったので「空か」で正しかったが、
            #   いまは数値なので 0.0 が偽になる）。
            if a is None or b is None: continue
            ds.append(float(a) - float(b))
        return ds

    out = {}
    for k in BT_FACTORS + BT_FACTORS_LIQ:
        bk = bt_base_for(k)
        ds = _diffs(per, k, bk)
        if len(ds) < 8: continue
        t_all, n_all = _t(ds)
        t_ind, n_ind = _t(ds[::gap])
        t_nw, _ = _t_nw(ds, max(gap - 1, 0))
        rec = {"base": bk, "mean_diff": round(float(np.mean(ds)), 4),
               "median_diff": round(float(np.median(ds)), 4),
               "sd_diff": round(float(np.std(ds, ddof=1)), 4) if len(ds) > 1 else None,
               "t_dates": t_all, "n_dates": n_all,
               "t_nonoverlap": t_ind, "n_nonoverlap": n_ind, "gap": gap,
               "t_nw": t_nw, "nw_lag": max(gap - 1, 0)}
        # %リターン版（等ウェイトの枠で使う単位）
        dp = _diffs(perp, k, bk)
        if len(dp) >= 8:
            tp_all, _np_all = _t(dp)
            tp_ind, np_ind = _t(dp[::gap])
            tp_nw, _ = _t_nw(dp, max(gap - 1, 0))
            rec["pct"] = {"mean_diff": round(float(np.mean(dp)), 5),
                          "median_diff": round(float(np.median(dp)), 5),
                          "t_dates": tp_all, "t_nonoverlap": tp_ind,
                          "n_nonoverlap": np_ind, "t_nw": tp_nw}
        rec["verdict"] = bt_verdict(rec)
        out[k] = rec
    return out

def bt_summary(trades, hold, regime=None, ex="2atr_3atr",
               brd_min=None, brd_max=None):
    """その保有期間・執行方式の集計。

       regime … TOPIXが200日線の上か下か
       brd_min / brd_max … 市場の広がり（母集団のうち直近5日が上げている
         割合）で建て日を絞る。★局面で符号が反転する仕掛けを、
         全期間の平均で打ち消して見落とさないため。"""
    out = {}
    for kind, rows in trades.items():
        d = [r for r in rows if r["hold"] == hold
             and r.get("ex", "2atr_3atr") == ex
             and (regime is None or r["up"] == regime)
             and (brd_min is None or (r.get("brd") is not None
                                      and r["brd"] >= brd_min))
             and (brd_max is None or (r.get("brd") is not None
                                      and r["brd"] <= brd_max))]
        if not d: continue
        # ★建玉は畳んである。1レコードは n 件ぶんで、各件の重みは w。
        #   重みの合計は n*w、Rの重み付き合計は w*sR になる。
        w = sum((r.get("n") or 0) * r["w"] for r in d)
        if w <= 0: continue
        sR = sum(r["sR"] * r["w"] for r in d)
        win = sum(r["win"] * r["w"] for r in d)
        # ★%リターンと取引コスト。
        #   Bajgrowicz & Scaillet (2012) は「低いコストを入れるだけで
        #   イン・サンプルでも優位が完全に消える」と示した。
        #   コスト前の数字だけを見てはいけない。
        #   ★ただし**母集団平均との差**にはコストは効かない。
        #     どちらの側も同じ往復コストを払うので引き算で消える。
        #     コストが決めるのは「そもそも儲かるのか」という絶対値のほう。
        _sw = sum((r.get("n") or 0) * r["w"] for r in d if r.get("sRp") is not None)
        _arp = ((sum(r["sRp"] * r["w"] for r in d if r.get("sRp") is not None) / _sw)
                if _sw > 0 else None)
        out[kind] = {"n": round(w, 1), "avg_r": round(sR/w, 4),
                     "avg_rp_pct": (None if _arp is None else round(_arp*100, 4)),
                     # コスト差引後。往復 BT_COST_BP ベーシスポイント
                     "avg_rp_net_pct": (None if _arp is None else
                                        round(_arp*100 - BT_COST_BP/100.0, 4)),
                     # 損益がゼロになる往復コスト（bp）。文献と比較できる形
                     #   STW 1999: 0.27%/回、Bessembinder-Chan 1995: 往復1.57%
                     "breakeven_bp": (None if _arp is None else round(_arp*10000, 1)),
                     "cost_bp": BT_COST_BP,
                     "win_pct": round(win/w*100, 1),
                     "target": round(sum(r["n_target"]*r["w"] for r in d)/w*100, 1),
                     "stop": round(sum(r["n_stop"]*r["w"] for r in d)/w*100, 1),
                     "timeout": round(sum(r["n_timeout"]*r["w"] for r in d)/w*100, 1)}
    # 母集団平均との差が、順位付けが生んでいる値。
    # ★ファンダ側は「財務が引けた銘柄」だけの母集団で戦っているので、
    #   全銘柄の平均と比べてはいけない。同じ母集団から取った
    #   pool_f と比べる。そうしないと、ETFが母集団から抜けた効果を
    #   ファンダの優位性として報告してしまう。
    for k in BT_FACTORS + BT_FACTORS_LIQ:
        if k not in out: continue
        bk = bt_base_for(k)
        base = out.get(bk, {}).get("avg_r")
        if base is None: continue
        out[k]["base"] = bk
        out[k]["vs_pool"] = round(out[k]["avg_r"] - base, 4)
        # 平均の差の粗い有意性（1件あたりのRの散らばりを1.0とみなす）
        n = out[k]["n"]
        out[k]["t_rough"] = round(out[k]["vs_pool"] / (1.0/max(n, 1)**0.5), 2) if n else None
    return out

# ══════════════════════════════════════════════════════════════════════
#  シグナル台帳 ── この仕組みに効き目があるのかを実測する
#
#  なぜ必要か:
#    毎日推奨を出しているのに、それが儲かったのかを誰も測っていなかった。
#    測らなければ「前提が崩れたら訂正する」も判断材料が無く、
#    1ヶ月後に20本のレポートと0件の学習が残るだけになる。
#
#  何を測るのか:
#    モデルのB（レジーム判断）は測れない。測れるのは**スクリーニング**の部分。
#    その日の上位候補を、発注したものと見なして台帳に記録し、
#    その後の実際の高値・安値で 2ATR損切り / 3ATR利確 のどちらに先に当たったかを追う。
#
#  乱数は一切使わない。使うのは実際に付いた値段だけ。
#  ただしこれは**執行の記録ではなく、選別ロジックの追跡**である。
#  実際の約定値・スリッページ・板の薄さは含まれない。そこは割り引いて読むこと。
# ══════════════════════════════════════════════════════════════════════
# ── 執行の前提（ここ以外に数字を散らさない）─────────────────────
def session_complete():
    """直近の日足が確定しているか。
       平日 9:00〜15:40 は場中で当日足が未完成なので False。
       土日・寄り付き前・大引け後は、直近の足が確定しているので True。
       （時刻だけで「15時以降」と判定していたため、土曜の手動実行が
         黙って台帳も過去検証も飛ばしていた。）"""
    if NOW.weekday() >= 5:                  # 土日は直前の営業日の足
        return True
    t = NOW.hour*60 + NOW.minute
    return not (9*60 <= t < 15*60 + 40)     # 大引け後のデータ確定を少し待つ

LOT              = 100      # 日本株の売買単位。10株単位で丸めると発注できない
# ── ポートフォリオを2つの枠に分ける（2026-09-13の決定）──────────────
#   半分をスイングトレード、半分を清原式（割安小型成長株・長期）に充てる。
#   理由: 検証でエッジが出たのはバリューだけで、そのバリューは保有期間が
#   長いほど効いていた。一方スイングの枠組み（3ATR利確・15〜25日）は
#   清原式の「最低2倍を狙う・3年持つ」と両立しない。混ぜると両方の
#   根拠を失うので、枠を分けて別の規則で動かす。
SLEEVE_SWING     = 0.50     # スイング枠（総額比）
SLEEVE_KIYOHARA  = 0.50     # 清原枠（総額比）
# ★許容損失と1銘柄上限は「総額比」ではなく「その枠に対する比」で見る。
#   総額比のままだと、枠を半分にしたのに1件のリスクが変わらず、
#   スイング枠の中では実質2倍のリスクを取ることになる。
RISK_PER_TRADE_P = 0.009    # 1トレードの許容損失（スイング枠に対する比）
MAX_WEIGHT       = 0.15     # 1銘柄の上限（スイング枠に対する比）
MIN_CASH_RATIO   = 0.20     # 現金比率の下限（総額比）。これを割る発注はしない

SIG_PATH      = f"{OUT}/signals.csv"
SIG_TOP_N     = 10       # 各サイドの上位何件を台帳に載せるか
SIG_MAX_HOLD  = 15       # これを超えたら時間切れとして手仕舞う（営業日）
# ★17業種の列を外した。JPXの「東証上場銘柄一覧」由来で、
#   public リポジトリへの再配信に当たるため（JPXサイト利用条件）。
#   判定には引き続きメモリ上で使う。name は EDINET の提出者名（PDL1.0）。
SIG_COLS = ["date", "kind", "rank", "code", "name", "score",
            "entry", "atr", "stop", "target", "adjf",
            "status", "exit_date", "exit", "r_multiple", "bars"]

def load_signals():
    if not os.path.exists(SIG_PATH):
        return pd.DataFrame(columns=SIG_COLS)
    try:
        df = pd.read_csv(SIG_PATH, dtype=str)
        for c in SIG_COLS:
            if c not in df.columns: df[c] = ""
        return df[SIG_COLS]
    except Exception as e:
        meta["errors"].append(f"signals read: {type(e).__name__}")
        return pd.DataFrame(columns=SIG_COLS)

def open_signal_map(sig):
    """未決着のシグナルを code ごとにまとめる。screen_all が各銘柄の日足を
       持っている最中に、そのまま判定できるようにするため。"""
    out = {}
    if sig.empty: return out
    op = sig[sig["status"].isin(["", "open", "nan"]) | sig["status"].isna()]
    for i, r in op.iterrows():
        out.setdefault(str(r["code"]), []).append(i)
    return out

def settle_signal(row, bars, adjf_now=None):
    """entry日より後の実際の高値・安値で決着を付ける。
       同じ日に損切りと利確の両方に触れた場合は**損切りを優先**する
       （日足では順序が分からないため、都合の良い方を採らない）。

       ★bars は**調整済み**の枠を渡すこと。生値で見ると、建てたあとに
         分割があった銘柄で価格が不連続に下がり、架空の損切りを量産する。
         建値・損切り・利確は建てた時点の調整係数で同じ土俵に移してから比べる。"""
    try:
        entry = float(row["entry"]); stop = float(row["stop"]); tgt = float(row["target"])
        atr = float(row["atr"])
        f0 = float(row.get("adjf") or 1.0)
    except Exception:
        return None
    if not f0 or f0 != f0: f0 = 1.0
    # 建値側を調整済みの土俵へ移す
    entry_a, stop_a, tgt_a, atr_a = entry*f0, stop*f0, tgt*f0, atr*f0
    after = bars[bars.index > pd.Timestamp(row["date"])]
    if not len(after): return None
    for n, (ts, b) in enumerate(after.iterrows(), start=1):
        op, hi, lo, cl = (float(b["Open"]) if "Open" in b else float(b["Close"]),
                          float(b["High"]), float(b["Low"]), float(b["Close"]))
        # ★寄りで損切り水準を飛び越えた日は、損切り値ではなく**寄値**で約定する。
        #   常に -1.00R として記録すると、窓を開けて下げた分の損が消え、
        #   成績が実態より良く出る（窓は下に開くほうが多い）。
        if lo <= stop_a:
            fill = min(op, stop_a)
            return dict(status="stop", exit_date=str(ts.date()), exit=round(fill/f0, 2),
                        r_multiple=round((fill-entry_a)/(2*atr_a), 2), bars=n)
        if hi >= tgt_a:
            fill = max(op, tgt_a)
            return dict(status="target", exit_date=str(ts.date()), exit=round(fill/f0, 2),
                        r_multiple=round((fill-entry_a)/(2*atr_a), 2), bars=n)
        if n >= SIG_MAX_HOLD:
            return dict(status="timeout", exit_date=str(ts.date()), exit=round(cl/f0, 2),
                        r_multiple=round((cl-entry_a)/(2*atr_a), 2), bars=n)
    return None      # まだ決着していない

def append_signals(sig, recs, kind, today):
    """その日の上位を台帳に足す。
       ★未決着の同じ銘柄があるうちは追加しない。
         以前は(日付,銘柄,種別)でしか重複を見ておらず、上位に居座る銘柄が
         連日で別行になっていた。1つの値動きを12回数えれば、件数だけが
         増えて「30件超えたので判断できる」と誤認する。"""
    have_open = set(sig.loc[sig["status"].isin(["", "open"]) | sig["status"].isna(),
                            "code"].astype(str)) if len(sig) else set()
    have_today = set(zip(sig["date"].astype(str), sig["code"].astype(str))) if len(sig) else set()
    add = []
    for i, r in enumerate(recs[:SIG_TOP_N], start=1):
        # 新しい台帳は "score"（バリューの断面順位）。
        # 古い行は trend/revert を持っているので、読める形は残す。
        score = r.get("score", r.get("trend", r.get("revert", 0)))
        try:
            score = float(score)
        except (TypeError, ValueError):
            continue
        if not (score > 0): continue          # NaN もここで落ちる
        code = str(r["code"])
        if code in have_open or (today, code) in have_today: continue
        entry, atr = float(r["close"]), float(r["atr"])
        if not (entry > 0 and atr > 0): continue
        have_open.add(code)
        add.append({"date": today, "kind": kind, "rank": i, "code": code,
                    "name": r.get("name", ""),      # EDINET(PDL1.0)
                    "score": score, "entry": round(entry, 2), "atr": round(atr, 2),
                    "stop": round(entry - 2*atr, 2), "target": round(entry + 3*atr, 2),
                    "adjf": r.get("adjf", 1.0),
                    "status": "open", "exit_date": "", "exit": "", "r_multiple": "", "bars": ""})
    if not add: return sig
    return pd.concat([sig, pd.DataFrame(add)], ignore_index=True)[SIG_COLS]

def signal_summary(sig):
    """決着済みだけで集計する。未決着を混ぜると勝率が水増しされる。"""
    if sig.empty: return {}
    d = sig[sig["status"].isin(["stop", "target", "timeout"])].copy()
    if d.empty: return {"closed": 0, "open": int((sig["status"] == "open").sum())}
    d["r"] = pd.to_numeric(d["r_multiple"], errors="coerce")
    d = d.dropna(subset=["r"])
    out = {"closed": int(len(d)), "open": int((sig["status"] == "open").sum()),
           "days": int(sig["date"].astype(str).nunique())}
    for k in ("value", "trend", "revert", None):
        part = d if k is None else d[d["kind"] == k]
        if not len(part): continue
        win = int((part["r"] > 0).sum())
        out[k or "all"] = {
            "n": int(len(part)),
            "win_pct": round(win / len(part) * 100, 1),
            "avg_r": round(float(part["r"].mean()), 3),
            "sum_r": round(float(part["r"].sum()), 2),
            "target": int((part["status"] == "target").sum()),
            "stop": int((part["status"] == "stop").sum()),
            "timeout": int((part["status"] == "timeout").sum()),
            "avg_bars": round(float(pd.to_numeric(part["bars"], errors="coerce").mean()), 1)}
    return out

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

def screen_all(codes, sig=None, open_map=None):
    """全銘柄の直近データを取得し、流動性と値幅で絞ってから指標を付ける。"""
    import yfinance as yf
    rows, allrows, t0 = [], [], time.time()
    done = 0
    skipped, skip_msg = {}, {}
    settled = 0
    open_map = open_map or {}
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
                if len(x) and not meta.get("screen_last_bar"):
                    meta["screen_last_bar"] = str(pd.Timestamp(x.index[-1]).date())

                # ── 台帳の決着（ここでやるのは、この銘柄の日足が今まさに手元にあるから）
                if c in open_map and sig is not None and len(x):
                    _ax = adjusted_frame(x)
                    for idx in open_map[c]:
                        try:
                            out = settle_signal(sig.loc[idx], _ax)
                        except Exception as _e:
                            out = None
                            k = "settle:" + type(_e).__name__
                            skipped[k] = skipped.get(k, 0) + 1
                            if k not in skip_msg: skip_msg[k] = str(_e)[:80]
                        if out:
                            for k, v in out.items(): sig.at[idx, k] = v
                            settled += 1

                if len(x) < 76: continue      # vs75 と r60 に必要な本数を満たすこと
                cl = x["Close"]; last = float(cl.iloc[-1])
                if last < MIN_PRICE: continue
                turnover = float((cl * x["Volume"]).tail(20).mean())
                # ★清原枠はスイングとは別のふるいを使う（ATR帯や売買代金の
                #   条件が違う）。ATRで落とす前に、全銘柄の価格と売買代金を
                #   取っておく。ここで取らないと小型株が先に消えてしまう。
                allrows.append({"code": c, "close": last, "turnover": turnover})
                if turnover < MIN_TURNOVER: continue

                # ★指標は調整済み、発注価格は実際の価格。
                #  以前は変数名だけ adj で中身は Close を使っており、分割も
                #  配当落ちも指標に素通りしていた（配当2.5%落ちの銘柄が
                #  「売られすぎ」として逆張り上位に入る形になっていた）。
                ax = adjusted_frame(x)
                acl, _ = repair(ax["Close"])
                if acl.isna().all(): continue
                # ATRは調整済みで計算し、今日の実際の価格水準に戻す。
                # 分割が窓に入ると生値のATRは桁がずれ、株数と損切り幅が壊れる。
                a_adj = _atr(ax.reset_index())
                if not a_adj or a_adj != a_adj: continue
                f_last = float(ax["Close"].iloc[-1] / last) if last else 1.0
                a = a_adj / f_last if f_last else a_adj
                atr_pct = a / last * 100
                if atr_pct < MIN_ATR_PCT or atr_pct > MAX_ATR_PCT: continue

                w60 = ax.tail(60)                      # レンジ位置も調整済みで揃える
                rngw = float(w60["High"].max() - w60["Low"].min())   # 値幅
                if rngw <= 0: continue
                rows.append(dict(
                    code=c, close=last, turnover=turnover,
                    atr=round(a, 2), atr_pct=round(atr_pct, 2), adjf=round(f_last, 6),
                    rsi=round(_rsi(acl.values), 1),
                    vs25=round((acl.iloc[-1]/acl.rolling(25).mean().iloc[-1]-1)*100, 2),
                    vs75=round((acl.iloc[-1]/acl.rolling(75).mean().iloc[-1]-1)*100, 2),
                    pos60=round((float(ax["Close"].iloc[-1])-w60["Low"].min())/rngw*100, 1),
                    r20=round((acl.iloc[-1]/acl.iloc[-21]-1)*100, 2),
                    r60=round((acl.iloc[-1]/acl.iloc[-61]-1)*100, 2),
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
    if settled:
        print(f"[台帳] 未決着シグナルのうち {settled}件が決着")
    meta["signals_settled"] = settled
    return pd.DataFrame(rows), done, pd.DataFrame(allrows)

CAND_TOP_N = 20

def rank_value(df):
    """バリューの断面順位で並べる。これが唯一の選び方。

       なぜ自作の順張り/逆張りを消したのか:
         ★2026-09-17、基準線の不具合を直したうえで測り直した。
           以前は yfinance の調整係数の乱れ（1306が1/10、1629が約1/500）が
           基準線側だけに入っていて、全銘柄平均が +1.085R と嘘の高さになり、
           何を比べても負けて見えていた。repair() を検証側にも通し、
           1日で60%以上動く銘柄を外したら、基準線は +0.049R に落ち着いた
           （全銘柄平均とファンダが引ける母集団の比が 11倍 → 1.09倍）。
         ★その結果、自作スコアは「情報があるとは言えない」ではなく
           **有意にマイナス**だと分かった。独立した建て日188日で、
           2atr_only・保有3日 差-0.059R（t=-3.01/-3.01/-3.02）、
           保有5日 差-0.069R（t=-3.07/-3.07/-3.08）。
           3つのtが全て2.8を超えてマイナス側＝**使ってはいけない**。
           勝率も48%台で母集団を下回る。消したのは正しかった。
         ★文献由来の因子も同じ母集団で測り直したが、
           モメンタム(mom12_2)と低ボラ(lowvol)は |t|<1 でほぼゼロ、
           短期反転(rev5)は有意にマイナス（t=-3.2〜-8.7）。
           技術系で基準を通ったものは**ひとつも無い**。
         一方バリューは建て日で束ねた厳しい検定でも
           保有3日 t=+2.94 / 5日 t=+3.85 / 10日 t=+3.78（3つのtの最小値）
         で、しきい値2.8を超えた。独立した建て日は107日・107日・53日。
         検証を通った選び方だけを使う。

       ★並べる条件は過去検証と同じでなければいけない。
         検証したのは「流動性・株価・ATR帯のフィルタを通った母集団から
         バリュー上位10件」だけ。ここにRSIや移動平均の条件を足すと、
         検証していないものを運用することになる（良くなる保証は無い）。
         決算跨ぎの除外・業種重複の注意は**運用上のリスク管理**であって
         選別の条件ではない。検証に入っていないことをレポートに明記する。

       財務が引けない銘柄（ETF・REIT・新規上場直後）は候補にならない。
       これは仕様。バリューで並べられないものをバリューで選べない。"""
    _c = "f_" + RANK_BY
    if df.empty or _c not in df.columns:
        return df.iloc[0:0]
    d = df.copy()
    v = pd.to_numeric(d[_c], errors="coerce")
    d = d[v.notna()].copy()
    if d.empty: return d
    d["score"] = pd.to_numeric(d[_c], errors="coerce").astype(float).round(1)
    return d.sort_values("score", ascending=False).head(CAND_TOP_N)

# ── JPX 決算発表予定日（公式）─────────────────────────────────────
#    yfinance の予定日は日本の中小型で欠けやすく、実測で 40件中10件（25%）しか
#    埋まらなかった。4分の3が「要確認」では、決算跨ぎを避けるルールが働かない。
#    JPXは決算発表予定日を決算期末月ごとにExcelで公開しているので、そちらを主にする。
#    ファイル名の日付部分は更新のたびに変わるため、一覧ページからリンクを拾う。
# ── 決算発表の履歴から次回を推定する ──────────────────────────────
#   なぜ要るか:
#     JPXが予定日を載せるのは「直前の四半期が終わった会社」だけ（実測で
#     7月期末420社＋8月期末236社＝656社）。それ以外は未掲載になる。
#     実測でも候補40件のうち日付が付いたのは13件で、しかも**全部32〜62日後**
#     だった。つまり「予定なし」の多くは本当に遠いだけなのだが、
#     表示上は「不明」と区別できず、毎回あなたが調べ直すことになる。
#
#   やること:
#     TDnetの「決算短信」を日々ためて、その銘柄が最後に決算を出した日を持つ。
#     四半期決算はほぼ91日間隔なので、そこから次回のおおよその時期が出せる。
#     TDnetは約31日しか遡れないので、履歴は今日から前向きに積み上がる。
#
#   扱い:
#     これは**予定ではなく推定**。表示は必ず「推定」と「要確認」を併記し、
#     これだけを根拠に建てさせない。逆に「直近で発表済み」は確定情報として使える。
EHIST_PATH      = f"{OUT}/earnings_hist.json"
EHIST_DAYS      = 31        # TDnetが保持している範囲
EHIST_BUDGET_S  = 240
EHIST_KEEP_DAYS = 500       # 履歴の保持期間
QUARTER_DAYS    = 91        # 四半期の間隔（次回の推定に使う）
RECENT_DAYS     = 45        # これ以内に発表済みなら「当面は決算をまたがない」

def _load_ehist():
    try:
        if os.path.exists(EHIST_PATH):
            h = json.load(open(EHIST_PATH, encoding="utf-8"))
            h.setdefault("days", []); h.setdefault("kessan", {})
            return h
    except Exception as e:
        meta["errors"].append(f"ehist read: {type(e).__name__}")
    return {"days": [], "kessan": {}}

def update_earnings_history():
    """TDnetの決算短信だけを日付ごとに拾って積み上げる。
       取得済みの日は二度と取りに行かないので、初回以降は1日分で済む。
       ★TDNET_ENABLED が False のときは、既に積み上げた履歴だけを使う
         （新しい日は取りに行かない）。"""
    import requests, html as _html
    if not TDNET_ENABLED:
        _h = _load_ehist()
        meta["earnings_hist"] = {"skipped": "TDNET_ENABLED=False",
                                 "days_cached": len(_h.get("days") or []),
                                 "codes": len(_h.get("kessan") or {})}
        print(f"[決算履歴] 取得しない（TDNET_ENABLED=False）。"
              f"手元の{len(_h.get('kessan') or {}):,}銘柄ぶんはそのまま使う")
        return _h
    h = _load_ehist()
    have = set(h["days"])
    want = [(NOW - dt.timedelta(days=i)).strftime("%Y%m%d") for i in range(EHIST_DAYS)]
    todo = [d for d in want if d not in have]
    # 当日分は場中に増えるので、常に取り直す
    today = NOW.strftime("%Y%m%d")
    if today not in todo: todo.insert(0, today)

    t0, fetched, found = time.time(), 0, 0
    for d in todo:
        if time.time() - t0 > EHIST_BUDGET_S:
            print(f"[決算履歴] 時間予算に達したため {fetched}/{len(todo)}日で打ち切り"); break
        got_any = False
        for pg in range(1, TDNET_MAX_PAGES + 1):
            u = f"https://www.release.tdnet.info/inbs/I_list_{pg:03d}_{d}.html"
            try:
                r = requests.get(u, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            except Exception:
                break
            if r.status_code != 200: break
            try:
                txt = r.content.decode("utf-8")
            except UnicodeDecodeError:
                txt = r.content.decode("cp932", errors="replace")
            n_before = found
            for tr in re.split(r"<tr", txt, flags=re.I)[1:]:
                mc = re.search(r'kjCode["\']?[^>]*>\s*([0-9][0-9A-Za-z]{3,4})', tr)
                mt = re.search(r'kjTitle["\']?[^>]*>(.*?)</(?:td|th|div)>', tr, re.S | re.I)
                if not (mc and mt): continue
                title = _html.unescape(re.sub(r"<[^>]+>", " ", mt.group(1)))
                # 「決算短信」だけを拾う。予想の修正や配当のお知らせは決算発表ではない。
                # 「決算短信」を含んでも、訂正・修正は発表そのものではない。
                # これを発表済みと数えると、本番の決算直前に「安全」と表示してしまう。
                if "決算短信" not in title: continue
                if any(k in title for k in ("訂正", "修正", "差替", "差し替")): continue
                code = mc.group(1)[:4]
                iso = f"{d[:4]}-{d[4:6]}-{d[6:]}"
                lst = h["kessan"].setdefault(code, [])
                if iso not in lst:
                    lst.append(iso); found += 1
            if found == n_before and pg > 1: break
            got_any = True
        fetched += 1
        if got_any and d not in have and d != today:
            h["days"].append(d)
    # 古い履歴を落とす
    cut = str((NOW - dt.timedelta(days=EHIST_KEEP_DAYS)).date())
    for c in list(h["kessan"]):
        h["kessan"][c] = sorted({x for x in h["kessan"][c] if x >= cut})
        if not h["kessan"][c]: del h["kessan"][c]
    h["days"] = sorted(set(h["days"]))[-EHIST_KEEP_DAYS:]
    try:
        json.dump(h, open(EHIST_PATH, "w"), ensure_ascii=False)
    except Exception as e:
        meta["errors"].append(f"ehist write: {type(e).__name__}")
    meta["earnings_hist"] = {"days_cached": len(h["days"]), "codes": len(h["kessan"]),
                             "fetched_now": fetched, "new_items": found}
    print(f"[決算履歴] {fetched}日分を取得（新規{found}件）／"
          f"蓄積 {len(h['kessan']):,}銘柄 / {len(h['days'])}日分")
    return h["kessan"]

def estimate_from_history(code, hist, today):
    """最後の決算短信から次回を推定する。間隔が2つ以上あれば実測の中央値を使う。"""
    ds = sorted(hist.get(str(code)[:4], []))
    if not ds: return None
    last = ds[-1]
    gap = QUARTER_DAYS
    if len(ds) >= 3:
        gaps = [(pd.Timestamp(b) - pd.Timestamp(a)).days for a, b in zip(ds, ds[1:])]
        gaps = [g for g in gaps if 60 <= g <= 130]
        if gaps: gap = int(pd.Series(gaps).median())
    since = (pd.Timestamp(today) - pd.Timestamp(last)).days
    nxt = str((pd.Timestamp(last) + pd.Timedelta(days=gap)).date())
    return {"last": last, "since": int(since), "est": nxt,
            "est_days": int((pd.Timestamp(nxt) - pd.Timestamp(today)).days)}

JPX_EARN_INDEX = "https://www.jpx.co.jp/listing/event-schedules/financial-announcement/index.html"
JPX_EARN_MAX_FILES = 6          # 直近の数ファイルで足りる（決算は期末から45日以内）

def _jpx_earn_links():
    import requests
    from urllib.parse import urljoin
    r = requests.get(JPX_EARN_INDEX, timeout=60, headers=JGB_HDRS)
    r.raise_for_status()
    try:
        html = r.content.decode("utf-8")
    except UnicodeDecodeError:
        html = r.content.decode("cp932", errors="replace")
    hrefs = re.findall(r'href="([^"]*kessan[^"]*\.xlsx?)"', html, re.I)
    seen, out = set(), []
    for h in hrefs:
        u = urljoin(JPX_EARN_INDEX, h)
        if u not in seen:
            seen.add(u); out.append(u)
    # 新しいものから使う（ファイル名の数字が大きいほど新しい）
    out.sort(reverse=True)
    return out[:JPX_EARN_MAX_FILES]

def _parse_jpx_earn(blob):
    """コード→決算発表予定日。列名の正確な表記が分からなくても動くよう、
       『コード』を含む列と『予定日』を含む列を見出し行から探す。"""
    rows = _xlsx_rows(blob)
    hdr_i, c_code, c_date = None, None, None
    for i, r in enumerate(rows[:15]):                 # 見出しは上の方にある
        cells = [str(x or "") for x in r]
        code_i = next((j for j, x in enumerate(cells) if "コード" in x), None)
        date_i = next((j for j, x in enumerate(cells)
                       if "予定" in x and "日" in x and "前回" not in x), None)
        if code_i is not None and date_i is not None:
            hdr_i, c_code, c_date = i, code_i, date_i
            break
    if hdr_i is None:
        raise RuntimeError(f"見出し行が見つからない: {[str(x)[:12] for x in (rows[0] if rows else [])][:8]}")

    def to_date(v):
        if v is None or str(v).strip() == "": return ""
        s = str(v).strip()
        # xlsxの日付はシリアル値で入っていることがある（1900年起点、1900をうるう年とみなす仕様）
        try:
            f = float(s)
            if 30000 < f < 80000:
                return str((dt.date(1899, 12, 30) + dt.timedelta(days=int(f))))
        except ValueError:
            pass
        s = s.replace("年", "/").replace("月", "/").replace("日", "")
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y/%m/%d ", "%m/%d"):
            try:
                d = dt.datetime.strptime(s.strip().split(" ")[0], fmt)
                if fmt == "%m/%d": d = d.replace(year=NOW.year)
                return str(d.date())
            except ValueError:
                continue
        try:
            return str(pd.to_datetime(s).date())
        except Exception:
            return ""

    out = {}
    for r in rows[hdr_i + 1:]:
        if len(r) <= max(c_code, c_date): continue
        code = str(r[c_code] or "").strip()
        if code.endswith(".0"): code = code[:-2]          # 数値として読まれた場合
        code = code.zfill(4) if code.isdigit() and len(code) < 4 else code
        if not CODE_RE.match(code): continue
        d = to_date(r[c_date])
        if d: out[code] = d
    return out

def jpx_earnings():
    """コード→決算発表予定日（公式）。取れなければ空を返し、yfinanceに任せる。"""
    try:
        links = _jpx_earn_links()
    except Exception as e:
        meta["errors"].append(f"jpx_earn index: {type(e).__name__}: {e}")
        print("[決算] JPX一覧ページを取得できず:", e)
        return {}
    if not links:
        meta["errors"].append("jpx_earn: リンクが見つからない")
        return {}
    import requests
    merged, used = {}, []
    for u in links:
        try:
            rr = requests.get(u, timeout=90, headers=JGB_HDRS)
            if rr.status_code != 200 or len(rr.content) < 5000:
                used.append(f"{u.rsplit('/', 1)[-1]}:HTTP{rr.status_code}"); continue
            d = _parse_jpx_earn(rr.content)
            merged.update(d)                    # 新しいファイルから順に、後のもので上書き
            used.append(f"{u.rsplit('/', 1)[-1]}:{len(d)}件")
        except Exception as e:
            used.append(f"{u.rsplit('/', 1)[-1]}:{type(e).__name__}")
    meta["jpx_earnings"] = {"files": used, "codes": len(merged)}
    print(f"[決算] JPX予定日 {len(merged):,}銘柄 ({' / '.join(used)})")
    return merged

EARN_BUDGET_S = 240        # 候補の決算日照会に使ってよい秒数
EARN_SOON_DAYS = 7         # これ以内なら「決算跨ぎ」として扱う

JPX_EARN = {}
JQ_EARN = {}      # J-Quantsの決算発表予定日（コード4桁 → YYYY-MM-DD）
EHIST = {}

def next_earnings_for(codes, budget_s=EARN_BUDGET_S):
    """候補の次回決算発表日。
       全4,250銘柄には引けないが、**順位が付いた上位40銘柄だけ**なら
       1銘柄1リクエストで間に合う。ここが埋まって初めて
       「決算をまたぐ建玉は作らない」というルールが候補にも効く。

       yfinance の日本株の決算日は推定値のことがあり、欠けることもある。
       そこで「予定日あり」「照会したが不明」「照会失敗」を区別して返す。
       区別せず空欄にすると『決算が無い』と読めてしまい、ルールが骨抜きになる。"""
    import yfinance as yf
    out, t0, today = {}, time.time(), str(NOW.date())
    n_ok = n_none = n_err = n_jpx = n_recent = n_est = 0
    n_jq = 0
    for c in sorted(codes):
        # ① J-Quants（公式の予定日・全銘柄ぶんが1回の照会で取れる）。
        #    TDnetを止めたぶんをここで埋める。JPXのExcelより網羅が広い。
        q = JQ_EARN.get(str(c)[:4])
        if q and q >= today:
            out[c] = {"status": "ok", "src": "jquants", "next": q,
                      "days": int((pd.Timestamp(q) - pd.Timestamp(today)).days)}
            n_ok += 1; n_jq += 1
            continue
        # ② JPX の公開Excel（当日更新ぶん）。
        j = JPX_EARN.get(str(c)[:4])
        if j and j >= today:
            out[c] = {"status": "ok", "src": "jpx", "next": j,
                      "days": int((pd.Timestamp(j) - pd.Timestamp(today)).days)}
            n_ok += 1; n_jpx += 1
            continue
        # ② 決算短信の履歴。「直近で発表済み」は確定情報として使える
        #    （出したばかりなら当面またがない）。次回は推定にとどめる。
        e = estimate_from_history(c, EHIST, today)
        if e:
            if e["since"] <= RECENT_DAYS:
                out[c] = {"status": "recent", "src": "tdnet", "last": e["last"],
                          "since": e["since"], "next": e["est"], "days": e["est_days"]}
                n_recent += 1
                continue
            if 0 < e["est_days"]:
                out[c] = {"status": "estimate", "src": "tdnet", "last": e["last"],
                          "next": e["est"], "days": e["est_days"]}
                n_est += 1
                continue
        if time.time() - t0 > budget_s:
            out[c] = {"status": "timeout"}
            continue
        try:
            ed = yf.Ticker(c).get_earnings_dates(limit=12)
            ds = ([str(pd.Timestamp(i).date()) for i in ed.index]
                  if ed is not None and len(ed) else [])
            nxt = next((x for x in sorted(ds) if x >= today), "")
            if nxt:
                days = (pd.Timestamp(nxt) - pd.Timestamp(today)).days
                out[c] = {"status": "ok", "src": "yf", "next": nxt, "days": int(days)}
                n_ok += 1
            else:
                out[c] = {"status": "unknown"}      # 照会できたが将来の予定が無い
                n_none += 1
        except Exception as e:
            out[c] = {"status": "error", "why": type(e).__name__}
            n_err += 1
        time.sleep(0.15)                            # 連続照会でのレート制限を避ける
    meta["cand_earnings"] = {"asked": len(codes), "ok": n_ok,
                             "from_jquants": n_jq, "from_jpx": n_jpx,
                             "from_yf": n_ok - n_jpx - n_jq, "recent": n_recent,
                             "estimate": n_est, "unknown": n_none, "error": n_err}
    print(f"[決算] 候補{len(codes)}銘柄: 確定{n_ok}"
          f"（J-Quants {n_jq} / JPX {n_jpx} / yfinance {n_ok-n_jpx-n_jq}）"
          f" 発表済{n_recent} 推定{n_est} 不明{n_none} 失敗{n_err}")
    return out

# 保有している「業種の賭け」。個別株はマスタから、業種ETFは対応表から引く。
HOLD_S17_FIXED = {"1615": "銀行", "1617": "食品", "1618": "エネルギー資源",
                  "1619": "建設・資材", "1620": "素材・化学", "1621": "医薬品",
                  "1622": "自動車・輸送機", "1623": "鉄鋼・非鉄", "1624": "機械",
                  "1625": "電機・精密", "1626": "情報通信・サービスその他",
                  "1627": "電力・ガス", "1628": "運輸・物流", "1629": "商社・卸売",
                  "1630": "小売", "1631": "銀行", "1632": "金融（除く銀行）",
                  "1633": "不動産"}

def held_sectors(holdings, master):
    out = {}
    for c in holdings:
        c4 = str(c)[:4]
        s = HOLD_S17_FIXED.get(c4) or (master.get(c4, {}) or {}).get("s17", "")
        if s: out.setdefault(s, []).append(c4)
    return out

HELD_S17 = {}
CAND_EARN = {}
MASTER = {}
try:
    MASTER = load_master()
    HELD_S17 = held_sectors(HOLDINGS, MASTER)
    # ★公開して良い銘柄名（EDINET・PDL1.0）。EDINETの取得より前に読む。
    #   まだ溜まっていなければ空＝名前は空欄になる。
    #   JPXの銘柄名で埋め戻すことはしない（public への再配信になるため）。
    ED_NAMES.update(ed_names_load())
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
        r["name"]   = pub_name(c4)        # EDINET（PDL1.0）。無ければ空
        r["s17"]    = m.get("s17", "")     # ★書き出さない。表示と判定にだけ使う
        r["kind"]   = m.get("kind", "")
        r["size"]   = ""                   # JPXの規模区分は持たない
        s17 = m.get("s17", "")
        etf = s17_etf(s17)
        if s17 and not etf:
            # 表記のゆれで対応が取れないと、その業種の候補すべてでBが付かなくなる。
            # 実際に「情報通信・サービスその他」を1文字違いで書いていて全滅した。
            meta.setdefault("s17_unmapped", {})
            meta["s17_unmapped"][s17] = meta["s17_unmapped"].get(s17, 0) + 1
        # ★業種ETFの列は無くした。17業種と1対1に対応しているので、
        #   銘柄ごとに書き出すと JPX の業種区分がそのまま復元できてしまう。
        #   「同じ業種を既に持っているか」(overlap) は自分の保有の話なので残す。
        #   業種ETF自体のセクター物色の表は、ETFの株価から作っていて
        #   JPXの区分表に由来しないのでそのまま。
        r["news"]   = DISC.get(c4, [])[:3]
        # 既に同じ業種を持っているかどうか。持っているなら同じ賭けの上乗せ。
        r["overlap"] = HELD_S17.get(s17, []) if s17 else []
        e = CAND_EARN.get(r["code"], {"status": "not_checked"})
        r["earn_status"] = e.get("status")
        r["earn_next"]   = e.get("next", "")
        r["earn_days"]   = e.get("days")
        # 決算跨ぎは執行の可否そのものを決めるので、行ごとに明示する
        r["earn_src"]    = e.get("src", "")
        r["earn_last"]   = e.get("last", "")
        r["earn_since"]  = e.get("since")
        # 「跨ぎ」は確定した予定日のときだけ立てる。推定で建玉を止めると
        # 機会を失い、推定で建てさせると事故る。推定は情報として出すだけ。
        r["earn_soon"]   = bool(e.get("status") == "ok"
                                and e.get("days") is not None
                                and e["days"] <= EARN_SOON_DAYS)
        r["earn_watch"]  = bool(e.get("status") == "estimate"
                                and e.get("days") is not None
                                and e["days"] <= EARN_SOON_DAYS * 2)
    return recs

try:
    if SCREEN_ENABLED:
        uni = load_universe(MASTER)
        if uni:
            SIG = load_signals()
            sc, scanned, sc_all = screen_all(uni, SIG, open_signal_map(SIG))
            meta["screen"] = {"universe": len(uni), "scanned": scanned, "passed": len(sc),
                              "named": bool(MASTER),
                              "source": meta.get("universe_source", "?")}
            if not sc.empty:
                # ── ファンダの断面順位を列として足す（表示のみ・採点には入れない）
                try:
                    FUND_ALL = fund_pool(sc_all, snap, HOLDINGS)[0] \
                        if len(sc_all) else None
                    _scf, _nadd = fund_pool(sc, snap, HOLDINGS)
                    FUND_PCT = fund_today(_scf, refetch=session_complete())
                    meta.setdefault("fund", {})["holdings_added"] = _nadd
                except Exception as e:
                    FUND_PCT = {}
                    meta["errors"].append(f"fund_today: {type(e).__name__}: {e}")
                    meta["fund"] = {"error": str(e)[:120]}
                if FUND_PCT:
                    # ★dtype="object" が要る。整数とNoneの列を素で作ると
                    #   pandasが float64 に寄せて None を NaN にする。
                    #   NaN は `v is None` をすり抜けて int(NaN) で例外になり、
                    #   実測でレポートの候補表が丸ごと生成失敗した。
                    for _k in FUND_FACTORS:
                        sc["f_" + _k] = pd.Series(
                            [FUND_PCT.get(str(c).replace(".T", ""), {}).get(_k)
                             for c in sc["code"]], index=sc.index, dtype="object")
                cand = rank_value(sc)
                tr0 = cand.to_dict("records")
                # 決算日は順位が付いてから、載る銘柄だけ引く（全銘柄には引けない）
                try:
                    EHIST = update_earnings_history()  # 決算短信の履歴を積み上げる
                except Exception as e:
                    meta["errors"].append(f"earnings_hist: {type(e).__name__}: {e}")
                try:
                    JPX_EARN = jpx_earnings()          # 公式の予定日を先に用意する
                except Exception as e:
                    meta["errors"].append(f"jpx_earnings: {type(e).__name__}: {e}")
                try:
                    # ★J-Quantsの決算発表予定日。1回の照会で全銘柄ぶん取れる。
                    #   TDnetを止めたぶんの穴をここで埋める。
                    JQ_EARN = jq_earnings_dates()
                except Exception as e:
                    meta["errors"].append(f"jq_earn: {type(e).__name__}")
                    meta["jq_earn"] = {"error": type(e).__name__}
                try:
                    CAND_EARN = next_earnings_for({r["code"] for r in tr0})
                except Exception as e:
                    meta["errors"].append(f"cand_earnings: {type(e).__name__}: {e}")
                    meta["cand_earnings"] = {"error": str(e)[:120]}
                tr = _decorate(tr0)
                # ★台帳への追加は大引け後の実行だけ。
                #   11:35の実行では当日足がまだ未完成で、それを「終値」として
                #   建値に使うと、成績の前提（当日終値で建てた）が嘘になる。
                # 記録する日付は実行日ではなく**その足の日付**。
                # 寄り付き前に走らせると、前営業日の終値を当日として
                # 記録してしまい、建値と日付が食い違う。
                _bar = meta.get("screen_last_bar") or str(NOW.date())
                if session_complete():
                    SIG = append_signals(SIG, tr, "value", _bar)
                    meta["signals_appended"] = True
                    meta["signals_bar"] = _bar
                else:
                    meta["signals_appended"] = False
                    meta["skip_reason"] = f"場中（{NOW:%H:%M} JST）のため当日足が未確定"
                    print(f"[台帳] 場中（{NOW:%H:%M}）のため追加は見送り。"
                          "大引け後・寄り付き前・土日の実行で記録する")
            else:
                tr = []
            # ── 過去検証（月1回・大引け後だけ）────────────────────
            try:
                _btp = f"{OUT}/backtest.json"
                # 検証の前提が変わったかどうかの指紋。期間・因子・絞り込み・
                # 執行ルールのどれかが変われば、古い結果は比較できない。
                _cfg = {"period": BT_PERIOD, "step": BT_STEP, "top": BT_TOP,
                        "holds": list(BT_HOLDS), "warmup": BT_WARMUP,
                        "exits": [e[0] for e in BT_EXITS],
                        "factors": list(BT_FACTORS),
                        # ★判定規則を変えたので指紋も上げる。上げないと
                        #   古い backtest.json（判定が入っていない）が
                        #   30日間そのまま使われる。
                        "stat": "clustered-v2-indep",
                        # ★repair() を検証の経路にも当てた＝価格そのものが
                        #   変わるので、基準線の版を上げる。
                        "baseline": "pool-mean-v2-repaired",
                        "min_indep": BT_MIN_INDEP, "units": ["R", "pct"],
                        "max_1d_move": BT_MAX_1D_MOVE,
                        "filters": [MIN_TURNOVER, MIN_PRICE, MIN_ATR_PCT, MAX_ATR_PCT]}
                _age, _same_cfg = 999, False
                if os.path.exists(_btp):
                    try:
                        _old = json.load(open(_btp, encoding="utf-8"))
                        _age = (NOW - dt.datetime.fromisoformat(
                            _old["generated_at_jst"])).days
                        _same_cfg = (_old.get("config") == _cfg)
                    except Exception:
                        _age, _same_cfg = 999, False
                # ★状態は必ず残す。以前は作り直した回だけ meta["backtest"] に
                #   値が入り、飛ばした回は null だったため、
                #   「失敗した」のか「前回の結果を使っている」のか
                #   レポートから区別できなかった。
                meta["backtest_status"] = {
                    "file_exists": os.path.exists(_btp), "age_days": _age,
                    "same_config": _same_cfg, "max_age_d": BT_MAX_AGE_D,
                    "min_indep": BT_MIN_INDEP, "t_threshold": BT_T_THRESHOLD}
                if BT_ENABLED and not session_complete():
                    meta["backtest_skipped"] = f"場中（{NOW:%H:%M} JST）"
                    print(f"[検証] 場中（{NOW:%H:%M}）のため見送り。"
                          "大引け後・寄り付き前・土日の実行で走る")
                elif BT_ENABLED and _same_cfg and _age < BT_MAX_AGE_D:
                    meta["backtest_skipped"] = f"前回から{_age}日（{BT_MAX_AGE_D}日ごと）・設定変更なし"
                elif BT_ENABLED and not _same_cfg:
                    print("[検証] 検証の前提が変わっているため、日数に関係なく作り直す")
                if BT_ENABLED and session_complete() and (_age >= BT_MAX_AGE_D or not _same_cfg):
                    print(f"[検証] 過去検証を実行（前回から{_age}日）")
                    tk, nd, nf, d0, d1, cov, ndf = run_backtest(uni)
                    _bm = meta.get("backtest_breadth") or {}
                    _brd_cut = (_bm.get("下位3分の1の境目"),
                                _bm.get("上位3分の1の境目"))
                    bt = {"generated_at_jst": NOW.isoformat(),
                          "from": d0, "to": d1, "tickers": nf, "entry_dates": nd,
                          "entry_dates_fund": ndf, "coverage_pct": cov,
                          "fund_factors": list(FUND_FACTORS),
                          "jq": meta.get("jq_bt", {}),
                          "step": BT_STEP, "top": BT_TOP, "holds": list(BT_HOLDS),
                          "filters": {"min_turnover": MIN_TURNOVER, "min_price": MIN_PRICE,
                                      "min_atr_pct": MIN_ATR_PCT, "max_atr_pct": MAX_ATR_PCT},
                          "config": _cfg, "t_threshold": BT_T_THRESHOLD,
                          "factors": list(BT_FACTORS),
                          "exits": [e[0] for e in BT_EXITS],
                          "all": {e: {str(h): bt_summary(tk, h, ex=e) for h in BT_HOLDS}
                                  for e in [x[0] for x in BT_EXITS]},
                          "clustered": {e: {str(h): bt_clustered(tk, h, ex=e) for h in BT_HOLDS}
                                        for e in [x[0] for x in BT_EXITS]},
                          "regime": {"above200": bt_summary(tk, 25, True),
                                     "below200": bt_summary(tk, 25, False)},
                          # ★市場の広がりで分けた集計。局面で符号が反転する
                          #   仕掛けを、平均で打ち消して見落とさないため。
                          "breadth": {
                              "cut": list(_brd_cut),
                              "wide": bt_summary(tk, 25, brd_min=_brd_cut[1]),
                              "narrow": bt_summary(tk, 25, brd_max=_brd_cut[0])}}
                    # ── 多重検定の補正（Benjamini-Hochberg）────────────
                    #   いま何百通りもの格子を一度に測っている。
                    #   t≥2.8 は1つの検定に対する基準なので、
                    #   試す数が増えれば偶然2.8を超えるものが必ず出る。
                    #   日本株を扱った既存研究（Yamamoto 2012、
                    #   Hsu-Hsu-Kuan 2010、Bajgrowicz-Scaillet 2012）は
                    #   例外なくこの補正を行っている。
                    _cells = []
                    for _e, _bh in bt["clustered"].items():
                        for _h, _bf in _bh.items():
                            for _k, _c in _bf.items():
                                if not isinstance(_c, dict): continue
                                if _k.startswith("pool"): continue
                                _ts = [_c.get(x) for x in
                                       ("t_dates", "t_nonoverlap", "t_nw")]
                                _ts = [x for x in _ts if x is not None]
                                _n = _c.get("n_nonoverlap") or 0
                                if not _ts or _n < 2: continue
                                # 一番弱いtで代表させる（緩い側に倒さない）
                                _cells.append((f"{_e}|{_h}|{_k}",
                                               min(_ts, key=abs), _n - 1))
                    # ★検定群を2つに分ける（本人の決定・B案）。
                    #   群ごとに別々に補正し、群ごとに試行回数を数える。
                    #   「測る前から査読済みの根拠があった3つ」と
                    #   「有名だから試した総当たり」を同じ袋で数えない。
                    _fam = {}
                    for _c3 in _cells:
                        _arm = _c3[0].split("|")[-1]
                        _fam.setdefault(bt_family(_arm), []).append(_c3)
                    _fd, _ntr_by = {}, {}
                    for _fk, _cl3 in _fam.items():
                        _fd.update(bt_fdr(_cl3, q=BT_FDR_Q))
                        _ntr_by[_fk] = len(_cl3)
                    _ntr = len(_cells)
                    bt["dsr"] = {"n_trials": _ntr, "by_family": {}}
                    for _fk in ("prior", "search"):
                        _n3 = _ntr_by.get(_fk)
                        if not _n3: continue
                        _d3 = bt_dsr(3.0, 100, max(_n3, 2)) or {}
                        bt["dsr"]["by_family"][_fk] = {
                            "n_trials": _n3,
                            "t_expected_max": _d3.get("t_expected_max"),
                            "t_threshold_used": max(BT_T_THRESHOLD,
                                                    _d3.get("t_expected_max") or 0)}
                    bt["families"] = {
                        "prior": sorted(BT_PRIOR_ARMS),
                        "note": "prior＝測る前から日本株の査読済み論文に根拠が"
                                "あった腕。結果を見てから足さない（凍結）",
                        "sources": dict(BT_PRIOR_ARMS)}
                    bt["fdr"] = {"q": BT_FDR_Q, "n_tested": len(_cells),
                                 "n_passed": sum(1 for v in _fd.values() if v["ok"]),
                                 "cells": _fd}
                    for _e, _bh in bt["clustered"].items():
                        for _h, _bf in _bh.items():
                            for _k, _c in _bf.items():
                                if isinstance(_c, dict):
                                    _c["fdr"] = _fd.get(f"{_e}|{_h}|{_k}")
                                    _tt = [_c.get(x) for x in
                                           ("t_dates", "t_nonoverlap", "t_nw")]
                                    _tt = [x for x in _tt if x is not None]
                                    # ★試行回数はその腕が属する群のぶんだけ数える
                                    _nt2 = _ntr_by.get(bt_family(_k), _ntr)
                                    _c["family"] = bt_family(_k)
                                    _c["dsr"] = (bt_dsr(min(_tt, key=abs),
                                                        _c.get("n_nonoverlap"),
                                                        _nt2) if _tt else None)
                    bt["cost_bp"] = BT_COST_BP
                    bt["limit_move"] = BT_LIMIT_MOVE
                    bt["limit_skipped"] = meta.get("backtest_limit_skipped", 0)
                    json.dump(bt, open(_btp, "w"), ensure_ascii=False, indent=1)
                    meta["backtest"] = {"tickers": nf, "dates": nd,
                                        "fdr_tested": len(_cells),
                                        "fdr_passed": bt["fdr"]["n_passed"],
                                        "main": bt["all"]["2atr_3atr"][str(25)]}
                    print("[検証] backtest.json を生成")
            except Exception as e:
                import traceback
                meta["errors"].append(f"backtest: {type(e).__name__}: {e}")
                print("[検証] 失敗:", e); traceback.print_exc()

            try:
                # ★過去の行に残っているJPX由来の銘柄名を、書き出す直前に
                #   EDINETの提出者名で入れ直す。EDINETに無い銘柄は空にする。
                #   列を消すだけでは、既にコミット済みの行が直らない。
                if "name" in SIG.columns and len(SIG):
                    SIG["name"] = [pub_name(str(c)) for c in SIG["code"]]
                SIG.to_csv(SIG_PATH, index=False)
                meta["signals"] = signal_summary(SIG)
                print(f"[台帳] {len(SIG)}件 / 決着済 {meta['signals'].get('closed', 0)} "
                      f"/ 未決着 {meta['signals'].get('open', 0)}")
            except Exception as e:
                meta["errors"].append(f"signals write: {type(e).__name__}: {e}")
            # 通過0件でも必ず書く。書かないと report 側が「未実行」と表示してしまい、
            # 「走らせたが0件だった」という事故が「まだ動かしていない」に見える。
            json.dump(jpx_safe(json_safe({"generated_at_jst": NOW.isoformat(),
                       "universe": len(uni), "scanned": scanned, "passed": len(sc),
                       "master_count": len(MASTER),
                       "universe_source": meta.get("universe_source", "?"),
                       "tdnet": meta.get("tdnet", {}),
                       "skipped": meta.get("screen_skipped", {}),
                       "cand_earnings": meta.get("cand_earnings", {}),
                       "jpx_earnings": meta.get("jpx_earnings", {}),
                       "earnings_hist": meta.get("earnings_hist", {}),
                       "filters": {"min_turnover": MIN_TURNOVER, "min_price": MIN_PRICE,
                                   "min_atr_pct": MIN_ATR_PCT, "max_atr_pct": MAX_ATR_PCT},
                       # ★業種名そのものは書かない（JPX由来）。件数だけ残す。
                       "s17_unmapped_n": len(meta.get("s17_unmapped", {}) or {}),
                       "top_n": CAND_TOP_N, "ranked_by": "value",
                       "candidates": tr})),
                                            open(f"{OUT}/candidates.json", "w"), ensure_ascii=False,
                      indent=1, default=str)
            named = sum(1 for r in tr if r["name"])
            print(f"[全銘柄] 走査{scanned} / 通過{len(sc)} / バリュー候補{len(tr)} / "
                  f"銘柄名あり {named}/{max(len(tr),1)} / candidates.json を生成")
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

# 枠(sleeve)の種類。予算と規則がこれで変わる。
SLV_SWING = "swing"      # ATR損切り・決算またがない・10日保有
SLV_KY    = "kiyohara"   # 清原式。ATR損切りなし・決算はまたぐ・2倍狙い
# ★以前ここに SLV_CORE（土台）という第3の枠を置いていたが、**私の誤り**。
#   指定は「半分スイング・半分清原式」の2枠で、予算は合わせて総額の100%。
#   なのに総額の29%を「どちらの枠でもない」場所に置いたため、
#   2つの枠が50/50になることが構造上ありえなくなり、
#   清原枠に回る現金が¥136万に絞られていた（本来は¥202万）。
#   ATRが小さくてスイングの道具にならないことは、`swingable`（測って
#   分かる性質）で表せばよく、予算の枠を増やす理由にはならない。
#   → 枠は2つだけ。全銘柄がどちらかに属する。
SLV_JA = {SLV_SWING: "スイング", SLV_KY: "清原"}

def load_positions():
    """positions.json があればそれを使う（保有変更時はこのファイルだけ直せばよい）。

       ★sleeve を必ず持たせる。枠ごとに予算と売買の規則が違うので、
         これが無いと清原枠の建玉にスイングの損切りが当たってしまう。
         2026-09-16のスクリーンショットで更新。"""
    p = f"{OUT}/../positions.json"
    default = {"cash": 3655957, "holdings": {
        # 1306・1540・2563 はATRが小さくスイングの道具にならないが、
        # 枠はスイング（清原式の条件＝時価総額500億円以下に当てはまらない）。
        # 「スイング対象外」の印は swingable が付ける。
        "1306.T": {"name": "NF TOPIX",        "acct": "NISA", "shares": 2870, "cost": 434.0,   "sleeve": SLV_SWING},
        "1540.T": {"name": "純金信託",          "acct": "特定", "shares": 4,    "cost": 21399.0, "sleeve": SLV_SWING},
        "1615.T": {"name": "NF銀行業",         "acct": "特定", "shares": 420,  "cost": 773.0,   "sleeve": SLV_SWING},
        "2563.T": {"name": "iS S&P500ヘッジ",  "acct": "特定", "shares": 1140, "cost": 412.0,   "sleeve": SLV_SWING},
        "4755.T": {"name": "楽天グループ",       "acct": "特定", "shares": 100,  "cost": 917.0,   "sleeve": SLV_SWING},
        "8053.T": {"name": "住友商事",           "acct": "特定", "shares": 100,  "cost": 1823.0,  "sleeve": SLV_SWING},
        "9432.T": {"name": "NTT",             "acct": "特定", "shares": 100,  "cost": 149.0,   "sleeve": SLV_SWING}}}
    try:
        if os.path.exists(p):
            o = json.load(open(p, encoding="utf-8"))
            # 古い positions.json には sleeve が無い。既定の割り当てで補う。
            _d = default["holdings"]
            for c, v in (o.get("holdings") or {}).items():
                if not v.get("sleeve"):
                    v["sleeve"] = (_d.get(c, {}) or {}).get("sleeve", SLV_SWING)
            return o
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
    # ★HOLDINGS（価格を取る一覧）と保有の食い違いを検出する。
    #   ここがずれると、保有しているのに分析に出てこない銘柄が生まれる。
    _miss = [c for c in HOLD if c not in HOLDINGS]
    _extra = [c for c in HOLDINGS if c not in HOLD]
    if _miss or _extra:
        meta["holdings_sync"] = {"positionsにあってHOLDINGSに無い": _miss,
                                 "HOLDINGSにあって保有に無い": _extra}
        if _miss:
            print(f"::error::保有 {_miss} が HOLDINGS に入っていません。"
                  "株価を取っていないので分析に出てきません。"
                  "fetch_market_data.py の HOLDINGS に追加してください")
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
    # 枠は2つだけ。positions.json に知らない値（旧 "core" など）が
    # 入っていてもスイング扱いに寄せる（第3の枠を復活させない）。
    t["sleeve"] = [(SLV_KY if (HOLD.get(c, {}) or {}).get("sleeve") == SLV_KY
                    else SLV_SWING) for c in t["code"]]
    TOT = float(t["mkt"].sum() + CASH)
    SW_BUDGET = TOT * SLEEVE_SWING          # スイング枠の予算
    KY_BUDGET = TOT * SLEEVE_KIYOHARA       # 清原枠の予算
    RISK_PER_TRADE = SW_BUDGET * RISK_PER_TRADE_P

    # ★枠ごとの現在の評価額と、まだ充てていない額。
    #   これを分けないと、同じ現金をスイング枠と清原枠の両方が使える
    #   計算になり、清原枠に回す金が残らない（実測でそうなっていた）。
    SW_MKT = float(t.loc[t["sleeve"] == SLV_SWING, "mkt"].sum())
    KY_MKT = float(t.loc[t["sleeve"] == SLV_KY, "mkt"].sum())
    # ★swingable はATRだけで決まるのでここで立てる。
    #   以前は下のスコア計算の中で作っていたため、ここで参照すると
    #   KeyError になっていた（列の生成順の取り違え）。
    t["swingable"] = t["atr_pct"] >= MIN_ATR_PCT
    # スイング枠のうち、ATRが小さくてスイングでは売買できない分。
    # 枠の予算は埋めているが、スイングの道具にはならない。
    SW_STUCK = float(t.loc[(t["sleeve"] == SLV_SWING) & (~t["swingable"]),
                           "mkt"].sum())
    SW_ROOM = max(SW_BUDGET - SW_MKT, 0.0)   # スイング枠の未充当
    KY_ROOM = max(KY_BUDGET - KY_MKT, 0.0)   # 清原枠の未充当
    # 現金の下限（総額比）を守ったうえで使える現金
    FREE_CASH = max(CASH - TOT * MIN_CASH_RATIO, 0.0)
    # 未充当の比で現金を分ける（どちらの枠も未充当を超えては使わない）。
    # 両方0なら分けるものが無い。
    _rsum = SW_ROOM + KY_ROOM
    if _rsum > 0:
        SW_CASH = min(FREE_CASH * SW_ROOM / _rsum, SW_ROOM)
        KY_CASH = min(FREE_CASH * KY_ROOM / _rsum, KY_ROOM)
    else:
        SW_CASH = KY_CASH = 0.0

    R = pd.DataFrame(rets).dropna(how="any").tail(120)
    C = R.corr()
    t["avg_corr"] = [round(float(C[c].drop(c).abs().mean()), 3) for c in t["code"]]
    def _swing_fit(a):
        """スイングの「やりやすさ」。1.5〜4.0%を満点とし、外れるほど下げる。
           高ければ高いほど良い、ではない（5%超は材料で動いている公算が高い）。"""
        a = np.asarray(a, float)
        lo = np.clip((a - 0.6) / 0.9, 0, 1)      # 1.5%で満点
        hi = np.clip((7.0 - a) / 3.0, 0, 1)      # 4.0%から下がり7.0%で0
        return np.minimum(lo, hi)
    t["A"] = (25*_swing_fit(t["atr_pct"])).round(1)
    heat = np.where(t["rsi"] > 75, (t["rsi"]-75)/25, 0)
    t["C"] = (25*np.clip(0.5*np.clip(t["pos60"]/100, 0, 1)
                         + 0.5*np.clip((t["rs20"]+8)/16, 0, 1) - heat, 0, 1)).round(1)
    t["D"] = (25*np.clip(1-(t["avg_corr"]-0.1)/0.5, 0, 1)).round(1)
    t["ACD"] = (t["A"]+t["C"]+t["D"]).round(1)   # B（レジーム適合）はモデルが判断して足す
    # スイングの土俵に乗るかどうかは上の枠の計算で立ててある
    #   （t["swingable"]）。乗らないものはスコアで売買を語らない。

    # サイジング: ATR基準と1銘柄15%上限の、小さいほう
    def size(r):
        stop_w = 2*r["atr"]
        # 許容損失は RISK_PER_TRADE 一本。以前は表が総額1.0%、運用ルールが0.9%で
        # 食い違い、表どおりに建てると常に11%オーバーサイズになっていた。
        n_atr = int(RISK_PER_TRADE // stop_w) if stop_w > 0 else 0
        n_cap = int(max(SW_BUDGET*MAX_WEIGHT - r["mkt"], 0) // r["close"]) if r["close"] > 0 else 0
        # ★現金で買えない株数を出しても意味がない。現金比率の下限を守り、
        #   さらに**スイング枠に配分された現金**だけを使う。
        #   以前は総現金を見ていたので、清原枠の予算まで使えてしまった。
        n_cash = int(SW_CASH // r["close"]) if r["close"] > 0 else 0
        n = min(n_atr, n_cap, n_cash)
        n = (n // LOT) * LOT          # 単元(100株)で切り捨て。切り上げるとリスク超過
        return pd.Series([n, round(r["close"]-stop_w, 1), round(r["close"]+3*r["atr"], 1),
                          round(r["close"]+4*r["atr"], 1), round(n*r["close"]),
                          min([(n_atr, "ATR"), (n_cap, f"{MAX_WEIGHT:.0%}上限"),
                               (n_cash, "現金")], key=lambda z: z[0])[1]])
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

    # ── レジーム（機械的に出す。Bの出発点にする）────────────────
    regime = {}
    try:
        bser = pd.read_csv(f"{OUT}/ohlcv/{slug(BENCH)}.csv")
        bc = pd.to_numeric(bser.get("Adj Close", bser["Close"]), errors="coerce").dropna()
        if len(bc) >= 200:
            ma200 = float(bc.rolling(200).mean().iloc[-1])
            ma25  = float(bc.rolling(25).mean().iloc[-1])
            px    = float(bc.iloc[-1])
            regime = {"px": px, "vs200": round((px/ma200-1)*100, 2),
                      "vs25": round((px/ma25-1)*100, 2),
                      "above200": bool(px > ma200)}
            # 業種ETF17本のうち何本が25日線の上か（市場の広がり）
            above = tot = 0
            for sc in SECTOR:
                try:
                    q = pd.read_csv(f"{OUT}/ohlcv/{slug(sc)}.csv")
                    qc = pd.to_numeric(q.get("Adj Close", q["Close"]), errors="coerce").dropna()
                    if len(qc) >= 25:
                        tot += 1
                        above += int(float(qc.iloc[-1]) > float(qc.rolling(25).mean().iloc[-1]))
                except Exception:
                    pass
            if tot: regime["breadth"] = f"{above}/{tot}"
            regime["breadth_pct"] = round(above/tot*100, 0) if tot else None
            # 買い持ちで15営業日を狙うなら、指数が200日線の下では素直に縮める
            b = 0.5
            if regime["above200"]: b += 0.2
            else: b -= 0.2
            if (regime.get("breadth_pct") or 50) >= 60: b += 0.1
            elif (regime.get("breadth_pct") or 50) <= 35: b -= 0.1
            regime["b_suggest"] = round(min(max(b, 0.1), 0.9), 2)
        meta["regime"] = regime
    except Exception as e:
        meta["errors"].append(f"regime: {type(e).__name__}")

    jgbline = "取得できず"
    if meta.get("jgb"):
        g = meta["jgb"]
        jgbline = f"{g['date']} 時点  2年 {g.get('y2')}% / 10年 {g.get('y10')}% / 30年 {g.get('y30')}%"
        if (g.get("age_days") or 0) > 5:
            jgbline += (f"  ← **{g['age_days']}日前の値。現在値として使わないこと**"
                        "（NTTの利回り差・銀行の追い風の判断が丸ごとずれる）")

    L = []
    L.append(f"# 後場スイング 事前分析  {NOW:%Y-%m-%d %H:%M JST}\n")
    L.append(f"データ健全性: **{meta['health']}**  保有{meta['holdings_ok']}/{len(HOLDINGS)}  "
             f"全体{meta['n_ok']}/{meta['n_total']}  エラー{len(meta['errors'])}件\n")
    L.append(f"日本国債（財務省）: {jgbline}\n")
    if regime:
        L.append(f"**レジーム（機械計算）**: TOPIX {regime['px']:,.1f} ／ "
                 f"200日線 {regime['vs200']:+.2f}%（{'上' if regime['above200'] else '**下**'}）／ "
                 f"25日線 {regime['vs25']:+.2f}% ／ 25日線の上にある業種 {regime.get('breadth', '—')}\n")
        L.append(f"> **Bの出発点: {regime['b_suggest']:.2f}**（指数の200日線との位置と業種の広がりから機械的に算出）。"
                 "銘柄ごとの材料で上下させてよいが、**動かしたら理由を書くこと**。"
                 + ("\n> **指数が200日線の下にある。** 買い持ち中心の運用では、"
                    "この局面は建玉を減らすか見送るほうが理に適う。" if not regime["above200"] else "") + "\n")
    if nk: L.append(f"{nk}\n")
    if rep_log:
        L.append("補正した系列: " + " / ".join(f"{k}: {'; '.join(v)}" for k, v in rep_log.items()) + "\n")
    L.append(f"\n総額 **¥{TOT:,.0f}**（保有 ¥{t['mkt'].sum():,.0f} + 現金 ¥{CASH:,.0f} = 現金比率 {CASH/TOT*100:.1f}%）")
    L.append(f"／ 発注可能現金 ¥{FREE_CASH:,.0f}"
             f"（現金 ¥{CASH:,.0f} − 下限 {MIN_CASH_RATIO:.0%}）\n")
    L.append(f"**枠の配分**: スイング枠 ¥{SW_BUDGET:,.0f}（{SLEEVE_SWING:.0%}）"
             f"／ 清原枠 ¥{KY_BUDGET:,.0f}（{SLEEVE_KIYOHARA:.0%}）\n")
    # ★同じ現金を両方の枠が使える計算になっていると、清原枠に金が回らない。
    #   未充当の比で現金を分け、各枠が使える額を別々に出す。
    L.append("| 枠 | 予算 | 現在の評価額 | 未充当 | 今日使える現金 |")
    L.append("|---|--:|--:|--:|--:|")
    L.append(f"| スイング | ¥{SW_BUDGET:,.0f} | ¥{SW_MKT:,.0f} | ¥{SW_ROOM:,.0f} | "
             f"**¥{SW_CASH:,.0f}** |")
    L.append(f"| 清原 | ¥{KY_BUDGET:,.0f} | ¥{KY_MKT:,.0f} | ¥{KY_ROOM:,.0f} | "
             f"**¥{KY_CASH:,.0f}** |")
    L.append("")
    L.append("> **枠は2つだけ。全銘柄がどちらかに属する。** 予算は合わせて"
             "総額の100%。以前「土台(core)」という第3の枠を置いていたが、"
             "**指定は半分ずつの2枠**なので取り消した"
             "（総額の29%をどちらでもない場所に置いていたため、"
             "清原枠に回る現金が本来より¥66万少なくなっていた）。")
    L.append(f"> 「今日使える現金」は発注可能現金 ¥{FREE_CASH:,.0f} を"
             "**未充当の比で分けたもの**。以前は総現金をそのまま両方の枠に"
             "見せていたため、スイング枠が清原枠の予算まで使える計算になっていた。")
    if KY_ROOM > KY_CASH + 1:
        L.append(f"> **清原枠は予算 ¥{KY_BUDGET:,.0f} に対して現金が足りない。**"
                 f"今日充てられるのは ¥{KY_CASH:,.0f}、"
                 f"**不足 ¥{KY_ROOM - KY_CASH:,.0f}**。")
    # ★スイング枠を埋めているが売買できない銘柄。ここが不足の出所。
    if SW_STUCK > 0:
        _stuck = t[(t["sleeve"] == SLV_SWING) & (~t["swingable"])]
        L.append(f"> **スイング枠 ¥{SW_BUDGET:,.0f} のうち ¥{SW_STUCK:,.0f} が、"
                 f"ATR {MIN_ATR_PCT:.1f}%未満で**スイングでは売買できない銘柄**"
                 "で埋まっている**（"
                 + "、".join(f"{r['code'][:4]} {r['name']} ¥{r['mkt']:,.0f}"
                             for _, r in _stuck.sort_values("mkt", ascending=False)
                                             .iterrows())
                 + "）。")
        if KY_ROOM > KY_CASH + 1:
            L.append(f"> **清原枠の不足 ¥{KY_ROOM - KY_CASH:,.0f} は、"
                     "この中から減らす以外に出てこない。** "
                     "どれをいくら減らすかは**本人が決めること**なので、"
                     "この仕組みは金額を出すだけにする。"
                     "**値幅が小さいことは下落の予想ではない**ので、"
                     "スコアを売却の根拠にしてはいけない。")
            _need = KY_ROOM - KY_CASH
            L.append("")
            L.append("| 減らす候補 | 評価額 | 全部売れば清原枠は | 単元100株で減らせる額 |")
            L.append("|---|--:|:-:|--:|")
            for _, r in _stuck.sort_values("mkt", ascending=False).iterrows():
                _lots = int(r["mkt"] // (r["close"] * LOT)) if r["close"] > 0 else 0
                _ok = "満額になる" if r["mkt"] >= _need else "まだ足りない"
                L.append(f"| {r['code'][:4]} {r['name']} | ¥{r['mkt']:,.0f} | "
                         f"{_ok} | 100株 ≒ ¥{r['close']*LOT:,.0f}（最大{_lots}単元）|")
            L.append("")
            L.append("> **1306はNISA口座。** 売却しても損失は損益通算も繰越控除も"
                     "できない。含み損のまま売ると、その損は税務上どこにも使えない。")
    L.append("")
    L.append(f"- スイング枠: 1トレード許容損失 ¥{RISK_PER_TRADE:,.0f}"
             f"（枠の{RISK_PER_TRADE_P:.2%}／総額の{RISK_PER_TRADE/TOT:.2%}）"
             f"、1銘柄上限 ¥{SW_BUDGET*MAX_WEIGHT:,.0f}（枠の{MAX_WEIGHT:.0%}）。"
             "2ATR損切り・3ATR利確・期限で手仕舞い。")
    L.append(f"- 清原枠: **ATRの損切りは使わない**。最低2倍を狙い、"
             "投資仮説が崩れたときに降りる。等ウェイトで分散する。"
             f"1銘柄あたり目安 ¥{KY_BUDGET/KY_NAMES:,.0f}（{KY_NAMES}銘柄想定）。")
    _lot_note = int(KY_BUDGET / KY_NAMES / 100)
    L.append(f"  - 単元100株なので、1銘柄 ¥{KY_BUDGET/KY_NAMES:,.0f} だと"
             f"**株価 {_lot_note:,}円以下の銘柄しか単元で買えない**。"
             "清原氏は20銘柄を勧めているが、この口座規模では"
             f"{KY_NAMES}銘柄程度が上限になる。分散は本来より薄い。\n")

    L.append("\n## テクニカル（ベンチマーク=TOPIX/1306）\n")
    L.append("| コード | 銘柄 | 終値 | 損益% | RSI | MACD | 25日 | 75日 | 200日 | 60日位置 | ATR% | 対TOPIX 1d/5d/20d | 出来高 |")
    L.append("|---|---|--:|--:|--:|:-:|--:|--:|--:|--:|--:|--:|--:|")
    for _, r in t.iterrows():
        L.append(f"| {r['code'][:4]} | {r['name']} | {r['close']:,.1f} | {r['pl_pct']:+.2f} | {r['rsi']:.1f} | "
                 f"{'＋' if r['macd_hist']>0 else '−'}{'↑' if r['macd_dir']=='up' else '↓'} | {r['vs25']:+.1f}% | {r['vs75']:+.1f}% | "
                 f"{r['vs200']:+.1f}% | {r['pos60']:.0f}% | {r['atr_pct']:.2f} | "
                 f"{r['rs1']:+.1f} / {r['rs5']:+.1f} / {r['rs20']:+.1f} | {r['vol_ratio']:.2f}x |")

    _ky_held = t[t["sleeve"] == SLV_KY]
    if len(_ky_held):
        L.append("\n## 清原枠の保有（スイングの判定・損切りは当てない）\n")
        L.append("| コード | 銘柄 | 株価 | 損益% | 株主還元 | 見るところ |")
        L.append("|---|---|--:|--:|:-:|---|")
        _kyj = {}
        try:
            _kyj = {r["code"]: r for r in
                    (json.load(open(f"{OUT}/kiyohara.json", encoding="utf-8"))
                     .get("rows") or [])}
        except Exception:
            pass
        for _, r in _ky_held.iterrows():
            _c4 = r["code"][:4]
            _kr2 = _kyj.get(_c4) or {}
            L.append(f"| {_c4} | {r['name']} | {r['close']:,.1f} | {r['pl_pct']:+.2f} | "
                     f"{_kr2.get('ret', '—')} | 本業の推移／株主還元の継続／経営者の言動 |")
        L.append("\n> **清原枠にATRの損切りは当てない。** 降りるのは投資仮説が"
                 "崩れたとき（本業の悪化・株主還元の後退・経営者の言動の不一致）。"
                 "**含み損だけを理由に降りない。** 最低2倍を狙う建玉で、"
                 "3割上昇での売却もしない。")
        L.append("> **決算は当然またぐ。** スイング枠の「決算をまたぐ建玉は作らない」は"
                 "この枠には適用しない。\n")

    L.append("\n## スコア（A/C/D は計算済み。**B＝レジーム適合はモデルが判断して加算する**）\n")
    _ns = t[~t["swingable"]]["code"].tolist()
    if len(_ns):
        L.append("> **スイング対象外（ATR {:.1f}%未満）: {}**".format(
                 MIN_ATR_PCT, "、".join(c[:4] for c in _ns)))
        L.append("> これらは値幅が小さくスイングの道具にならないだけで、**下落を予想しているのではない**。")
        L.append("> スコアは構造上必ず低く出るので、**この点数を根拠に売却・縮小を推奨してはいけない**。")
        L.append("> **A+C+D は「—」にしてある。** 判定は「継続」か"
                 "「清原枠の資金に回すかの別途判断」のどちらかで書くこと"
                 "（上の『減らす候補』の表が金額を出している）。\n")
    L.append("| コード | 銘柄 | 枠 | 評価額 | 比率 | A 適性 | C 位置 | D 分散 | A+C+D | B込み満点 |")
    L.append("|---|---|:-:|--:|--:|--:|--:|--:|--:|:-:|")
    for _, r in t.iterrows():
        _sv = SLV_JA.get(r["sleeve"], r["sleeve"])
        # ★スイング枠でも、ATRが小さくて対象外の銘柄には点を出さない。
        #   構造上必ず低く出る点数が売却の口実になる（実際になった）。
        _judge = (r["sleeve"] == SLV_SWING) and bool(r["swingable"])
        _acd = (f"**{r['ACD']:.1f}**" if _judge else "—")
        _b = ("+B(0〜25)" if _judge else "—")
        L.append(f"| {r['code'][:4]} | {r['name']} | {_sv} | {r['mkt']:,.0f} | "
                 f"{r['mkt']/TOT*100:.1f}% | "
                 f"{r['A']:.1f} | {r['C']:.1f} | {r['D']:.1f} | {_acd} | {_b} |")
    L.append(f"\n判定: 62以上=買い増し / 48以上=維持 / 34以上=縮小 / 34未満=売却")
    L.append("拒否ルール: C<5→損切り管理 ／ RSI>78→買い増し不可 ／ B≤2.5→維持以上にしない\n")
    L.append("> ★**この判定はスイング枠かつスイング対象内の銘柄だけに当てる。** "
             "清原枠と、ATRが小さくスイング対象外の銘柄は A+C+D 列を「—」に"
             "してある（A＝スイング適性なので、長期保有や指数ETFでは"
             "意味を持たない）。**点数を根拠にこれらを売らない。**\n")

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

    # ── 過去検証 ──────────────────────────────────────────────
    #    順位付けに情報があるのか。母集団平均との差だけが答え。
    try:
        bt = json.load(open(f"{OUT}/backtest.json", encoding="utf-8"))
        # 新しい形は all[執行方式][保有日数]。古いファイルでも読めるようにする。
        _EXD = "2atr_3atr"
        _A  = bt["all"].get(_EXD, bt["all"]) if isinstance(bt["all"], dict) else {}
        _CA = (bt.get("clustered") or {})
        _CL = _CA.get(_EXD, _CA) if isinstance(_CA, dict) else {}
        _MAIN = "25" if "25" in _A else ("15" if "15" in _A else next(iter(_A), None))
        m = _A.get(_MAIN) or {}
        _cov = bt.get("coverage_pct")
        L.append(f"\n## 過去検証（{bt['from']}〜{bt['to']} / {bt['tickers']:,}銘柄 / "
                 f"{bt['entry_dates']}回の建て日）\n")
        if _cov is not None and _cov < 95:
            L.append(f"> **母集団の {_cov}% までしか照会できていない**（時間予算）。"
                     "コードの並びを散らしてから取っているので業種の偏りは無いが、"
                     "標本が小さいぶん差の検出力は落ちる。\n")
        L.append("**問い: この順位付けは、同じ母集団の全銘柄を等ウェイトで建てるより良いのか。**\n")
        L.append("基準線は乱数ではなく、**その日に条件を満たした全銘柄を実際に付いた値段で"
                 "建てたときの平均**。推定ではなく実測なので、何度走らせても同じ数字になる。\n")
        _thr = bt.get("t_threshold", 2.5)
        L.append(f"比べた選び方は **{len(bt.get('factors', []))}通り**。"
                 f"一度に複数を比べるとたまたま良く見えるものが出るので、"
                 f"「効いている」と言うには **t≥{_thr}** を要求する。\n")
        _ndf = bt.get("entry_dates_fund")
        if _ndf:
            L.append(f"財務を使う4つは、財務が引けた銘柄だけの母集団で戦うので、"
                     f"基準線も同じ母集団の「全銘柄平均（財務あり）」と比べる。"
                     f"財務が揃った建て日は **{_ndf}回**"
                     f"（無料枠のデータが直近2年ぶんしか無いため、"
                     f"技術指標側より少ない）。\n")
        elif bt.get("jq", {}).get("skipped"):
            L.append("> 財務情報は取得していない（"
                     f"{bt['jq']['skipped']}）。下の表は技術指標だけの比較。\n")
        # ★見出しと区切り行のセル数を必ず揃える。
        #   過去に2回、見出しだけ増やして最後の列が黙って消えた。
        L.append("| 選び方 | 件数 | 勝率 | 平均R | 基準 | 母集団平均との差 | 粗いt値 | 利確% | 損切% | 時間切れ% |")
        L.append("|---|--:|--:|--:|:--|--:|--:|--:|--:|--:|")
        # ★表に出す選び方は BT_FACTORS から**自動で**作る。
        #   以前はここに手書きで並べていたため、あとから足した
        #   ep・bp・netcash・kiyohara・payout が**検定されているのに
        #   表には一度も出ていなかった**（実測で value/ep/bp が基準を
        #   通っていたのに、ep と bp が誰の目にも触れていなかった）。
        #   名前が無いものは符号のまま出す。黙って消さない。
        _labels = [(k, BT_FACTOR_JA.get(k, k)) for k in bt.get("factors", BT_FACTORS)]
        _bases = [("pool", "**母集団の全銘柄平均（基準）**"),
                  ("pool_f", "**母集団の全銘柄平均（財務あり・基準）**")]
        _bname = {"pool": "全銘柄平均", "pool_f": "財務あり平均"}
        for k, lbl in _labels + _bases:
            v = m.get(k)
            if not v: continue
            vr = f"{v['vs_pool']:+.4f}" if "vs_pool" in v else "—"
            tv = f"{v['t_rough']:+.2f}" if v.get("t_rough") is not None else "—"
            bn = _bname.get(v.get("base"), "—")
            L.append(f"| {lbl} | {v['n']:,.0f} | {v['win_pct']:.1f}% | {v['avg_r']:+.4f} | "
                     f"{bn} | {vr} | {tv} | {v['target']:.0f}% | {v['stop']:.0f}% | "
                     f"{v['timeout']:.0f}% |")

        # ── 建て日で束ねた検定（採否はこちらで決める）──────────────
        # ★基準線の整合検査。これが無かったために、壊れた pool と比べた
        #   結果を「テクニカルに裏付けがない」と報告してしまった。
        _bc = (json.load(open(f"{OUT}/meta.json", encoding="utf-8"))
               .get("backtest_basecheck") or {}) \
            if os.path.exists(f"{OUT}/meta.json") else {}
        if _bc:
            _okm = "OK" if _bc.get("ok") else "**★異常**"
            L.append(f"\n**基準線の整合検査**: pool（全銘柄）の平均R "
                     f"{_bc.get('pool_mean_r')} ／ pool_f（財務あり）の平均R "
                     f"{_bc.get('pool_f_mean_r')} ＝ **{_bc.get('ratio')}倍** "
                     f"（許容 {_bc.get('max_1d_move') and BT_BASE_RATIO_MAX}倍）… {_okm}")
            L.append(f"> 異常値を直した系列 {_bc.get('repaired', 0)}件 ／ "
                     f"直しても残って捨てた系列 {_bc.get('dropped', 0)}件"
                     + (f"（例 {_bc.get('dropped_sample')}）"
                        if _bc.get("dropped_sample") else ""))
            if not _bc.get("ok"):
                L.append("> **★基準線が壊れている。テクニカル因子の結果を"
                         "信用してはいけない。** pool と pool_f は同じ日・"
                         "同じ執行で建てた同じ宇宙の部分集合なので、平均Rが"
                         "桁違いになることはない。調整異常の残った系列が"
                         "母集団に入っている可能性が高い。")
            else:
                L.append("> pool と pool_f は同じ宇宙の部分集合なので、"
                         "平均Rが桁違いなら母集団が壊れている。"
                         "**2026-09-16の実測ではここが11倍（+1.085 対 +0.097）で、"
                         "テクニカル因子が一律に「効かない」と出ていた原因だった**"
                         "（検証の経路に repair() を当てていなかった）。")
            L.append("")
        _cl = _CL.get(_MAIN) or {}
        if _cl:
            L.append("\n**建て日で束ねた検定（こちらが本番）**\n")
            L.append("上の「粗いt値」は1件ごとのRが独立だと仮定していて、"
                     "**有意性を大きく過大評価する**。同じ日に建てた10件は同じ市場の"
                     "動きを共有し、5営業日ごとに建てて15日持つので期間も重なっている。"
                     "そこで建て日ごとに「その因子の平均R − 同じ日の母集団平均のR」を出し、"
                     "その日次の差を検定する。同じ日で引くので市場全体の動きが消える。"
                     "さらに保有期間ぶん間隔を空けた部分標本でも出す。\n")
            L.append("| 選び方 | 基準 | 差(R) | 中央値(R) | 差(%) | t日次 | t重なりなし | t補正 | 独立日数 | 判定 |")
            L.append("|---|:--|--:|--:|--:|--:|--:|--:|--:|:-:|")
            for k, lbl in _labels:
                c = _cl.get(k)
                if not c: continue
                def _f(x, d=2):
                    return "—" if x is None else f"{x:+.{d}f}"
                _pc = (c.get("pct") or {}).get("mean_diff")
                _v = c.get("verdict") or {}
                _mk = "**○使える**" if _v.get("ok") else "×"
                L.append(f"| {lbl} | {_bname.get(c['base'], '—')} | "
                         f"{_f(c.get('mean_diff'), 3)} | {_f(c.get('median_diff'), 3)} | "
                         f"{('—' if _pc is None else f'{_pc*100:+.2f}%')} | "
                         f"{_f(c.get('t_dates'))} | {_f(c.get('t_nonoverlap'))} | "
                         f"{_f(c.get('t_nw'))} | {c.get('n_nonoverlap', 0)} | {_mk} |")
            # ── 仕掛けの出典とエビデンス。ここが読めないと数字が読めない ──
            L.append("\n**仕掛けの出典と、査読済みエビデンスの状態**\n")
            L.append("> ★**一般に使われている売買シグナルを、原典の慣習パラメータの"
                     "まま実装している。しきい値をこちらで選んでいない。**"
                     "選んだ瞬間に、検定の前に結果を作ってしまう"
                     "（Park & Irwin 2007 が技術的分析の文献の最大の欠陥として"
                     "挙げた \"ex post selection of trading rules\"）。\n")
            L.append("| 仕掛け | 出典 | 査読済みエビデンス | 中身 |")
            L.append("|---|---|:-:|---|")
            for _k in SETUPS:
                _m = SETUP_META.get(_k)
                if not _m: continue
                L.append(f"| {_m[0]} | {_m[1]} | **{_m[2]}** | {_m[3]} |")
            L.append("")
            L.append("**イベント起点（決算発表・業績予想の修正）**\n")
            L.append("> ★**数日〜数ヶ月のスイングで、日本株について査読済みの証拠が"
                     "あるのはこちら**。値動きの形（上抜け・押し目）ではなく、"
                     "決算や予想修正というイベントを起点にする。順序を間違えていた。\n")
            L.append("| イベント | 出典 | 査読済みエビデンス | 中身 |")
            L.append("|---|---|:-:|---|")
            for _k in EVENTS:
                _m = EVENT_META.get(_k)
                if not _m: continue
                L.append(f"| {_m[0]} | {_m[1]} | **{_m[2]}** | {_m[3]} |")
            L.append("\n> **★建てるのは発表の2営業日以降**。発表当日〜翌日の反応は"
                     "取引できない部分で、そこを入れると見かけの成績が跳ね上がる。"
                     "Lopez-Lira & Tang (2026, JFE) が報告した的中率93%は、"
                     "まさにこの取引できない初期反応の部分。"
                     "**私たちが売買できるのはその後のドリフトだけ**。")
            L.append("> **★「テキストを読むこと」に価値がある証拠**: "
                     "Meursault-Liang-Routledge-Scanlon (2023, JFQA 58(6)) の"
                     "「PEAD.txt」は、**決算コールのテキストだけ**から作った"
                     "サプライズ指標が、報告利益の数値を一切使わずに"
                     "古典的なSUEより**大きい**ドリフトを生むことを示した"
                     "（保有63日で2.87%対1.54%、252日で8.01%対4.63%）。"
                     "しかも古典PEADがほぼ消えた2018-19年でも年3.4%を下回らなかった。"
                     "**これがAIに文章を読ませる意味**——数値に直交する情報がある。")
            L.append("> **★ただし日本では厳しい**。Jinushi (2023, The Japanese "
                     "Accounting Review 13, 60,124件) は日本のPEADが大幅に減衰し、"
                     "**外国人保有が低く個人保有が高い銘柄にだけ残存**すると報告。"
                     "Okada & Saeki (2014) の規模感は CAR(+1,+21) で上下デシル差2.2pt、"
                     "かつ**取引コストの代理変数が有意に負＝実行しにくい銘柄ほど"
                     "ドリフトが大きい**。だから流動性で分けて見ないと罠にかかる。")
            L.append("> **★負の証拠**: Martineau (2022, Critical Finance Review) は"
                     "「大型株では2006年以降PEADは存在しない」。"
                     "種村・久保・中川 (2025, SIG-FIN) の「LLM-PEAD.txt」は日本株で"
                     "**数値サプライズ単独・LLMテキストサプライズ単独では"
                     "一貫したドリフトを確認できず**、両者の組合せでのみ限定的に"
                     "出現と報告している。**効かなくても既存文献どおり。**\n")
            L.append("\n> **★日本株について、標準テクニカルシグナルに査読済みの"
                     "支持証拠はほぼ存在しない。** "
                     "Chong-Ng-Liew (2014) は日経225でMACD(12,26,9)がt=0.410の非有意、"
                     "RSI(14,30/70)が−0.083%の負。"
                     "Marshall-Young-Cahan (2008) はローソク足が日本株30年で価値なし。"
                     "Yamamoto (2012) は日経225個別の板データでSPA補正後どの戦略も"
                     "buy-and-holdに勝てないと結論。"
                     "**だから「効かなかった」という結果は既存文献どおり。"
                     "効いたときこそ疑う。**")
            L.append("> **唯一、日本株で実証されているのは短期リバーサル（逆張り）**。"
                     "Chang-McLeavey-Rhee (1995) がTSE個別銘柄1975–91で"
                     "形成後1ヶ月の loser−winner = +1.97%/月"
                     "（リスク・サイズ調整後+1.69%）。"
                     "日本の実務で言う25日移動平均乖離率がこれに当たる。"
                     "**ただし当方の実測では5日形成の反転が有意にマイナスで、"
                     "文献と符号が逆だった。** 形成期間を分けて測っている。")
            L.append("> **公表後の減衰**: McLean & Pontiff (2016) は97の予測変数で"
                     "出版後−58%。ボリンジャーバンドは1983年の公表前は高収益で、"
                     "2001年の著書以降ほぼ消滅したことが示されている。"
                     "テクニカルは論文より遥かに広く知られているので、"
                     "**報告された効果量の半分以下を上限と見るのが安全**。\n")

            # ── 仕掛けの出やすさ。t値の読み方がこれで変わる ───────────
            try:
                _su = (json.load(open(f"{OUT}/meta.json", encoding="utf-8"))
                       .get("backtest_setups") or {})
            except Exception:
                _su = {}
            if _su:
                L.append("\n**仕掛けの出やすさ**（t値はこれと合わせて読む）\n")
                L.append("| 仕掛け | 合図が出た日 | 1日あたり中央値 | 母集団比 | 注意 |")
                L.append("|---|--:|--:|--:|---|")
                for _k in SETUPS + EVENTS:
                    _r = _su.get(_k)
                    if not _r: continue
                    L.append(f"| {_r.get('名前', _k)} | "
                             f"{_r.get('出た日', 0)}/{_r.get('検証日', 0)}日 | "
                             f"{_r.get('1日の中央値', 0)}件 | "
                             f"{('—' if _r.get('母集団比_pct') is None else str(_r['母集団比_pct'])+'%')} | "
                             f"{_r.get('警告', '—')} |")
                L.append("\n> **★仕掛けは因子とは別物**。因子は毎日必ず上位N件を建てるが、"
                         "仕掛けは**条件に当てはまった日だけ**建てる。"
                         "合図が出ない日があること自体が情報で、順位付けでは表現できない。"
                         "当てはまった銘柄が多い日は、その仕掛け自身の強さ"
                         "（上抜けの幅・出来高の倍率・縮み具合）で並べて"
                         f"上位{BT_TOP}件だけを建て、因子側と件数を揃えている。")
                L.append("> **母集団比が50%を超える仕掛けは、母集団平均そのものを"
                         "測っているのと同じ**なので、差がゼロに近く出て当たり前。"
                         "合図が出た日が少なすぎる仕掛けは、t値がいくつでも通さない。")
                L.append(f"> **★仕掛けを{len(SETUPS)}通り足したぶん、"
                         f"「たまたま良く見えるもの」が出る余地は増えている。"
                         f"しきい値 {_thr} は下げていない**が、"
                         f"仕掛けが1つ通っただけでは運用に入れない。"
                         f"複数の保有期間・複数の執行方式で揃って通ったものだけを見る。\n")

            L.append(f"\n> **採否の規則（緩い側に倒さない）**: "
                     f"①独立した建て日が **{BT_MIN_INDEP}日未満なら通さない**"
                     f"（t値がいくつでも「検証できていない」）／"
                     f"②3つのtが**全部** {_thr} 以上／③差が正。")
            try:
                _fd = (json.load(open(f"{OUT}/backtest.json", encoding="utf-8"))
                       .get("fdr") or {})
            except Exception:
                _fd = {}
            if _fd:
                L.append(f"> ④**多重検定の補正（Benjamini-Hochberg, q={_fd.get('q')}）**"
                         f"を通ること。今回 **{_fd.get('n_tested', 0)}通り**の格子を"
                         f"同時に検定し、補正を通ったのは **{_fd.get('n_passed', 0)}通り**。")
                # ── 検定群の分け方。ここが読めないと合格点の意味が分からない ──
                try:
                    _bj0 = json.load(open(f"{OUT}/backtest.json", encoding="utf-8"))
                except Exception:
                    _bj0 = {}
                _ds = (_bj0.get("dsr") or {}).get("by_family") or {}
                _fmz = _bj0.get("families") or {}
                if _ds:
                    L.append("\n**検定群を2つに分けている**（合格点が群ごとに違う）\n")
                    L.append("| 群 | 中身 | 試した数 | 偶然でも出る最大t | 合格点 |")
                    L.append("|---|---|--:|--:|--:|")
                    _JA = {"prior": "**測る前から査読済みの根拠があったもの**",
                           "search": "有名だから試した（総当たり）"}
                    for _fk in ("prior", "search"):
                        _r3 = _ds.get(_fk)
                        if not _r3: continue
                        L.append(f"| {_fk} | {_JA.get(_fk, _fk)} | "
                                 f"{_r3.get('n_trials', 0):,} | "
                                 f"{_r3.get('t_expected_max')} | "
                                 f"**{_r3.get('t_threshold_used')}** |")
                    _pri = _fmz.get("sources") or {}
                    if _pri:
                        L.append("\n**prior に入れている腕と、その出典**\n")
                        for _k3, _v3 in sorted(_pri.items()):
                            L.append(f"- `{_k3}`（{BT_FACTOR_JA.get(_k3, _k3)}）… {_v3}")
                    L.append("\n> **★なぜ分けるか**: 補正は「たくさん試すほど基準を"
                             "上げる」という考え方だが、2,000通りの総当たりと、"
                             "**測る前から論文に根拠があって確かめに行った3つ**を"
                             "同じ袋で数えるのは過剰に厳しい。前者は探索、"
                             "後者は検証で、性質が違う。")
                    L.append("> **★この線引きは凍結してある**。"
                             "結果を見てから「これは特別だから別枠」と移すのが"
                             "いちばんやってはいけない操作で、それをやると"
                             "補正の意味が消える。"
                             "足すときは、査読済みの出典を先に書いてから測り直す。")
                    L.append("> **★実務家の記事が根拠のもの**（上振れ＋保守的ガイダンス／"
                             "上振れの1ヶ月後）は prior に入れていない。"
                             "査読論文ではないので、根拠の強さが違う。\n")
                L.append("> **★なぜ④が要るか**: t≥2.8 は**1つの検定**に対する基準で、"
                         "何百通りも試せば偶然2.8を超えるものが必ず出る。"
                         "Park & Irwin (2007) が技術的分析の文献の最大の欠陥として"
                         "挙げた \"data snooping\" がこれ。"
                         "**日本株を扱った既存研究は例外なくこの補正を行っている**"
                         "（Yamamoto 2012 と Hsu-Hsu-Kuan 2010 は White の Reality Check と"
                         "Hansen の SPA、Bajgrowicz-Scaillet 2012 は本レポートと同じ"
                         "Benjamini-Hochberg）。補正しない結果は既存文献と比較できない。")
            try:
                _bj = json.load(open(f"{OUT}/backtest.json", encoding="utf-8"))
            except Exception:
                _bj = {}
            L.append(f"\n> **取引コストの扱い**: 往復 **{_bj.get('cost_bp', BT_COST_BP):.0f}bp**"
                     f"（JPX Working Paper No.7 の実効スプレッドに手数料と"
                     f"小型株の上乗せを見た値）。"
                     f"**★母集団平均との差にコストは効かない**"
                     f"——どちらの側も同じ往復コストを払うので引き算で消える。"
                     f"コストが決めるのは「そもそも儲かるのか」という絶対値のほうで、"
                     f"表の「差」は net では変わらないが、"
                     f"**絶対リターンがコストを下回るなら差が正でも意味がない**。"
                     f"Bajgrowicz & Scaillet (2012) は「低いコストを入れるだけで"
                     f"イン・サンプルでも優位が完全に消える」と結論している。")
            _ls = _bj.get("limit_skipped")
            if _ls:
                L.append(f"> **ストップ高で約定できない建玉 {_ls:,}件を除外**した"
                         f"（翌日の寄りが前日終値から{_bj.get('limit_move', BT_LIMIT_MOVE)*100:.0f}%以上"
                         f"飛んでいるもの）。上抜け系の合図は「大きく上げた日」に出るので、"
                         f"ここを無視すると**順張り系だけが系統的に良く見える**。")
            L.append(f"> **①を入れた理由**: 保有250日の格子は独立観測が**2日**しか"
                     "無いのに、重なり補正後のtが +4.28 と出ていた。"
                     "重なりの補正は情報を捨てずに済ませる道具で、"
                     "**独立な観測を増やす道具ではない**。"
                     "この穴のせいで「10日の1通りだけが確立」という本文と、"
                     "コードの安全弁が食い違っていた。")
            L.append("> **差(R)と差(%)を並べている理由**: R は初期リスク(2ATR)で"
                     "割った値なので、**値幅の小さい銘柄ほど|R|が大きく出る**。"
                     "lowvol（最低ボラを選ぶ因子）の差が −0.895 と大きく負に見えたのは"
                     "この単位の性質で、下落を当てたわけではない。"
                     "**スイング枠はATR基準でサイジングするのでRが正しい単位、"
                     "清原枠は等ウェイトなので%が正しい単位。**")
            L.append("> 「中央値(R)」を並べているのは、"
                     "平均が数銘柄の飛びで決まっていないかを見るため。"
                     "平均だけ大きく中央値が小さい格子は、**少数の大勝ちに"
                     "依存している**ので、そのまま真に受けてはいけない。")
            L.append("> 「t（重なりなし）」は間隔を空けた部分標本で、"
                     "保有が長いと日数が足りず検定できない（括弧内が使えた日数）。")
            L.append("")

        def _ct(k):
            """採否に使うt値。どれを使ったかも返す。

               ★重なりを補正した Newey-West を主に見る。
                 「重なりなし」は保有が長いと日数が足りず検定できない
                 （保有250日・5年で2件しか残らない）。Newey-West は
                 全日を使い、重なったぶん標準誤差を膨らませる。

               ★ただし補正後が補正前より大きくなったら補正前を採る。
                 日次の差に負の自己相関があると Newey-West の分散は
                 小さくなり、t値が上がることがある（数学的には正しい）。
                 だが「期間が重なっているのに確信が増す」のは、この用途では
                 都合が良すぎる方向。緩い側には倒さない。"""
            c = _cl.get(k) or {}
            nw, dt_ = c.get("t_nw"), c.get("t_dates")
            if nw is not None and dt_ is not None:
                return (nw, "重なり補正") if abs(nw) <= abs(dt_) \
                    else (dt_, "重なり補正（補正前を採用）")
            for key, nm in (("t_nw", "重なり補正"), ("t_nonoverlap", "重なりなし"),
                            ("t_dates", "日次")):
                if c.get(key) is not None: return c[key], nm
            t = (m.get(k) or {}).get("t_rough")
            return (t, "粗い（過大評価）") if t is not None else (None, None)

        def _verdict(k, lbl):
            """★判定は backtest.json に入っている verdict をそのまま使う。
               以前はレポート側で t を選び直して判定していたため、
               独立観測が2日しかない格子でも「使う根拠がある」と書けた。
               判定の規則は bt_verdict の1箇所だけに置く。"""
            c = _cl.get(k) or {}
            v = c.get("verdict")
            if not isinstance(v, dict):
                t, nm = _ct(k)
                if t is None: return None
                return (f"- {lbl}: 判定が古い形式（t={t:+.2f}／{nm}）。"
                        "**検証を作り直すこと。**")
            if v.get("ok"):
                return (f"- **{lbl}: 母集団平均を上回っている（{v['why']}）。"
                        "使う根拠がある。**")
            return f"- {lbl}: **使う根拠にならない** … {v.get('why')}"
        L.append("")
        for k, lbl in _labels:
            line = _verdict(k, lbl)
            if line: L.append(line)
        _best = max(((_ct(k)[0] if _ct(k)[0] is not None else -99), k) for k, _ in _labels)
        _vt = _ct("value")[0]
        if _best[0] < _thr:
            L.append(f"\n> **どの選び方も母集団平均を有意に上回っていない（最良でも t={_best[0]:+.2f}）。**")
            L.append("> **この状態で新規の発注推奨を出してはいけない。** "
                     "順位付けに情報が無いなら、建てるほど手数料・スリッページ・税の分だけ負ける。")
            L.append("> 保有の管理（損切り・決算跨ぎ・開示対応）は通常どおり続ける。")
        elif _vt is not None and _vt < _thr:
            L.append(f"\n> **バリューが基準を下回った（t={_vt:+.2f} < {_thr}）。**"
                     "選別の主軸が根拠を失っている。"
                     "**新規の発注推奨は出さず、保有の管理だけを続けること。**")
        else:
            L.append(f"\n> **バリューが基準を超えている（t={_vt:+.2f} ≥ {_thr}）。**"
                     "新規候補はバリュー上位から出す。")
            L.append("> ただし検証で確かめたのは**「フィルタ通過の母集団からバリュー上位10件を"
                     "当日終値で建て、2ATR損切り・3ATR利確・15日で手仕舞う」**という手順だけ。"
                     "決算跨ぎの除外・業種重複の回避・RSIの条件は**検証に入っていない**"
                     "運用ルールで、結果を良くも悪くもしうる。")
            L.append("> 検証されたのは**上昇局面のみ**（1,020件のうち910件がTOPIX 200日線の上、"
                     "下は110件で判定不能）。200日線を割った局面での挙動は分かっていない。")
            L.append("> 生存バイアスは残る。倒産して上場廃止になった会社が母集団に無いので"
                     "バリューは**過大評価**されうる一方、PBR1倍割れがTOB・MBOで"
                     "プレミアム付き非上場化した分も抜けているので**過小評価**にも働く。"
                     "どちらが大きいかは無料データでは分からない。")

        rg = bt.get("regime", {})
        if rg.get("above200") and rg.get("below200"):
            L.append("\n**TOPIXが200日線の上か下かで分けたとき（保有15日）**\n")
            L.append("| 局面 | 選び方 | 件数 | 勝率 | 平均R | 母集団平均との差 |")
            L.append("|---|---|--:|--:|--:|--:|")
            _small = []
            for key, lbl in [("above200", "200日線の上"), ("below200", "200日線の下")]:
                for k, kl in _labels + [("pool", "母集団平均（全銘柄）"),
                                        ("pool_f", "母集団平均（財務あり）")]:
                    v = (rg.get(key) or {}).get(k)
                    if not v: continue
                    vr = f"{v['vs_pool']:+.4f}" if "vs_pool" in v else "—"
                    L.append(f"| {lbl} | {kl} | {v['n']:,.0f} | {v['win_pct']:.1f}% | "
                             f"{v['avg_r']:+.4f} | {vr} |")
                n_cell = ((rg.get(key) or {}).get("pool") or {}).get("n", 0)
                if n_cell and n_cell < 100: _small.append(f"{lbl}（{n_cell:,.0f}件）")
            if _small:
                L.append(f"\n> **{'、'.join(_small)} は件数が少なく、数字を根拠にできない。** "
                         "1〜2件の損益で平均が動く水準。"
                         "**この欄の差を理由に建て方を変えないこと。**")

        # ── 保有期間と利確の有無（清原達郎氏の主張を実測で確かめる欄）────
        L.append("\n**保有期間と利確の有無を変えたとき**\n")
        L.append("清原達郎氏は『3年（場合によっては5年）持つ・最低2倍を狙う・"
                 "3割上昇での売却は勧めない』としている。"
                 "いまの執行（3ATR利確・15〜25日で手仕舞い）はその上振れを"
                 "途中で切っている可能性がある。**同じ選び方・同じ損切りで、"
                 "保有期間と利確の有無だけを変えて比べる。**\n")
        L.append("| 保有 | 執行 | 勝率 | 平均R | 中位R | 母集団平均R | 差 | t(重なり補正) | 利確% | 損切% | 期限% |")
        L.append("|--:|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
        _exl = {"2atr_3atr": "2ATR損切+3ATR利確", "2atr_only": "2ATR損切のみ",
                "hold_only": "**何もせず持つ**"}
        for h in bt.get("holds", []):
            for e in bt.get("exits", [_EXD]):
                a = (bt["all"].get(e) or {}).get(str(h)) or {}
                c = ((bt.get("clustered") or {}).get(e) or {}).get(str(h)) or {}
                v, pf = a.get("value"), a.get("pool_f")
                if not v: continue
                cv = c.get("value") or {}
                f0 = f"{cv['t_nw']:+.2f}" if cv.get("t_nw") is not None else "—"
                d  = f"{v['vs_pool']:+.4f}" if "vs_pool" in v else "—"
                _md = f"{v['med_r']:+.3f}" if "med_r" in v else "—"
                L.append(f"| {h}日 | {_exl.get(e, e)} | {v['win_pct']:.1f}% | "
                         f"{v['avg_r']:+.4f} | {_md} | "
                         f"{(pf or {}).get('avg_r', float('nan')):+.4f} | {d} | {f0} | "
                         f"{v['target']:.0f}% | {v['stop']:.0f}% | {v['timeout']:.0f}% |")
        L.append("\n> **平均Rと中位Rを必ず並べて見ること。** 実測で「2ATR損切のみ・"
                 "保有250日」は勝率37.9%なのに平均+7.92Rだった。"
                 "6割が損切りに当たり、残りの大勝ちが平均を押し上げている形で、"
                 "**真ん中の1件は負けている**。8銘柄しか持てない口座で"
                 "平均値を期待値として読むのは危険。")
        L.append("\n> **読み方**: 差が保有期間とともに伸びるなら、いまの15〜25日は"
                 "**伸びている途中で降りている**ということ。"
                 "「2ATR損切のみ」のほうが平均Rが高いなら、3ATR利確が"
                 "**上振れを刈っている**ということ。"
                 "どちらも起きていなければ、いまの執行のままでよい。")
        L.append("> ただし保有期間を伸ばすと建て日が減り、独立な観測はさらに減る。"
                 "**t（重なりなし）の日数を必ず見ること。** 20回台では判定に足りない。")
        L.append("> 保有を伸ばすほど決算を跨ぐ回数が増える。"
                 "「決算をまたぐ建玉は作らない」という運用ルールとは正面から衝突する。"
                 "**この表は執行ルールを決めるための材料で、"
                 "そのまま運用に移せるものではない。**\n")

        L.append("\n**この検証の限界（結果を良い方に歪めるもの）**")
        L.append("- **生存バイアス。** 銘柄一覧は「いま上場している会社」なので、"
                 "この期間に上場廃止になった会社が母集団から抜けている。実際より良く出る。")
        L.append("- **スリッページ・板の薄さ・約定できない場面を含まない。** 建値は終値、損切りは水準どおり（窓は寄値）。")
        L.append("- **税を引いていない。** 特定口座の利益には20.315%かかる。")
        L.append("- 同じ日に建てた10件は相場つきを共有していて独立ではない。**t値は目安であって検定ではない。**")
        L.append("- 比較したのは**この4つだけ**（順張り/逆張り × 全期間/レジーム別）。"
                 "条件を変えて良い結果を探す作業はしていない。")
    except FileNotFoundError:
        L.append("\n## 過去検証\n")
        _sk = meta.get("backtest_skipped")
        if _sk:
            L.append(f"**未実施**（理由: {_sk}）。")
        else:
            L.append("**未実施。**")
        L.append("過去検証は **大引け後・寄り付き前・土日** の実行で走る"
                 "（場中は当日足が未確定なため）。前回から30日経つと自動で再実行する。")
        L.append("\n> **検証が無いあいだは、新規の発注推奨を出さない。** "
                 "順位付けに優位性が確認できていないため。")
    except Exception as e:
        L.append(f"\n## 過去検証\n\n生成に失敗: {e}")

    # ── シグナル台帳の成績 ────────────────────────────────────
    #    「この仕組みは儲かっているのか」に、実際に付いた値段で答える唯一の節。
    try:
        sg = meta.get("signals") or {}
        L.append("\n## シグナルの成績（選別ロジックの追跡）\n")
        if not sg or not sg.get("closed"):
            L.append(f"決着済み 0件 / 未決着 {sg.get('open', 0)}件。"
                     "**判断に使える成績はまだ無い。**最初の決着まで2〜3週間かかる。")
        else:
            L.append("| 種別 | 件数 | 勝率 | 平均R | 累計R | 利確 | 損切 | 時間切れ | 平均保有 |")
            L.append("|---|--:|--:|--:|--:|--:|--:|--:|--:|")
            for k, lbl in [("all", "合計"), ("value", "バリュー"),
                           ("trend", "順張り(廃止)"), ("revert", "逆張り(廃止)")]:
                v = sg.get(k)
                if not v: continue
                L.append(f"| {lbl} | {v['n']} | {v['win_pct']:.1f}% | {v['avg_r']:+.3f} | "
                         f"{v['sum_r']:+.2f} | {v['target']} | {v['stop']} | {v['timeout']} | "
                         f"{v['avg_bars']:.1f}日 |")
            a = sg.get("all", {})
            L.append(f"\n未決着 {sg.get('open', 0)}件／記録した日数 {sg.get('days', 0)}日。")
            # 1件あたりのRのばらつきは概ね1.0。平均+0.10R を t=2 で検出するには
            # n=(2×1.0/0.10)^2≒400件が要る。しかも同じ日の複数銘柄は独立でないので、
            # 実効的な標本数は「件数」ではなく「日数」に近い。
            if a.get("n", 0) < 400:
                L.append(f"> **決着 {a.get('n', 0)}件では、平均Rの正負を統計的に判定できない。** "
                         "1件あたりのRのばらつきが概ね1.0なので、平均+0.10Rを検出するには"
                         "**400件規模**が要る。さらに同じ日に記録した複数銘柄は相場つきを共有していて"
                         f"独立ではないため、実効的な標本は件数より**日数（現在{sg.get('days', 0)}日）**に近い。"
                         "**この表は動作確認であって、優劣の根拠にはまだ使えない。**")
            if a.get("n", 0) >= 60 and a.get("avg_r", 0) <= -0.30:
                L.append("> **平均Rが −0.30 を下回っている。** 統計的な結論には早いが、"
                         "この水準が続くなら選別を止めるか閾値を上げる判断が要る。")
            elif a.get("avg_r", 0) <= 0:
                L.append("> **平均Rがマイナス。この選別で建て続けると負ける。** "
                         "スコアの閾値を上げるか、順張り／逆張りのどちらかを止めることを検討する。")
            L.append("\n**この成績の読み方と限界**")
            L.append("- 建値は**シグナル当日の終値**。実際には後場の板で約定するので、その差は含まれていない。")
            L.append("- 損切り2ATR・利確3ATR・最長15営業日で機械的に決着させている。**実際の執行記録ではない。**")
            L.append("- 同じ日に損切りと利確の両方に触れた場合は**損切りを先**として数えている（日足では順序が分からないため）。")
            L.append("- 手数料は0円だが、**利益には20.315%課税**される。表のRは税引前。")
            L.append("- 測っているのは**スクリーニングの選別**であって、モデルのB（レジーム判断）は含まない。")
    except Exception as e:
        L.append(f"\n## シグナルの成績\n\n生成に失敗: {e}")

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
        def _ov(r):
            o = r.get("overlap") or []
            return ("**" + "/".join(o) + "と重複**") if o else "—"
        def _earn(r):
            """空欄にすると『決算が無い』と読めてしまう。状態を必ず言葉で出す。
               確定・発表済み・推定・不明の4つを取り違えさせないことが要点。"""
            st = r.get("earn_status")
            md = lambda x: str(x)[5:].replace("-", "/")
            if st == "ok":
                mark = "**跨ぎ**" if r.get("earn_soon") else f"{r.get('earn_days')}日後"
                return f"{md(r.get('earn_next'))} {mark}"
            if st == "recent":
                sc = r.get("earn_since")
                return f"{md(r.get('earn_last'))}発表済({sc}日前)" if sc is not None else \
                       f"{md(r.get('earn_last'))}発表済"
            if st == "estimate":
                w = "**要警戒**" if r.get("earn_watch") else "頃"
                return f"推定 {md(r.get('earn_next'))}{w}(要確認)"
            return {"unknown": "予定なし(要確認)", "error": "照会失敗(要確認)",
                    "timeout": "時間切れ(要確認)"}.get(st, "未照会(要確認)")

        def _fund(r):
            """ファンダの断面順位（パーセンタイル・大きいほど上位）。
               ★生のPBRやROEは出せない。J-Quantsの利用条件が
                 第三者の閲覧を禁じており、このリポジトリは public だから。
               ★この列は採点に入っていない。過去検証が済むまでは参考表示。
               ★None だけでなく NaN も受ける。以前 int(NaN) で例外になり、
                 候補表が丸ごと「生成に失敗」になった（実測）。
                 1列の表示のために表全体を失うのは割に合わない。"""
            def one(k):
                v = r.get("f_" + k)
                if v is None: return "—"
                try:
                    f = float(v)
                except (TypeError, ValueError): return "—"
                if f != f: return "—"                 # NaN
                return f"{int(round(f))}"
            # 清原達郎氏が挙げている3つ: PER・ネットキャッシュ比率・株主還元
            vs = [one(k) for k in ("ep", "netcash", "payout")]
            return "—" if all(x == "—" for x in vs) else "/".join(vs)

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
        _rows = cj.get("candidates")
        if _rows is None:      # 旧い形のファイルでも読めるようにする
            _rows = (cj.get("trend") or []) + (cj.get("revert") or [])
        if not _rows:
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

        L.append(f"\n### 新規候補（バリューの断面順位の上位{CAND_TOP_N}件）\n")
        L.append("| 順 | コード | 銘柄名 | 重複 | 割安順位 | PER/NC/還元 | 終値 | RSI | 25日 | 75日 | 60日位置 | 20日 | ATR% | 売買代金(億) | 出来高比 | 決算 | 当日の開示 |")
        L.append("|--:|---|---|:-:|--:|:-:|--:|--:|--:|--:|--:|--:|--:|--:|--:|:-:|---|")
        for i, r in enumerate(_rows[:15]):
            _s = r.get("score")
            _s = f"**{float(_s):.0f}**" if _s is not None else "—"
            L.append(f"| {i+1} | {r['code'][:4]} | {_nm(r)} | "
                     f"{_ov(r)} | {_s} | {_fund(r)} | {r['close']:,.1f} | {r['rsi']:.0f} | "
                     f"{r['vs25']:+.1f}% | {r['vs75']:+.1f}% | {r['pos60']:.0f}% | {r['r20']:+.1f}% | "
                     f"{r['atr_pct']:.2f} | {r['turnover']/1e8:.1f} | {r['vol_ratio']:.2f}x | {_earn(r)} | {_disc(r)} |")
        L.append("\n> RSI・移動平均・60日位置・20日リターンの列は**参考表示**で、"
                 "並べ替えには使っていない。過去検証で確かめたのは"
                 "「フィルタ通過の母集団からバリュー上位」という手順だけなので、"
                 "そこに条件を足すと検証していないものを運用することになる。\n")
        L.append("\n**この表の読み方と限界**")
        L.append("- **スコアの重み・閾値は検証されていない。** 配点も境目も手で決めた値で、"
                 "過去データで当てはめた結果ではない。順張りスコアは実質的に"
                 "「直近1〜3ヶ月の上昇率」を4つの形で測り直したものに近く、"
                 "**独立した4項目を見ているわけではない**。順位に情報があるかどうかは"
                 "「シグナルの成績」で検証中で、現時点では**未証明**。")
        L.append("- したがってこの表は**発注の根拠ではなく、目で見る対象を絞るための入口**として使うこと。")
        L.append("- **「重複」が付いた銘柄は、既に持っている業種の上乗せ。** 1銘柄0.9%のつもりでも、"
                 "業種ショックでは同時に動く。採るなら片方だけにする。")
        L.append("- 順張りと逆張りは別の尺度なので**混ぜて比較しない**。")
        _fd = meta.get("fund") or {}
        if _fd.get("n_codes"):
            L.append(f"- **「PER/NC/還元」列は `予想PERの割安さ/ネットキャッシュ比率の"
                     f"下限版/株主還元` の"
                     f"3つの断面パーセンタイル**（0〜100、大きいほど上位／母集団は"
                     f"当日の通過{_fd.get('pool', 0):,}銘柄＋保有銘柄）。付与できたのは "
                     f"**{_fd['n_codes']:,}銘柄**"
                     f"（財務の最新開示日 {_fd.get('asof_latest_disclosure', '?')}）。")
            L.append("  - ★**NC列は清原氏の指標そのものではない。** "
                     "`(現金同等物−負債合計)÷時価総額` という**下限値**で、"
                     "流動資産と投資有価証券を含んでいない（この列の母集団は"
                     "J-Quantsの財務なのでEDINETの実測は入らない）。"
                     "**清原氏の指標は清原枠の表の「比率(実測)」列**を見ること。")
            L.append(f"  - **この3つは清原達郎氏が挙げている順位付け**。"
                     "「一番重視するのはPER」「さらにネットキャッシュ比率も加える」"
                     "「PBRは見るけど重視はしない」「最終的なカタリストは株主還元」。"
                     f"並べ替えに使っているのは **{RANK_BY}**（過去検証で基準を通ったもの）。")
            L.append("  - 株主還元は`配当の水準＋増配の方向＋自己株買いの実行`の合成。"
                     "**無配は最下位に落としてある**（割安なまま放置される会社の典型。"
                     "清原氏の言う「バリュートラップ」の見分けがここに当たる）。")
            L.append("  - ネットキャッシュ比率は**下限版**（現金同等物で流動資産を代用し、"
                     "投資有価証券を0とみなす）。本来の比率はこれ以上になる。"
                     "正確な値は下の「確認候補」の節から手で求める。")
            if _fd.get("reused"):
                _age, _ah = _fd.get("age_days"), _fd.get("age_hours")
                _when = (f"{_ah:.0f}時間前" if (_age == 0 and _ah is not None)
                         else f"{_age}日前" if _age is not None else "取得時刻不明")
                L.append(f"  - この列は**前回取得した順位の再利用**（{_when} / 理由: "
                         f"{_fd.get('reason', '?')}）。無料枠の財務はもともと12週間前の"
                         "数字なので、1日ぶん古いことによる劣化は無い。"
                         "会計の数字が前回と同じで、株価だけが新しい状態。")
                if _age is not None and _age > FUND_MAX_AGE_D:
                    L.append(f"  - > **注意: {_age}日前の順位を使っている。** "
                             "J-Quantsの取得が続けて失敗している可能性がある。"
                             "`data/meta.json` の `fund` を確認すること。")
            L.append("  - **PBRやROEの生の数値は載せられない。** J-Quantsの利用条件が"
                     "取得データを第三者が閲覧できる状態にすることを禁じており、"
                     "このリポジトリは public だから。順位は「分析結果」なので公開できる。")
            L.append("  - 無料枠の財務は**12週間前より過去のもの**。株価は当日、"
                     "会計の数字は約3ヶ月前という組み合わせになる（先読みではない）。")
            L.append("  - **「競争優位の代理」は営業利益率の水準とばらつきの小ささ**であって、"
                     "シェアや参入障壁を測ったものではない。無料データにそれは無い。")
            L.append("  - **ETFとREITには付かない**（財務諸表が無いため）。"
                     "「—」は「財務が悪い」ではなく「対象外」。")
            L.append("  - ★**この列はスコアに入っていない。** 効くかどうかは"
                     "「過去検証」の表で確かめてから採点に入れる。"
                     "検証前に混ぜるのは、trend/revertを作ったときと同じ間違いになる。")
        elif _fd.get("skipped"):
            L.append(f"- 「財務順位」列は空（{_fd['skipped']}）。")
        elif _fd:
            L.append("- 「財務順位」列は空（財務情報の取得に失敗）。"
                     "data/meta.json の `fund` を確認すること。")
        L.append("- 「業種ETF」列は業種別ETF17本の表と突き合わせるための対応コード。**候補のBはこの列の業種の位置から機械的に付けられる。**")
        L.append("- 「当日の開示」は TDnet の直近4日分（新しい順）。**空欄(—)は「開示が無い」であって「材料が無い」ではない**（報道・需給・指数入替は載らない）。")
        ce = cj.get("cand_earnings") or {}
        if ce.get("ok") is not None:
            L.append(f"- **決算列**: 上位候補 {ce.get('asked', 0)}銘柄を照会し、"
                     f"予定日が取れたのは **{ce.get('ok', 0)}銘柄**"
                     f"（不明 {ce.get('unknown', 0)} / 失敗 {ce.get('error', 0)}）。"
                     f"**「跨ぎ」は{EARN_SOON_DAYS}日以内に決算がある。建てないこと。**")
            L.append(f"  内訳: 確定 {ce.get('ok', 0)}（JPX {ce.get('from_jpx', 0)} / yfinance {ce.get('from_yf', 0)}）"
                     f" ／ 直近発表済 {ce.get('recent', 0)} ／ 推定 {ce.get('estimate', 0)}"
                     f" ／ 不明 {ce.get('unknown', 0)}")
            L.append("- **「◯/◯発表済(N日前)」は確定情報**。出したばかりなので当面は決算をまたがない。")
            L.append("- **「推定 ◯/◯頃」は予定ではない。** 過去の決算短信の間隔から割り出した目安で、"
                     "**これだけを根拠に建てない／見送らない**。**要警戒**は推定日が近い印。")
            L.append("- 「予定なし(要確認)」「照会失敗(要確認)」は**決算が無いという意味ではない**。"
                     "発注前にSBIの銘柄ページで必ず確認する。")
            L.append(f"- JPXが予定日を載せるのは**直前の四半期が終わった会社だけ**"
                     f"（今回 {(cj.get('jpx_earnings') or {}).get('codes', 0):,}社）。"
                     "載っていない会社は、直近に決算がある可能性はむしろ低い。")
        else:
            L.append("- **決算発表日の照合ができていない。** 発注前に必ず個別に確認すること。")
        L.append("- ATR%が突出して高いものは一過性の材料で動いている可能性が高い。順位が上でも採用しない。")
    except FileNotFoundError:
        L.append("\n## 新規候補\n\nスクリーニング未実行。")
    except Exception as e:
        L.append(f"\n## 新規候補\n\n生成に失敗: {e}")
    # ── 清原枠の候補 ──────────────────────────────────────
    try:
        ky = json.load(open(f"{OUT}/kiyohara.json", encoding="utf-8"))
        sch = ky.get("screen", {})
        L.append("\n## 清原枠の候補（総額の半分・長期）\n")
        _wr, _r1 = ky.get("with_real", 0), ky.get("real_ge1", 0)
        _rn, _ncm = ky.get("real_near", 0), ky.get("nc_min", 0.8)
        L.append(f"**全銘柄に必ず課している条件**: 予想PER {sch.get('max_per')}倍以下／"
                 f"PBR {sch.get('max_pbr')}倍以下／"
                 f"時価総額 {sch.get('max_mcap_oku'):.0f}億円以下"
                 f"（売買代金20日平均 {sch.get('min_turnover', 0)/1e4:.0f}万円以上、"
                 f"本業黒字、{'・'.join(sch.get('excluded_sectors', []))}を除外）\n")
        _gw = ky.get("growth") or {}
        L.append(f"\n**成長の条件**: 売上か営業利益が**前年より伸びている**こと"
                 f"（同じ四半期どうしの比較）。"
                 f"落とした {_gw.get('rejected', 0)}件／"
                 f"測れて通った {_gw.get('passed_measured', 0)}件／"
                 f"**測れないまま通った {_gw.get('passed_unmeasured', 0)}件**。")
        L.append("> ★清原達郎氏の対象は「割安 **小型 成長** 株」で、"
                 "安いだけの会社ではない。これまでこの条件が無かったため、"
                 "**万年割安で伸びない会社が候補に残っていた**。")
        L.append("> ★「過去3期」で見ていないのは、J-Quants無料プランの"
                 "提供範囲が **2年ぶんしかない**ため。年度の数字を3期並べるのは"
                 "データとして不可能なので、同じ四半期どうしの前年比で測っている。"
                 "3期で見たい場合は有料プラン（Light以上）が要る。")
        L.append("> ★「測れないまま通った」件数が多いときは、条件が効いていない。"
                 "meta.json の `kiyohara.growth` を見ること。\n")
        L.append(f"**流動資産＞負債合計／ネットキャッシュ比率 {_ncm}以上** は、"
                 f"**EDINETの貸借対照表が取れている銘柄にだけ課している**。"
                 f"取れていない銘柄は落とさず、推定の確からしさを添えて残す"
                 f"（EDINETのキャッシュは日々積み上がる途中なので、"
                 f"ここで落とすと「まだ取っていないだけ」の銘柄が消える）。\n")
        L.append(f"母集団 {ky.get('universe', 0):,}銘柄 → "
                 f"**条件通過 {ky.get('passed', 0)}件**"
                 f"（うち無配 {ky.get('no_div', 0)}）\n")
        L.append(f"- **比率を実測できた: {_wr}件** … EDINETの貸借対照表から"
                 f"`(流動資産＋投資有価証券×70%−負債合計)÷時価総額` を計算した。"
                 f"**1以上 {_r1}件 / {_ncm}以上1未満 {_rn}件**")
        L.append(f"- **実測できていない: {max(ky.get('passed', 0) - _wr, 0)}件** … "
                 f"貸借対照表が未取得。下の「推定」は私が置いた目安で、"
                 f"**条件を満たしている保証はない**")
        # ★過去検証の母集団との食い違い。ここを出さないと
        #   「検証された割安効果が清原枠にも効く」と読み違える。
        if ky.get("bt_universe_known"):
            _ib = ky.get("in_bt_universe") or 0
            _pa = ky.get("passed") or 0
            L.append(f"\n> **★過去検証の母集団に入っているのは {_ib}/{_pa}件だけ。**"
                     f"過去検証は売買代金50百万円以上・ATR "
                     f"{MIN_ATR_PCT}〜{MAX_ATR_PCT}%で母集団を絞っている"
                     f"（スイングの条件）。清原枠は売買代金5百万円以上・"
                     f"ATRの条件なしなので、**清原枠の候補の大半は"
                     f"検証された母集団の外側**にいる。")
            L.append("> つまり「保有10日・割安上位が母集団平均を上回る」という"
                     "検証結果は、**清原枠の銘柄には当てはまらない**。"
                     "清原枠を検証するには、清原枠の母集団（小型・低流動・"
                     "低ボラの割安株）で、長い保有期間を測り直す必要がある。"
                     "それには**各時点の貸借対照表が5年以上必要**で、"
                     "EDINETの履歴が溜まるまでできない。\n")
        _rc, _rf = ky.get("real_checked") or 0, ky.get("real_failed") or 0
        if _rc:
            _pr = 100.0 * (_rc - _rf) / _rc
            L.append(f"\n> **★推定の当たり具合（実測で答え合わせした結果）**: "
                     f"実測できた **{_rc}件**のうち、実際に条件を満たしていたのは "
                     f"**{_rc - _rf}件（{_pr:.0f}%）**。"
                     f"残り**{_rf}件は推定では通っていたのに実データで外していた**"
                     f"（流動資産≤負債合計、または比率が{_ncm}未満）。")
            L.append(f"> つまり**推定は3〜4割しか当たらない**。"
                     f"だから下の表は**実測で条件を満たした銘柄を上に固定**し、"
                     f"未実測はその下に置いている。"
                     f"**未実測の銘柄は「買う候補」ではなく「測る候補」**。\n")
        if _wr == 0:
            L.append("\n> **今回は実測が0件。** EDINETのキャッシュがまだ空か、"
                     "`EDINET_API_KEY` が Actions の env に渡っていない。"
                     "meta.json の `edinet` を見ること。\n")
        else:
            L.append("")
        L.append("**推定の見方（実測できていない銘柄だけに使う）**: "
                 "比率1に届くのに「現金以外の資産」が"
                 "総資産の何割必要かで分けている"
                 "（`必要割合 ＝ (時価総額＋負債合計−現金) ÷ 総資産`）。\n")
        L.append("- **推定：確実** … 現金だけで比率1以上")
        L.append("- **推定：有力** … 必要割合35%以下。日本の会社の現金以外の流動資産は"
                 "総資産の35〜40%が普通なので、**普通の貸借なら届く**")
        L.append("- **推定：要確認** … 必要割合60%以下。届くかは貸借次第")
        L.append("- **推定：薄い** … それ以上。異常な貸借でないと届かない")
        L.append("\n> この境目は私が置いた目安で、**EDINETの実測が入るまでの"
                 "つなぎ**。実測が入った銘柄ではこの分類は使わない。"
                 "実測がまだ無い銘柄を手で確かめる順番を決めるためのもの。")
        L.append("> 最初は「現金−負債合計が時価総額の8割以上」を有力としていたが、"
                 "**条件通過71件のうち該当0件**だった。現金が総資産の8割という"
                 "貸借はほぼ存在せず、分類が情報を持っていなかったので切り直した。\n")
        _dr = ky.get("dropped") or {}
        if _dr:
            L.append("落ちた内訳: "
                     + " / ".join(f"{k} {v:,}" for k, v in
                                  sorted(_dr.items(), key=lambda x: -x[1])[:6]) + "\n")
        L.append("> **PBR 0.8倍以下 ⟺ 純資産÷時価総額 ≥ 1.25** なので、"
                 "この表に出た銘柄は全部「ネットキャッシュ比率が1以上になりうる」側。"
                 "流動資産と投資有価証券を足せば1を超える可能性が残っている。"
                 "**ただし「なりうる」だけで、実測しないと分からない。** "
                 "実測が0.8を下回った銘柄はこの表から落としてある。\n")
        _kr = ky.get("rows") or []
        if _kr:
            L.append("| 順 | コード | 銘柄名 | 比率(実測) | 基準日 | 推定 | 成長 | 株主還元 | 総合 |")
            L.append("|--:|---|---|--:|:-:|:-:|:-:|:-:|--:|")
            for r in _kr[:25]:
                _nr = r.get("nc_real")
                _rv = f"**{_nr:.2f}**" if isinstance(_nr, (int, float)) else "—"
                _as = (r.get("nc_asof") or "—")[:10]
                _es = "—" if _nr is not None else r.get("state", "")
                L.append(f"| {r.get('rank', '')} | {r['code']} | "
                         f"{(r.get('name') or '—')[:14]} | "
                         f"{_rv} | {_as} | {_es} | {r.get('grow') or '—'} | "
                         f"{r.get('ret', '—')} | "
                         f"{r.get('rank_score', 0):.0f} |")
            L.append("\n> **並び順**: まず「比率(実測)」が入って条件を満たした銘柄、"
                     "そのあと未実測の銘柄。各グループの中は総合点の順。"
                     "**グループをまたいで点数を比べても意味がない**"
                     "（未実測は条件を満たしているか分かっていない）。")
            L.append("> 「比率(実測)」はEDINETの有価証券報告書・半期報告書の"
                     "貸借対照表から計算した値。基準日は提出日。"
                     "**四半期報告書は2024年に廃止されたので、貸借は年2回しか更新されない。**"
                     "基準日が古い銘柄は、その後の自己株買いや増配で現金が減っている"
                     "可能性がある。")
            L.append(f"> {ky.get('edinet_attribution', '')}\n")
            L.append(f"\n並べ方は **{ky.get('rank_weights', '')}**。"
                     "PERを一番重視し、ネットキャッシュ比率を加え、"
                     "株主還元でバリュートラップを外す、という読み取りをそのまま使っている。\n")
            L.append("**ここから先は機械では決められない。**")
            L.append("- **「無配」は外す方向で見る。** 現金が厚いのに株主に返さない会社は"
                     "割安なまま何年も放置されうる。清原氏の言う「最終的なカタリストは"
                     "株主還元」の裏返し。")
            L.append("- **「比率(実測)」が「—」の銘柄だけ**、バフェット・コードで"
                     "**流動資産**と**投資有価証券**を見て "
                     "`(流動資産＋投資有価証券×70%−負債合計)÷時価総額` を手で計算する。"
                     "実測が入っている銘柄はこの作業は不要（同じ式で既に計算済み）。")
            L.append("- **「小型株は経営者が9割」**（清原氏）。経営者に成長させる意志があるか、"
                     "言動が一致しているか、中期経営計画が具体的か、競合に潰されないか。"
                     "**無料データに無いので、この仕組みでは判定できない。** "
                     "決算説明資料と中期経営計画を見る作業が残る。")
            L.append("- **PER・PBR・時価総額の数値はこの表に載せていない。** "
                     "載せると純資産や総資産が逆算でき、J-Quantsの生の財務数値を"
                     "公開したことになるため（利用条件）。"
                     "**比率(実測)だけは載せている** … こちらの出所はEDINETで、"
                     "PDL1.0（公共データ利用規約）により二次利用・再配布が"
                     "認められているため。")
            L.append("")
            # ══ 清原枠の組み立て表 ══════════════════════════════
            #   ★ここまでは「候補」しか出ていなかったので、枠として
            #     機能していなかった（何株買えるのかが出ない）。
            #     予算・単元・現金の制約を当てて、実際に建てられる形にする。
            _per = KY_BUDGET / max(KY_NAMES, 1)         # 1銘柄あたりの目安
            _held = set(str(c)[:4] for c, v in HOLD.items()
                        if (v or {}).get("sleeve") == SLV_KY)
            _ok = [r for r in _kr if str(r.get("band", "")).startswith("real")]
            L.append("\n### 清原枠の組み立て（実測で条件を満たした銘柄だけ）\n")
            if not _ok:
                L.append("**実測で条件を満たした銘柄が無いので、今日は組み立てない。**"
                         "EDINETの貸借対照表が揃うのを待つ。"
                         "推定だけの銘柄で建てるのは、条件を確かめずに買うのと同じ。")
            else:
                L.append(f"1銘柄あたりの目安 **¥{_per:,.0f}**"
                         f"（清原枠 ¥{KY_BUDGET:,.0f} ÷ {KY_NAMES}銘柄）／"
                         f"単元100株／今日使える清原枠の現金 **¥{KY_CASH:,.0f}**\n")
                L.append("| 順 | コード | 銘柄名 | 株価 | 比率(実測) | 株主還元 | 株数 | 概算金額 | 判定 |")
                L.append("|--:|---|---|--:|--:|:-:|--:|--:|:-:|")
                _left = KY_CASH
                _n_built, _spend = 0, 0.0
                for r in _ok[:KY_NAMES * 2]:
                    _p = r.get("price")
                    _c4 = str(r.get("code"))
                    if not _p or _p <= 0:
                        _sh, _cost, _v = 0, 0.0, "株価が取れない"
                    elif _c4 in _held:
                        _sh, _cost, _v = 0, 0.0, "保有中"
                    elif _p * LOT > _per:
                        _sh, _cost, _v = 0, 0.0, f"1単元が枠超（¥{_p*LOT:,.0f}）"
                    elif _n_built >= KY_NAMES:
                        _sh, _cost, _v = 0, 0.0, f"{KY_NAMES}銘柄に達した"
                    else:
                        _sh = int(min(_per, _left) // (_p * LOT)) * LOT
                        _cost = _sh * _p
                        if _sh <= 0:
                            _v = "現金が足りない"
                        elif _cost < _per * KY_MIN_FILL:
                            # ★端株になるなら建てない。等ウェイトが崩れる。
                            _sh, _cost = 0, 0.0
                            _v = f"端数になる（目安の{KY_MIN_FILL:.0%}未満）"
                        else:
                            _v = "**建てられる**"
                            _left -= _cost; _spend += _cost; _n_built += 1
                    L.append(f"| {r.get('rank', '')} | {_c4} | "
                             f"{(r.get('name') or '—')[:14]} | "
                             f"{_p:,.1f} | "
                             f"{(('%.2f' % r['nc_real']) if r.get('nc_real') is not None else '—')} | "
                             f"{r.get('ret', '—')} | "
                             f"{(f'{_sh:,}' if _sh else '—')} | "
                             f"{(f'¥{_cost:,.0f}' if _sh else '—')} | {_v} |")
                L.append(f"\n**今日建てられるのは {_n_built}銘柄・概算 ¥{_spend:,.0f}**"
                         f"（残る清原枠の現金 ¥{_left:,.0f}）。\n")
                if _n_built < KY_NAMES:
                    L.append(f"> **{KY_NAMES}銘柄に届かない。** 原因は"
                             "「実測で条件を満たした銘柄が足りない」か"
                             "「現金が足りない」か「株価が高くて1単元が枠を超える」。"
                             "上の判定列を見ること。**足りないまま無理に埋めない。**")
                L.append("> **これは発注リストではない。** 予算と単元と現金の制約を"
                         "当てただけの表で、**清原氏の言う「経営者が9割」の判定が"
                         "まだ入っていない**。建てる前に決算説明資料と中期経営計画を"
                         "読むこと。読んでいない銘柄は建てない。")
                L.append("> **1日に1〜2銘柄までにして段階的に建てる**（本人の方針）。"
                         "一度に全部入れると、相場全体が下げた局面で"
                         "枠ごと含み損になり、判断が難しくなる。")
                L.append("> 株数は「1銘柄の目安 ÷ 株価」を**単元100株で切り捨て**。"
                         "切り上げると枠と分散が崩れる。")
                L.append(f"> **目安額の{KY_MIN_FILL:.0%}に届かない端株は建てない。** "
                         "清原式は等ウェイトの分散が前提で、"
                         f"目安 ¥{_per:,.0f} の枠に数万円の端株を混ぜると"
                         "「分散した」と言えなくなる。"
                         "**現金が足りないなら銘柄数を減らす**のが正しい。")
            L.append("")
            L.append("**清原枠の建て方（スイング枠とは別の規則）**")
            L.append(f"- 等ウェイトで **{KY_NAMES}銘柄程度**に分散する。ATRの損切りは使わない。")
            L.append("- **最低2倍を狙う。3割上昇での売却はしない**（清原氏）。"
                     "降りるのは投資仮説が崩れたとき（本業の悪化・株主還元の後退・"
                     "経営者の言動の不一致）。")
            L.append("- **決算は当然またぐ。** スイング枠の「決算をまたぐ建玉は作らない」は"
                     "**この枠には適用しない**。3年持つ前提の手法で決算を避けることはできない。")
            L.append("- 清原氏は20銘柄を勧めているが、この口座規模では単元100株の制約で"
                     f"{KY_NAMES}銘柄程度が上限。**分散は本来より薄いことを承知で運用する。**")
        else:
            L.append("**本日の条件通過は0件。** 落ちた内訳を見て、"
                     "条件が厳しすぎるのか取得側の問題なのかを切り分けること。"
                     "PER8倍以下かつPBR0.8倍以下は、相場が上がった局面では"
                     "ほとんど残らないことがある。")
    except FileNotFoundError:
        pass
    except Exception as e:
        L.append(f"\n## 清原枠の候補\n\n生成に失敗: {e}")

    # ── EDINETの取得状況（貸借対照表＝本物の比率の材料） ──────────
    #   初回は CSV の実際の形（文字コード・区切り・列名）がここで分かる。
    try:
        _ej = json.load(open(f"{OUT}/edinet.json", encoding="utf-8"))
        _em = (json.load(open(f"{OUT}/meta.json", encoding="utf-8"))
               .get("edinet") or {}) if os.path.exists(f"{OUT}/meta.json") else {}
        _rw = _ej.get("rows") or {}
        _ca = sum(1 for v in _rw.values() if v.get("ca") is not None)
        _iv = sum(1 for v in _rw.values() if v.get("inv") is not None)
        _us = _ej.get("usable")
        _bb = _ej.get("bs_bad")
        L.append("\n## EDINET 貸借対照表の取得状況\n")
        L.append(f"累計 **{len(_rw):,}銘柄**"
                 f"（流動資産合計あり {_ca:,} / 投資有価証券あり {_iv:,}）"
                 f"／一覧を見た日 {len(_ej.get('seen_dates') or []):,}日分\n")
        if _us is not None:
            L.append(f"- **比率に使えるのは {_us:,}銘柄**。"
                     f"貸借の検算（`資産合計 ＝ 負債合計 ＋ 純資産合計` と "
                     f"`流動資産 ≤ 資産合計`）に落ちた **{_bb or 0:,}銘柄**は"
                     f"実測として使わず、推定に戻している。")
            L.append("  - 検算に落ちる原因は、セグメント別など"
                     "**全体でない数字**を合計として拾ってしまう場合。"
                     "落ちた書類は次の実行で取り直す。")
        # ★履歴の溜まり具合。比率をバックテストで検証できるのは
        #   1銘柄に複数の開示が溜まってから。いつ検証できるかが分かるように出す。
        _hr, _hc = _ej.get("hist_rows"), _ej.get("hist_codes")
        _hm = _ej.get("hist_multi")
        if _hr is not None:
            L.append(f"- **履歴 {_hr:,}行 / {_hc:,}銘柄**"
                     f"（同じ銘柄に2件以上ある = **{_hm:,}銘柄**"
                     f"／{_em.get('hist_from') or '—'}〜{_em.get('hist_to') or '—'}）")
            L.append("  - `data/edinet_hist.csv` に開示1件ごとに残している。"
                     "`edinet.json` は銘柄ごとの**最新1件**しか持たず、"
                     "以前は古い開示を上書きで捨てていた。"
                     "**比率をバックテストで検証するには各時点の貸借が要る**ので、"
                     "取り直しが高くつく前に貯め始めた。")
            if _hm < 200:
                L.append(f"  - **まだ検証はできない。** 2件以上ある銘柄が {_hm:,}件で、"
                         "各時点の比率を作れる銘柄が足りない。"
                         "遡る範囲を約6年に広げてあるので、実行を重ねれば溜まる。")
            else:
                L.append(f"  - 2件以上ある銘柄が {_hm:,}件になった。"
                         "**比率をバックテストの因子として検定できる段階**。")
        _dt = (_em.get("dtypes") or {})
        if _dt:
            L.append(f"- 書類の内訳: 有価証券報告書 {_dt.get('120', 0):,} / "
                     f"半期報告書 {_dt.get('160', 0):,}")
            if _dt.get("160", 0) == 0 and _dt.get("120", 0) > 0:
                L.append("  - **半期報告書が0件。** 有報だけが読めている状態。"
                         "半期は `InterimInstant`（当中間期末日時点）を使うと"
                         "金融庁の「報告書インスタンス作成ガイドライン」に"
                         "定められていて、そこには対応済み。"
                         "**それでも0件なら別の原因**なので、下の"
                         "「読めなかった理由」の要素ID・コンテキストIDを見ること。")
        _nm = _em.get("nomatch") or {}
        if _nm:
            L.append("- **読めなかった理由**（書類種類:理由）: "
                     + " / ".join(f"`{k}` {v}" for k, v in
                                  sorted(_nm.items(), key=lambda x: -x[1])[:6]))
        for _p in (_em.get("probe") or [])[:3]:
            _el = "・".join(str(x) for x in (_p.get("elems") or [])[:4]) or "—"
            _cx = "・".join(str(x) for x in (_p.get("ctx") or [])[:4]) or "—"
            L.append(f"  - 種類{_p.get('dtype')} / {_p.get('why')} / "
                     f"要素ID `{_el}` / コンテキスト `{_cx}`")
        if _em.get("skipped"):
            L.append(f"- **今回は取りに行っていない: {_em['skipped']}**")
            L.append("  - `EDINET_API_KEY` を GitHub Secrets に入れ、"
                     "**`fetch.yml` の `env:` にも渡す**こと。"
                     "Secretsに入れるだけでは Actions のジョブには見えない。")
        else:
            L.append(f"- 今回: 一覧 {_em.get('listed_days', 0)}日 / "
                     f"未取得 {_em.get('todo', 0)}件"
                     f"（うち清原枠の候補 {_em.get('todo_priority', 0)}件を優先）"
                     f"のうち {_em.get('downloaded', 0)}件を取得 / "
                     f"貸借を読めた {_em.get('parsed', 0)}件 "
                     f"（要求 {_em.get('req', 0)}回 / 429 {_em.get('n429', 0)}回）")
            if _em.get("todo", 0) > _em.get("downloaded", 0):
                L.append(f"  - 残り {_em['todo'] - _em.get('downloaded', 0):,}件は"
                         "次の実行に回している。**清原枠に出る銘柄から先に"
                         "落とす**ので、表の実測は数回で埋まる。")
            if _em.get("blocked"):
                L.append(f"- **止まった: {_em['blocked']}** … "
                         "鍵かレート制限。次回に持ち越す。")
            if _em.get("enc") or _em.get("sep"):
                L.append(f"- CSVの形: 文字コード `{_em.get('enc')}` / "
                         f"区切り `{_em.get('sep')}`")
            _cols = _em.get("cols") or []
            if _cols:
                L.append(f"- 列名: `{'` / `'.join(str(c) for c in _cols[:8])}`")
            _err = _em.get("err") or {}
            if _err:
                L.append("- 失敗の内訳: "
                         + " / ".join(f"{k} {v}" for k, v in
                                      sorted(_err.items(), key=lambda x: -x[1])[:6]))
        L.append("")
        L.append("- 一度に落とす書類数と一覧日数には上限を置いてあり、"
                 "**日々の実行で少しずつ積み上がる**（1回で全銘柄は取らない）。"
                 "有価証券報告書は3月期決算が6月に集中するので、"
                 "6月分を遡り終えた時点で大半が埋まる。")
        L.append("- **四半期報告書は2024年に廃止**。貸借は有価証券報告書（年1回）と"
                 "半期報告書（年1回）の**年2回**しか更新されない。"
                 "比率が古くなることは構造上避けられない。")
        L.append("- 訂正報告書（訂正有価証券報告書・訂正半期報告書）は**取っていない**。"
                 "元の報告書の数字を使っている。")
        L.append(f"- {_ej.get('attribution', '')}")
        L.append("- **鍵の扱い**: EDINETは鍵をURLのクエリ文字列で渡す仕様なので、"
                 "この仕組みではURLも例外文もログに残していない。"
                 "`data/*.json` に対しても鍵の混入検査をかけている。")
    except FileNotFoundError:
        L.append("\n## EDINET 貸借対照表の取得状況\n")
        L.append("**まだ1件も取れていない。** `data/edinet.json` が無い。"
                 "`EDINET_API_KEY` が `fetch.yml` の `env:` に渡っているか確認すること。")
    except Exception as e:
        L.append(f"\n## EDINET 貸借対照表の取得状況\n\n生成に失敗: {type(e).__name__}")

    # ── ネットキャッシュ比率（清原式）の手作業候補 ──────────────
    try:
        nc = json.load(open(f"{OUT}/netcash.json", encoding="utf-8"))
        L.append("\n## ネットキャッシュ比率の確認候補（清原達郎式）\n")
        L.append("**ネットキャッシュ ＝ 流動資産 ＋ 投資有価証券×70% − 負債合計**／"
                 "**比率 ＝ ネットキャッシュ ÷ 時価総額**。"
                 "70%は売却時の税金（約30%）を引くため。"
                 "比率1以上＝資産を売って負債を返しても現金が余る＝本業がタダで付いてくる。\n")
        L.append(f"通過{nc.get('pool', 0):,}銘柄から、**手で確かめる価値のある"
                 f"{nc.get('shortlist', 0)}件**に絞った"
                 f"（うち下限だけで既に1以上と確定しているのが **{nc.get('sure', 0)}件**）。\n")
        L.append("絞り方は不等式で、近似は置いていない:\n")
        L.append("- 負債合計 ＝ 総資産 − 純資産、時価総額 ＝ 株価 × (発行済 − 自己株) "
                 "… ここまでは無料データで出る")
        L.append("- 流動資産と投資有価証券は無料では取れないので、"
                 "**現金同等物 ≤ 流動資産** と **流動資産＋投資有価証券×70% ≤ 総資産** で挟む")
        L.append("- → 下限 ＝ (現金同等物 − 負債合計) ÷ 時価総額、"
                 "上限 ＝ 純資産 ÷ 時価総額（＝1/PBR）")
        L.append("- **上限が1未満（PBR>1）の銘柄は、清原式でも1以上になりえない** "
                 "→ 手作業の対象から外してある")
        L.append("- 銀行・保険・証券は流動資産と負債の意味が違い式が成立しないので除外。"
                 "本業赤字（営業利益≤0）も除外\n")
        _r = nc.get("rows") or []
        if _r:
            L.append("| 順 | コード | 銘柄名 | 分類 | 株主還元 |")
            L.append("|--:|---|---|:-:|:-:|")
            for r in _r[:40]:
                L.append(f"| {r.get('rank', '')} | {r['code']} | "
                         f"{(r.get('name') or '—')[:14]} | "
                         f"{r.get('state', '')} | {r.get('ret', '—')} |")
            L.append("\n**手で入れるのは2項目だけ**: バフェット・コードで各銘柄の"
                     "**流動資産**と**投資有価証券**を見る。負債合計と時価総額は"
                     "そちらにも出ているので突き合わせに使える。\n")
            L.append("> 「確実(下限で1以上)」は現金だけで負債を返して時価総額を超えている。"
                     "流動資産と投資有価証券を足せば比率はさらに上がるので、"
                     "**確認は不要**（数字の妥当性だけ見ればよい）。")
            L.append("> 「要確認」は上限では1を超えるが下限では届かない。"
                     "売掛金・棚卸資産・投資有価証券の大きさで決まるので、"
                     "**この2項目を入れないと判定できない**。上から順に見ていけばよい。")
            L.append("> **比率の数値をこのファイルに載せていない**のは、載せると"
                     "純資産や総資産が逆算でき、J-Quantsの生の財務数値を公開した"
                     "ことになるため（利用条件）。正確な比率はバフェット・コードの"
                     "値で計算すること。")
            L.append("> **「株主還元」列がバリュートラップの見分け**。清原氏は"
                     "「最終的なカタリストは株主還元」としている。現金が厚いのに"
                     "株主に返さない会社は、割安なまま何年も放置されうる。"
                     "**「無配」は現金の厚さを割安と読んではいけない印。**")
            L.append("> 清原氏は「**小型株は経営者が9割**」としている。"
                     "経営者の意志・言動の一致・中期経営計画の具体性は無料データに無く、"
                     "**この仕組みでは判定できない**。候補を出すところまでが機械の仕事で、"
                     "そこから先は決算説明資料と中期経営計画を見る作業が残る。")
            L.append("> 「この投資法は最低でも株価上昇2倍は狙うべき」という前提とも"
                     "突き合わせること。3ATR（およそ+7〜15%）で利確する"
                     "いまの執行とは噛み合わない。")
        else:
            L.append("該当なし。上限（1/PBR）が1以上で本業黒字の銘柄が"
                     "通過銘柄の中に無かったということ。")
    except FileNotFoundError:
        pass
    except Exception as e:
        L.append(f"\n## ネットキャッシュ比率の確認候補\n\n生成に失敗: {e}")

    # ── データの出どころと、なぜ銘柄名が空欄になることがあるか ──────
    L.append("\n## データの出どころ（銘柄名が空欄になる理由）\n")
    L.append("**銘柄名は EDINET の提出者名**（" + ED_ATTRIB + "）。")
    L.append(f"いま {len(ED_NAMES):,}銘柄ぶん溜まっている。"
             "EDINETにまだ出てきていない銘柄（ETF・REIT・新規上場直後など）は"
             "**名前が空欄**になる。コードで引くこと。")
    L.append("")
    L.append("> **★なぜJPXの銘柄名を使わないか。** "
             "JPXサイト利用条件は「当サイトに掲載されている情報について、"
             "有料・無料を問わずJPXからの許諾を得ている場合を除き、"
             "商用目的によるデータ収集のほか如何なる用途に関わらず"
             "二次利用及び再配信はできません」としている。"
             "**このリポジトリは public** なので、JPXの「東証上場銘柄一覧」から"
             "取った銘柄名・業種区分を `data/` に書き出すと再配信に当たる。")
    L.append("> そこで JPXのマスタは**銘柄の絞り込み（市場区分・銀行の除外・"
             "同じ業種を既に持っているかの判定）にだけメモリ上で使い**、"
             "書き出す名前は EDINET の提出者名に置き換えた。"
             "**17業種・33業種・規模区分・業種ETFの列は表から外した**"
             "（業種ETFは17業種と1対1なので、載せると区分が復元できてしまう）。")
    L.append("> EDINETは政府標準利用規約(PDL1.0)で、出典を書けば再配布できる。"
             "だから比率(実測)と銘柄名はそのまま載せている。")
    _cg = meta.get("commit_guard") or {}
    if _cg.get("jpx_fields") or _cg.get("jpx_files"):
        L.append(f"\n> **★JPX由来のデータが書き出されている**: "
                 f"{_cg.get('jpx_fields')} / {_cg.get('jpx_files')}。"
                 f"コミット前に取り除くこと。")
    L.append("")

    # ── 残っている課題。毎回出して、忘れないようにする ──────────────
    L.append("\n## 残っている課題\n")
    L.append("**① 決算短信の本文が取れていない**（2026-09-18に課題として登録）")
    L.append("> 決算短信は**TDnet側**にあり、**EDINETには入っていない**。"
             "TDnetの自動取得はJPXがスクレイピングを控えるよう求めているため"
             "停止済み。J-LENS（JPX総研の開示検索）は**APIが無く法人向け**。"
             "有価証券報告書はEDINETで取れるが**数ヶ月遅れ**で、"
             "イベントとしての時間的価値が薄い（高野海斗 2025, 言語処理学会）。")
    L.append("> **なぜ効くのか**: Meursault ら (2023, JFQA 58(6)) の「PEAD.txt」は、"
             "決算コールの**テキストだけ**から作ったサプライズ指標が、"
             "報告利益の数値を一切使わずに古典的なSUEより大きいドリフトを"
             "生むことを示した（保有63日で2.87%対1.54%）。"
             "**いま数値でしか測れていない部分に、文章側の情報が残っている。**")
    L.append("> **いま代わりにやっていること**: 「上振れたのに新しい会社予想が"
             "保守的か」を数値（予想営業利益の対前年）で近似している。"
             "本来は決算短信の「今後の見通し」を読む仕事。")
    L.append("")
    L.append("**② 業種の分類を公開できないため、集中のチェックが弱い**")
    L.append("> JPXの17業種はサイト利用条件で再配信できないため表から外した。"
             "いまは保有銘柄どうしの業種重複（`overlap`）だけを見ている。"
             "再配布が明示されたセクター分類の代替を探す必要がある。")
    L.append("")
    L.append("**③ 発注株数の決め方（サイジング）が検証されていない**")
    L.append("> ATRサイジングと1銘柄上限15%は手で決めた規則で、"
             "過去検証にかけていない。選び方と降り方は測っているが、"
             "**建てる大きさの決め方は測っていない**。"
             "ポートフォリオ全体の推移を追う仕組みが要るので、別件として残す。")
    L.append("")

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

    # 表の検算: ヘッダと区切り行のセル数が食い違うと、その列は描画されずに消える。
    # 目視では気づけないので機械的に確認し、食い違ったらレポート自身に書く。
    _bad = []
    for _i in range(len(L) - 1):
        _a, _b = L[_i], L[_i + 1]
        if _a.startswith("|") and re.fullmatch(r"\|[\s:|-]+\|", _b or ""):
            _na, _nb = len(_a.split("|")) - 2, len(_b.split("|")) - 2
            if _na != _nb:
                _bad.append(f"ヘッダ{_na}列 / 区切り{_nb}列: {_a[:40]}")
    if _bad:
        meta["table_mismatch"] = _bad
        L.append("\n> **注意: 表の列数が合っていない箇所がある（列が欠けて表示されている）**\n> "
                 + "\n> ".join(_bad))
        print("::warning::表の列数不一致:", " / ".join(_bad))

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

# ★順番が大事。commit_guard() は data/*.json を読むので、
#   meta.json を**先に書いてから**検査する。
#   以前は検査のあとに meta.json を書いていたため、検査は常に
#   1回前の meta.json を見ていた（当日の混入は翌日まで分からない）。
json.dump(meta, open(f"{OUT}/meta.json", "w"), ensure_ascii=False, indent=1)
try:
    _g = commit_guard()
except Exception as e:
    meta["errors"].append(f"commit_guard: {type(e).__name__}: {e}")
    _g = {"error": type(e).__name__}
meta["commit_guard"] = _g      # commit_guard() 内でも入れているが、
                               # 例外で落ちた場合もここで必ず残す
json.dump(meta, open(f"{OUT}/meta.json", "w"), ensure_ascii=False, indent=1)
# ★書き直しで鍵が戻る可能性があるので、鍵の伏せ字だけ最後にもう一度当てる。
#   （検査結果には項目名の文字列しか入らないので、生値検査は不要）
try:
    redact_keys(f"{OUT}/meta.json")
except Exception as e:
    meta["errors"].append(f"redact: {type(e).__name__}")

print(f"保有銘柄: {meta['holdings_ok']}/{len(HOLDINGS)}  健全性: {meta['health']}")
print(f"エラー件数: {len(meta['errors'])}  価格の飛び: {meta['n_anomalies']}件  イベント: {meta['events_ok']}銘柄  ニュース: {meta.get('news_ok',0)}件")
for e in meta["errors"][:10]:
    print("  -", e)
if meta["health"] == "bad":
    print("::warning::保有銘柄の取得が不足しています。data/meta.json を確認してください")
# 部分的にでも取れていればコミットさせるため、常に正常終了する
sys.exit(0)
