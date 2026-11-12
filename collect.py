#!/usr/bin/env python3
"""
프리마켓 '과거' 데이터 수집기 — 전 종목, 날짜별 4시점(08:00 / 08:30 / 09:00 / 09:30) + 전일·전전일 종가
(토스 서버에 저장된 과거 1분봉에서 뽑아옴: 2022-11-23 ~ 어제)

  python collect.py --probe     연결·데이터 형식 확인 (영어로 출력, 먼저 한 번 실행)
  python collect.py             수집 시작/이어서 (중단돼도 다시 실행하면 이어서 받음)
  python collect.py --status    진행 상황

· 토스증권 Open API 1분봉(KRX+NXT 통합 시세)을 씀 — 자동매매와 다른 호출 한도라 매매에 영향 없음
· 기간: 2025-03-04(넥스트레이드 프리마켓 시작) ~ 어제
· 결과: data/premarket.db (SQLite) → 단계가 끝날 때마다 rave-stock-data 저장소 research/ 에 CSV(gzip) 업로드
· 진행 상황은 카톡으로 (시작, 25%마다, 끝)
"""
import base64
import csv
import gzip
import io
import json
import logging
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

import bot  # 토스 API 연결 · 카톡 · GitHub 설정 재사용
from bot import KST, ApiError, Toss, to_int

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"
DATA.mkdir(exist_ok=True)
DB = DATA / "premarket.db"

NXT_START = date(2025, 3, 4)
MIN1_START = date(2022, 11, 23)
RATE = 17            # 초당 호출 수 (차트 한도 20 안쪽)
POINTS = {"p0800": (8, 0), "p0830": (8, 30), "p0900": (9, 0), "p0930": (9, 30)}

log = logging.getLogger("collect")
log.setLevel(logging.INFO)
_h = logging.FileHandler(BASE / "collect.log", encoding="utf-8")
_h.setFormatter(logging.Formatter("%(asctime)s %(message)s", "%Y-%m-%d %H:%M:%S"))
log.addHandler(_h)
log.addHandler(logging.StreamHandler(sys.stdout))


# ─────────────── 호출 (속도 제한) ───────────────
class Api:
    def __init__(self):
        self.t = Toss()
        self.last = 0.0

    def get(self, path, params=None):
        wait = 1.0 / RATE - (time.time() - self.last)
        if wait > 0:
            time.sleep(wait)
        self.last = time.time()
        for attempt in range(5):
            try:
                return self.t._req("GET", path, account=False, params=params)
            except ApiError as e:
                if e.status in (401, 500, 502, 503, 504) and attempt < 4:
                    time.sleep(2 + attempt * 3)
                    continue
                raise
            except requests.RequestException:
                if attempt < 4:
                    time.sleep(3 + attempt * 5)
                    continue
                raise

    def candles(self, symbol, interval, before=None, count=200):
        p = {"symbol": symbol, "interval": interval, "count": count, "adjusted": "true"}
        if before:
            p["before"] = before
        res = self.get("/api/v1/candles", p) or {}
        return res.get("candles") or [], res.get("nextBefore")


def ts_kst(v):
    """캔들 시각 → KST datetime (ISO 문자열 / epoch 초·밀리초 모두 처리)"""
    if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()):
        v = float(v)
        if v > 1e11:
            v /= 1000
        return datetime.fromtimestamp(v, KST)
    d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    if d.tzinfo is None:
        d = d.replace(tzinfo=KST)
    return d.astimezone(KST)


# ─────────────── 종목 목록 ───────────────
MARKET_TRIES = [["KOSPI", "KOSDAQ"], ["KR"], ["KRX"], ["DOMESTIC"], ["kospi", "kosdaq"], ["kr"]]


def _items(res):
    if isinstance(res, list):
        return res
    for v in (res or {}).values():
        if isinstance(v, list):
            return v
    return []


def list_symbols(api, verbose=False):
    """전체 종목 — market 값을 몇 가지 시도해서 되는 걸 씀"""
    for markets in MARKET_TRIES:
        items, ok = [], True
        for m in markets:
            try:
                items += _items(api.get("/api/v1/stocks/all", {"market": m}))
            except ApiError as e:
                if verbose:
                    print(f"    market={m}: {e.status} {e.code} {e.data or ''}")
                ok = False
                break
        if not ok or not items:
            continue
        out, seen = [], set()
        for it in items:
            if not isinstance(it, dict):
                continue
            sym = next((str(it[k]) for k in ("symbol", "code", "stockCode", "shortCode") if it.get(k)), None)
            if not sym or not sym.isdigit() or len(sym) != 6 or sym in seen:
                continue
            seen.add(sym)
            if "securityType" in it and str(it["securityType"]).upper() != "STOCK":
                continue                      # ETF·ETN 등 제외
            if it.get("isCommonShare") is False:
                continue                      # 우선주 제외
            name = next((str(it[k]) for k in ("name", "stockName", "koreanName", "nameKr", "shortName") if it.get(k)), "")
            kind = " ".join(str(it.get(k, "")) for k in ("market", "marketType", "securityType", "type", "category", "exchange"))
            out.append({"symbol": sym, "name": name, "kind": kind.strip()})
        if out:
            if verbose:
                print(f"    market={markets} -> OK")
            return out, items[0]
    return [], None


# ─────────────── DB ───────────────
def db():
    c = sqlite3.connect(DB)
    c.execute("""CREATE TABLE IF NOT EXISTS days(
        symbol TEXT, date TEXT, name TEXT, prev2_close INTEGER, prev_close INTEGER,
        p0800 INTEGER, p0830 INTEGER, p0900 INTEGER, p0930 INTEGER,
        pre_volume INTEGER, open_volume INTEGER, PRIMARY KEY(symbol, date))""")
    c.execute("CREATE TABLE IF NOT EXISTS symbols(symbol TEXT PRIMARY KEY, name TEXT, kind TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS done(symbol TEXT, phase INTEGER, PRIMARY KEY(symbol, phase))")
    c.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
    return c


def meta(c, k, v=None):
    if v is None:
        r = c.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return r[0] if r else None
    c.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (k, str(v)))
    c.commit()


# ─────────────── 하루치 4시점 ───────────────
def daily_closes(api, symbol, since):
    """일봉 → {날짜: 종가}"""
    out, before = {}, None
    while True:
        cs, nb = api.candles(symbol, "1d", before)
        if not cs:
            break
        for k in cs:
            d = ts_kst(k["timestamp"]).date()
            out[d] = to_int(k.get("closePrice"))
        oldest = min(ts_kst(k["timestamp"]).date() for k in cs)
        if oldest <= since or not nb:
            break
        before = nb
    return out


def day_points(api, symbol, d):
    """d일 08:00~09:30 1분봉 → 4시점 가격 + 거래량"""
    cs, _ = api.candles(symbol, "1m", f"{d.isoformat()}T09:30:59+09:00", 100)
    bars = []
    for k in cs:
        t = ts_kst(k["timestamp"])
        if t.date() == d and (8, 0) <= (t.hour, t.minute) <= (9, 30):
            bars.append((t.hour, t.minute, to_int(k.get("openPrice")), to_int(k.get("closePrice")), to_int(k.get("volume"))))
    if not bars:
        return None
    bars.sort()

    # 토스는 거래가 없는 시간에도 거래량 0인 빈 봉을 채워 줌 → 실제 거래가 있던 봉만 사용
    def first_open(lo, hi):          # lo 이상 hi 미만, 거래 있던 첫 봉의 시가
        for h, m, o, c, v in bars:
            if lo <= (h, m) < hi and v > 0:
                return o
        return None

    def last_close(lo, upto):        # lo 이상 upto 이하, 거래 있던 마지막 봉의 종가
        r = None
        for h, m, o, c, v in bars:
            if lo <= (h, m) <= upto and v > 0:
                r = c
        return r

    return {
        "p0800": first_open((8, 0), (8, 30)),
        "p0830": last_close((8, 0), (8, 30)),
        "p0900": first_open((9, 0), (9, 30)),
        "p0930": last_close((9, 0), (9, 30)),
        "pre_volume": sum(v for h, m, o, c, v in bars if (h, m) < (9, 0)),
        "open_volume": sum(v for h, m, o, c, v in bars if (h, m) >= (9, 0)),
    }


# ─────────────── 업로드 · 알림 ───────────────
def kakao(text):
    try:
        bot._kakao_send_now(text)
    except Exception as e:
        log.info(f"kakao skip: {e}")


def upload(name, raw: bytes):
    if not bot.GH_FILE.exists():
        log.info("github.json 없음 — 업로드 건너뜀 (data/ 폴더에만 저장)")
        return False
    cfg = json.loads(bot.GH_FILE.read_text())
    url = f"https://api.github.com/repos/{cfg['repo']}/contents/research/{name}"
    h = {"Authorization": f"Bearer {cfg['token']}", "Accept": "application/vnd.github+json"}
    r = requests.get(url, headers=h, timeout=30)
    body = {"message": f"research {name}", "content": base64.b64encode(raw).decode()}
    if r.status_code == 200:
        body["sha"] = r.json()["sha"]
    r = requests.put(url, headers=h, json=body, timeout=300)
    log.info(f"upload {name}: {r.status_code}")
    return r.status_code in (200, 201)


def export(c, phase):
    lo, hi = (NXT_START, date(2100, 1, 1)) if phase == 1 else (MIN1_START, NXT_START)
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        w = io.TextIOWrapper(gz, encoding="utf-8-sig", newline="")
        cw = csv.writer(w)
        cw.writerow(["종목코드", "종목명", "날짜", "전전일종가", "전일종가", "08:00", "08:30", "09:00", "09:30", "프리마켓거래량", "09시~09:30거래량"])
        for row in c.execute("""SELECT symbol,name,date,prev2_close,prev_close,p0800,p0830,p0900,p0930,pre_volume,open_volume
                                FROM days WHERE date>=? AND date<? AND p0900 IS NOT NULL ORDER BY date,symbol""",
                             (lo.isoformat(), hi.isoformat())):
            cw.writerow(row)
        w.flush()
        w.detach()
    raw = buf.getvalue()
    name = "premarket_2025-03_to_now.csv.gz" if phase == 1 else "premarket_2022-11_to_2025-02.csv.gz"
    (DATA / name).write_bytes(raw)
    log.info(f"export {name}: {len(raw)/1e6:.1f}MB")
    return upload(name, raw), name, len(raw)


# ─────────────── 실행 ───────────────
def trading_days(api, lo, hi):
    days = sorted(d for d in daily_closes(api, "005930", lo) if lo <= d <= hi)
    return days


def run_phase(api, c, phase, syms):
    yesterday = datetime.now(KST).date() - timedelta(days=1)
    lo, hi = (NXT_START, yesterday) if phase == 1 else (MIN1_START, NXT_START - timedelta(days=1))
    cal = trading_days(api, lo, hi)
    todo = [s for s in syms if not c.execute("SELECT 1 FROM done WHERE symbol=? AND phase=?", (s["symbol"], phase)).fetchone()]
    total, start_n = len(syms), len(syms) - len(todo)
    label = "(2025.3.4~어제)" if phase == 1 else "(2022.11~2025.2)"
    log.info(f"phase {phase}: {len(cal)} trading days, {len(todo)}/{total} symbols left")
    kakao(f"📥 프리마켓 데이터 수집 {label} 시작\n종목 {total}개 중 {len(todo)}개 남음 · 거래일 {len(cal)}일\n"
          f"예상 {len(todo) * len(cal) / RATE / 3600:.0f}시간 정도 걸려요.")
    next_report = 0.25
    for i, s in enumerate(todo, 1):
        sym = s["symbol"]
        try:
            closes = daily_closes(api, sym, lo - timedelta(days=14))
            have = {r[0] for r in c.execute("SELECT date FROM days WHERE symbol=?", (sym,))}
            prev2 = prev = None
            for d in sorted(closes):
                if d < lo or d > hi:
                    prev2, prev = prev, closes[d]
                    continue
                if d.isoformat() not in have:
                    pt = day_points(api, sym, d)
                    if pt:
                        c.execute("INSERT OR REPLACE INTO days VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                                  (sym, d.isoformat(), s["name"], prev2, prev, pt["p0800"], pt["p0830"], pt["p0900"],
                                   pt["p0930"], pt["pre_volume"], pt["open_volume"]))
                prev2, prev = prev, closes[d]
            c.execute("INSERT OR REPLACE INTO done VALUES(?,?)", (sym, phase))
            c.commit()
        except ApiError as e:
            log.info(f"{sym} skip: {e}")
            if e.status in (401, 403):
                raise
            continue
        done_frac = (start_n + i) / total
        if i % 50 == 0:
            log.info(f"phase {phase}: {start_n + i}/{total} ({done_frac:.0%})")
        if next_report <= done_frac < 1:
            kakao(f"📥 수집 {label} {done_frac:.0%} 진행 ({start_n + i}/{total}종목)")
            while next_report <= done_frac:
                next_report += 0.25
    ok, name, size = export(c, phase)
    kakao(f"✅ 수집 {label} 끝!\n파일: rave-stock-data / research / {name} ({size/1e6:.0f}MB)"
          + ("" if ok else "\n(업로드 실패 — 서버 data 폴더에 있어요)"))


def main():
    args = sys.argv[1:]
    api = Api()
    if args[:1] == ["--probe"]:
        print("[1] stocks/all")
        syms, sample = list_symbols(api, verbose=True)
        print(f"    -> {len(syms)} symbols. first raw item keys: {list(sample.keys()) if isinstance(sample, dict) else sample}")
        if syms:
            print(f"    e.g. {syms[0]['symbol']} kind='{syms[0]['kind']}'")
        cs, nb = api.candles("005930", "1d", None, 3)
        print(f"[2] daily candle sample: {cs[:1]}")
        d = NXT_START + timedelta(days=7)
        pt = day_points(api, "005930", d)
        print(f"[3] 005930 on {d}: {pt}")
        cs, _ = api.candles("005930", "1m", f"{d.isoformat()}T09:30:59+09:00", 100)
        times = sorted(ts_kst(k["timestamp"]).strftime("%H:%M") for k in cs if ts_kst(k["timestamp"]).date() == d)
        print(f"[4] 1m bars that day: {len(times)}, first={times[:3]}, last={times[-2:]}")
        print(f"[5] has 08:xx bars (NXT premarket) = {any(t.startswith('08') for t in times)}")
        old = date(2022, 12, 1)
        print(f"[6] oldest check {old}: {day_points(api, '005930', old)}")
        return
    c = db()
    if args[:1] == ["--status"]:
        n = c.execute("SELECT COUNT(*) FROM symbols").fetchone()[0]
        for ph in (1,):
            d = c.execute("SELECT COUNT(*) FROM done WHERE phase=?", (ph,)).fetchone()[0]
            print(f"phase {ph}: {d}/{n} symbols done")
        print("rows:", c.execute("SELECT COUNT(*) FROM days").fetchone()[0])
        return
    syms = [{"symbol": r[0], "name": r[1], "kind": r[2]} for r in c.execute("SELECT * FROM symbols")]
    if not syms:
        syms, _ = list_symbols(api)
        if not syms:
            sys.exit("종목 목록을 받지 못했어요. --probe 결과를 보여주세요.")
        c.executemany("INSERT OR REPLACE INTO symbols VALUES(:symbol,:name,:kind)", syms)
        c.commit()
        log.info(f"symbols: {len(syms)}")
    for tries in range(8):        # 일시적 오류로 멈추면 5분 쉬고 이어서 (받은 데이터는 건너뜀)
        try:
            run_phase(api, c, 1, syms)   # 프리마켓이 생긴 2025-03-04 이후만
            break
        except Exception as e:
            log.info(f"stopped: {e} — retry in 5 min ({tries + 1}/8)")
            if tries == 7:
                kakao(f"⚠️ 데이터 수집이 오류로 멈췄어요: {str(e)[:80]}")
                return
            time.sleep(300)
            api = Api()
    kakao("🎉 프리마켓 데이터 수집이 모두 끝났어요. 파일을 받아서 보내주시면 확률표로 정리해 드릴게요.")


if __name__ == "__main__":
    main()
