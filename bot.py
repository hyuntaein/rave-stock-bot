#!/usr/bin/env python3
"""
레이브 주식 자동매매 봇 — 토스증권 Open API (국내주식)

구조
  · 주문 카드 (config.yaml 의 stocks)  : "이 가격대에서 이만큼 산다"
      현재가가 [매수 범위] 안에 오면 예산만큼 지정가 매수
      금액/매수 범위를 바꾸거나 '같은 조건으로 다시 매매'를 누르면 새 주문으로 인식
  · 보유 묶음 (봇이 산 주식)            : 체결될 때마다 묶음이 생기고, 묶음마다 매도가·손절가를 따로 가짐
      [매도가] 이상 → 묶음 전량 지정가 매도
      [손절 범위] 안 → 묶음 전량 매도 (범위 시작가 아래로는 안 팔리는 지정가)
      가격이 범위를 건너뛰고 아래로 지나가면(프리마켓 갭 등) 그 조건은 끝 → 웹앱에서 다시 설정
        (매수 범위 → 그 매수 주문 종료 / 손절 범위 → 그 묶음의 손절 꺼짐, 익절은 유지)
      웹앱 '조건 해제'(config.yaml 의 released_lots) → 봇이 더 이상 팔지 않음 (주식은 계좌에 그대로)
  · 봇이 사지 않은 주식(원래 갖고 있던 주식)은 절대 건드리지 않음

사용법
  python bot.py              봇 실행
  python bot.py --status     주문/묶음 상태 보기
  python bot.py --kakao      카카오톡 알림 연결
  python bot.py --github     웹앱 현황 연결
"""
import csv
import json
import logging
import os
import base64
import fcntl
import re
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
import yaml
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=True)

BASE_URL = os.environ.get("TOSS_BASE_URL", "https://openapi.tossinvest.com")
KST = ZoneInfo("Asia/Seoul")
CONFIG_FILE = BASE_DIR / "config.yaml"
STATE_FILE = BASE_DIR / "state.json"
TRADES_FILE = BASE_DIR / "trades.csv"
LOG_FILE = BASE_DIR / "bot.log"
TOKEN_FILE = BASE_DIR / ".toss_token.json"
TOKEN_LOCK = BASE_DIR / ".toss_token.lock"

FINAL_STATUSES = {"FILLED", "CANCELED", "REJECTED", "REPLACED"}
ALREADY_DONE_CODES = {"already-filled", "already-canceled", "already-rejected", "already-modified"}

# ─────────────────────────── 로그 ───────────────────────────
log = logging.getLogger("bot")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
_fmt.converter = lambda secs: datetime.fromtimestamp(secs, KST).timetuple()
_fh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
_fh.setFormatter(_fmt)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
log.addHandler(_fh)
log.addHandler(_sh)


# ─────────────────────────── 호가 단위 ───────────────────────────
def tick_size(price: int) -> int:
    """국내주식 호가 단위 (2023년 이후 KRX 기준)"""
    for limit, tick in ((2_000, 1), (5_000, 5), (20_000, 10), (50_000, 50),
                        (200_000, 100), (500_000, 500)):
        if price < limit:
            return tick
    return 1_000


def tick_down(price: int) -> int:
    t = tick_size(price)
    return price // t * t


def tick_up(price: int) -> int:
    t = tick_size(price)
    return -(-price // t) * t


def won(n) -> str:
    return f"{int(n):,}원"


def to_int(v) -> int:
    if v in (None, ""):
        return 0
    return int(Decimal(str(v)))


# ─────────────────────────── 토스증권 API ───────────────────────────
class ApiError(Exception):
    def __init__(self, status, code, message, data=None):
        super().__init__(f"{status} {code} {message} {data or ''}")
        self.status, self.code, self.message, self.data = status, code, message, data


class Toss:
    def __init__(self):
        self.client_id = os.environ.get("TOSS_CLIENT_ID", "").strip()
        self.client_secret = os.environ.get("TOSS_CLIENT_SECRET", "").strip()
        if not self.client_id or not self.client_secret:
            sys.exit("❌ .env 파일에 TOSS_CLIENT_ID / TOSS_CLIENT_SECRET 을 넣어주세요.")
        seq = os.environ.get("TOSS_ACCOUNT_SEQ", "").strip()
        self._seq = int(seq) if seq else None
        self._tok, self._exp, self._revoked = None, 0.0, None
        self.s = requests.Session()

    def token(self) -> str:
        """토스는 키당 토큰 1개만 유효 → 봇·수집기가 서버 안 파일로 같은 토큰을 공유"""
        if self._tok and time.time() < self._exp - 300:
            return self._tok
        with open(TOKEN_LOCK, "a+") as lf:
            fcntl.flock(lf, fcntl.LOCK_EX)
            try:
                d = json.loads(TOKEN_FILE.read_text())
                if d["exp"] - 300 > time.time() and d["tok"] != self._revoked:
                    self._tok, self._exp = d["tok"], d["exp"]
                    return self._tok
            except Exception:
                pass
            r = self.s.post(f"{BASE_URL}/oauth2/token", data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }, timeout=10)
            if r.status_code == 403:
                sys.exit("❌ 403: 이 서버의 IP가 토스증권 '허용 IP'에 등록되지 않았습니다.")
            r.raise_for_status()
            d = r.json()
            self._tok = d["access_token"]
            self._exp = time.time() + int(d.get("expires_in", 3600))
            tmp = TOKEN_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps({"tok": self._tok, "exp": self._exp}))
            os.chmod(tmp, 0o600)
            tmp.replace(TOKEN_FILE)
            log.info("토큰 발급 완료 (공유)")
            return self._tok

    def account_seq(self) -> int:
        if self._seq is None:
            accounts = self._req("GET", "/api/v1/accounts", account=False)
            brokerage = [a for a in accounts if a.get("accountType") == "BROKERAGE"] or accounts
            if not brokerage:
                sys.exit("❌ 사용할 수 있는 계좌가 없습니다.")
            self._seq = int(brokerage[0]["accountSeq"])
            log.info(f"계좌 선택: {brokerage[0].get('accountNo')} (accountSeq={self._seq})")
        return self._seq

    def _req(self, method, path, account=True, params=None, body=None, retries=4):
        for attempt in range(retries):
            headers = {"Authorization": f"Bearer {self.token()}"}
            if account:
                headers["X-Tossinvest-Account"] = str(self.account_seq())
            try:
                r = self.s.request(method, BASE_URL + path, headers=headers,
                                   params=params, json=body, timeout=10)
            except requests.RequestException as e:
                # 주문은 clientOrderId(멱등키)가 같으므로 재시도해도 중복 주문이 나가지 않음
                if attempt == retries - 1:
                    raise
                log.warning(f"네트워크 오류, 재시도: {e}")
                time.sleep(1 + attempt)
                continue
            if r.status_code == 429:
                wait = float(r.headers.get("Retry-After", 1)) + 0.2 * attempt
                time.sleep(wait)
                continue
            if r.status_code == 401 and attempt < 2:
                # 다른 프로그램이 새 토큰을 받아 이 토큰이 폐기됨 → 공유 파일의 새 토큰으로 다시 시도
                self._revoked, self._tok = self._tok, None
                time.sleep(0.5)
                continue
            if r.status_code >= 400:
                try:
                    err = r.json().get("error", {})
                except ValueError:
                    err = {}
                raise ApiError(r.status_code, err.get("code"),
                               err.get("message") or r.text[:200], err.get("data"))
            return r.json().get("result")
        raise ApiError(429, "rate-limit-exceeded", "재시도 한도 초과")

    def prices(self, symbols) -> dict:
        res = self._req("GET", "/api/v1/prices", account=False,
                        params={"symbols": ",".join(symbols)})
        return {p["symbol"]: to_int(p["lastPrice"]) for p in res or [] if p.get("lastPrice")}

    def holdings(self) -> dict:
        """실제 계좌 보유: {종목코드: (수량, 평균매입가)}"""
        res = self._req("GET", "/api/v1/holdings") or {}
        out = {}
        for it in res.get("items") or []:
            out[it["symbol"]] = (to_int(it.get("quantity")), to_int(it.get("averagePurchasePrice")))
        return out

    def sellable(self, symbol) -> int:
        res = self._req("GET", "/api/v1/sellable-quantity", params={"symbol": symbol}) or {}
        return to_int(res.get("sellableQuantity"))

    def place_order(self, symbol, side, qty, order_type, price=None) -> str:
        body = {
            "clientOrderId": uuid.uuid4().hex[:32],
            "symbol": symbol, "side": side, "orderType": order_type,
            "quantity": str(int(qty)),
        }
        if order_type == "LIMIT":
            body["price"] = str(int(price))
        return self._req("POST", "/api/v1/orders", body=body)["orderId"]

    def get_order(self, order_id) -> dict:
        return self._req("GET", f"/api/v1/orders/{order_id}")

    def cancel_order(self, order_id):
        return self._req("POST", f"/api/v1/orders/{order_id}/cancel", body={})


# ─────────────────────────── 설정 ───────────────────────────
def load_config():
    with open(CONFIG_FILE, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    st = cfg.get("settings", {})
    max_budget = int(st.get("max_budget_per_stock", 10_000_000))
    stocks, errors = [], []
    for i, s in enumerate(cfg.get("stocks") or []):
        name = s.get("name") or f"{i + 1}번 종목"
        if not s.get("enabled", True):
            continue
        try:
            item = {
                "id": str(s.get("id") or f"{str(s['symbol']).zfill(6)}#{i}"),
                "name": name,
                "symbol": str(s["symbol"]).zfill(6),
                "budget": int(s["budget"]),
                "buy_min": int(s["buy_min"]), "buy_max": int(s["buy_max"]),
                "sell_min": int(s["sell_min"]),
                "stop_loss": int(s["stop_loss"]),
                "stop_min": int(s["stop_min"]) if s.get("stop_min") else None,
                "round": int(s.get("round", 0) or 0),
            }
        except (KeyError, ValueError, TypeError) as e:
            errors.append(f"{name}: 값이 빠졌거나 숫자가 아님 ({e})")
            continue
        if item["stop_min"] is not None and not item["stop_min"] < item["stop_loss"]:
            errors.append(f"{name}: 손절 범위 시작이 손절 범위 끝보다 낮아야 합니다")
        if not (item["stop_loss"] < item["buy_min"] <= item["buy_max"] < item["sell_min"]):
            errors.append(f"{name}: 손절가 < 매수하한 ≤ 매수상한 < 매도하한 순서여야 합니다")
        if item["budget"] > max_budget:
            errors.append(f"{name}: 예산 {won(item['budget'])}이 안전한도 {won(max_budget)}를 넘습니다")
        if item["budget"] >= 100_000_000:
            errors.append(f"{name}: 1억원 이상 주문은 지원하지 않습니다")
        item["buy_price"] = tick_down(item["buy_max"])
        item["sell_price"] = tick_up(item["sell_min"])
        item["target_qty"] = item["budget"] // item["buy_price"] if item["buy_price"] else 0
        if item["target_qty"] < 1:
            errors.append(f"{name}: 예산으로 1주도 살 수 없습니다")
        stocks.append(item)
    if errors:
        raise ValueError("설정 오류:\n  - " + "\n  - ".join(errors))
    settings = {
        "dry_run": bool(st.get("dry_run", True)),
        "interval": max(2, int(st.get("check_interval_sec", 5))),
        "order_timeout": int(st.get("order_timeout_sec", 60)),
        "watch": st.get("watch_start", "08:00"),
        "start": st.get("trade_start", "09:00"),
        "end": st.get("trade_end", "15:19"),
        "released": {str(x) for x in (cfg.get("released_lots") or [])},
        "overrides": _read_overrides(cfg.get("lot_overrides")),
    }
    return settings, stocks


def _read_overrides(raw):
    out = {}
    for lid, v in (raw or {}).items():
        try:
            o = {"sell_min": int(v["sell_min"]), "stop_loss": int(v["stop_loss"]),
                 "stop_min": int(v["stop_min"]) if v.get("stop_min") else None}
        except (KeyError, TypeError, ValueError):
            continue
        if (o["stop_min"] is None or o["stop_min"] < o["stop_loss"]) and o["stop_loss"] < o["sell_min"]:
            out[str(lid)] = o
    return out


# ─────────────────────────── 상태 저장 ───────────────────────────
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def save_state(state):
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


# ─────────────────────────── 카카오톡 알림 (나에게 보내기) ───────────────────────────
KAKAO_FILE = BASE_DIR / "kakao.json"
WEBAPP_URL = "https://hyuntaein.github.io/rave-stock-bot/"
_kakao_lock = threading.Lock()


def _clean_paste(v: str) -> str:
    """터미널 붙여넣기 때 섞여 들어오는 ^[[200~ 같은 표시 제거"""
    v = re.sub(r"\x1b\[20[01]~|\^\[\[20[01]~", "", v or "")
    return v.strip().strip("~").strip()


def _kakao_token_request(cfg, data):
    data = dict(data, client_id=cfg["rest_key"])
    if cfg.get("client_secret"):
        data["client_secret"] = cfg["client_secret"]
    r = requests.post("https://kauth.kakao.com/oauth/token", data=data, timeout=10)
    if r.status_code != 200:
        raise RuntimeError(f"카카오 토큰 오류 {r.status_code}: {r.text[:300]}")
    return r.json()


def _kakao_save(cfg, tok):
    cfg["access_token"] = tok["access_token"]
    cfg["access_exp"] = time.time() + int(tok.get("expires_in", 21599))
    if tok.get("refresh_token"):      # 만료 1달 전부터 새 refresh_token이 같이 옴
        cfg["refresh_token"] = tok["refresh_token"]
        cfg["refresh_exp"] = time.time() + int(tok.get("refresh_token_expires_in", 5184000))
    tmp = KAKAO_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    os.chmod(tmp, 0o600)
    tmp.replace(KAKAO_FILE)


def _kakao_send_now(text):
    with _kakao_lock:
        if not KAKAO_FILE.exists():
            return
        cfg = json.loads(KAKAO_FILE.read_text(encoding="utf-8"))
        if time.time() > cfg.get("access_exp", 0) - 600:
            _kakao_save(cfg, _kakao_token_request(cfg, {"grant_type": "refresh_token",
                                                        "refresh_token": cfg["refresh_token"]}))
        tpl = {"object_type": "text", "text": text[:200],
               "link": {"web_url": WEBAPP_URL, "mobile_web_url": WEBAPP_URL},
               "button_title": "설정 열기"}
        r = requests.post("https://kapi.kakao.com/v2/api/talk/memo/default/send",
                          headers={"Authorization": f"Bearer {cfg['access_token']}"},
                          data={"template_object": json.dumps(tpl, ensure_ascii=False)}, timeout=10)
        if r.status_code != 200:
            raise RuntimeError(f"카카오 전송 오류 {r.status_code}: {r.text[:300]}")


EVENTS_FILE = BASE_DIR / "events.json"
try:
    EVENTS = json.loads(EVENTS_FILE.read_text(encoding="utf-8"))[-60:]
except Exception:
    EVENTS = []


def notify(text, kakao=True):
    """웹앱 '최근 기록'에 남기고, 카톡으로도 보냄 — 실패해도 매매는 계속"""
    EVENTS.append({"t": datetime.now(KST).isoformat(timespec="seconds"), "text": text})
    del EVENTS[:-60]
    try:
        EVENTS_FILE.write_text(json.dumps(EVENTS, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass
    if not kakao:
        return

    def run():
        try:
            _kakao_send_now(text)
        except Exception as e:
            log.warning(f"카톡 알림 실패: {e}")
    threading.Thread(target=run, daemon=True).start()


def kakao_setup():
    print("\n=== 카카오톡 알림 연결 ===\n")
    rest = _clean_paste(input("1) REST API 키를 붙여넣고 엔터: "))
    secret = _clean_paste(input("2) Client Secret 코드 (없으면 그냥 엔터): "))
    url = ("https://kauth.kakao.com/oauth/authorize?response_type=code&scope=talk_message"
           f"&client_id={rest}&redirect_uri={requests.utils.quote(WEBAPP_URL, safe='')}")
    print("\n3) 아래 주소를 복사해서 PC 브라우저 주소창에 붙여넣고 카카오 로그인/동의를 하세요:\n")
    print(url + "\n")
    print("   로그인하면 웹앱 화면에 '카카오 인증 코드'가 나옵니다. 그 코드를 복사하세요.\n")
    code = _clean_paste(input("4) 인증 코드를 붙여넣고 엔터: "))
    m = re.search(r"code=([^&\s]+)", code)
    if m:
        code = m.group(1)
    cfg = {"rest_key": rest, "client_secret": secret}
    try:
        tok = _kakao_token_request(cfg, {"grant_type": "authorization_code",
                                         "redirect_uri": WEBAPP_URL, "code": code})
    except Exception as e:
        sys.exit(f"\n❌ 연결 실패: {e}\n   (코드는 한 번만 쓸 수 있어요. 3)부터 다시 해주세요)")
    _kakao_save(cfg, tok)
    try:
        _kakao_send_now("✅ 레이브 자동매매 카톡 알림이 연결됐어요.\n앞으로 사고팔 때마다 여기로 알려드릴게요.")
        print("\n✅ 연결 완료! 카카오톡 '나와의 채팅'을 확인해 보세요.")
    except Exception as e:
        sys.exit(f"\n❌ 토큰은 받았지만 전송 실패: {e}\n   카카오 콘솔에서 '카카오톡 메시지 전송' 동의항목과 웹 도메인 등록을 확인해 주세요.")


# ─────────────────────────── 웹앱 현황 (비공개 저장소에 status.json) ───────────────────────────
GH_FILE = BASE_DIR / "github.json"


class StatusPublisher:
    """현황을 백그라운드에서 GitHub 비공개 저장소에 올림 — 실패해도 매매에는 영향 없음"""

    def __init__(self):
        self.pending, self.sha, self.lock = None, None, threading.Lock()
        self.last_err = 0
        threading.Thread(target=self._loop, daemon=True).start()

    def submit(self, payload):
        with self.lock:
            self.pending = payload

    def _loop(self):
        while True:
            time.sleep(3)
            with self.lock:
                payload, self.pending = self.pending, None
            if payload is None or not GH_FILE.exists():
                continue
            try:
                self._push(payload)
            except Exception as e:
                if time.time() - self.last_err > 600:
                    log.warning(f"웹앱 현황 업로드 실패: {e}")
                    self.last_err = time.time()

    def _push(self, payload):
        cfg = json.loads(GH_FILE.read_text(encoding="utf-8"))
        url = f"https://api.github.com/repos/{cfg['repo']}/contents/status.json"
        h = {"Authorization": f"Bearer {cfg['token']}", "Accept": "application/vnd.github+json"}
        body = json.dumps(payload, ensure_ascii=False, indent=1).encode("utf-8")
        for _ in range(2):
            if self.sha is None:
                r = requests.get(url, headers=h, timeout=10)
                self.sha = r.json().get("sha") if r.status_code == 200 else None
            data = {"message": "status", "content": base64.b64encode(body).decode()}
            if self.sha:
                data["sha"] = self.sha
            r = requests.put(url, headers=h, json=data, timeout=15)
            if r.status_code in (200, 201):
                self.sha = r.json()["content"]["sha"]
                return
            if r.status_code in (409, 422):
                self.sha = None
                continue
            raise RuntimeError(f"{r.status_code} {r.text[:200]}")


def github_setup():
    print("\n=== 웹앱 현황 연결 ===\n")
    token = _clean_paste(input("1) GitHub 토큰(github_pat_...)을 붙여넣고 엔터: "))
    repo = _clean_paste(input("2) 현황 저장소 이름 (그냥 엔터 = hyuntaein/rave-stock-data): ")) or "hyuntaein/rave-stock-data"
    r = requests.get(f"https://api.github.com/repos/{repo}",
                     headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}, timeout=10)
    if r.status_code != 200:
        sys.exit(f"\n❌ 저장소에 접근할 수 없어요 ({r.status_code}). 토큰에 {repo} 권한(Contents: Read and write)이 있는지 확인하세요.")
    if not r.json().get("private"):
        print("\n⚠️  이 저장소는 공개(Public)예요. 주문 내역이 누구에게나 보일 수 있어요.")
    GH_FILE.write_text(json.dumps({"token": token, "repo": repo}), encoding="utf-8")
    os.chmod(GH_FILE, 0o600)
    p = StatusPublisher()
    p._push({"updated_at": datetime.now(KST).isoformat(timespec="seconds"), "setup_test": True, "stocks": [], "events": EVENTS})
    print("\n✅ 연결 완료! 1분 안에 웹앱 위쪽에 현황이 나타나요.")


def record_trade(row):
    new = not TRADES_FILE.exists()
    with open(TRADES_FILE, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["시각", "종목", "코드", "구분", "사유", "수량", "평균가", "금액", "모의여부"])
        w.writerow(row)


def stop_text(l):
    return f"{won(l['stop_min'])}~{won(l['stop_loss'])}" if l.get("stop_min") else f"{won(l['stop_loss'])}↓"


# ─────────────────────────── 봇 ───────────────────────────
def new_ticket(run=1):
    return {"done": False, "paused": False, "reject_count": 0, "open_order": None, "bought": 0,
            "run": run, "cfg_sig": None, "manual_buy_stop": False}


class Bot:
    def __init__(self):
        self.api = Toss()
        self.settings, self.stocks = load_config()
        self.cfg_mtime = CONFIG_FILE.stat().st_mtime
        self.code_mtime = Path(__file__).stat().st_mtime
        raw = load_state()
        self.state = raw if raw.get("version") == 2 else self.migrate(raw)
        self.closed_until = None
        self.last_notice = ""
        self.recent_fill = {}            # 종목별 마지막 체결 시각 (계좌 반영 지연 대비)
        self.publisher = StatusPublisher()
        self.last_prices, self.last_pub, self.last_fp = {}, 0.0, None
        self.started_at = datetime.now(KST).isoformat(timespec="seconds")
        seed_events_from_trades()
        save_state(self.state)

    @property
    def dry(self):
        return self.settings["dry_run"]

    @property
    def tickets(self):
        return self.state["tickets"]

    @property
    def lots(self):
        return self.state["lots"]

    # ---------- 예전 상태 파일(종목 단위) → 주문/묶음 구조로 옮기기 ----------
    def migrate(self, old):
        new = {"version": 2, "tickets": {}, "lots": {}, "pnl": 0}
        if not old:
            return new
        log.info("📦 예전 상태 파일을 주문/보유 묶음 구조로 옮깁니다")
        for sym, st in old.items():
            if not isinstance(st, dict):
                continue
            card = next((s for s in self.stocks if s["symbol"] == sym), None)
            tid = card["id"] if card else f"{sym}#old"
            held = int(st.get("held_qty", 0)) - int(st.get("sim_offset", 0))
            oo = st.get("open_order")
            if oo and str(oo.get("id", "")).startswith("SIM-"):
                oo = None
            t = new_ticket()
            if st.get("done") and held <= 0:
                t["done"] = True
                if card:
                    t["cfg_sig"] = self.sig(card)
            if oo and oo.get("side") == "BUY":
                t["open_order"] = oo
            new["tickets"][tid] = t
            if held > 0:
                total = int(st.get("held_qty", 0)) or held
                avg = int(st.get("buy_amount", 0) / total) if total else 0
                lid = f"{tid}@0"
                new["lots"][lid] = {
                    "id": lid, "symbol": sym, "name": card["name"] if card else sym, "ticket": tid, "run": 0,
                    "qty": held, "cost": avg * held,
                    "sell_min": card["sell_min"] if card else None, "stop_loss": card["stop_loss"] if card else None,
                    "stop_min": card["stop_min"] if card else None,
                    "opened_at": st.get("last_fill_at") or datetime.now(KST).isoformat(timespec="seconds"),
                    "open_order": oo if (oo and oo.get("side") == "SELL") else None,
                    "no_take_profit": bool(st.get("no_take_profit")), "reject_count": 0, "sim": False, "pnl": 0,
                }
                log.info(f"  [{sym}] 보유 {held}주 → 보유 묶음 {lid} (매도 {card and won(card['sell_min'])} / 손절 {card and won(card['stop_loss'])})")
        return new

    @staticmethod
    def sig(s):
        return [s["budget"], s["buy_min"], s["buy_max"], s["round"]]

    def reload_config_if_changed(self):
        m = CONFIG_FILE.stat().st_mtime
        if m == self.cfg_mtime:
            return
        self.cfg_mtime = m
        was_dry = self.dry
        try:
            self.settings, self.stocks = load_config()
            log.info("⚙️  config.yaml 변경 감지 → 새 설정 적용")
        except Exception as e:
            log.error(f"새 설정을 적용하지 못했습니다 (이전 설정 유지): {e}")
            return
        if was_dry and not self.dry:
            # 모의로 사고판 기록이 실전에 섞이지 않게 정리
            for lid in [k for k, l in self.lots.items() if l.get("sim")]:
                del self.lots[lid]
            for tid, t in list(self.tickets.items()):
                oo = t.get("open_order")
                if not oo or str(oo.get("id", "")).startswith("SIM-"):
                    self.tickets[tid] = new_ticket(t.get("run", 1) + 1)
            save_state(self.state)
            log.warning("🔴 실전 모드로 전환 — 모의 매매 기록을 정리했습니다")
            notify("🔴 실전 모드로 전환됐어요.\n이제 조건이 맞으면 실제 주문이 나갑니다.")
        elif not was_dry and self.dry:
            log.info("🧪 모의 모드로 전환")
            notify("🧪 모의 모드로 전환됐어요. 실제 주문은 나가지 않아요.")

    def notice_once(self, msg):
        if msg != self.last_notice:
            log.info(msg)
            self.last_notice = msg

    def phase(self, now):
        """closed / watch(프리마켓: 가격 감시·알림만) / trade(주문 가능)"""
        if now.weekday() >= 5:
            return "closed"
        hm = now.strftime("%H:%M")
        if self.settings["watch"] <= hm < self.settings["start"]:
            return "watch"
        if self.settings["start"] <= hm < self.settings["end"]:
            if self.closed_until and now < self.closed_until:
                return "closed"
            return "trade"
        return "closed"

    def run(self):
        mode = "🧪 모의 모드 (실제 주문 안 나감)" if self.dry else "🔴 실전 모드 (실제 주문)"
        log.info(f"봇 시작 — {mode}")
        if not self.dry:
            lines = [f"· 주문 {s['name']} {won(s['buy_min'])}~{won(s['buy_max'])} 매수" for s in self.stocks
                     if not self.tickets.get(s["id"], {}).get("done")]
            lines += [f"· 보유 {l['name']} {l['qty']}주 / {won(l['sell_min'])}↑ 매도 / {stop_text(l)} 손절"
                      for l in self.lots.values()]
            notify("▶️ 자동매매 봇이 시작됐어요 (실전 모드).\n" + "\n".join(lines))
        self.api.account_seq()
        while True:
            try:
                if Path(__file__).stat().st_mtime != self.code_mtime:
                    log.info("♻️  봇 프로그램이 업데이트되어 다시 시작합니다")
                    save_state(self.state)
                    sys.exit(0)   # systemd가 10초 뒤 새 코드로 자동 재시작
                self.reload_config_if_changed()
                now = datetime.now(KST)
                ph = self.phase(now)
                if ph == "closed":
                    self.notice_once("장 시간이 아니라 대기 중…")
                    self.housekeeping(now)
                    self.publish(now, market=False)
                    time.sleep(30)
                    continue
                self.tick(now, watch_only=(ph == "watch"))
                self.publish(now, market=True, phase=ph)
            except ApiError as e:
                log.error(f"API 오류: {e}")
            except Exception:
                log.exception("예상치 못한 오류 (계속 실행)")
            time.sleep(self.settings["interval"])

    # ---------- 매 주기 ----------
    def housekeeping(self, now):
        """설정 변경 반영 · 꺼진 주문 취소 · 조건 해제 (장 시간 밖에서도 처리)"""
        changed = self.sync_tickets()
        changed |= self.cancel_orphan_buys()
        changed |= self.handle_releases()
        if changed:
            save_state(self.state)

    def tick(self, now, watch_only=False):
        self.housekeeping(now)
        self.watch_only = watch_only
        active = [s for s in self.stocks
                  if not self.tickets[s["id"]]["done"] and not self.tickets[s["id"]]["paused"]]
        lots = list(self.lots.values())
        symbols = sorted({s["symbol"] for s in active} | {l["symbol"] for l in lots})
        if not symbols:
            self.notice_once("진행 중인 주문과 보유 묶음이 없습니다")
            return
        self.notice_once(f"{'프리마켓 감시(주문 없음)' if watch_only else '감시 중'}: 주문 {len(active)}개 · 보유 묶음 {len(lots)}개")
        prices = self.api.prices(symbols)
        self.last_prices.update(prices)
        if not self.dry and not watch_only and any(not l.get("sim") for l in lots):
            self.reconcile(self.api.holdings(), now)
        for s in active:
            price = prices.get(s["symbol"])
            if price:
                try:
                    self.step_ticket(s, self.tickets[s["id"]], price, now)
                except ApiError as e:
                    self.ticket_error(s, self.tickets[s["id"]], e)
        for lot in list(self.lots.values()):
            price = prices.get(lot["symbol"])
            if price:
                try:
                    self.step_lot(lot, price, now)
                except ApiError as e:
                    self.lot_error(lot, e)
        save_state(self.state)

    def sync_tickets(self):
        changed = False
        for s in self.stocks:
            t = self.tickets.setdefault(s["id"], new_ticket())
            sig = self.sig(s)
            if t["cfg_sig"] is not None and t["cfg_sig"] != sig:
                if t["done"] and not t["open_order"]:
                    self.tickets[s["id"]] = t = new_ticket(t["run"] + 1)
                    log.info(f"[{s['name']}] ⚙️  조건 변경 → 새 주문으로 인식")
                    notify(f"🆕 [{s['name']}] 새 매수 주문으로 등록했어요.\n"
                           f"{won(s['buy_min'])}~{won(s['buy_max'])}에 오면 {s['target_qty']}주를 사서 새 묶음으로 관리해요.",
                           kakao=False)
                else:
                    if t["paused"]:
                        t["paused"], t["reject_count"] = False, 0
                        log.info(f"[{s['name']}] ⚙️  조건 변경 → 일시정지 해제")
                    if t.get("manual_buy_stop"):
                        t["manual_buy_stop"] = False
                changed = True
            t["cfg_sig"] = sig
            # 이번 회차에 산 묶음은 카드의 매도가·손절가를 따라감 (예전 묶음은 그대로)
            for lot in self.lots.values():
                if lot["ticket"] == s["id"] and lot.get("run") == t["run"] and lot["id"] not in self.settings["overrides"]:
                    changed |= self.set_lot_conditions(lot, s["sell_min"], s["stop_min"], s["stop_loss"], s["name"])
        # 웹앱 '조건 수정'으로 직접 바꾼 묶음
        for lid, o in self.settings["overrides"].items():
            if lid in self.lots:
                changed |= self.set_lot_conditions(self.lots[lid], o["sell_min"], o["stop_min"], o["stop_loss"])
        return changed

    def set_lot_conditions(self, lot, sell_min, stop_min, stop_loss, name=None):
        new = (sell_min, stop_min, stop_loss, name or lot["name"])
        if (lot["sell_min"], lot.get("stop_min"), lot["stop_loss"], lot["name"]) == new:
            return False
        lot["sell_min"], lot["stop_min"], lot["stop_loss"], lot["name"] = new
        lot["no_take_profit"] = False
        if lot.get("stop_expired"):
            lot["stop_expired"] = False
            notify(f"🔁 [{lot['name']}] 손절 조건을 다시 켰어요: {stop_text(lot)}", kakao=False)
        lot["stop_zone"] = None
        log.info(f"[{lot['name']}] ⚙️  묶음 조건 변경 → 매도 {won(sell_min)} / 손절 {stop_text(lot)}")
        return True

    def cancel_orphan_buys(self):
        """웹앱에서 끄거나 지운 주문 카드에 걸려 있던 매수 주문 취소"""
        on = {s["id"] for s in self.stocks}
        changed = False
        for tid, t in self.tickets.items():
            oo = t.get("open_order")
            if not oo or tid in on:
                continue
            if not str(oo["id"]).startswith("SIM-"):
                try:
                    self.api.cancel_order(oo["id"])
                    info = self.api.get_order(oo["id"])
                    filled = to_int((info.get("execution") or {}).get("filledQuantity"))
                    if filled:
                        log.warning(f"[{oo.get('symbol', tid)}] 취소 전 {filled}주가 이미 체결됐어요 (보유 묶음으로 추가하지 않음)")
                except ApiError as e:
                    if e.code not in ALREADY_DONE_CODES:
                        log.error(f"[{tid}] 꺼진 주문 취소 실패: {e}")
                        continue
            notify(f"🛑 [{oo.get('name', tid)}] 주문 카드를 꺼서 걸려 있던 매수 주문을 취소했어요.", kakao=not self.dry)
            t["open_order"] = None
            changed = True
        return changed

    def handle_releases(self):
        changed = False
        for lid in list(self.lots):
            if lid not in self.settings["released"]:
                continue
            lot = self.lots[lid]
            oo = lot.get("open_order")
            if oo and not str(oo["id"]).startswith("SIM-"):
                try:
                    self.api.cancel_order(oo["id"])
                except ApiError as e:
                    if e.code not in ALREADY_DONE_CODES:
                        log.error(f"[{lot['name']}] 조건 해제 중 매도 주문 취소 실패 (다음에 다시 시도): {e}")
                        continue
            del self.lots[lid]
            log.info(f"[{lot['name']}] 🔓 보유 묶음 {lot['qty']}주 조건 해제")
            notify(f"🔓 [{lot['name']}] 보유 묶음 {lot['qty']}주의 매도·손절 조건을 해제했어요.\n"
                   f"봇은 더 이상 이 묶음을 팔지 않아요. 주식은 계좌에 그대로 있어요.", kakao=not self.dry)
            changed = True
        return changed

    def reconcile(self, real, now):
        """계좌 수량이 봇 기록보다 적으면 (앱에서 판 경우) 묶음 수량을 맞춤"""
        by_sym = {}
        for l in self.lots.values():
            if not l.get("sim"):
                by_sym.setdefault(l["symbol"], []).append(l)
        for sym, lots in by_sym.items():
            if any(l.get("open_order") for l in lots):
                continue
            if time.time() - self.recent_fill.get(sym, 0) < 60:
                continue
            have = real.get(sym, (0, 0))[0]
            total = sum(l["qty"] for l in lots)
            deficit = total - have
            if deficit <= 0:
                continue
            for l in sorted(lots, key=lambda x: x["opened_at"], reverse=True):
                if deficit <= 0:
                    break
                cut = min(l["qty"], deficit)
                avgc = l["cost"] / l["qty"] if l["qty"] else 0
                l["qty"] -= cut
                l["cost"] = int(avgc * l["qty"])
                deficit -= cut
                if l["qty"] == 0:
                    del self.lots[l["id"]]
                    msg = f"📱 [{l['name']}] 계좌에 이 묶음 주식이 없어서 조건을 해제했어요 (앱에서 판 것으로 보여요)."
                else:
                    msg = f"📱 [{l['name']}] 계좌 수량이 {cut}주 적어서 묶음을 {l['qty']}주로 맞췄어요 (앱에서 판 것으로 보여요)."
                log.info(msg)
                notify(msg)

    # ---------- 매수 (주문 카드) ----------
    def step_ticket(self, s, t, price, now):
        if t["open_order"] and getattr(self, "watch_only", False):
            return
        if t["open_order"]:
            self.check_buy(s, t, price, now)
            return
        remain = s["target_qty"] - t["bought"]
        if remain <= 0:
            t["done"] = True
            return
        zone = "above" if price > s["buy_max"] else ("below" if price < s["buy_min"] else "in")
        last, t["last_zone"] = t.get("last_zone"), zone
        if zone == "below" and last in ("above", "in"):
            t["done"], t["expired"] = True, True
            log.warning(f"[{s['name']}] 🚫 가격이 매수 범위를 지나 {won(price)}로 내려감 → 이 매수 주문 종료")
            notify(f"🚫 [{s['name']}] 가격이 매수 범위({won(s['buy_min'])}~{won(s['buy_max'])})를 지나쳐 "
                   f"{won(price)}까지 내려갔어요.\n이 매수 주문은 끝냈어요. 사려면 웹앱에서 범위를 다시 설정하세요.",
                   kakao=not self.dry)
            return
        if zone == "in" and not getattr(self, "watch_only", False):
            log.info(f"[{s['name']}] 🛒 매수 구간 진입 — 현재가 {won(price)} → {remain}주 {won(s['buy_price'])} 이하 지정가 매수")
            oid = "SIM-" + uuid.uuid4().hex[:12] if self.dry else \
                self.api.place_order(s["symbol"], "BUY", remain, "LIMIT", s["buy_price"])
            t["open_order"] = {"id": oid, "side": "BUY", "type": "LIMIT", "qty": remain, "price": s["buy_price"],
                               "reason": "구간매수", "placed_at": now.isoformat(), "cancel_requested": False,
                               "symbol": s["symbol"], "name": s["name"]}
            notify(f"📝 [{s['name']}] 매수 주문 접수\n{remain}주 · {won(s['buy_price'])} 지정가"
                   + ("" if self.dry else "\n취소하려면 토스 앱 → 주문내역 → 대기에서 취소하세요."), kakao=not self.dry)

    def fetch(self, o, price):
        if self.dry or str(o["id"]).startswith("SIM-"):
            if o["type"] == "MARKET":
                px = price
            elif o["side"] == "BUY":
                if price > o["price"]:
                    return "PENDING", 0, 0
                px = price
            else:
                if price < o["price"]:
                    return "PENDING", 0, 0
                px = price
            return "FILLED", o["qty"], px
        info = self.api.get_order(o["id"])
        ex = info.get("execution") or {}
        return info.get("status"), to_int(ex.get("filledQuantity")), \
            to_int(ex.get("averageFilledPrice")) or (o["price"] or price)

    def check_buy(self, s, t, price, now):
        o = t["open_order"]
        status, filled, avg = self.fetch(o, price)
        if status in FINAL_STATUSES:
            t["open_order"] = None
            if filled > 0:
                self.add_to_lot(s, t, filled, avg, now)
                t["reject_count"] = 0
            if t["bought"] >= s["target_qty"]:
                t["done"] = True
            if status == "CANCELED" and not o["cancel_requested"]:
                t["done"], t["manual_buy_stop"] = True, True
                log.info(f"[{s['name']}] 📱 앱에서 매수 주문 취소 확인 → 이 주문 중단")
                notify(f"📱 [{s['name']}] 앱에서 매수 주문을 취소한 걸 확인했어요.\n"
                       f"이 주문은 멈출게요. 다시 사려면 웹앱에서 숫자를 바꾸거나 '같은 조건으로 다시 매매'를 누르세요.")
            elif status == "CANCELED" and not filled:
                notify(f"↩️ [{s['name']}] 매수 주문 취소 완료 — 체결 없음", kakao=False)
            elif status == "REJECTED":
                self.ticket_error(s, t, ApiError(0, "REJECTED", "증권사에서 주문 거부"))
            return
        if o["cancel_requested"]:
            return
        age = (now - datetime.fromisoformat(o["placed_at"])).total_seconds()
        if age > self.settings["order_timeout"]:
            log.info(f"[{s['name']}] 매수 미체결 시간 초과 → 취소 요청")
            notify(f"↩️ [{s['name']}] 매수 주문이 {self.settings['order_timeout']}초 안에 안 사져서 취소 요청", kakao=False)
            if not str(o["id"]).startswith("SIM-"):
                try:
                    self.api.cancel_order(o["id"])
                except ApiError as e:
                    if e.code not in ALREADY_DONE_CODES:
                        raise
            o["cancel_requested"] = True

    def add_to_lot(self, s, t, filled, avg, now):
        lid = f"{s['id']}@{t['run']}"
        lot = self.lots.get(lid)
        if not lot:
            lot = self.lots[lid] = {
                "id": lid, "symbol": s["symbol"], "name": s["name"], "ticket": s["id"], "run": t["run"],
                "qty": 0, "cost": 0, "sell_min": s["sell_min"], "stop_loss": s["stop_loss"], "stop_min": s["stop_min"],
                "opened_at": now.isoformat(timespec="seconds"), "open_order": None,
                "no_take_profit": False, "reject_count": 0, "sim": self.dry, "pnl": 0}
        lot["qty"] += filled
        lot["cost"] += filled * avg
        t["bought"] += filled
        self.recent_fill[s["symbol"]] = time.time()
        log.info(f"[{s['name']}] ✔ 매수 체결 {filled}주 @ {won(avg)} → 보유 묶음 {lot['qty']}주")
        notify(f"🛒 [{s['name']}] 매수 체결\n{filled}주 × {won(avg)} = {won(filled * avg)}\n"
               f"이 묶음 {lot['qty']}주 · {won(lot['sell_min'])}↑ 매도 / {stop_text(lot)} 손절", kakao=not self.dry)
        record_trade([datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"), s["name"], s["symbol"], "매수", "구간매수",
                      filled, avg, filled * avg, "모의" if self.dry else "실전"])

    def ticket_error(self, s, t, e):
        if e.code == "order-hours-closed":
            self.closed_until = datetime.now(KST) + timedelta(minutes=10)
            log.warning("거래 가능 시간이 아닙니다 (휴장일 등). 10분 뒤 다시 확인합니다.")
            return
        t["reject_count"] += 1
        log.error(f"[{s['name']}] 매수 주문 실패 ({t['reject_count']}/3): {e}")
        if t["reject_count"] >= 3:
            t["paused"] = True
            notify(f"⚠️ [{s['name']}] 매수 주문이 3번 연속 실패해서 이 주문을 일시정지했어요.\n"
                   f"사유: {e.code or e}\n웹앱에서 숫자를 고치면 다시 시작해요.")

    # ---------- 매도 (보유 묶음) ----------
    def step_lot(self, lot, price, now):
        if lot.get("open_order") and getattr(self, "watch_only", False):
            return
        if lot.get("open_order"):
            self.check_sell(lot, price, now)
            return
        if lot["qty"] <= 0:
            del self.lots[lot["id"]]
            return
        if lot.get("sell_min") is None or lot.get("stop_loss") is None:
            return
        stop_hit = price <= lot["stop_loss"]
        take_hit = price >= lot["sell_min"] and not lot.get("no_take_profit")
        floor = lot.get("stop_min")
        if floor:
            zone = "above" if price > lot["stop_loss"] else ("below" if price < floor else "in")
            last, lot["stop_zone"] = lot.get("stop_zone"), zone
            if zone == "below" and last in ("above", "in") and not lot.get("stop_expired"):
                lot["stop_expired"] = True
                log.warning(f"[{lot['name']}] 🚫 가격이 손절 범위를 지나 {won(price)}로 내려감 → 손절 조건 꺼짐")
                notify(f"🚫 [{lot['name']}] 가격이 손절 범위({won(floor)}~{won(lot['stop_loss'])})를 지나쳐 "
                       f"{won(price)}까지 내려갔어요.\n이 묶음({lot['qty']}주)의 손절 조건은 껐어요. 봇은 팔지 않아요.\n"
                       f"웹앱 현황 → 이 묶음 '조건 수정'에서 손절 범위를 다시 설정하세요.",
                       kakao=not (self.dry or lot.get("sim")))
            if lot.get("stop_expired") or zone == "below":
                stop_hit = False
        if getattr(self, "watch_only", False):
            return          # 프리마켓: 범위 지나침만 확인하고 주문은 9시부터
        if not (stop_hit or take_hit):
            return
        qty = lot["qty"] if (self.dry or lot.get("sim")) else min(lot["qty"], self.api.sellable(lot["symbol"]))
        if qty <= 0:
            self.notice_once(f"[{lot['name']}] 매도 조건이지만 매도 가능 수량이 0주 (앱에 매도 주문이 걸려 있는지 확인)")
            return
        if stop_hit and floor:
            otype, px, reason = "LIMIT", tick_up(floor), "손절"
            log.warning(f"[{lot['name']}] 🔻 손절 범위 진입 — 현재가 {won(price)} → {qty}주 {won(px)} 이상 지정가 매도")
        elif stop_hit:
            otype, px, reason = "MARKET", None, "손절"
            log.warning(f"[{lot['name']}] 🔻 손절 조건 — 현재가 {won(price)} ≤ {won(lot['stop_loss'])} → {qty}주 시장가 매도")
        else:
            otype, px, reason = "LIMIT", tick_up(lot["sell_min"]), "익절"
            log.info(f"[{lot['name']}] 🎯 익절 조건 — 현재가 {won(price)} → {qty}주 {won(px)} 지정가 매도")
        sim = self.dry or lot.get("sim")
        oid = "SIM-" + uuid.uuid4().hex[:12] if sim else self.api.place_order(lot["symbol"], "SELL", qty, otype, px)
        lot["open_order"] = {"id": oid, "side": "SELL", "type": otype, "qty": qty, "price": px, "reason": reason,
                             "placed_at": now.isoformat(), "cancel_requested": False}
        how = "시장가" if px is None else (f"{won(px)} 이상에만 파는 지정가" if reason == "손절" else won(px) + " 지정가")
        notify(f"📝 [{lot['name']}] {reason} 매도 주문 접수\n{qty}주 · {how}"
               + ("" if sim else "\n취소하려면 토스 앱 → 주문내역 → 대기에서 취소하세요."), kakao=not sim)

    def check_sell(self, lot, price, now):
        o = lot["open_order"]
        status, filled, avg = self.fetch(o, price)
        sim = self.dry or lot.get("sim")
        if status in FINAL_STATUSES:
            lot["open_order"] = None
            if filled > 0:
                avgc = lot["cost"] / lot["qty"] if lot["qty"] else avg
                pnl = int((avg - avgc) * filled)
                lot["qty"] -= filled
                lot["cost"] = int(avgc * lot["qty"])
                lot["pnl"] = lot.get("pnl", 0) + pnl
                self.state["pnl"] = self.state.get("pnl", 0) + pnl
                lot["reject_count"] = 0
                self.recent_fill[lot["symbol"]] = time.time()
                icon = "🔻" if o["reason"] == "손절" else "🎯"
                tail = "이 묶음은 정리 끝" if lot["qty"] == 0 else f"이 묶음 남은 수량 {lot['qty']}주"
                log.info(f"[{lot['name']}] ✔ {o['reason']} 매도 체결 {filled}주 @ {won(avg)} — 손익 {pnl:+,}원 ({tail})")
                notify(f"{icon} [{lot['name']}] {o['reason']} 매도 체결\n{filled}주 × {won(avg)} = {won(filled * avg)}\n"
                       f"손익 약 {pnl:+,}원 (수수료·세금 제외)\n{tail}", kakao=not sim)
                record_trade([datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"), lot["name"], lot["symbol"], "매도",
                              o["reason"], filled, avg, filled * avg, "모의" if sim else "실전"])
                if lot["qty"] <= 0:
                    del self.lots[lot["id"]]
                    return
            if status == "CANCELED" and not o["cancel_requested"]:
                if o["reason"] != "손절":
                    lot["no_take_profit"] = True
                    notify(f"📱 [{lot['name']}] 앱에서 매도 주문을 취소한 걸 확인했어요.\n"
                           f"이 묶음의 익절 매도는 멈추고, 손절가는 계속 지켜볼게요.")
            elif status == "CANCELED" and not filled:
                notify(f"↩️ [{lot['name']}] 매도 주문 취소 완료 — 체결 없음", kakao=False)
            elif status == "REJECTED":
                self.lot_error(lot, ApiError(0, "REJECTED", "증권사에서 주문 거부"))
            return
        if o["cancel_requested"]:
            return
        age = (now - datetime.fromisoformat(o["placed_at"])).total_seconds()
        why = None
        if o["reason"] == "익절" and price <= lot["stop_loss"]:
            why = "손절가 도달 — 손절 우선"
        elif o["reason"] == "익절" and age > self.settings["order_timeout"] and price < lot["sell_min"]:
            why = "가격이 매도가 아래로 내려감"
        elif o["reason"] == "손절" and o["type"] == "LIMIT" and age > self.settings["order_timeout"]:
            why = "손절 주문이 시간 안에 안 팔림 — 다시 판단"
        if why:
            log.info(f"[{lot['name']}] 매도 주문 취소 요청 ({why})")
            notify(f"↩️ [{lot['name']}] 봇이 매도 주문 취소 요청 ({why})", kakao=False)
            if not str(o["id"]).startswith("SIM-"):
                try:
                    self.api.cancel_order(o["id"])
                except ApiError as e:
                    if e.code not in ALREADY_DONE_CODES:
                        raise
            o["cancel_requested"] = True

    def lot_error(self, lot, e):
        if e.code == "order-hours-closed":
            self.closed_until = datetime.now(KST) + timedelta(minutes=10)
            log.warning("거래 가능 시간이 아닙니다 (휴장일 등). 10분 뒤 다시 확인합니다.")
            return
        lot["reject_count"] = lot.get("reject_count", 0) + 1
        log.error(f"[{lot['name']}] 매도 주문 실패 ({lot['reject_count']}): {e}")
        if lot["reject_count"] % 3 == 0:
            notify(f"⚠️ [{lot['name']}] 매도 주문이 계속 실패하고 있어요 ({lot['reject_count']}번).\n"
                   f"사유: {e.code or e}\n토스 앱에서 직접 확인해 주세요. 봇은 계속 다시 시도해요.")

    # ---------- 웹앱 현황 ----------
    def build_status(self, now, market):
        def order_view(oo):
            if not oo:
                return None
            return {"side": "매수" if oo["side"] == "BUY" else "매도", "qty": oo["qty"], "price": oo["price"],
                    "type": "시장가" if oo["type"] == "MARKET" else "지정가", "reason": oo["reason"],
                    "placed_at": oo["placed_at"], "canceling": oo.get("cancel_requested", False)}
        tickets = []
        for s in self.stocks:
            t = self.tickets.get(s["id"]) or new_ticket()
            if t["paused"]:
                label = "일시정지"
            elif t.get("manual_buy_stop"):
                label = "매수 중단 (앱에서 취소)"
            elif t.get("expired"):
                label = "범위 지나감 — 다시 설정 필요"
            elif t["done"]:
                label = "매수 완료"
            elif t["open_order"]:
                label = "매수 주문 대기 중"
            elif t["bought"]:
                label = f"일부 매수 ({t['bought']}/{s['target_qty']}주)"
            else:
                label = "매수 대기"
            tickets.append({"id": s["id"], "name": s["name"], "symbol": s["symbol"],
                            "price": self.last_prices.get(s["symbol"]), "label": label,
                            "bought": t["bought"], "target": s["target_qty"],
                            "buy_min": s["buy_min"], "buy_max": s["buy_max"], "order": order_view(t["open_order"])})
        lots = []
        for l in sorted(self.lots.values(), key=lambda x: x["opened_at"]):
            notes = []
            if l.get("no_take_profit"):
                notes.append("앱에서 매도 취소 → 익절 멈춤 (손절은 감시 중)")
            if l.get("sim"):
                notes.append("모의 매매 묶음")
            if l.get("stop_expired"):
                notes.append("🚫 손절 범위를 지나쳐서 손절이 꺼졌어요 — '조건 수정'으로 다시 설정하세요")
            lots.append({"id": l["id"], "name": l["name"], "symbol": l["symbol"], "qty": l["qty"],
                         "avg": int(l["cost"] / l["qty"]) if l["qty"] else None,
                         "price": self.last_prices.get(l["symbol"]), "sell_min": l["sell_min"],
                         "stop_loss": l["stop_loss"], "stop_min": l.get("stop_min"),
                         "stop_expired": bool(l.get("stop_expired")),
                         "opened_at": l["opened_at"], "pnl": l.get("pnl", 0),
                         "order": order_view(l.get("open_order")), "notes": notes})
        return {"updated_at": now.isoformat(timespec="seconds"), "market_open": market,
                "mode": "모의" if self.dry else "실전", "started_at": self.started_at,
                "pnl": self.state.get("pnl", 0), "tickets": tickets, "lots": lots, "events": EVENTS[-30:]}

    def publish(self, now, market, phase=None):
        if not GH_FILE.exists():
            return          # 현황 저장소 연결 전 — 연결되면 바로 올라감
        payload = self.build_status(now, market)
        payload["phase"] = phase or ("trade" if market else "closed")
        strip = lambda rows: [{k: v for k, v in r.items() if k != "price"} for r in rows]
        fp = json.dumps([payload["mode"], payload["phase"], payload["events"][-1:],
                         strip(payload["tickets"]), strip(payload["lots"])], ensure_ascii=False)
        wait = 60 if market else 600
        if fp != self.last_fp or time.time() - self.last_pub >= wait:
            self.last_fp, self.last_pub = fp, time.time()
            self.publisher.submit(payload)


def seed_events_from_trades():
    """예전 체결 기록(trades.csv) 중 '최근 기록'에 없는 것을 복원"""
    if not TRADES_FILE.exists():
        return
    try:
        with open(TRADES_FILE, encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))[1:]
    except Exception:
        return
    have = {(e["t"][:16], e["text"][:1]) for e in EVENTS}
    added = False
    for r in rows[-30:]:
        try:
            ts, name, sym, side, reason, qty, avg, amount, mode = r[:9]
            t = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST).isoformat(timespec="seconds")
        except Exception:
            continue
        icon = "🛒" if side == "매수" else ("🔻" if reason == "손절" else "🎯")
        if (t[:16], icon) in have:
            continue
        have.add((t[:16], icon))
        tag = " (모의)" if mode == "모의" else ""
        EVENTS.append({"t": t, "text": f"{icon} [{name}] {side} 체결{tag}\n{qty}주 × {won(avg)} = {won(amount)}"})
        added = True
    if added:
        EVENTS.sort(key=lambda e: e["t"])
        del EVENTS[:-60]
        try:
            EVENTS_FILE.write_text(json.dumps(EVENTS, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass


# ─────────────────────────── 실행 ───────────────────────────
def main():
    args = sys.argv[1:]
    if args[:1] == ["--github"]:
        github_setup()
        return
    if args[:1] == ["--kakao"]:
        kakao_setup()
        return
    if args[:1] == ["--kakao-test"]:
        _kakao_send_now("🔔 테스트 알림입니다. 카톡 연결이 잘 되어 있어요.")
        print("보냈습니다. 카카오톡 '나와의 채팅'을 확인하세요.")
        return
    if args[:1] == ["--status"]:
        st = load_state()
        if st.get("version") != 2:
            print("아직 새 구조로 옮겨지지 않았습니다 (봇이 한 번 실행되면 옮겨져요).")
            return
        for tid, t in st["tickets"].items():
            flag = "완료" if t["done"] else "일시정지" if t["paused"] else "진행중"
            print(f"주문 {tid}: {flag} | 산 수량 {t['bought']}주 | 대기 주문 {'있음' if t['open_order'] else '없음'}")
        for lid, l in st["lots"].items():
            print(f"묶음 {lid}: {l['name']} {l['qty']}주 | 매도 {l['sell_min']} / 손절 {l.get('stop_min')}~{l['stop_loss']}")
        print(f"누적 실현손익 {st.get('pnl', 0):+,}원")
        return
    try:
        Bot().run()
    except ValueError as e:
        sys.exit(f"❌ {e}")
    except KeyboardInterrupt:
        log.info("봇 종료")


if __name__ == "__main__":
    main()
