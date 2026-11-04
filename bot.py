#!/usr/bin/env python3
"""
레이브 주식 자동매매 봇 — 토스증권 Open API (국내주식)

동작 규칙 (종목마다 config.yaml 에서 설정)
  1) 현재가가 [매수 하한 ~ 매수 상한] 안에 들어오면 → 예산만큼 지정가 매수
  2) 보유 중 현재가가 [매도 하한] 이상이 되면 → 전량 지정가 매도 (익절)
  3) 보유 중 현재가가 [손절가] 이하로 떨어지면 → 전량 시장가 매도 (손절)
  4) 매도가 끝나면 그 종목은 '완료' — 다시 돌리려면  python bot.py --reset 종목코드

계좌 연동 (account_sync: true)
  매번 실제 토스 계좌의 보유 수량을 읽어 판단합니다.
  핸드폰으로 사고판 수량도 반영 → 매도·손절은 '계좌에 있는 그 종목 전체',
  매수는 '예산 목표 수량 - 이미 가진 수량'만큼만.

사용법
  python bot.py            봇 실행
  python bot.py --status   현재 종목별 상태 보기
  python bot.py --reset 005930   해당 종목 상태 초기화 (처음부터 다시)
"""
import csv
import json
import logging
import os
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
        self._tok, self._exp = None, 0.0
        self.s = requests.Session()

    def token(self) -> str:
        if self._tok and time.time() < self._exp - 300:
            return self._tok
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
        log.info("토큰 발급 완료")
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
            if r.status_code == 401 and attempt == 0:
                self._tok = None
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
    for i, s in enumerate(cfg.get("stocks") or [], 1):
        name = s.get("name") or f"{i}번 종목"
        if not s.get("enabled", True):
            continue
        try:
            item = {
                "name": name,
                "symbol": str(s["symbol"]).zfill(6),
                "budget": int(s["budget"]),
                "buy_min": int(s["buy_min"]), "buy_max": int(s["buy_max"]),
                "sell_min": int(s["sell_min"]),
                "stop_loss": int(s["stop_loss"]),
                "sync": bool(s.get("account_sync", True)),
                "round": int(s.get("round", 0) or 0),
            }
        except (KeyError, ValueError, TypeError) as e:
            errors.append(f"{name}: 값이 빠졌거나 숫자가 아님 ({e})")
            continue
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
        "start": st.get("trade_start", "09:00"),
        "end": st.get("trade_end", "15:19"),
    }
    return settings, stocks


# ─────────────────────────── 상태 저장 ───────────────────────────
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def save_state(state):
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


def new_stock_state():
    return {"held_qty": 0, "buy_amount": 0, "buy_complete": False, "done": False,
            "paused": False, "reject_count": 0, "open_order": None, "realized_pnl": 0,
            "sim_offset": 0}


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


def notify(text):
    """카톡 알림 — 실패해도 매매는 계속 (별도 스레드에서 전송)"""
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


def record_trade(row):
    new = not TRADES_FILE.exists()
    with open(TRADES_FILE, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["시각", "종목", "코드", "구분", "사유", "수량", "평균가", "금액", "모의여부"])
        w.writerow(row)


# ─────────────────────────── 봇 ───────────────────────────
class Bot:
    def __init__(self):
        self.api = Toss()
        self.settings, self.stocks = load_config()
        self.cfg_mtime = CONFIG_FILE.stat().st_mtime
        self.code_mtime = Path(__file__).stat().st_mtime
        self.state = load_state()
        self.closed_until = None
        self.last_notice = ""

    @property
    def dry(self):
        return self.settings["dry_run"]

    def reload_config_if_changed(self):
        m = CONFIG_FILE.stat().st_mtime
        if m != self.cfg_mtime:
            self.cfg_mtime = m
            was_dry = self.dry
            try:
                self.settings, self.stocks = load_config()
                log.info("⚙️  config.yaml 변경 감지 → 새 설정 적용")
            except Exception as e:
                log.error(f"새 설정을 적용하지 못했습니다 (이전 설정 유지): {e}")
                return
            if was_dry and not self.dry:
                # 모의로 사고판 기록이 실전에 섞이지 않게 초기화
                for sym, st in list(self.state.items()):
                    oo = st.get("open_order")
                    if not oo or str(oo.get("id", "")).startswith("SIM-"):
                        self.state[sym] = new_stock_state()
                save_state(self.state)
                log.warning("🔴 실전 모드로 전환 — 모의 매매 기록을 초기화했습니다")
                notify("🔴 실전 모드로 전환됐어요.\n이제 조건이 맞으면 실제 주문이 나갑니다.")
            elif not was_dry and self.dry:
                log.info("🧪 모의 모드로 전환")
                notify("🧪 모의 모드로 전환됐어요. 실제 주문은 나가지 않아요.")

    def notice_once(self, msg):
        if msg != self.last_notice:
            log.info(msg)
            self.last_notice = msg

    def market_open(self, now):
        if now.weekday() >= 5:
            return False
        if self.closed_until and now < self.closed_until:
            return False
        hm = now.strftime("%H:%M")
        return self.settings["start"] <= hm < self.settings["end"]

    def run(self):
        mode = "🧪 모의 모드 (실제 주문 안 나감)" if self.dry else "🔴 실전 모드 (실제 주문)"
        log.info(f"봇 시작 — {mode}")
        if not self.dry:
            notify("▶️ 자동매매 봇이 시작됐어요 (실전 모드).\n" +
                   "\n".join(f"· {s['name']} {won(s['buy_min'])}~{won(s['buy_max'])} 매수 / "
                             f"{won(s['sell_min'])}↑ 매도 / {won(s['stop_loss'])}↓ 손절" for s in self.stocks))
        self.api.account_seq()
        for s in self.stocks:
            log.info(f"  {s['name']}({s['symbol']}) 예산 {won(s['budget'])} → 목표 {s['target_qty']}주 | "
                     f"매수 {won(s['buy_min'])}~{won(s['buy_max'])} | "
                     f"매도 {won(s['sell_min'])} 이상 | 손절 {won(s['stop_loss'])} 이하")
        while True:
            try:
                if Path(__file__).stat().st_mtime != self.code_mtime:
                    log.info("♻️  봇 프로그램이 업데이트되어 다시 시작합니다")
                    save_state(self.state)
                    sys.exit(0)   # systemd가 10초 뒤 새 코드로 자동 재시작
                self.reload_config_if_changed()
                now = datetime.now(KST)
                if not self.market_open(now):
                    self.notice_once("장 시간이 아니라 대기 중…")
                    time.sleep(30)
                    continue
                self.tick(now)
            except ApiError as e:
                log.error(f"API 오류: {e}")
            except Exception:
                log.exception("예상치 못한 오류 (계속 실행)")
            time.sleep(self.settings["interval"])

    def tick(self, now):
        active = []
        for s in self.stocks:
            st = self.state.setdefault(s["symbol"], new_stock_state())
            sig = [s["budget"], s["buy_min"], s["buy_max"], s["sell_min"], s["stop_loss"], s["round"]]
            if st.get("cfg_sig") is not None and st["cfg_sig"] != sig:
                if st["done"] and st["held_qty"] == 0 and not st["open_order"]:
                    self.state[s["symbol"]] = st = new_stock_state()
                    log.info(f"[{s['name']}] ⚙️  조건 변경 → 새로 시작")
                elif st["paused"]:
                    st["paused"], st["reject_count"] = False, 0
                    log.info(f"[{s['name']}] ⚙️  조건 변경 → 일시정지 해제")
                if st.get("manual_buy_stop") and not st["done"]:
                    st["manual_buy_stop"], st["buy_complete"] = False, False
                    log.info(f"[{s['name']}] ⚙️  조건 변경 → 매수 재개")
                if st.get("no_take_profit"):
                    st["no_take_profit"] = False
                    log.info(f"[{s['name']}] ⚙️  조건 변경 → 익절 매도 재개")
            st["cfg_sig"] = sig
            if not st["done"] and not st["paused"]:
                active.append(s)
        self.cancel_orphan_orders()
        if not active:
            self.notice_once("진행 중인 종목이 없습니다 (모두 완료 또는 일시정지)")
            return
        self.notice_once(f"감시 중: {', '.join(s['name'] for s in active)}")
        prices = self.api.prices([s["symbol"] for s in active])
        real = self.api.holdings() if any(s["sync"] for s in active) else {}
        for s in active:
            price = prices.get(s["symbol"])
            if not price:
                continue
            st = self.state[s["symbol"]]
            for k, v in new_stock_state().items():   # 예전 상태 파일 호환
                st.setdefault(k, v)
            if s["sync"]:
                self.sync_holding(s, st, real.get(s["symbol"], (0, 0)), now)
            try:
                self.step(s, st, price, now)
            except ApiError as e:
                self.handle_order_error(s, e)
            save_state(self.state)

    def handle_order_error(self, s, e):
        st = self.state[s["symbol"]]
        if e.code == "order-hours-closed":
            self.closed_until = datetime.now(KST) + timedelta(minutes=10)
            log.warning("거래 가능 시간이 아닙니다 (휴장일 등). 10분 뒤 다시 확인합니다.")
            return
        st["reject_count"] += 1
        log.error(f"[{s['name']}] 주문 실패 ({st['reject_count']}/3): {e}")
        if st["reject_count"] >= 3:
            st["paused"] = True
            log.error(f"[{s['name']}] 연속 실패로 이 종목을 일시정지했습니다. "
                      f"원인 확인 후 --reset 하거나 상태를 점검하세요.")
            notify(f"⚠️ [{s['name']}] 주문이 3번 연속 실패해서 일시정지했어요.\n"
                   f"사유: {e.code or e}\n웹앱에서 숫자를 고치면 다시 시작해요.")

    def sync_holding(self, s, st, real, now):
        """실제 계좌 수량을 봇 장부에 반영 (핸드폰으로 사고판 것 포함)"""
        if st["open_order"]:
            return                      # 주문 진행 중엔 주문 결과로 판단
        last = st.get("last_fill_at")
        if last and (now - datetime.fromisoformat(last)).total_seconds() < 30:
            return                      # 체결 직후 계좌 반영 지연 대비 30초 유예
        real_qty, real_avg = real
        new_qty = max(0, real_qty + (st["sim_offset"] if self.dry else 0))
        if new_qty == st["held_qty"]:
            return
        log.info(f"[{s['name']}] 📱 계좌 수량 변화 감지: {st['held_qty']}주 → {new_qty}주 (앱 거래 반영)")
        st["held_qty"] = new_qty
        if self.dry:
            st["buy_amount"] = max(0, real_avg * real_qty + st.get("sim_cost", 0))
        else:
            st["buy_amount"] = real_avg * new_qty

    def sell_qty(self, s, st) -> int:
        if s["sync"] and not self.dry:
            return min(self.api.sellable(s["symbol"]), st["held_qty"]) if st["held_qty"] else 0
        return st["held_qty"]

    # 한 종목 판단
    def step(self, s, st, price, now):
        if st["open_order"]:
            self.check_open_order(s, st, price, now)
            return

        held = st["held_qty"]
        stop_hit = held > 0 and price <= s["stop_loss"]
        take_hit = held > 0 and price >= s["sell_min"] and not st.get("no_take_profit")
        if stop_hit or take_hit:
            st["buy_complete"] = True
            qty = self.sell_qty(s, st)
            if qty <= 0:
                self.notice_once(f"[{s['name']}] 매도 조건이지만 매도 가능 수량이 0주 (앱에서 매도 주문 걸려 있는지 확인)")
            elif stop_hit:
                log.warning(f"[{s['name']}] 🔻 손절 조건 — 현재가 {won(price)} ≤ {won(s['stop_loss'])} → {qty}주 시장가 매도")
                self.order(s, st, "SELL", qty, "MARKET", None, "손절", price, now)
            else:
                log.info(f"[{s['name']}] 🎯 익절 조건 — 현재가 {won(price)} → {qty}주 {won(s['sell_price'])} 지정가 매도")
                self.order(s, st, "SELL", qty, "LIMIT", s["sell_price"], "익절", price, now)
        elif not st["buy_complete"] and s["buy_min"] <= price <= s["buy_max"]:
            remain = s["target_qty"] - held
            if remain <= 0:
                st["buy_complete"] = True
                return
            log.info(f"[{s['name']}] 🛒 매수 구간 진입 — 현재가 {won(price)} → {remain}주 {won(s['buy_price'])} 이하 지정가 매수")
            self.order(s, st, "BUY", remain, "LIMIT", s["buy_price"], "구간매수", price, now)

        if st["buy_complete"] and st["held_qty"] == 0 and not st["open_order"]:
            st["done"] = True
            log.info(f"[{s['name']}] ✅ 매매 완료 — 누적 손익(수수료·세금 제외) {st['realized_pnl']:+,}원")

    def order(self, s, st, side, qty, otype, price, reason, cur_price, now):
        if self.dry:
            oid = "SIM-" + uuid.uuid4().hex[:12]
        else:
            oid = self.api.place_order(s["symbol"], side, qty, otype, price)
        st["open_order"] = {"id": oid, "side": side, "type": otype, "qty": qty, "price": price,
                            "reason": reason, "placed_at": now.isoformat(), "cancel_requested": False}
        if not self.dry:
            how = "시장가" if otype == "MARKET" else f"{won(price)} 지정가"
            notify(f"📝 [{s['name']}] {'매수' if side == 'BUY' else '매도'} 주문 접수 ({reason})\n"
                   f"{qty}주 · {how}\n취소하려면 토스 앱 → 주문내역 → 대기에서 취소하세요.")

    def cancel_orphan_orders(self):
        """웹앱에서 끄거나 지운 종목에 걸려 있던 주문 취소"""
        on = {s["symbol"] for s in self.stocks}
        changed = False
        for sym, st in self.state.items():
            oo = st.get("open_order")
            if not oo or sym in on:
                continue
            if not self.dry and not str(oo["id"]).startswith("SIM-"):
                try:
                    self.api.cancel_order(oo["id"])
                except ApiError as e:
                    if e.code not in ALREADY_DONE_CODES:
                        log.error(f"[{sym}] 꺼진 종목 주문 취소 실패: {e}")
                        continue
                notify(f"🛑 [{sym}] 종목을 꺼서 걸려 있던 주문을 취소했어요.")
            log.info(f"[{sym}] 종목이 꺼져서 대기 주문을 취소했습니다")
            st["open_order"] = None
            st["last_fill_at"] = datetime.now(KST).isoformat()
            changed = True
        if changed:
            save_state(self.state)

    def check_open_order(self, s, st, price, now):
        o = st["open_order"]
        if self.dry:
            # 모의 체결: 지정가는 유리한 쪽으로 즉시 체결된 것으로 가정
            if o["type"] == "MARKET":
                fill_px = price
            elif o["side"] == "BUY":
                fill_px = min(price, o["price"])
            else:
                fill_px = max(price, o["price"])
            status, filled, avg = "FILLED", o["qty"], fill_px
        else:
            info = self.api.get_order(o["id"])
            ex = info.get("execution") or {}
            status = info.get("status")
            filled = to_int(ex.get("filledQuantity"))
            avg = to_int(ex.get("averageFilledPrice")) or (o["price"] or price)

        if status in FINAL_STATUSES:
            self.apply_fill(s, st, o, status, filled, avg)
            st["last_fill_at"] = now.isoformat()
            st["open_order"] = None
            if status == "REJECTED":
                self.handle_order_error(s, ApiError(0, "REJECTED", "거래소/증권사에서 주문 거부"))
            elif filled > 0:
                st["reject_count"] = 0
            if status == "CANCELED" and not o["cancel_requested"]:
                # 봇이 취소한 게 아니라 사장님이 토스 앱에서 직접 취소함 → 같은 주문 반복 금지
                if o["side"] == "BUY":
                    st["buy_complete"], st["manual_buy_stop"] = True, True
                    log.info(f"[{s['name']}] 📱 앱에서 매수 주문 취소 확인 → 매수 중단")
                    notify(f"📱 [{s['name']}] 앱에서 매수 주문을 취소한 걸 확인했어요.\n"
                           f"더 이상 사지 않아요. 다시 사려면 웹앱에서 숫자를 바꾸거나 '같은 조건으로 다시 매매'를 누르세요.")
                elif o["reason"] != "손절":
                    st["no_take_profit"] = True
                    log.info(f"[{s['name']}] 📱 앱에서 매도 주문 취소 확인 → 익절 매도 중단 (손절은 계속 감시)")
                    notify(f"📱 [{s['name']}] 앱에서 매도 주문을 취소한 걸 확인했어요.\n"
                           f"익절 매도는 멈추고, 손절가는 계속 지켜볼게요.")
            return

        # 아직 미체결 — 취소가 필요한지 판단
        if o["cancel_requested"]:
            return
        age = (now - datetime.fromisoformat(o["placed_at"])).total_seconds()
        why = None
        if price <= s["stop_loss"] and not (o["side"] == "SELL" and o["type"] == "MARKET"):
            why = "손절가 도달 — 손절 우선"
        elif age > self.settings["order_timeout"]:
            if o["side"] == "BUY":
                why = "매수 미체결 시간 초과"
            elif o["type"] == "LIMIT" and price < s["sell_min"]:
                why = "가격이 매도 구간을 벗어남"
        if why:
            log.info(f"[{s['name']}] 주문 취소 요청 ({why})")
            try:
                self.api.cancel_order(o["id"])
            except ApiError as e:
                if e.code not in ALREADY_DONE_CODES:
                    raise
            o["cancel_requested"] = True

    def apply_fill(self, s, st, o, status, filled, avg):
        if filled <= 0:
            log.info(f"[{s['name']}] 주문 종료({status}) — 체결 없음")
            return
        amount = filled * avg
        if self.dry:   # 모의 체결분은 실제 계좌에 없으므로 따로 기억
            st["sim_offset"] += filled if o["side"] == "BUY" else -filled
            if o["side"] == "BUY":
                st["sim_cost"] = st.get("sim_cost", 0) + amount
            elif st["held_qty"]:
                st["sim_cost"] = st.get("sim_cost", 0) - int(st["buy_amount"] / st["held_qty"] * filled)
        if o["side"] == "BUY":
            st["held_qty"] += filled
            st["buy_amount"] += amount
            if st["held_qty"] >= s["target_qty"]:
                st["buy_complete"] = True
            log.info(f"[{s['name']}] ✔ 매수 체결 {filled}주 @ {won(avg)} (보유 {st['held_qty']}주)")
            if not self.dry:
                notify(f"🛒 [{s['name']}] 매수 체결\n{filled}주 × {won(avg)} = {won(amount)}\n"
                       f"보유 {st['held_qty']}주")
        else:
            held_before = st["held_qty"]
            avg_buy = st["buy_amount"] / held_before if held_before else avg
            pnl = int((avg - avg_buy) * filled)
            st["realized_pnl"] += pnl
            st["held_qty"] -= filled
            st["buy_amount"] = int(avg_buy * st["held_qty"])
            log.info(f"[{s['name']}] ✔ 매도 체결({o['reason']}) {filled}주 @ {won(avg)} — 손익 {pnl:+,}원 (남은 {st['held_qty']}주)")
            if not self.dry:
                icon = "🔻" if o["reason"] == "손절" else "🎯"
                notify(f"{icon} [{s['name']}] {o['reason']} 매도 체결\n{filled}주 × {won(avg)} = {won(amount)}\n"
                       f"손익 약 {pnl:+,}원 (수수료·세금 제외)\n남은 수량 {st['held_qty']}주")
        record_trade([datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"), s["name"], s["symbol"],
                      "매수" if o["side"] == "BUY" else "매도", o["reason"], filled, avg, amount,
                      "모의" if self.dry else "실전"])


# ─────────────────────────── 실행 ───────────────────────────
def main():
    args = sys.argv[1:]
    if args[:1] == ["--kakao"]:
        kakao_setup()
        return
    if args[:1] == ["--kakao-test"]:
        _kakao_send_now("🔔 테스트 알림입니다. 카톡 연결이 잘 되어 있어요.")
        print("보냈습니다. 카카오톡 '나와의 채팅'을 확인하세요.")
        return
    if args[:1] == ["--status"]:
        state = load_state()
        if not state:
            print("저장된 상태가 없습니다.")
        for sym, st in state.items():
            flag = "완료" if st["done"] else "일시정지" if st["paused"] else "진행중"
            print(f"{sym}: {flag} | 보유 {st['held_qty']}주 | 미체결 {'있음' if st['open_order'] else '없음'} "
                  f"| 누적손익 {st['realized_pnl']:+,}원")
        return
    if args[:1] == ["--reset"] and len(args) == 2:
        state = load_state()
        sym = args[1].zfill(6)
        st = state.get(sym)
        if st and (st["held_qty"] > 0 or st["open_order"]):
            sys.exit(f"⚠️  {sym}은 보유 주식 또는 미체결 주문이 있어 초기화하지 않았습니다. 앱에서 먼저 정리하세요.")
        state.pop(sym, None)
        save_state(state)
        print(f"{sym} 상태를 초기화했습니다. 다음 실행 때 처음부터 다시 매매합니다.")
        return
    try:
        Bot().run()
    except ValueError as e:
        sys.exit(f"❌ {e}")
    except KeyboardInterrupt:
        log.info("봇 종료")


if __name__ == "__main__":
    main()
