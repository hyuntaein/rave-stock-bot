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
import sys
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
            elif not was_dry and self.dry:
                log.info("🧪 모의 모드로 전환")

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
            st["cfg_sig"] = sig
            if not st["done"] and not st["paused"]:
                active.append(s)
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
        take_hit = held > 0 and price >= s["sell_min"]
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
        else:
            held_before = st["held_qty"]
            avg_buy = st["buy_amount"] / held_before if held_before else avg
            pnl = int((avg - avg_buy) * filled)
            st["realized_pnl"] += pnl
            st["held_qty"] -= filled
            st["buy_amount"] = int(avg_buy * st["held_qty"])
            log.info(f"[{s['name']}] ✔ 매도 체결({o['reason']}) {filled}주 @ {won(avg)} — 손익 {pnl:+,}원 (남은 {st['held_qty']}주)")
        record_trade([datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"), s["name"], s["symbol"],
                      "매수" if o["side"] == "BUY" else "매도", o["reason"], filled, avg, amount,
                      "모의" if self.dry else "실전"])


# ─────────────────────────── 실행 ───────────────────────────
def main():
    args = sys.argv[1:]
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
