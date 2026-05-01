# strategy/initial_cycle.py
# DB증권 초기(초단위) 사이클
# - market -> symbol 통일
# - setting.csv가 market/symbol 어느 쪽이든 동작하도록 보정
# - 전략(초루프: initial만 / 보유중이면 TP 생성) 유지

import os
import time
import pandas as pd
from typing import Dict, Any

from api.price import get_current_ask_price
from api.account import get_accounts
from manager.order_executor import execute_buy_orders, execute_sell_orders
from strategy.casino_strategy import generate_sell_orders
from strategy.buy_entry import update_buy_log_status
from strategy.sell_entry import update_sell_log_status_by_uuid


# =========================================
# 공통 유틸
# =========================================
def _to_symbol(market_or_symbol: str) -> str:
    s = (market_or_symbol or "").strip()
    s = s.replace("KRW-", "").strip()
    return s.upper()


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(str(x).replace(",", "").strip())
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default


def ensure_market_symbol_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    setting_df 또는 로그 DF에서 market/symbol 컬럼을 항상 보장.
    - DB증권 모드에서는 실질적으로 symbol만 쓰지만,
      기존 전략/로그 호환을 위해 market 컬럼도 같이 유지할 수 있게 한다.
    """
    df = df.copy()

    if "symbol" not in df.columns and "market" in df.columns:
        df["symbol"] = df["market"].astype(str).apply(_to_symbol)

    if "market" not in df.columns and "symbol" in df.columns:
        df["market"] = "KRW-" + df["symbol"].astype(str).apply(_to_symbol)

    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).apply(_to_symbol)

    if "market" in df.columns:
        df["market"] = df["market"].astype(str).str.strip()

    return df


# =========================================
# 보유자산(holdings) 구성
# =========================================
def get_current_holdings() -> Dict[str, Dict[str, Any]]:
    """
    DB증권 해외주식 계좌에서 현재 보유 종목 정보를 조회하여 holdings dict로 반환.
    - key: symbol (예: "TQQQ")
    - balance: 보유수량(가능수량 + 주문중수량 등 포함 여부는 account 변환 로직에 따름)
    - avg_price: 평균매수가
    - current_price: 현재 매도 1호가
    - total_value: 평가금액(달러/원화 여부는 계좌/시세 기준에 따라 달라질 수 있음)

    ⚠️ 기존 Upbit 코드의 '100원 미만 제거'는 미국주식에 부적절할 수 있어
       여기서는 아주 작은 수량만 걸러내는 방식으로 변경(전략 영향 최소화).
    """
    accounts = get_accounts()
    holdings: Dict[str, Dict[str, Any]] = {}

    for acc in accounts:
        # get_accounts()가 DB증권 형태로 변환된 dict를 준다는 전제
        # acc 예시에서 통화/종목키가 다를 수 있으므로 가능한 키들을 폭넓게 대응
        sym = acc.get("currency") or acc.get("symbol") or acc.get("AstkIsuNo")
        if not sym:
            continue

        symbol = _to_symbol(sym)

        # 수량/평단
        balance = _safe_float(acc.get("balance"), 0.0) + _safe_float(acc.get("locked"), 0.0)
        avg_price = _safe_float(acc.get("avg_buy_price"), 0.0)

        # 수량이 거의 0이면 스킵
        if balance <= 0:
            continue

        try:
            price = get_current_ask_price(symbol)  # ✅ symbol 기반
        except Exception as e:
            print(f"[initial_cycle.py] ⚠ {symbol} 현재가 조회 실패 → 스킵: {e}")
            continue

        # 호가가 0이면(휴장/권한 문제) 스킵
        if price <= 0:
            print(f"[initial_cycle.py] ⚠ {symbol} ask=0 → 스킵(휴장/권한 가능)")
            continue

        total = balance * price

        holdings[symbol] = {
            "balance": balance,
            "avg_price": avg_price,
            "current_price": price,
            "total_value": total,
        }

    print(f"[initial_cycle.py] holdings 구성 완료: {len(holdings)}종목")
    return holdings


# =========================================
# 초루프
# =========================================
def run_initial_cycle():
    """
    초단위 initial 매매 로직 (DB증권용, market->symbol 통일)

    전략 유지:
    1) 전량 매도 상태 + pending 주문 없음:
       - 해당 종목 buy_log 정리
       - initial 주문 row만 생성 (filled='update')
       - 초루프에서 initial만 발주

    2) 보유 상태인데 TP 매도 주문이 없다면:
       - generate_sell_orders로 TP 매도 주문 row 생성/수정
       - 초루프에서 execute_sell_orders로 발주
    """

    # ---------- CSV 불러오기 ----------
    setting_df = pd.read_csv("setting.csv")
    setting_df = ensure_market_symbol_columns(setting_df)

    try:
        buy_log_df = pd.read_csv("buy_log.csv")
    except Exception:
        buy_log_df = pd.DataFrame(columns=[
            "time", "market", "symbol",
            "target_price", "buy_amount", "buy_units",
            "buy_type", "buy_uuid", "filled",
        ])

    try:
        sell_log_df = pd.read_csv("sell_log.csv")
    except Exception:
        sell_log_df = pd.DataFrame(columns=[
            "time", "market", "symbol",
            "avg_buy_price", "quantity",
            "target_sell_price", "sell_uuid", "filled",
        ])

    buy_log_df = ensure_market_symbol_columns(buy_log_df)
    sell_log_df = ensure_market_symbol_columns(sell_log_df)

    # ---------- 상태 업데이트 ----------
    update_buy_log_status()
    sell_log_df = update_sell_log_status_by_uuid(sell_log_df)

    # ---------- 현재 보유 현황 ----------
    holdings = get_current_holdings()

    # ---------- 종목별 처리 ----------
    now_ts = pd.Timestamp.now()

    for _, s in setting_df.iterrows():
        symbol = _to_symbol(s.get("symbol"))
        market = s.get("market")  # 로그 호환용으로 유지 (실사용은 symbol)

        # unit_size: DB증권에서는 "수량(정수)"로 쓰는 게 안전
        unit_size = _safe_int(s.get("unit_size"), 0)
        if unit_size <= 0:
            print(f"[initial_cycle.py] ⚠ {symbol} unit_size(수량)가 0 이하 → 스킵")
            continue

        # 현재가 조회(실패/0이면 스킵)
        try:
            price = get_current_ask_price(symbol)
        except Exception as e:
            print(f"[initial_cycle.py] ⚠ [초루프] {symbol} 현재가 조회 실패 → 스킵: {e}")
            continue

        if price <= 0:
            print(f"[initial_cycle.py] ⚠ [초루프] {symbol} ask=0 → 스킵(휴장/권한 가능)")
            continue

        has_coin = symbol in holdings

        coin_buy_logs = buy_log_df[buy_log_df["symbol"] == symbol]
        coin_sell_logs = sell_log_df[sell_log_df["symbol"] == symbol]

        has_pending_buy = any(coin_buy_logs["filled"].isin(["update", "wait"]))
        has_pending_sell = any(coin_sell_logs["filled"].isin(["update", "wait"]))

        # =====================================================
        # 1) 전량 매도 상태 + pending 주문 없음
        #    → initial 매수만 생성/발주
        # =====================================================
        if (not has_coin) and (not has_pending_buy) and (not has_pending_sell):
            print(f"⚡ [초루프] {symbol} 전량매도 → initial 매수만 진입")

            # (1) 이 종목에 대한 기존 buy_log 정리
            buy_log_df = buy_log_df[buy_log_df["symbol"] != symbol]

            # (2) initial row 생성
            # 주의: 기존 Upbit는 buy_amount(금액) 기반이었지만,
            #       DB 주식은 수량 기반으로 주문하는 구조(order.py).
            initial_row = {
                "time": now_ts,
                "market": market if market else f"KRW-{symbol}",  # 호환용
                "symbol": symbol,
                "target_price": price,          # 시장가/유사시장가면 참고용으로만 기록
                "buy_amount": unit_size,         # ⚠ 여기서는 '수량'을 넣음 (전략 의미는 "초루프 initial 1회")
                "buy_units": 1,
                "buy_type": "initial",
                "buy_uuid": None,
                "filled": "update",
            }

            buy_log_df = pd.concat([buy_log_df, pd.DataFrame([initial_row])], ignore_index=True)

        # =====================================================
        # 2) 보유 상태인데 TP 주문이 없다 → TP 매도 주문 생성
        # =====================================================
        if has_coin and (not has_pending_sell):
            print(f"⚡ [초루프] {symbol} 보유 중 → TP 매도 주문 생성")

            h = holdings[symbol]

            # 이 종목만 포함하는 setting / sell_log 구성
            temp_setting = setting_df[setting_df["symbol"] == symbol].copy()
            temp_sell_df = sell_log_df[sell_log_df["symbol"] == symbol].copy()

            updated = generate_sell_orders(temp_setting, {symbol: h}, temp_sell_df)

            # 병합
            sell_log_df = sell_log_df[sell_log_df["symbol"] != symbol]
            sell_log_df = pd.concat([sell_log_df, updated], ignore_index=True)

    # ---------- 실제 주문 실행 ----------
    buy_log_df = execute_buy_orders(buy_log_df)
    sell_log_df = execute_sell_orders(sell_log_df)

    # ---------- CSV 저장 ----------
    buy_log_df.to_csv("buy_log.csv", index=False)
    sell_log_df.to_csv("sell_log.csv", index=False)

    print("[initial_cycle.py] ✅ run_initial_cycle 종료")
