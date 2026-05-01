# api/price.py
# -*- coding: utf-8 -*-
# ✅ 국내주식 호가/현재가 조회 모듈

import os
import time
from typing import Dict, Any, Tuple

import requests

from api.auth import get_auth_headers

# ======================================================
# 기본 설정
# ======================================================
DB_BASE_URL = os.getenv("DB_BASE_URL", "https://openapi.dbsec.co.kr:8443")
ORDERBOOK_PATH = "/api/v1/quote/kr-stock/inquiry/orderbook"

PUBLIC_CALL_SLEEP = float(os.getenv("PUBLIC_CALL_SLEEP", "0.55"))


# ======================================================
# 공통 유틸
# ======================================================
def _to_symbol(market_or_symbol: str) -> str:
    """국내 주식 종목코드는 순수 숫자 6자리 또는 'A'로 시작하는 7자리로 사용"""
    s = (market_or_symbol or "").strip()
    s = s.replace("KRW-", "").strip()
    # A005930 형태도 허용하나, 호가조회 등에서는 보통 숫자 6자리나 A+6자리 모두 호환됨.
    # 안전하게 A가 없으면 붙여주는 방식을 사용할 수도 있으나, 여기서는 원본 유지 후 필요시 처리.
    return s.upper()


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(str(x).replace(",", "").strip())
    except Exception:
        return default


def _pick_first_nonzero(*vals: float) -> float:
    for v in vals:
        if v and v > 0:
            return v
    return 0.0


# ======================================================
# DB API 호가 조회
# ======================================================
def _request_orderbook(symbol: str) -> Dict[str, Any]:
    sym = _to_symbol(symbol)
    clean_sym = sym if sym.startswith("A") else f"A{sym}"

    headers = get_auth_headers({
        "content-type": "application/json; charset=utf-8",
        "cont_yn": "N",
        "cont_key": "",
    })

    payload = {
        "In": {
            "InputCondMrktDivCode": "J",  # J: 주식 (KRX)
            "InputIscd1": clean_sym
        }
    }

    time.sleep(PUBLIC_CALL_SLEEP)

    resp = requests.post(
        f"{DB_BASE_URL}{ORDERBOOK_PATH}",
        headers=headers,
        json=payload,
        timeout=15
    )

    if resp.status_code != 200:
        raise Exception(f"[DB API] 호가조회 실패 {resp.status_code} - {resp.text}")

    data = resp.json()
    if data.get("rsp_cd") not in (None, "00000"):
        raise Exception(f"[DB API] 호가조회 오류 {data.get('rsp_msg')}")

    return data


# ======================================================
# 국내주식 최우선 호가 추출 로직
# ======================================================
def _get_best_bid_ask(symbol: str) -> Tuple[float, float, str]:
    """
    국내주식 호가를 조회하여 최우선 매수(Bid)/매도(Ask) 가격을 반환.
    실패 시 (0.0, 0.0, "") 반환.
    """
    try:
        data = _request_orderbook(symbol)
        out = data.get("Out", {}) or {}

        bid = _pick_first_nonzero(
            _safe_float(out.get("Bidp1")),
            _safe_float(out.get("Bidp2")),
            _safe_float(out.get("Bidp3")),
            _safe_float(out.get("Bidp4")),
            _safe_float(out.get("Bidp5")),
        )

        ask = _pick_first_nonzero(
            _safe_float(out.get("Askp1")),
            _safe_float(out.get("Askp2")),
            _safe_float(out.get("Askp3")),
            _safe_float(out.get("Askp4")),
            _safe_float(out.get("Askp5")),
        )

        if bid > 0 or ask > 0:
            return bid, ask, "J"

    except Exception as e:
        print(f"[price.py] ❌ {symbol} 호가 조회 실패 (사유: {e})")

    return 0.0, 0.0, ""


# ======================================================
# 외부 노출 API
# ======================================================
def get_current_ask_price(market: str) -> float:
    bid, ask, _ = _get_best_bid_ask(market)
    return ask


def get_current_bid_price(market: str) -> float:
    bid, ask, _ = _get_best_bid_ask(market)
    return bid


def get_best_bid_ask(market: str) -> Tuple[float, float]:
    bid, ask, _ = _get_best_bid_ask(market)
    return bid, ask


def get_spread_pct(market: str) -> float:
    bid, ask = get_best_bid_ask(market)
    if bid <= 0:
        return 0.0
    return (ask - bid) / bid


def is_spread_too_wide(market: str, threshold_pct: float = 0.04) -> bool:
    sp = get_spread_pct(market)
    return sp >= threshold_pct