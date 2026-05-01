# api/market_status.py
# -*- coding: utf-8 -*-

import os
import time
from datetime import datetime
from typing import Optional, Dict, Any

import requests

from api.auth import get_auth_headers

DB_BASE_URL = os.getenv("DB_BASE_URL", "https://openapi.dbsec.co.kr:8443")
# 국내주식 단일 현재가 조회 API를 시장 상태 체크용으로 활용
PATH = "/api/v1/quote/kr-stock/inquiry/price"

# 센서 기준 설정 (국내 대장주 삼성전자 활용)
DEFAULT_SYMBOLS = ["A005930"]
SENSOR_CALL_SLEEP = 0.15


def is_regular_market_time() -> bool:
    """
    [핵심] 한국 정규장 시간 체크 (KST 09:00 ~ 15:30)
    시스템 시간(KST)을 기준으로 판단합니다.
    """
    now = datetime.now()
    if now.weekday() >= 5:  # 주말(토, 일) 휴장
        return False

    now_val = now.hour * 100 + now.minute
    return 900 <= now_val < 1530  # 09:00 ~ 15:30


def report_order_not_allowed_time(reason: str = "") -> None:
    print(f"[market_status] ⚠️ 주문 알림(8819 등): {reason}")


def get_block_status() -> Dict[str, Any]:
    return {"is_blocked": False, "remain_sec": 0}


def is_market_open_by_conclusion(symbol: str, timeout: int = 10) -> Optional[bool]:
    """
    특정 종목의 현재가를 조회하여 API가 정상 응답하는지(장이 열려있는지) 확인.
    - True: 정상 데이터 수신
    - False: 데이터 없음 (휴장)
    - None: API 통신 에러
    """
    headers = get_auth_headers({
        "content-type": "application/json; charset=utf-8",
        "cont_yn": "N",
        "cont_key": ""
    })

    # 종목코드 A 포함 7자리 표준화
    clean_sym = symbol if symbol.startswith("A") else f"A{symbol}"

    payload = {
        "In": {
            "InputCondMrktDivCode": "J",  # J: 주식 (KRX)
            "InputIscd1": clean_sym
        }
    }

    try:
        resp = requests.post(f"{DB_BASE_URL}{PATH}", headers=headers, json=payload, timeout=timeout)
        if resp.status_code != 200:
            return None

        data = resp.json()
        if data.get("rsp_cd") == "2679":
            return False

        out = data.get("Out", {})
        # 현재가가 존재하면 시장이 열려있거나 데이터가 있다고 판단
        return bool(out and out.get("Prpr"))
    except Exception:
        return None


def get_market_open_status(timeout: int = 10) -> Dict[str, Any]:
    """최종 장 오픈 상태 판정 (API 에러 방어 로직 포함)"""
    if not is_regular_market_time():
        return {"is_open": False, "reason": "NOT_REGULAR_TIME_KST"}

    soft_open = False
    api_error_occurred = False

    for sym in DEFAULT_SYMBOLS:
        res = is_market_open_by_conclusion(sym, timeout)
        if res is True:
            soft_open = True
            break
        if res is None:
            api_error_occurred = True
        time.sleep(SENSOR_CALL_SLEEP)

    if soft_open:
        return {"is_open": True, "reason": "MARKET_DATA_DETECTED"}
    if api_error_occurred:
        return {"is_open": None, "reason": "API_ERROR_DETECTED (Token or Network issue)"}

    return {"is_open": False, "reason": "NO_MARKET_DATA (HOLIDAY?)"}


def is_market_open(timeout: int = 10) -> Optional[bool]:
    return get_market_open_status(timeout).get("is_open")