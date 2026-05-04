# api/account.py
# -*- coding: utf-8 -*-

import os
import time
from typing import Dict, Any, List, Tuple

import requests

from api.auth import get_auth_headers

# DB증권 기본 URL
DB_BASE_URL = os.getenv("DB_BASE_URL", "https://openapi.dbsec.co.kr:8443")

# 국내주식 관련 API 엔드포인트
UNEXECUTED_ORDER_PATH = "/api/v1/trading/kr-stock/inquiry/transaction-history"
ORDER_ABLE_QTY_PATH = "/api/v1/trading/kr-stock/inquiry/able-orderqty"
STOCK_BALANCE_PATH = "/api/v1/trading/kr-stock/inquiry/balance"
ACCOUNT_DEPOSIT_PATH = "/api/v1/trading/kr-stock/inquiry/acnt-deposit"

# 호출 간 sleep (안정성)
PRIVATE_CALL_SLEEP = float(os.getenv("PRIVATE_CALL_SLEEP", "0.25"))


def _safe_float(x: Any, default: float = 0.0) -> float:
    """숫자 문자열/숫자/None을 안전하게 float로 변환"""
    try:
        if x is None:
            return default
        s = str(x).strip()
        if not s:
            return default
        s = s.replace(",", "")
        return float(s)
    except Exception:
        return default


def _safe_int_qty(x: Any, default: int = 0) -> int:
    """수량을 안전하게 int로 변환 (국내 주식은 1주 단위 정수)"""
    try:
        f = _safe_float(x, 0.0)
        if f <= 0:
            return default
        return int(round(f))
    except Exception:
        return default


def get_unexecuted_orders(bns_tp_code: str = "0") -> List[Dict[str, Any]]:
    """국내주식 미체결 내역 조회 (ExecYn: 2 - 미체결)"""
    url = f"{DB_BASE_URL}{UNEXECUTED_ORDER_PATH}"
    headers = get_auth_headers({
        "content-type": "application/json; charset=utf-8",
        "cont_yn": "N",
        "cont_key": "",
    })

    payload = {
        "In": {
            "ExecYn": "2",
            "BnsTpCode": bns_tp_code,
            "IsuTpCode": "0",
            "QryTp": "0",
            "TrdMktCode": "0",
            "SorTpYn": "2"
        }
    }

    time.sleep(PRIVATE_CALL_SLEEP)
    resp = requests.post(url, headers=headers, json=payload, timeout=15)

    if resp.status_code != 200:
        raise Exception(f"[DB API] 미체결 조회 실패: {resp.status_code} - {resp.text}")

    data = resp.json()
    rsp_cd = data.get("rsp_cd")

    if rsp_cd not in ("00000",):
        print(f"[account.py] 미체결 조회 응답 코드: {rsp_cd} - {data.get('rsp_msg')}")

    return data.get("Out1", []) or []


def get_orderable_quantity(symbol: str, price: float, is_buy: bool = True) -> int:
    """특정 종목의 주문 가능 수량 조회"""
    url = f"{DB_BASE_URL}{ORDER_ABLE_QTY_PATH}"
    headers = get_auth_headers({
        "content-type": "application/json; charset=utf-8",
        "cont_yn": "N",
        "cont_key": "",
    })

    isu_no = symbol if symbol.startswith("A") else f"A{symbol}"
    bns_tp = "2" if is_buy else "1"
    ord_prc = price if is_buy else 0

    payload = {
        "In": {
            "BnsTpCode": bns_tp,
            "IsuNo": isu_no,
            "OrdPrc": ord_prc
        }
    }

    time.sleep(PRIVATE_CALL_SLEEP)
    resp = requests.post(url, headers=headers, json=payload, timeout=15)

    if resp.status_code != 200:
        raise Exception(f"[DB API] 주문가능수량 조회 실패: {resp.status_code} - {resp.text}")

    data = resp.json()
    if data.get("rsp_cd") != "00000":
        print(f"[account.py] 경고: 주문가능수량 응답 {data.get('rsp_cd')} - {data.get('rsp_msg')}")

    out = data.get("Out", {})
    return _safe_int_qty(out.get("OrdAbleQty"), 0)


def get_stock_balance(cont_yn: str = "N", cont_key: str = "") -> Tuple[Dict[str, Any], Dict[str, str]]:
    """국내주식 잔고 조회 (연속조회 대응)"""
    url = f"{DB_BASE_URL}{STOCK_BALANCE_PATH}"
    headers = get_auth_headers({
        "content-type": "application/json; charset=utf-8",
        "cont_yn": cont_yn,
        "cont_key": cont_key or "",
    })

    payload = {
        "In": {
            "QryTpCode0": "0"
        }
    }

    time.sleep(PRIVATE_CALL_SLEEP)
    resp = requests.post(url, headers=headers, json=payload, timeout=15)

    if resp.status_code != 200:
        raise Exception(f"[DB API] 주식잔고 조회 실패: {resp.status_code} - {resp.text}")

    return resp.json(), dict(resp.headers)


def get_accounts_raw() -> Dict[str, Any]:
    """국내주식 잔고 '전체 페이지' 조회 및 요약 데이터 병합"""
    cont_yn = "N"
    cont_key = ""

    all_positions: List[Dict[str, Any]] = []
    out_summary: Dict[str, Any] = {}

    while True:
        data, hdr = get_stock_balance(cont_yn=cont_yn, cont_key=cont_key)

        rsp_cd = str(data.get("rsp_cd", "")).strip()

        if rsp_cd not in ("00000", ""):
            if rsp_cd == "2679":  # 조회 내역 없음
                break
            raise Exception(f"[DB API] 잔고조회 오류: rsp_cd={rsp_cd}, msg={data.get('rsp_msg')}")

        out = data.get("Out", {}) or {}
        out1 = data.get("Out1", []) or []

        if not out_summary:
            out_summary = out

        if isinstance(out1, list) and out1:
            all_positions.extend(out1)

        next_cont_yn = (hdr.get("cont_yn") or hdr.get("Cont_Yn") or "").strip()
        next_cont_key = (hdr.get("cont_key") or hdr.get("Cont_Key") or "").strip()

        if not next_cont_yn:
            next_cont_yn = str(data.get("cont_yn", "")).strip()
        if not next_cont_key:
            next_cont_key = str(data.get("cont_key", "")).strip()

        if next_cont_yn == "Y" and next_cont_key:
            cont_yn = "Y"
            cont_key = next_cont_key
            print(f"[account.py] 잔고 연속조회... cont_key={cont_key}")
            time.sleep(PRIVATE_CALL_SLEEP)
            continue

        break

    return {
        "out_summary": out_summary,
        "positions": all_positions,
    }


def get_accounts_symbol_map() -> Dict[str, Dict[str, Any]]:
    """종목번호(A제외 6자리) 기준으로 보유현황을 dict로 반환"""
    raw = get_accounts_raw()
    pos = raw.get("positions", []) or []

    result: Dict[str, Dict[str, Any]] = {}

    for item in pos:
        sym = (item.get("IsuNo") or "").strip()
        if not sym:
            continue

        clean_sym = sym[1:] if sym.startswith("A") else sym

        qty = _safe_int_qty(item.get("BalQty0"), 0)
        if qty <= 0:
            continue

        # API 원본 데이터 추출
        avg = _safe_float(item.get("BookUprc"), 0.0)
        eval_amt = _safe_float(item.get("EvalAmt"), 0.0)
        eval_pnl = _safe_float(item.get("EvalPnlAmt"), 0.0)

        # 💡 [핵심 패치] 당일 매수 등으로 증권사가 평단가를 0원으로 줬을 경우, 수학적 역산으로 진짜 평단가 복구!
        if avg <= 0 and qty > 0:
            buy_amt = eval_amt - eval_pnl  # 총 매입금액 = 평가금액 - 평가손익
            if buy_amt > 0:
                avg = buy_amt / qty        # 진짜 평단가 = 총 매입금액 / 수량

        result[clean_sym] = {
            "symbol": clean_sym,
            "quantity": qty,
            "avg_buy_price": avg,
            "eval_amt": eval_amt,
            "eval_pnl": eval_pnl,
            "return_rate": _safe_float(item.get("Ernrat"), 0.0)
        }

    return result


def get_account_deposit() -> Dict[str, float]:
    """계좌예수금조회 API를 호출하여 최신 예수금 정보 반환"""
    url = f"{DB_BASE_URL}{ACCOUNT_DEPOSIT_PATH}"
    headers = get_auth_headers({
        "content-type": "application/json; charset=utf-8",
        "cont_yn": "N",
        "cont_key": "",
    })

    payload = {
        "In": {}
    }

    time.sleep(PRIVATE_CALL_SLEEP)
    resp = requests.post(url, headers=headers, json=payload, timeout=15)

    if resp.status_code != 200:
        raise Exception(f"[DB API] 계좌예수금 조회 실패: {resp.status_code} - {resp.text}")

    data = resp.json()
    if data.get("rsp_cd") != "00000":
        raise Exception(f"[DB API] 계좌예수금 조회 오류: rsp_cd={data.get('rsp_cd')}, msg={data.get('rsp_msg')}")

    out1 = data.get("Out1", {})

    return {
        "deposit": _safe_float(out1.get("DpsBalAmt"), 0.0),
        "withdrawable": _safe_float(out1.get("WthdwAbleAmt"), 0.0),
        "d1_deposit": _safe_float(out1.get("PrsmptDpsD1"), 0.0),
        "d2_deposit": _safe_float(out1.get("PrsmptDpsD2"), 0.0),
    }


def get_position_qty(symbol: str) -> int:
    """특정 종목의 보유 '주식 수량(정수)' 반환 (체결기준)"""
    symbol = (symbol or "").strip()
    clean_sym = symbol[1:] if symbol.startswith("A") else symbol

    pos_map = get_accounts_symbol_map()
    return pos_map.get(clean_sym, {}).get("quantity", 0)


# =======================================================================
# 💡 [호환성 패치] 기존 전략 파일들이 호출하는 리스트형 반환 래퍼 함수 추가
# =======================================================================
def get_accounts() -> List[Dict[str, Any]]:
    """
    [호환성 패치] 기존 전략 파일(buy_entry, sell_entry 등)이
    요구하는 리스트형 계좌 데이터를 국내 주식 규격에 맞춰 반환합니다.
    """
    accounts = []

    # 1. KRW (원화 예수금)
    try:
        cash_info = get_account_deposit()
        accounts.append({
            "currency": "KRW",
            "balance": str(cash_info.get("deposit", 0.0)),
            "locked": "0",
            "avg_buy_price": "0",
            "orderable": str(cash_info.get("deposit", 0.0)),
            "withdrawable": str(cash_info.get("withdrawable", 0.0)),
        })
    except Exception as e:
        print(f"[account.py] 예수금 조회 실패 (무시됨): {e}")

    # 2. 보유 종목 (국내 주식)
    try:
        pos_map = get_accounts_symbol_map()
        for sym, info in pos_map.items():
            accounts.append({
                "currency": sym,  # order_executor.py 에서 사용
                "symbol": sym,  # buy/sell_entry.py 에서 사용
                "IsuNo": sym,  # 원본 키값 호환
                "balance": str(info["quantity"]),
                "locked": "0",
                "avg_buy_price": str(info["avg_buy_price"]), # ✅ 복구된 진짜 평단가가 전파됨
                "eval_amt": str(info["eval_amt"]),
            })
    except Exception as e:
        print(f"[account.py] 보유 주식 조회 실패 (무시됨): {e}")

    return accounts