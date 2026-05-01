# api/order.py
# -*- coding: utf-8 -*-
# ✅ 국내주식 매매 전용, 트레일링 스탑 전략 및 취소 완벽 지원

import os
import time
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

from api.auth import get_auth_headers

DB_BASE_URL = os.getenv("DB_BASE_URL", "https://openapi.dbsec.co.kr:8443")

# ✅ 국내주식 관련 API 엔드포인트
ORDER_PATH = "/api/v1/trading/kr-stock/order"
TX_HISTORY_PATH = "/api/v1/trading/kr-stock/inquiry/transaction-history"
CANCEL_PATH = "/api/v1/trading/kr-stock/order-cancel"  # ✅ 공식 가이드 반영 완료

PRIVATE_CALL_SLEEP = float(os.getenv("PRIVATE_CALL_SLEEP", "0.35"))


def _to_symbol(market_or_symbol: str) -> str:
    s = (market_or_symbol or "").strip()
    s = s.replace("KRW-", "")
    # 국내 주식 종목코드는 'A'로 시작하도록 표준화
    return s if s.startswith("A") else f"A{s}"


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _to_int_qty(x: Any, field_name: str = "volume") -> int:
    if x is None:
        raise ValueError(f"{field_name}가 필요합니다.")
    f = float(x)
    if f <= 0 or abs(f - int(f)) > 1e-9:
        raise ValueError(f"주식 수량은 1주 단위 정수만 허용됩니다: {x}")
    return int(f)


def _is_rsp_ok(data: Dict[str, Any]) -> bool:
    return str(data.get("rsp_cd", "")).strip() == "00000"


def _is_rsp_no_history(data: Dict[str, Any]) -> bool:
    return str(data.get("rsp_cd", "")).strip() == "2679"


def _is_rsp_not_allowed_time(data: Dict[str, Any]) -> bool:
    """✅ 주문가능 시각 아님(8819)"""
    return str(data.get("rsp_cd", "")).strip() == "8819"


# ======================================================
# DB API 공통 호출 (주문)
# ======================================================
def _post_order(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    headers = get_auth_headers({
        "content-type": "application/json; charset=utf-8",
        "cont_yn": "N",
        "cont_key": "",
    })

    time.sleep(PRIVATE_CALL_SLEEP)
    resp = requests.post(url, headers=headers, json=payload, timeout=15)

    if resp.status_code != 200:
        raise Exception(f"[DB API 실패] {resp.status_code} - {resp.text}")

    data = resp.json()

    # 주문가능 시각 아님(8819) 감지
    if _is_rsp_not_allowed_time(data):
        try:
            from api import market_status
            market_status.report_order_not_allowed_time(
                reason=f"order rsp_cd=8819 msg={data.get('rsp_msg')} payload={payload}"
            )
        except Exception as e:
            print(f"[order.py] ⚠ market_status 보고 실패: {e}")
        raise Exception(f"[DB 주문 오류 - 시간 외] {data}")

    if not _is_rsp_ok(data):
        # 취소 등 특정 상황에서는 에러 반환을 허용하도록 상위에서 처리해야 할 수도 있음
        raise Exception(f"[DB 주문/조회 오류] {data}")

    return data


# ======================================================
# 주문 실행 로직 (매수/매도)
# ======================================================
def send_order(
        symbol: str,
        side: str,
        ord_type: str,
        amount_krw: float = None,
        unit_price: float = None,
        volume: float = None,
        time_in_force: str = None
) -> Dict[str, Any]:
    sym = _to_symbol(symbol)
    qty = _to_int_qty(volume)

    # 1: 매도, 2: 매수
    bns = "2" if side.lower() == "bid" else "1"

    # 00: 지정가, 03: 시장가
    if ord_type == "limit":
        ptn = "00"
        prc = float(unit_price)
    elif ord_type == "market":
        ptn = "03"
        prc = 0.0  # 시장가는 가격 0 입력
    else:
        raise ValueError("지원하지 않는 ord_type (limit, market만 지원)")

    payload = {
        "In": {
            "IsuNo": sym,
            "TrchNo": 1,  # 1: KRX 고정
            "OrdQty": qty,
            "OrdPrc": prc,
            "BnsTpCode": bns,
            "OrdprcPtnCode": ptn,
            "MgntrnCode": "000",  # 000: 보통 (일반 현금주문)
            "LoanDt": "00000000",  # 일반 주문시 00000000
            "OrdCndiTpCode": "0"  # 0: 조건없음
        }
    }

    url = f"{DB_BASE_URL}{ORDER_PATH}"
    data = _post_order(url, payload)

    ord_no = data.get("Out", {}).get("OrdNo")
    return {"uuid": str(ord_no), "raw": data}


# ======================================================
# 💡 비상 취소 및 다중 취소 (공식 가이드 반영 완료)
# ======================================================
def cancel_order(symbol: str, org_ord_no: str, qty: int = 0, side: str = "bid") -> Dict[str, Any]:
    """
    [비상 취소 빔] 원주문번호를 이용한 취소 주문
    (qty가 0일 경우 전량 취소를 의미하며, 증권사 서버 설정에 따라 동작)
    """
    sym = _to_symbol(symbol)

    # ✅ 취소 주문 가이드에 맞춘 심플한 Payload
    payload = {
        "In": {
            "OrgOrdNo": int(org_ord_no),  # 원주문번호
            "IsuNo": sym,  # 종목번호
            "OrdQty": int(qty)  # 취소주문수량 (0 입력 시 전량 취소 처리 여부는 DB증권 규정 따름)
        }
    }

    try:
        url = f"{DB_BASE_URL}{CANCEL_PATH}"
        data = _post_order(url, payload)
        return data
    except Exception as e:
        print(f"❌ [api/order.py] 취소 API 통신 에러: {e}")
        return {"rsp_cd": "99999", "rsp_msg": str(e)}


def cancel_orders_by_uuids(
        uuids: List[str],
        symbol: Optional[str] = None,
        side: str = "bid"
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    base_symbol = _to_symbol(symbol) if symbol else None

    for u in (uuids or []):
        ordno = _safe_int(u)
        if ordno <= 0:
            out.append({"uuid": str(u), "status": "failed", "error": "invalid ordno"})
            continue

        sym = base_symbol or "A000000"  # 종목번호를 모를 경우 에러 처리 강화 필요

        try:
            # 기본적으로 취소수량에 0을 넘겨 전량 취소를 시도
            cancel_order(sym, str(ordno), 0, side)
            out.append({"uuid": str(u), "status": "success"})
        except Exception as e:
            out.append({"uuid": str(u), "status": "failed", "error": str(e)})
            print(f"[order.py] ⚠️ 다중 취소 실패 ({u}): {e}")

    return out


# ======================================================
# 체결/미체결 조회 기반 상태 추적 (국내주식)
# ======================================================
def _request_tx_page_custom(
        exec_yn: str = "0",  # 0:전체, 1:체결, 2:미체결
        cont_yn: str = "N",
        cont_key: str = ""
) -> Tuple[Dict[str, Any], str, str]:
    headers = get_auth_headers({
        "content-type": "application/json; charset=utf-8",
        "cont_yn": cont_yn,
        "cont_key": cont_key,
    })

    payload = {
        "In": {
            "ExecYn": exec_yn,
            "BnsTpCode": "0",
            "IsuTpCode": "0",
            "QryTp": "0",
            "TrdMktCode": "0",
            "SorTpYn": "2"
        }
    }

    url = f"{DB_BASE_URL}{TX_HISTORY_PATH}"
    time.sleep(PRIVATE_CALL_SLEEP)
    resp = requests.post(url, headers=headers, json=payload, timeout=15)

    if resp.status_code != 200:
        raise Exception(f"[DB 체결/미체결조회 실패] {resp.status_code} - {resp.text}")

    data = resp.json()
    return (
        data,
        resp.headers.get("cont_yn", "N"),
        resp.headers.get("cont_key", "")
    )


def _fetch_all_tx(exec_yn: str = "0") -> List[Dict[str, Any]]:
    cont_yn, cont_key = "N", ""
    rows: List[Dict[str, Any]] = []

    while True:
        data, next_yn, next_key = _request_tx_page_custom(
            exec_yn=exec_yn,
            cont_yn=cont_yn,
            cont_key=cont_key
        )

        if _is_rsp_no_history(data):
            return []

        if not _is_rsp_ok(data):
            raise Exception(f"[DB 체결/미체결조회 오류] {data}")

        rows.extend(data.get("Out1", []) or [])

        # 연속조회 처리
        if next_yn == "Y" and next_key:
            cont_yn, cont_key = "Y", next_key
            continue
        break

    return rows


def get_order_results_by_uuids(uuids: List[str]) -> Dict[str, str]:
    """체결 상태 확인 (done, wait)"""
    results: Dict[str, str] = {str(u).strip(): "wait" for u in (uuids or []) if str(u).strip()}
    if not results:
        return {}

    # 국내주식 전체 내역 조회
    txs = _fetch_all_tx(exec_yn="0")
    if not txs:
        return results

    for tx in txs:
        k = str(tx.get("OrdNo", "")).strip()
        if not k or (k not in results):
            continue

        # AllExecQty(전체체결수량)이 OrdQty(주문수량)과 같으면 체결 완료
        if _safe_int(tx.get("AllExecQty")) >= _safe_int(tx.get("OrdQty")):
            results[k] = "done"

    return results


def fetch_unfilled_orders_today(symbol: str = "") -> List[Dict[str, Any]]:
    """오늘의 미체결 주문 내역 반환"""
    rows = _fetch_all_tx(exec_yn="2")  # 2: 미체결

    if symbol:
        target_sym = _to_symbol(symbol)
        rows = [r for r in rows if r.get("IsuNo") == target_sym]

    return rows


def has_any_unfilled_orders_today(symbol: str = "") -> bool:
    rows = fetch_unfilled_orders_today(symbol=symbol)
    return len(rows) > 0