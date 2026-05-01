# manager/order_executor.py
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
from typing import Any

from api.order import send_order, cancel_order
from api.account import get_accounts

# 💡 [핵심 추가] 패닉 셀 시에도 국내주식 호가단위를 지키기 위해 import
from utils.price_utils import adjust_price_to_tick

PRIVATE_CALL_SLEEP = float(os.getenv("PRIVATE_CALL_SLEEP", "0.35"))


def _to_symbol(x):
    return str(x or "").strip().replace("KRW-", "").upper()


def _safe_float(x):
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return 0.0


def _drop_market_col(df: pd.DataFrame) -> pd.DataFrame:
    if df is not None and "market" in df.columns:
        return df.drop(columns=["market"])
    return df


def _get_real_balance(symbol: str) -> float:
    accounts = get_accounts()
    for acc in (accounts or []):
        acc_sym = _to_symbol(acc.get("currency", ""))
        if acc_sym == symbol:
            return float(acc.get("balance", 0.0))
    return 0.0


def execute_buy_orders(buy_log_df: pd.DataFrame) -> pd.DataFrame:
    print("[order_executor.py] ✅ 매수 그물망 발주 및 유령주문 취소(Cleanup) 실행 시작")
    if buy_log_df.empty: return _drop_market_col(buy_log_df)

    indices_to_drop = []

    for idx, row in buy_log_df.iterrows():
        status = str(row.get("filled", "")).strip().lower()
        symbol = _to_symbol(row.get("symbol"))
        qty = _safe_float(row.get("buy_units"))
        vol = max(1, int(qty))

        # -------------------------------------------------------
        # 1. 취소 대기(cancel_req) 처리
        # -------------------------------------------------------
        if status == "cancel_req":
            uuid = str(row.get("buy_uuid", "")).strip()
            if uuid and uuid.isdigit():
                print(f"🔫 [order_executor.py][cancel] 잔고 0 달성! {symbol} 유령 주문(UUID:{uuid}) 강제 취소 발사!")
                try:
                    resp = cancel_order(symbol=symbol, org_ord_no=uuid, qty=vol, side="bid")
                    rsp_cd = str((resp or {}).get("rsp_cd", ""))
                    if rsp_cd == "00000":
                        print(f"✅ {symbol} (UUID:{uuid}) 취소 완료!")
                    else:
                        err_msg = (resp or {}).get("rsp_msg", "이유 알 수 없음")
                        print(f"⚠️ {symbol} 취소 실패 (이미 체결/오류): {err_msg}")
                except Exception as e:
                    print(f"❌ {symbol} 취소 통신 에러: {e}")
                time.sleep(1.0)

            indices_to_drop.append(idx)
            continue

        # -------------------------------------------------------
        # 2. 신규 주문(update) 처리
        # -------------------------------------------------------
        if status != "update": continue

        b_type = str(row.get("buy_type", "")).strip().lower()
        target_p = _safe_float(row.get("target_price"))

        if b_type == "scout_flow":
            ord_type = "market"
            price = 0
        else:
            ord_type = "limit"
            price = target_p

        if vol <= 0 or (ord_type == "limit" and price <= 0):
            print(f"⚠️ {symbol} 주문 오류 - 수량/가격 오류 (타입:{ord_type}, 수량:{vol}, 가격:{price})")
            continue

        try:
            if ord_type == "market":
                print(f"[order_executor.py][buy] 🚀 {symbol} {ord_type} 정찰병 시장가 즉시 투입! (수량: {vol})")
            else:
                print(f"[order_executor.py][buy] 🆕 {symbol} {ord_type} 하락 타점 발주 (가격: {int(price):,}원, 수량: {vol})")

            resp = send_order(symbol=symbol, side="bid", ord_type=ord_type, volume=vol, unit_price=price)

            uuid = (resp or {}).get("uuid", "")
            if uuid:
                buy_log_df.at[idx, "buy_uuid"] = str(uuid)
                buy_log_df.at[idx, "filled"] = "wait"
            else:
                err_msg = (resp or {}).get("message", "이유 알 수 없음")
                print(f"❌ {symbol} 매수 거부 당함 (UUID 없음) - API 응답: {err_msg}")

            time.sleep(1.0)
        except Exception as e:
            print(f"❌ {symbol} 매수 통신 에러: {e}")

        time.sleep(PRIVATE_CALL_SLEEP)

    if indices_to_drop:
        buy_log_df = buy_log_df.drop(indices_to_drop).reset_index(drop=True)
        print(f"[order_executor.py] 🧹 취소 완료된 옛날 그물망 {len(indices_to_drop)}건 장부에서 삭제 완료!")

    print("[order_executor.py] ✅ 매수/취소 루프 완료")
    return _drop_market_col(buy_log_df)


def execute_sell_orders(sell_log_df: pd.DataFrame, current_prices: dict) -> pd.DataFrame:
    print("[order_executor.py] ✅ 즉각 스나이퍼 매도 실행 시작 (방해물 철거 작전 포함)")
    if sell_log_df.empty: return sell_log_df

    try:
        buy_path = "buy_log.csv"
        if not os.path.exists(buy_path) and os.path.exists("data/buy_log.csv"):
            buy_path = "data/buy_log.csv"

        buy_df = pd.read_csv(buy_path) if os.path.exists(buy_path) else pd.DataFrame()
    except Exception as e:
        print(f"[order_executor.py] ⚠️ 매수 장부 읽기 실패 (방해물 철거 건너뜀): {e}")
        buy_df = pd.DataFrame()

    for idx, row in sell_log_df.iterrows():
        if str(row.get("filled", "")).strip().lower() != "update": continue

        symbol = _to_symbol(row.get("symbol"))
        qty = _safe_float(row.get("quantity"))
        curr_p = float(current_prices.get(symbol, 0))

        order_qty_int = int(qty)
        if order_qty_int <= 0 or curr_p <= 0:
            print(f"[order_executor.py] 🚨 {symbol} 수량 또는 현재가 오류 -> 스킵")
            continue

        try:
            real_qty = _get_real_balance(symbol)
            real_qty_int = int(real_qty)
            final_order_qty = min(order_qty_int, real_qty_int) if real_qty_int > 0 else order_qty_int

            if final_order_qty <= 0:
                print(f"⚠️ {symbol} 실제 매도 가능 잔고 부족 -> 스킵")
                continue

            # -------------------------------------------------------
            # 🛡️ 스나이퍼 길 터주기 (매수 취소)
            # -------------------------------------------------------
            if not buy_df.empty and "symbol" in buy_df.columns:
                active_buys = buy_df[(buy_df["symbol"] == symbol) & (buy_df["filled"] == "wait")]

                for _, b_row in active_buys.iterrows():
                    u_id = str(b_row.get("buy_uuid", "")).strip()
                    b_vol = max(1, int(_safe_float(b_row.get("buy_units", 1))))

                    if u_id and u_id.isdigit():
                        print(f"🧹 [자전거래 방어] {symbol} 매도 전 방해물(매수그물망 UUID:{u_id}) 강제 철거 중...")
                        try:
                            cancel_order(symbol=symbol, org_ord_no=u_id, qty=b_vol, side="bid")
                        except Exception as ce:
                            print(f"⚠️ 방해물 철거 통신 에러 (무시하고 매도 진행): {ce}")

                        time.sleep(0.5)

            # -------------------------------------------------------
            # 💥 [핵심 패치] 국내주식 틱 단위에 맞춘 지정가 매도 투척
            # -------------------------------------------------------
            # 소수점 가격이 나가면 에러가 나므로 adjust_price_to_tick로 유효한 호가로 변환합니다.
            raw_panic_price = curr_p * 0.96
            panic_sell_price = adjust_price_to_tick(raw_panic_price, ticker=symbol)

            print(
                f"[order_executor.py][sell] 💥 {symbol} 전량 익절/손절 투척! (수량:{final_order_qty}, 패닉지정가:{int(panic_sell_price):,}원)")

            resp = send_order(symbol=symbol, side="ask", ord_type="limit", volume=final_order_qty,
                              unit_price=panic_sell_price)

            new_uuid = (resp or {}).get("uuid", "")
            if new_uuid:
                print(f"✅ {symbol} 매도 접수 성공! (uuid: {new_uuid})")
                sell_log_df.at[idx, "filled"] = "done"
            else:
                err_msg = (resp or {}).get("message", "이유 알 수 없음")
                print(f"❌ {symbol} 매도 거부 당함 - API 응답: {err_msg}")

            time.sleep(1.0)
        except Exception as e:
            print(f"❌ {symbol} 매도 통신 에러: {e}")

        time.sleep(PRIVATE_CALL_SLEEP)

    return sell_log_df