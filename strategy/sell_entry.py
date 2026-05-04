# strategy/sell_entry.py
# -*- coding: utf-8 -*-

import os
import time
from datetime import datetime
from typing import Dict, Any

import pandas as pd

from api.account import get_accounts
from api.price import get_current_ask_price
from strategy.casino_strategy import generate_sell_orders
from manager.order_executor import execute_sell_orders


def _to_symbol(x):
    """
    [핵심 수정] 엑셀에서 앞자리 0이 날아가도 무조건 6자리 문자로 복구합니다.
    """
    s = str(x or "").strip().replace("KRW-", "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.startswith("A"):
        s = s[1:]
    return s.zfill(6).upper()


def _safe_float(x):
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return 0.0


def log_trade_history(symbol: str, avg_buy_price: float, sell_price: float, quantity: float):
    history_file = "data/trade_history.csv"

    os.makedirs(os.path.dirname(history_file), exist_ok=True)

    profit_pct = (sell_price - avg_buy_price) / avg_buy_price * 100
    profit_krw = (sell_price - avg_buy_price) * quantity

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    new_data = {
        "time": [now_str],
        "symbol": [symbol],
        "avg_buy_price": [int(avg_buy_price)],
        "sell_price": [int(sell_price)],
        "quantity": [int(quantity)],
        "profit_pct": [round(profit_pct, 2)],
        "profit_krw": [int(profit_krw)]
    }

    df_new = pd.DataFrame(new_data)

    if os.path.exists(history_file):
        df_new.to_csv(history_file, mode='a', header=False, index=False)
    else:
        df_new.to_csv(history_file, mode='w', header=True, index=False)

    status = "🔴 손절" if profit_pct < 0 else "🟢 익절"
    print(f"📝 [히스토리 기록] {symbol} 전량 매도 완료 ({status}) -> 수익률: {profit_pct:.2f}%, 수익금: {int(profit_krw):,}원")


def run_sell_entry_flow() -> None:
    print("\n[sell_entry.py] 🎯 트레일링 스탑 매도 플로우 가동")

    try:
        # 💡 [핵심 패치] 파일을 읽을 때 무조건 문자열(str)로 읽어서 0 증발 방지
        setting_df = pd.read_csv("setting.csv", dtype=str)
        accounts = get_accounts()
    except Exception as e:
        print(f"🚨 기초 데이터 로드 실패: {e}")
        return

    holdings = {}
    current_prices = {}

    for acc in (accounts or []):
        bal = _safe_float(acc.get("balance"))
        if bal <= 0: continue

        sym_raw = acc.get("symbol") or acc.get("currency") or acc.get("IsuNo") or acc.get("ticker")
        sym = _to_symbol(sym_raw)

        if not sym: continue
        if sym in ["000KRW", "000USD"]: continue

        curr_p = 0.0
        try:
            time.sleep(0.5)
            curr_p = float(get_current_ask_price(sym))
        except Exception:
            pass

        if curr_p > 0:
            current_prices[sym] = curr_p
            acc["current_price"] = curr_p
            acc["avg_price"] = _safe_float(acc.get("avg_buy_price"))
            holdings[sym] = acc

    sell_log_df = pd.DataFrame(
        columns=["time", "symbol", "avg_buy_price", "quantity", "highest_price", "sell_uuid", "filled"])

    if os.path.exists("sell_log.csv"):
        try:
            temp_df = pd.read_csv("sell_log.csv", dtype=str) # 💡 문자열 읽기
            if not temp_df.empty: sell_log_df = temp_df
        except pd.errors.EmptyDataError:
            pass

    updated_sell_df = generate_sell_orders(setting_df, holdings, sell_log_df, current_prices)

    if updated_sell_df is not None and not updated_sell_df.empty:
        mask = updated_sell_df["filled"].fillna("").astype(str).str.strip().str.lower() == "update"

        if mask.any():
            print(f"[sell_entry.py] 💥 {mask.sum()}건 매도 조건 달성! 즉시 스나이퍼 매도 실행")

            orders_to_execute = updated_sell_df[mask].copy()

            try:
                final_df = execute_sell_orders(orders_to_execute, current_prices)

                for idx, row in final_df.iterrows():
                    if row["filled"] == "done":
                        sym = str(row["symbol"])
                        avg_p = float(row["avg_buy_price"])
                        sell_p = float(current_prices.get(sym, avg_p))
                        qty = float(row["quantity"])

                        log_trade_history(sym, avg_p, sell_p, qty)

                        updated_sell_df = updated_sell_df[updated_sell_df["symbol"] != sym]

            except Exception as e:
                print(f"🚨 매도 실행 에러: {e}")

        if "market" in updated_sell_df.columns:
            updated_sell_df = updated_sell_df.drop(columns=["market"])
        updated_sell_df.to_csv("sell_log.csv", index=False)
        print("[sell_entry.py] ✅ 매도 감시 로그(sell_log.csv) 저장 완료")