# strategy/sell_entry.py
import os
import time
import pandas as pd
from datetime import datetime
from typing import Dict, Any

from api.account import get_accounts
from api.price import get_current_ask_price
from strategy.casino_strategy import generate_sell_orders
from manager.order_executor import execute_sell_orders


def _to_symbol(x):
    return str(x or "").strip().replace("KRW-", "").upper()


def _safe_float(x):
    try:
        return float(str(x).replace(",", "").strip())
    except:
        return 0.0


# ✅ [신규 추가] 영구 보존용 거래 내역(투자 분석용)을 남기는 함수
def log_trade_history(symbol: str, avg_buy_price: float, sell_price: float, quantity: float):
    history_file = "data/trade_history.csv"

    # 💡 [핵심 방어] data 폴더가 없으면 에러가 나므로, 자동으로 폴더를 생성해 줍니다.
    os.makedirs(os.path.dirname(history_file), exist_ok=True)

    profit_pct = (sell_price - avg_buy_price) / avg_buy_price * 100
    profit_usd = (sell_price - avg_buy_price) * quantity

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    new_data = {
        "time": [now_str],
        "symbol": [symbol],
        "avg_buy_price": [round(avg_buy_price, 2)],
        "sell_price": [round(sell_price, 2)],
        "quantity": [quantity],
        "profit_pct": [round(profit_pct, 2)],
        "profit_usd": [round(profit_usd, 2)]
    }

    df_new = pd.DataFrame(new_data)

    if os.path.exists(history_file):
        df_new.to_csv(history_file, mode='a', header=False, index=False)
    else:
        df_new.to_csv(history_file, mode='w', header=True, index=False)

    status = "🔴 손절" if profit_pct < 0 else "🟢 익절"
    print(f"📝 [히스토리 기록] {symbol} 전량 매도 완료 ({status}) -> 수익률: {profit_pct:.2f}%, 수익금: ${profit_usd:.2f}")


def run_sell_entry_flow() -> None:
    print("\n[sell_entry.py] 🎯 트레일링 스탑 매도 플로우 가동")

    try:
        setting_df = pd.read_csv("setting.csv")
        accounts = get_accounts()
    except Exception as e:
        print(f"🚨 기초 데이터 로드 실패: {e}")
        return

    # 잔고(holdings) 및 현재가 수집
    holdings = {}
    current_prices = {}

    for acc in (accounts or []):
        bal = _safe_float(acc.get("balance"))
        if bal <= 0: continue

        # 💡 [호환성 강화] DB증권 API가 내려주는 다양한 키값을 모두 커버합니다.
        sym_raw = acc.get("symbol") or acc.get("currency") or acc.get("AstkIsuNo") or acc.get("ticker")
        sym = _to_symbol(sym_raw)

        if not sym: continue

        # 현금(KRW, USD)은 주식이 아니므로 가격 조회 및 매도 감시에서 스킵!
        if sym in ["KRW", "USD"]: continue

        curr_p = 0.0
        try:
            time.sleep(0.5)  # API 딜레이
            curr_p = float(get_current_ask_price(sym))
        except:
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
            temp_df = pd.read_csv("sell_log.csv", dtype=str)
            if not temp_df.empty: sell_log_df = temp_df
        except pd.errors.EmptyDataError:
            pass

    # 💡 브레인(casino_strategy)에게 현재가를 넘겨주어 고점 갱신 및 트레일링 스탑 여부 판별
    updated_sell_df = generate_sell_orders(setting_df, holdings, sell_log_df, current_prices)

    if updated_sell_df is not None and not updated_sell_df.empty:
        # 💡 [핵심 수정] trigger_sell 대신 "update" 명령어를 감지하도록 수정
        mask = updated_sell_df["filled"].fillna("").astype(str).str.strip().str.lower() == "update"

        if mask.any():
            print(f"[sell_entry.py] 💥 {mask.sum()}건 매도 조건 달성! 즉시 스나이퍼 지정가 매도 실행")

            # 실행할 주문들만 뽑아서 행동대장에게 넘김
            orders_to_execute = updated_sell_df[mask].copy()

            try:
                # 실제 증권사에 매도 API 쏘기 (성공 시 반환값에 done 처리됨)
                final_df = execute_sell_orders(orders_to_execute, current_prices)

                # 성공적으로 던져진(done) 종목 처리 및 로그 삭제
                for idx, row in final_df.iterrows():
                    if row["filled"] == "done":
                        sym = str(row["symbol"])
                        avg_p = float(row["avg_buy_price"])
                        # 시장가 근접으로 던졌으므로 대략 현재가로 기록
                        sell_p = float(current_prices.get(sym, avg_p))
                        qty = float(row["quantity"])

                        # 1️⃣ 영구 보존용 거래 히스토리 기록
                        log_trade_history(sym, avg_p, sell_p, qty)

                        # 2️⃣ sell_log.csv에서 해당 종목 완벽 삭제 (다음 턴 리셋)
                        updated_sell_df = updated_sell_df[updated_sell_df["symbol"] != sym]

            except Exception as e:
                print(f"🚨 매도 실행 에러: {e}")

        # 갱신된(또는 청소된) 로그 파일 덮어쓰기
        if "market" in updated_sell_df.columns: updated_sell_df = updated_sell_df.drop(columns=["market"])
        updated_sell_df.to_csv("sell_log.csv", index=False)
        print("[sell_entry.py] ✅ 매도 감시 로그(sell_log.csv) 저장 완료")