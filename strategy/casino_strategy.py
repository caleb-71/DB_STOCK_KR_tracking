# strategy/casino_strategy.py
# -*- coding: utf-8 -*-

import time
import traceback
from typing import Any

import pandas as pd

from utils.price_utils import adjust_price_to_tick


def _to_symbol(x: Any) -> str:
    # 종목코드 A 접두사 처리는 api 계층에서 하므로 여기선 순수 텍스트 정제만 수행
    return str(x or "").strip().replace("KRW-", "").upper()


def _safe_str(val: Any) -> str:
    if val is None: return ""
    if pd.isna(val): return ""
    if isinstance(val, pd.Series):
        return str(val.iloc[0]).strip() if not val.empty else ""
    return str(val).strip()


def generate_buy_orders(
        setting_df: pd.DataFrame,
        buy_log_df: pd.DataFrame,
        current_prices: dict,
        holdings: dict = None
) -> pd.DataFrame:
    """
    [국내주식] 잔고 0주(전량 매도) 감지 시 유령 주문 자동 취소(cancel_req) 및 정찰병 즉시 투입
    """
    print("\n[casino_strategy.py] 🧠 매수 두뇌 가동 (generate_buy_orders)")

    if holdings is None: holdings = {}

    try:
        if hasattr(setting_df, "columns"):
            if "symbol" not in setting_df.columns and "market" in setting_df.columns:
                setting_df["symbol"] = setting_df["market"]

        settings = setting_df.to_dict('records') if hasattr(setting_df, 'to_dict') else []
        logs = buy_log_df.to_dict('records') if hasattr(buy_log_df, 'to_dict') and not buy_log_df.empty else []

        new_entries = []
        now_ts = pd.Timestamp.now()
        indices_to_drop = []

        for st in settings:
            symbol = _to_symbol(st.get("symbol"))
            if not symbol or symbol not in current_prices: continue

            curr_p = float(current_prices.get(symbol, 0.0))
            if curr_p <= 0: continue

            # 실제 보유 수량 확인
            qty = float(holdings.get(symbol, {}).get("balance", 0))
            coin_logs = [l for idx, l in enumerate(logs) if _to_symbol(l.get("symbol")) == symbol]

            # ========================================================
            # 🧹 [유령 주문 청소] 잔고가 0인데 장부에 옛날 그물망이 남아있다면?
            # ========================================================
            if qty == 0 and coin_logs:
                has_scout = any(str(l.get("buy_type")).strip().lower() == "scout_flow" for l in coin_logs)

                if not has_scout:
                    print(f"🧹 {symbol} 전량 익절 상태 확인! 장부의 유령 주문들을 'cancel_req'로 변경합니다.")
                    for log in logs:
                        if _to_symbol(log.get("symbol")) == symbol:
                            log["filled"] = "cancel_req"
                            log["time"] = now_ts

                    coin_logs = []
                    time.sleep(0.1)

            unit_size = float(st.get("unit_size", 0))
            s_pct = float(st.get("small_flow_pct", 0))
            l_pct = float(st.get("large_flow_pct", 0))
            s_multi = float(st.get("small_flow_units", 0))
            l_multi = float(st.get("large_flow_units", 0))

            # -----------------------------------------------------
            # 상황 1: 완전 신규 진입 (정찰병 투입 + 그물망 2개 생성)
            # -----------------------------------------------------
            if not coin_logs:
                print(f"📌 {symbol} 신규 진입 타점 계산 -> 🚀 정찰병 선발대 투입 및 하락 그물망 2개 생성")

                scout_budget = unit_size * 1.0
                scout_shares = max(1, int(scout_budget / curr_p)) if curr_p > 0 else 1

                new_entries.append({
                    "time": now_ts, "symbol": symbol,
                    "target_price": curr_p,
                    "buy_amount": scout_budget, "buy_units": scout_shares,
                    "buy_type": "scout_flow", "buy_uuid": "", "filled": "update"
                })

                s_target_p = adjust_price_to_tick(curr_p * (1 - s_pct), ticker=symbol)
                s_budget = unit_size * s_multi
                s_shares = max(1, int(s_budget / s_target_p)) if s_target_p > 0 else 1

                new_entries.append({
                    "time": now_ts, "symbol": symbol,
                    "target_price": s_target_p,
                    "buy_amount": s_budget, "buy_units": s_shares,
                    "buy_type": "small_flow", "buy_uuid": "", "filled": "update"
                })

                l_target_p = adjust_price_to_tick(curr_p * (1 - l_pct), ticker=symbol)
                l_budget = unit_size * l_multi
                l_shares = max(1, int(l_budget / l_target_p)) if l_target_p > 0 else 1

                new_entries.append({
                    "time": now_ts, "symbol": symbol,
                    "target_price": l_target_p,
                    "buy_amount": l_budget, "buy_units": l_shares,
                    "buy_type": "large_flow", "buy_uuid": "", "filled": "update"
                })

                print(f"   👉 Scout({int(curr_p):,}원): {scout_shares}주 / Small({int(s_target_p):,}원): {s_shares}주 / Large({int(l_target_p):,}원): {l_shares}주 장전 완료")
                continue

            # -----------------------------------------------------
            # 상황 2: 기존 그물망 유지보수 및 체결 시 신규 그물망 생성
            # -----------------------------------------------------
            for idx, log in enumerate(logs):
                if _to_symbol(log.get("symbol")) != symbol: continue

                status = _safe_str(log.get("filled")).lower()
                b_type = _safe_str(log.get("buy_type")).lower()
                t_price = float(log.get("target_price", 0))

                pct = s_pct if "small" in b_type else l_pct
                multiplier = s_multi if "small" in b_type else l_multi

                if status in ["", "nan", "none"]:
                    log["filled"] = "update"
                    log["time"] = now_ts

                elif status == "done":
                    if b_type == "scout_flow":
                        indices_to_drop.append(idx)
                        print(f"🔭 {symbol} 정찰병 매수 완료! 대세 상승장 탑승 준비 완료 🚀")
                        continue

                    base_price = min(t_price, curr_p)
                    next_price = adjust_price_to_tick(base_price * (1 - pct), ticker=symbol)

                    budget = unit_size * multiplier
                    actual_shares = max(1, int(budget / next_price)) if next_price > 0 else 1

                    print(f"♻️ {symbol} {b_type} 체결! -> 평단가 낮추기: {int(base_price):,}원 기준 추가 하락({int(next_price):,}원)에 {actual_shares}주 투척")

                    new_entries.append({
                        "time": now_ts, "symbol": symbol, "target_price": next_price,
                        "buy_amount": budget, "buy_units": actual_shares, "buy_type": b_type,
                        "buy_uuid": "", "filled": "update"
                    })
                    indices_to_drop.append(idx)

                elif status in ["cancel", "canceled"]:
                    log["buy_uuid"] = ""
                    log["filled"] = "update"
                    log["time"] = now_ts

        for i in sorted(indices_to_drop, reverse=True):
            del logs[i]

        final_list = logs + new_entries

        if not final_list:
            if hasattr(buy_log_df, 'columns'): return pd.DataFrame(columns=buy_log_df.columns)
            return pd.DataFrame(columns=["time", "symbol", "target_price", "buy_amount", "buy_units", "buy_type", "buy_uuid", "filled"])

        res_df = pd.DataFrame(final_list)
        if "market" in res_df.columns: res_df = res_df.drop(columns=["market"])

        for c in ["symbol", "buy_type", "buy_uuid", "filled"]:
            if c in res_df.columns:
                res_df[c] = res_df[c].astype(str).replace("nan", "").str.strip()

        print("[casino_strategy.py] 매수 두뇌 연산 완료")
        return res_df

    except Exception as e:
        print(f"[casino_strategy.py] 🚨 매수 로직 에러:\n{traceback.format_exc()}")
        raise e


def generate_sell_orders(
        setting_df: pd.DataFrame,
        holdings: dict,
        sell_log_df: pd.DataFrame,
        current_prices: dict
) -> pd.DataFrame:
    """
    [국내주식] 절대 수익 자물쇠(Absolute Profit Lock) 및 트레일링 스탑 감시
    """
    print("\n[casino_strategy.py] 🧠 매도 두뇌 가동 (트레일링 스탑 감시 중...)")

    try:
        settings = setting_df.to_dict('records') if hasattr(setting_df, 'to_dict') else []
        logs = sell_log_df.to_dict('records') if hasattr(sell_log_df, 'to_dict') and not sell_log_df.empty else []

        now_ts = pd.Timestamp.now()
        existing_map = {_to_symbol(l.get("symbol")): l for l in logs}

        for st in settings:
            symbol = _to_symbol(st.get("symbol"))
            if symbol not in holdings or symbol not in current_prices: continue

            h = holdings.get(symbol, {})
            avg_p = float(h.get("avg_price", 0))
            curr_p = float(current_prices.get(symbol, 0))
            qty = float(h.get("balance", 0))

            activation_pct = float(st.get("activation_pct", 0))
            trailing_drop_pct = float(st.get("trailing_drop_pct", 0))
            min_profit_pct = float(st.get("min_profit_pct", 0))

            if qty <= 0 or avg_p <= 0 or curr_p <= 0: continue

            current_profit_pct = (curr_p - avg_p) / avg_p
            existing = existing_map.get(symbol)

            # 신규 진입 종목 장부에 등록
            if not existing:
                print(f"🆕 {symbol} 신규 매도(트레일링) 감시 대기열 등록 (평단가: {int(avg_p):,}원)")
                logs.append({
                    "time": now_ts, "symbol": symbol,
                    "avg_buy_price": avg_p, "quantity": qty,
                    "highest_price": curr_p,
                    "sell_uuid": "", "filled": "tracking"
                })
                continue

            status = _safe_str(existing.get("filled")).lower()

            # 유령 결재 서류 파쇄기
            if status == "update":
                min_profit_price = avg_p * (1 + min_profit_pct)
                if curr_p < min_profit_price:
                    print(f"🗑️ {symbol} 과거 매도 결재(update) 발견! 하지만 갭 하락으로 수익선 붕괴 -> 결재 파기(tracking 복귀)!")
                    existing["filled"] = "tracking"
                    status = "tracking"
                else:
                    continue

            if status in ["wait", "done"]:
                continue

            old_q = float(existing.get("quantity", 0))
            if abs(old_q - qty) > 0.0001:
                existing["avg_buy_price"] = avg_p
                existing["quantity"] = qty
                existing["highest_price"] = max(curr_p, avg_p)

            highest_p = float(existing.get("highest_price", avg_p))

            # --------------------------------------------------------
            # 💡 최고점 추적
            # --------------------------------------------------------
            if curr_p > highest_p:
                existing["highest_price"] = curr_p
                highest_p = curr_p
                if current_profit_pct >= activation_pct:
                    print(f"📈 {symbol} 수익 런(Run) 중! 최고점 갱신 -> {int(curr_p):,}원 (수익률: +{current_profit_pct * 100:.2f}%)")

            # --------------------------------------------------------
            # 🔒 절대 수익 자물쇠 (Absolute Profit Lock)
            # --------------------------------------------------------
            min_profit_price = avg_p * (1 + min_profit_pct)

            if curr_p < min_profit_price:
                continue

            is_sell_triggered = False

            # 조건 1. 트레일링 스탑
            if highest_p >= avg_p * (1 + activation_pct):
                drop_from_high = (highest_p - curr_p) / highest_p
                if drop_from_high >= trailing_drop_pct:
                    print(f"🚨 {symbol} 트레일링 스탑 발동! 고점({int(highest_p):,}원) 대비 {trailing_drop_pct * 100}% 하락 감지 -> 💥 전량 익절 실행!")
                    is_sell_triggered = True

            # 조건 2. 마지노선 붕괴 방어
            elif highest_p >= avg_p * (1 + (activation_pct * 0.5)):
                buffer_pct = min_profit_pct + 0.01
                if current_profit_pct <= buffer_pct:
                    print(f"🛡️ {symbol} 마지노선 붕괴 방어! 현재가({int(curr_p):,}원)가 최소 보장 수익선 인접 -> 💥 익절 실행!")
                    is_sell_triggered = True

            if is_sell_triggered:
                existing["filled"] = "update"
                existing["time"] = now_ts

        if not logs:
            return pd.DataFrame(columns=["time", "symbol", "avg_buy_price", "quantity", "highest_price", "sell_uuid", "filled"])

        res = pd.DataFrame(logs)
        if "market" in res.columns: res = res.drop(columns=["market"])

        for c in ["symbol", "sell_uuid", "filled"]:
            if c in res.columns: res[c] = res[c].astype(str).replace("nan", "").str.strip()

        print("[casino_strategy.py] 매도 두뇌 연산 완료")
        return res

    except Exception as e:
        print(f"[casino_strategy.py] 🚨 매도 로직 에러:\n{traceback.format_exc()}")
        raise e