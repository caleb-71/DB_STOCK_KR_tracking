# strategy/buy_entry.py
# -*- coding: utf-8 -*-

import os
import time
from typing import Dict, Any, Optional

import pandas as pd

from api.account import get_accounts
from api.order import get_order_results_by_uuids, cancel_orders_by_uuids
from api.price import get_current_ask_price
from strategy.casino_strategy import generate_buy_orders
from manager.order_executor import execute_buy_orders

_LAST_BUY_STATUS_CHECK_TS = 0.0
BUY_STATUS_CHECK_MIN_INTERVAL_SEC = 20
BUY_STATUS_CHECK_BATCH_SIZE = 20
BUY_STATUS_CHECK_BATCH_SLEEP_SEC = 0.5


def _to_symbol(market_or_symbol: Any) -> str:
    """
    [핵심 수정] 엑셀에서 앞자리 0이 날아가도 무조건 6자리 문자로 복구합니다.
    """
    s = str(market_or_symbol or "").strip()
    s = s.replace("KRW-", "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.startswith("A"):
        s = s[1:]
    return s.zfill(6).upper()  # 💡 5자리 이하면 앞에 0을 채워서 6자리로 강제 고정


def _clean_uuid(uuid_val: Any) -> str:
    s = str(uuid_val).strip()
    if s.lower() == "nan" or s == "": return ""
    if s.endswith(".0"): return s[:-2]
    return s


def _ensure_symbol_column(df: pd.DataFrame, df_name: str = "df") -> pd.DataFrame:
    if df is None or df.empty: return df
    if "symbol" in df.columns:
        df = df.copy()
        df["symbol"] = df["symbol"].apply(_to_symbol)
        return df
    if "market" in df.columns:
        df = df.copy()
        df["symbol"] = df["market"].apply(_to_symbol)
        return df
    return df


def _extract_symbol_from_account(acc: Dict[str, Any]) -> Optional[str]:
    for key in ("symbol", "IsuNo", "isu_no", "ticker", "currency"):
        v = acc.get(key)
        if v:
            s = _to_symbol(v)
            if s and s != "000KRW": return s
    return None


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return default


def _is_no_history_error(e: Exception) -> bool:
    msg = str(e)
    return ("'rsp_cd': '2679'" in msg) or ('"rsp_cd": "2679"' in msg) or ("조회내역" in msg)


def clean_buy_log_for_fully_sold_symbols(buy_log_df: pd.DataFrame, holdings: Dict[str, Any]) -> pd.DataFrame:
    if buy_log_df is None or buy_log_df.empty: return buy_log_df

    buy_log_df = _ensure_symbol_column(buy_log_df, "buy_log_df")
    raw_logs = buy_log_df.to_dict('records')
    valid_symbols = set((holdings or {}).keys())

    sym_groups = {}
    for log in raw_logs:
        sym = _to_symbol(log.get("symbol"))
        if sym not in sym_groups: sym_groups[sym] = []
        sym_groups[sym].append(log)

    rows_to_keep = []
    for sym, group_logs in sym_groups.items():
        if sym in valid_symbols:
            rows_to_keep.extend(group_logs)
        else:
            has_done_order = any(
                str(l.get("filled", "")).strip().lower() == "done"
                for l in group_logs
            )

            if has_done_order:
                print(f"🎯 빗자루 출동! {sym} 전량 수익 실현 감지 -> 잔여 그물망 취소 및 매수 로그 파기")
                for l in group_logs:
                    if str(l.get("filled", "")).strip().lower() == "wait":
                        uuid = _clean_uuid(l.get("buy_uuid"))
                        if uuid:
                            try:
                                time.sleep(0.3)
                                cancel_orders_by_uuids([uuid], symbol=sym)
                            except Exception as e:
                                print(f"⚠️ {sym} 취소 중 에러(무시): {e}")
            else:
                rows_to_keep.extend(group_logs)

    if not rows_to_keep: return pd.DataFrame(columns=buy_log_df.columns)
    return pd.DataFrame(rows_to_keep)


def update_buy_log_status() -> None:
    global _LAST_BUY_STATUS_CHECK_TS
    if time.time() - _LAST_BUY_STATUS_CHECK_TS < BUY_STATUS_CHECK_MIN_INTERVAL_SEC: return
    _LAST_BUY_STATUS_CHECK_TS = time.time()

    if not os.path.exists("buy_log.csv"): return

    try:
        df = pd.read_csv("buy_log.csv", dtype=str)
    except pd.errors.EmptyDataError:
        return

    df = _ensure_symbol_column(df, "buy_log_df")
    if df.empty: return

    mask_wait = df["filled"].fillna("").str.strip().str.lower() == "wait"
    mask_uuid = df["buy_uuid"].fillna("").str.strip() != ""
    pending_df = df[mask_wait & mask_uuid]

    if pending_df.empty: return

    uuid_list = []
    for raw_uuid in pending_df["buy_uuid"].tolist():
        clean_u = _clean_uuid(raw_uuid)
        if clean_u: uuid_list.append(clean_u)

    uuid_list = list(dict.fromkeys(uuid_list))
    status_map = {}

    try:
        for i in range(0, len(uuid_list), BUY_STATUS_CHECK_BATCH_SIZE):
            batch = uuid_list[i:i + BUY_STATUS_CHECK_BATCH_SIZE]
            res = get_order_results_by_uuids(batch)
            if res: status_map.update({str(k).strip(): str(v).strip() for k, v in res.items()})
            time.sleep(BUY_STATUS_CHECK_BATCH_SLEEP_SEC)

        changed = False
        for idx, row in df.iterrows():
            uuid = _clean_uuid(row.get("buy_uuid"))

            if uuid in status_map:
                new_st = status_map[uuid]
                current_st = str(row.get("filled", "")).strip()
                if current_st != new_st:
                    df.at[idx, "filled"] = new_st
                    df.at[idx, "buy_uuid"] = uuid
                    changed = True

        if changed:
            df.to_csv("buy_log.csv", index=False)
            print("[buy_entry.py] ✅ 매수 미체결 주문 상태 최신화 완료")
    except Exception as e:
        if not _is_no_history_error(e): print(f"[buy_entry.py] 상태 업데이트 스킵: {e}")


def run_buy_entry_flow() -> None:
    print("\n[buy_entry.py] 🛒 매수 전략 플로우 가동")

    try:
        # 💡 [핵심 패치] 파일을 읽을 때 무조건 문자열(str)로 읽어서 0 증발 방지
        setting_df = pd.read_csv("setting.csv", dtype=str)
        setting_df = _ensure_symbol_column(setting_df, "setting_df")
    except Exception as e:
        print(f"🚨 기초 데이터(setting.csv) 로드 실패: {e}")
        return

    update_buy_log_status()

    time.sleep(1)
    accounts = get_accounts()

    holdings = {}
    for acc in (accounts or []):
        symbol = _extract_symbol_from_account(acc)
        balance = _safe_float(acc.get("balance"), 0.0)
        eval_amt = _safe_float(acc.get("eval_amt"), 0.0)

        if symbol and balance > 0:
            holdings[symbol] = {
                "balance": balance,
                "avg_price": _safe_float(acc.get("avg_buy_price")),
                "eval_amt": eval_amt
            }

    buy_log_df = pd.DataFrame(
        columns=["time", "symbol", "target_price", "buy_amount", "buy_units", "buy_type", "buy_uuid", "filled"])

    if os.path.exists("buy_log.csv"):
        try:
            temp_df = pd.read_csv("buy_log.csv", dtype=str) # 💡 문자열 읽기
            if not temp_df.empty: buy_log_df = temp_df
        except pd.errors.EmptyDataError:
            pass

    buy_log_df = _ensure_symbol_column(buy_log_df, "buy_log_df")

    current_prices = {}
    for _, row in setting_df.iterrows():
        symbol = row["symbol"]
        try:
            time.sleep(0.5)
            ask = _safe_float(get_current_ask_price(symbol), 0.0)
            if ask > 0: current_prices[symbol] = ask
        except Exception:
            continue

    if not current_prices:
        print("⚠️ 현재가 수집 실패로 매수 플로우를 건너뜁니다.")
        return

    buy_log_df = clean_buy_log_for_fully_sold_symbols(buy_log_df, holdings)

    updated_buy_log_df = generate_buy_orders(setting_df, buy_log_df, current_prices, holdings)

    if updated_buy_log_df is None or updated_buy_log_df.empty: return

    update_mask = updated_buy_log_df["filled"].fillna("").astype(str).str.strip().str.lower() == "update"

    if update_mask.any():
        print(f"[buy_entry.py] 🚀 {update_mask.sum()}건 신규/정정 주문 증권사 발송 시작...")
        try:
            final_df = execute_buy_orders(updated_buy_log_df)

            if "buy_uuid" in final_df.columns:
                final_df["buy_uuid"] = final_df["buy_uuid"].apply(_clean_uuid)

            final_df.to_csv("buy_log.csv", index=False)
        except Exception as e:
            print(f"🚨 주문 실행 에러: {e}")
            updated_buy_log_df.to_csv("buy_log.csv", index=False)
    else:
        if "buy_uuid" in updated_buy_log_df.columns:
            updated_buy_log_df["buy_uuid"] = updated_buy_log_df["buy_uuid"].apply(_clean_uuid)
        updated_buy_log_df.to_csv("buy_log.csv", index=False)
        print("[buy_entry.py] ℹ️ 이번 분루프에는 신규 매수 주문이 없습니다.")