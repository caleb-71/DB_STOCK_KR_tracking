# strategy/buy_entry.py
import os
import time
import pandas as pd
from typing import Dict, Any, Optional

from api.account import get_accounts
from api.order import get_order_results_by_uuids, cancel_orders_by_uuids
from api.price import get_current_ask_price
from strategy.casino_strategy import generate_buy_orders
from manager.order_executor import execute_buy_orders

_LAST_BUY_STATUS_CHECK_TS = 0.0
BUY_STATUS_CHECK_MIN_INTERVAL_SEC = 20
BUY_STATUS_CHECK_BATCH_SIZE = 20
BUY_STATUS_CHECK_BATCH_SLEEP_SEC = 0.5  # ✅ API 과부하 방지를 위해 0.5초로 살짝 늘림


def _to_symbol(market_or_symbol: str) -> str:
    s = str(market_or_symbol or "").strip()
    s = s.replace("KRW-", "").strip()
    return s.upper()


def _clean_uuid(uuid_val: Any) -> str:
    s = str(uuid_val).strip()
    if s.lower() == "nan" or s == "": return ""
    if s.endswith(".0"): return s[:-2]
    return s


def _ensure_symbol_column(df: pd.DataFrame, df_name: str = "df") -> pd.DataFrame:
    if df is None or df.empty: return df
    if "symbol" in df.columns:
        df = df.copy()
        df["symbol"] = df["symbol"].astype(str).apply(_to_symbol)
        return df
    if "market" in df.columns:
        df = df.copy()
        df["symbol"] = df["market"].astype(str).apply(_to_symbol)
        return df
    return df


def _extract_symbol_from_account(acc: Dict[str, Any]) -> Optional[str]:
    for key in ("symbol", "AstkIsuNo", "isu_no", "ticker", "currency"):
        v = acc.get(key)
        if v:
            s = _to_symbol(str(v))
            if s and s != "KRW": return s
    return None


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(str(x).replace(",", "").strip())
    except:
        return default


def _is_no_history_error(e: Exception) -> bool:
    msg = str(e)
    return ("'rsp_cd': '2679'" in msg) or ('"rsp_cd": "2679"' in msg) or ("조회내역이 없습니다" in msg)


def clean_buy_log_for_fully_sold_symbols(buy_log_df: pd.DataFrame, holdings: Dict[str, Any]) -> pd.DataFrame:
    """
    [핵심 리팩토링] Initial 주문이 없어진 새로운 전략에 맞춘 전량 매도 감지 로직
    """
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
        # 잔고가 있다면 정상 유지
        if sym in valid_symbols:
            rows_to_keep.extend(group_logs)
        else:
            # 💡 [새로운 로직] 잔고가 0일 때, 'done'(체결) 상태인 주문이 하나라도 존재한다면?
            # -> 이전에 그물망(small/large)이 체결되어 보유 중이다가 트레일링 스탑으로 전량 익절된 것!
            has_done_order = any(
                str(l.get("filled", "")).strip().lower() == "done"
                for l in group_logs
            )

            if has_done_order:
                print(f"🎯 빗자루 출동! {sym} 전량 수익 실현 감지 -> 잔여 그물망 취소 및 매수 로그 완전 파기")
                for l in group_logs:
                    if str(l.get("filled", "")).strip().lower() == "wait":
                        uuid = _clean_uuid(l.get("buy_uuid"))
                        if uuid:
                            try:
                                time.sleep(0.3)  # ✅ 취소 API 연사 방지
                                cancel_orders_by_uuids([uuid])
                            except Exception as e:
                                print(f"⚠️ {sym} 취소 중 에러(무시): {e}")
                # rows_to_keep에 안 담으므로 로그에서 완벽히 삭제됨
            else:
                # 잔고가 0이더라도 done이 없으면? -> 방금 막 프로그램 켜서 wait 주문만 걸어둔 상태이므로 살려둠
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
        setting_df = pd.read_csv("setting.csv")
        setting_df = _ensure_symbol_column(setting_df, "setting_df")
    except Exception as e:
        print(f"🚨 기초 데이터(setting.csv) 로드 실패: {e}")
        return

    # 💡 [순서 변경] 상태 업데이트를 먼저 하고 잔고를 나중에 가져와야 증권사 딜레이에 의한 오작동을 방지합니다.
    update_buy_log_status()

    time.sleep(1)  # 잔고 조회 전 API 휴식
    accounts = get_accounts()

    holdings = {}
    for acc in (accounts or []):
        symbol = _extract_symbol_from_account(acc)
        balance = _safe_float(acc.get("balance"), 0.0)
        if symbol and balance > 0:
            holdings[symbol] = {"balance": balance, "avg_price": _safe_float(acc.get("avg_buy_price"))}

    buy_log_df = pd.DataFrame(
        columns=["time", "symbol", "target_price", "buy_amount", "buy_units", "buy_type", "buy_uuid", "filled"])
    if os.path.exists("buy_log.csv"):
        try:
            temp_df = pd.read_csv("buy_log.csv", dtype=str)
            if not temp_df.empty: buy_log_df = temp_df
        except pd.errors.EmptyDataError:
            pass

    buy_log_df = _ensure_symbol_column(buy_log_df, "buy_log_df")

    # ✅ [API 과부하 방어] 종목별로 현재가를 가져올 때 0.5초의 대기 시간을 줍니다.
    current_prices = {}
    for _, row in setting_df.iterrows():
        symbol = row["symbol"]
        try:
            time.sleep(0.5)
            ask = _safe_float(get_current_ask_price(symbol), 0.0)
            if ask > 0: current_prices[symbol] = ask
        except:
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