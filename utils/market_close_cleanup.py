# utils/market_close_cleanup.py
# 장마감/시간외 감지 + 로그 정리(정책 반영)
# ✅ 변경 요약
# - (삭제) 호가 기반 장마감 판단 제거 (entry.py가 판단)
# - buy_log.csv : flow 주문의 filled만 "" 로 비움
# - sell_log.csv: 무조건 전체 내용 삭제(트레일링 스탑 양식 적용)

import os
import time
import pandas as pd
from typing import List, Optional

# (선택) 호출 간격 제한
_LAST_CLEANUP_TS = 0.0
CLEANUP_MIN_INTERVAL_SEC = 30


def run_market_close_cleanup_if_needed(is_market_open: Optional[bool], reason: str = "") -> None:
    # ✅ 쿨다운
    global _LAST_CLEANUP_TS
    now_ts = time.time()
    if now_ts - _LAST_CLEANUP_TS < CLEANUP_MIN_INTERVAL_SEC:
        remain = int(CLEANUP_MIN_INTERVAL_SEC - (now_ts - _LAST_CLEANUP_TS))
        print(f"[market_close_cleanup] ⏭ cleanup 스킵(쿨다운 {remain}s) reason={reason}")
        return
    _LAST_CLEANUP_TS = now_ts

    if is_market_open is True:
        print(f"[market_close_cleanup] ✅ 장중(True) → cleanup 미수행 reason={reason}")
        return

    # ✅ 핵심: False(장마감) 또는 None(체크실패)면 정리 수행
    if is_market_open is None:
        print(f"[market_close_cleanup] ⚠ 시장상태 체크 실패(None) → 안전모드 cleanup 수행 reason={reason}")
    else:
        print(f"[market_close_cleanup] ✅ 장마감/거래불가(False) → cleanup 수행 reason={reason}")

    apply_market_close_cleanup()


def _to_symbol(x: str) -> str:
    s = (x or "").strip().replace("KRW-", "").strip()
    return s.upper()


def _safe_read_csv(path: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=columns) if columns else pd.DataFrame()

    try:
        df = pd.read_csv(path, dtype=str) # ✅ 처음부터 모든 데이터를 문자로 읽어들임 (안전성 강화)
        return df
    except Exception as e:
        print(f"[market_close_cleanup] ⚠ CSV 읽기 실패({path}) err={e} → 빈 DF로 대체")
        return pd.DataFrame(columns=columns) if columns else pd.DataFrame()


def _ensure_symbol_column(df: pd.DataFrame, df_name: str = "df") -> pd.DataFrame:
    if df is None:
        return df
    if df.empty:
        return df

    df = df.copy()

    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).apply(_to_symbol)
        return df

    if "market" in df.columns:
        df["symbol"] = df["market"].astype(str).apply(_to_symbol)
        print(f"[market_close_cleanup] ✅ {df_name}: 'market' → 'symbol' 생성(호환)")
        return df

    return df


def apply_market_close_cleanup() -> None:
    print("[market_close_cleanup] ✅ 로그 정리 시작")

    # utils/apply_market_close_cleanup.py 내부의 apply_market_close_cleanup 함수 중 (A) 영역만 수정

    # -----------------------
    # (A) buy_log 정리: flow의 filled와 buy_uuid를 함께 비움 (DB증권 API 에러 방지)
    # -----------------------
    buy_log_cols = ["time", "symbol", "target_price", "buy_amount", "buy_units", "buy_type", "buy_uuid", "filled"]
    buy_df = _safe_read_csv("buy_log.csv", columns=buy_log_cols)
    buy_df = _ensure_symbol_column(buy_df, "buy_log_df")

    if buy_df is None or buy_df.empty:
        print("[market_close_cleanup] buy_log.csv 비어있음 → flow 정리 스킵")
    else:
        if "buy_type" not in buy_df.columns or "filled" not in buy_df.columns:
            print("[market_close_cleanup] ⚠ buy_log.csv 스키마 부족(buy_type/filled) → 스킵")
        else:
            # flow 주문 대상을 찾음
            flow_mask = buy_df["buy_type"].astype(str).str.strip().isin(["small_flow", "large_flow"])

            # (추가) initial 주문이라도 체결 대기중(wait/update)이면 같이 초기화 대상에 포함 (선택사항이나 권장)
            # pending_mask = buy_df["filled"].isin(["wait", "update"])
            # target_mask = flow_mask | pending_mask

            if flow_mask.any():
                buy_df["filled"] = buy_df["filled"].fillna("").astype(str)
                buy_df["buy_uuid"] = buy_df["buy_uuid"].fillna("").astype(str)  # UUID 컬럼도 문자열화

                # 💡 [핵심] filled와 buy_uuid를 동시에 비워줌 (DB증권 Day Order 특성 반영)
                buy_df.loc[flow_mask, "filled"] = ""
                buy_df.loc[flow_mask, "buy_uuid"] = ""

                buy_df.to_csv("buy_log.csv", index=False)
                print(f"[market_close_cleanup] ✅ buy_log: flow filled & UUID 비움 완료 ({int(flow_mask.sum())}건)")
            else:
                print("[market_close_cleanup] buy_log: 정리할 flow row 없음 → 스킵")

    # -----------------------
    # (B) sell_log 정리: 트레일링 스탑 고점(highest_price) 유지, UUID만 초기화
    # -----------------------
    # 💡 target_sell_price -> highest_price 로 교체하신 회원님의 스키마 반영
    sell_log_cols = ["time", "symbol", "avg_buy_price", "quantity", "highest_price", "sell_uuid", "filled"]

    try:
        sell_df = _safe_read_csv("sell_log.csv", columns=sell_log_cols)

        if not sell_df.empty:
            # 💡 [핵심] 어제 기록된 highest_price는 보존하고, DB증권에서 소멸된 주문 번호만 날림
            sell_df["sell_uuid"] = ""

            # 오늘 장이 열렸을 때 트레일링 감시 로직이 즉각 반응하도록 상태를 update로 변경
            sell_df["filled"] = "update"

            sell_df.to_csv("sell_log.csv", index=False)
            print(
                f"[market_close_cleanup] ✅ sell_log: 트레일링 스탑 최고가(highest_price) 유지, UUID/상태 초기화 완료 ({len(sell_df)}건)")
        else:
            print("[market_close_cleanup] sell_log.csv 비어있음 → 스킵")

    except Exception as e:
        print(f"[market_close_cleanup] ❌ sell_log 업데이트 실패: {e}")