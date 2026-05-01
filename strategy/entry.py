# strategy/entry.py
# -*- coding: utf-8 -*-

import time
from datetime import datetime

from strategy.buy_entry import run_buy_entry_flow
from strategy.sell_entry import run_sell_entry_flow

try:
    from api.auth import force_issue_new_token
except ImportError:
    force_issue_new_token = None
    print("[entry.py] ⚠ api.auth.force_issue_new_token import 실패")

try:
    import pytz
except ImportError:
    pytz = None

try:
    from utils.market_close_cleanup import run_market_close_cleanup_if_needed
except Exception as e:
    print(f"[entry.py] ⚠ market_close_cleanup import 실패: {e}")
    run_market_close_cleanup_if_needed = None

try:
    from api.market_status import get_market_open_status
except Exception as e:
    print(f"[entry.py] ⚠ api.market_status import 실패: {e}")
    get_market_open_status = None


def get_now_kst_str():
    """한국 표준시(KST) 기준 현재 시간 문자열 반환"""
    if pytz:
        kst = pytz.timezone('Asia/Seoul')
        return datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S KST")
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S (System Time)")


def run_casino_entry():
    print(f"[entry.py] ▶ 국내주식 자동매매 시스템 가동 (현재 시각: {get_now_kst_str()})")

    last_minute = None
    last_reason = "최초 실행 대기 중"

    # 매일 아침 8시 갱신을 추적하기 위한 변수
    last_refresh_date = None

    while True:
        try:
            # 기본적으로 시스템 시간(한국 서버 기준 KST)을 사용
            now = datetime.now()
            current_date = now.date()

            # =====================================================
            # 💡 [핵심 패치 1] 매일 아침 8시 토큰 선제 갱신 (한국장 기준)
            # 프로그램 최초 실행 시(None) 또는 날짜가 바뀌고 아침 8시가 넘었을 때 1회 갱신
            # =====================================================
            if last_refresh_date is None or (last_refresh_date != current_date and now.hour >= 8):
                print(f"\n🔑 [{now.strftime('%Y-%m-%d %H:%M:%S')}] 장 시작 전 API 토큰 선제 발급 (안전 확보)...")
                if callable(force_issue_new_token):
                    try:
                        force_issue_new_token()
                        last_refresh_date = current_date
                        time.sleep(2)
                    except Exception as e:
                        print(f"❌ 토큰 갱신 에러: {e}")
                else:
                    last_refresh_date = current_date

            # 분이 바뀔 때마다(1분 주기로) 메인 로직 실행
            if last_minute is None or now.minute != last_minute:
                now_kst = get_now_kst_str()
                print(f"\n=======================================================")
                print(f"🕒 [분루프 시작] {now_kst}")
                print(f"=======================================================")

                # --- (1) 장 상태 확인 ---
                last_market_open = None
                if callable(get_market_open_status):
                    try:
                        st = get_market_open_status(timeout=10)
                        last_market_open = st.get("is_open")
                        last_reason = st.get("reason", "no reason provided")

                        if last_market_open is True:
                            status_icon = "🟢 OPEN"
                        elif last_market_open is False:
                            status_icon = "🔴 CLOSED"
                        else:
                            status_icon = "🟡 UNKNOWN (API ERROR)"

                        print(f"[entry.py] 시장 상태: {status_icon} | 사유: {last_reason}")
                    except Exception as e:
                        last_reason = f"상태 체크 중 예외 발생: {e}"
                        print(f"[entry.py] ❌ 장 상태 판단 실패: {last_reason}")

                # --- (2) 장이 확실히 닫혀있을 때 (False) ---
                if last_market_open is False:
                    if callable(run_market_close_cleanup_if_needed):
                        try:
                            run_market_close_cleanup_if_needed(last_market_open, reason=f"Loop check: {last_reason}")
                        except Exception as e:
                            print(f"[entry.py] ⚠ Cleanup 실행 중 오류(무시): {e}")
                    print(f"[entry.py] ⏭ 장 마감 상태이므로 이번 분루프 매매 로직 스킵 (이유: {last_reason})")

                # --- (3) API 에러 등으로 장 상태를 모를 때 (None) ---
                elif last_market_open is None:
                    print(f"[entry.py] ⚠️ 통신 오류로 장 상태 확인 불가. 장부(CSV)를 안전하게 보존하고 1분 대기합니다.")

                # --- (4) 정규장이 열려있을 때 (True) ---
                else:
                    # =====================================================
                    # 💡 [핵심 패치 2] 한국장 개장 직후 3분 방어막 (09:00 ~ 09:02)
                    # 시가 단일가 매매 직후의 비정상적 변동성을 피하기 위한 대기
                    # =====================================================
                    is_grace_period = False

                    if pytz:
                        kst_now = datetime.now(pytz.timezone('Asia/Seoul'))
                        if kst_now.hour == 9 and 0 <= kst_now.minute <= 2:
                            is_grace_period = True
                    else:
                        if now.hour == 9 and 0 <= now.minute <= 2:
                            is_grace_period = True

                    if is_grace_period:
                        print(f"[entry.py] 🛡️ 개장 직후 호가 안정화 대기 중 (가짜 호가 방어막 3분 가동)... 매매 스킵")
                    else:
                        print(f"[entry.py] ✅ 장 오픈 확인 -> 매수/매도 전략 가동")
                        run_buy_entry_flow()
                        print("[entry.py] ⏳ 매수 로직 완료, 매도(트레일링 스탑) 스캔 전 3초 대기...")
                        time.sleep(3)
                        run_sell_entry_flow()

                last_minute = now.minute

            time.sleep(1)

        except Exception as e:
            print(f"[entry.py] 🚨 메인 무한 루프 에러 발생: {e}")
            time.sleep(5)