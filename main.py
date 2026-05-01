# main.py
# -*- coding: utf-8 -*-

import os
import sys
import time
import logging
import traceback
import pandas as pd
from dotenv import load_dotenv

from strategy.entry import run_casino_entry
from utils.alert import send_discord_alert
from api.auth import force_issue_new_token  # ✅ 봇 시작 시 토큰 강제 초기화용

load_dotenv()

# ✅ 로그 설정: error.log 파일로 에러 저장
logging.basicConfig(
    filename="error.log",
    filemode="a",  # 이어쓰기
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.ERROR
)

# 💡 새로운 트레일링 스탑 전략에 맞춘 신규 열(Column) 정의
REQUIRED_COLUMNS = {
    "setting.csv": [
        "symbol", "unit_size", "small_flow_pct", "small_flow_units",
        "large_flow_pct", "large_flow_units",
        "activation_pct", "trailing_drop_pct", "min_profit_pct"
    ],
    "buy_log.csv": [
        "time", "symbol", "target_price", "buy_amount",
        "buy_units", "buy_type", "buy_uuid", "filled"
    ],
    "sell_log.csv": [
        "time", "symbol", "avg_buy_price", "quantity",
        "highest_price", "sell_uuid", "filled"
    ],
}


def ensure_csv_files():
    print("[main.py] 🗂️ CSV 장부 파일 검사 시작")

    for filename, expected_columns in REQUIRED_COLUMNS.items():
        if not os.path.exists(filename):
            print(f"📄 '{filename}' 파일이 없어 새로 생성합니다.")
            df = pd.DataFrame(columns=expected_columns)
            df.to_csv(filename, index=False)
        else:
            try:
                df = pd.read_csv(filename)
                existing_columns = df.columns.tolist()
                if existing_columns != expected_columns:
                    print(f"❌ '{filename}' 파일의 열이 예상과 다릅니다.")
                    print(f"    ▶ 예상: {expected_columns}")
                    print(f"    ▶ 실제: {existing_columns}")
                    print("🚫 프로그램을 종료합니다. 기존 파일을 백업 후 삭제하고 다시 실행해주세요.")
                    sys.exit(1)
                else:
                    print(f"✅ '{filename}' 파일 포맷 정상 확인 완료.")
            except pd.errors.EmptyDataError:
                # 파일이 완전히 비어있을 때 발생하는 에러 방어
                print(f"⚠️ '{filename}' 파일이 비어있어 헤더를 초기화합니다.")
                df = pd.DataFrame(columns=expected_columns)
                df.to_csv(filename, index=False)


def main():
    print("=" * 60)
    print("🚀 DB_STOCK_domestic_tracking 자동매매 봇 구동 시작")
    print("=" * 60)

    ensure_csv_files()

    # ✅ 봇 시작 시 DB증권 토큰 강제 발급 (만료 걱정 없는 클린 시작)
    try:
        force_issue_new_token()
    except Exception as e:
        print(f"[main.py] ⚠️ 토큰 갱신 실패 (기존 토큰 재사용 시도): {e}")

    run_casino_entry()
    print("[main.py] 🛑 프로그램 정상 종료")


# ✅ 루프: 에러 발생 시 디스코드 알림 + 로그 기록 후 자동 재시작
if __name__ == "__main__":
    while True:
        try:
            main()
            # 메인 함수가 자연스럽게 종료되었다면 의도된 종료이므로 루프 탈출
            break

        except SystemExit as e:
            error_message = f"🚨 [main.py] SystemExit 발생 (종료 코드: {e.code})"
            print(error_message)
            logging.error(error_message)

            try:
                send_discord_alert(error_message)
            except Exception as alert_error:
                print(f"❌ 디스코드 알림 전송 실패: {alert_error}")

            # 💡 [핵심 패치] CSV 열 불일치 같은 치명적 오류(1)는 무한루프 도배 방지를 위해 탈출
            if e.code == 1:
                print("🚫 치명적 오류로 인해 봇을 완전히 종료합니다.")
                break

        except Exception as e:
            # 일반 런타임 예외 처리 (API 끊김 등)
            error_message = f"🚨 [main.py] 런타임 예외 발생: {e}\n{traceback.format_exc()}"
            print(error_message)
            logging.error(error_message, exc_info=True)

            try:
                send_discord_alert(f"🚨 [자동매매 봇 에러] {e}")
            except Exception as alert_error:
                print(f"❌ 디스코드 알림 전송 실패: {alert_error}")

        # 에러 발생 시 디스코드 도배를 막고 통신 안정화를 위해 10초 대기 후 루프 재시작
        print("⏳ [main.py] 에러 복구 대기... 10초 후 다음 루프 재시작")
        time.sleep(10)