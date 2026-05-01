# utils/alert.py

import os
import requests
from dotenv import load_dotenv

# ---------------------------------------------------------
# 🔹 .env 로드 (프로그램 최초 실행 시 한 번만 로딩됨)
# ---------------------------------------------------------
load_dotenv()

# ---------------------------------------------------------
# 🔹 환경 변수에서 디스코드 웹훅 주소 가져오기
# ---------------------------------------------------------
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")


# ---------------------------------------------------------
# 🔹 디스코드 메시지 전송 함수
# ---------------------------------------------------------
def send_discord_alert(message: str):
    """
    디스코드로 알림 메시지를 전송.
    .env에 DISCORD_WEBHOOK_URL이 없으면 출력만 하고 종료.
    """
    if not DISCORD_WEBHOOK_URL:
        print("❌ DISCORD_WEBHOOK_URL 환경 변수가 설정되지 않았습니다.")
        return False

    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json={"content": message},
            timeout=5
        )

        # 에러 코드 반환 시 로깅
        if response.status_code >= 400:
            print(f"❌ 디스코드 웹훅 응답 오류: {response.status_code} / {response.text}")
            return False

        return True

    except Exception as e:
        print(f"❌ 디스코드 알림 전송 실패: {e}")
        return False
