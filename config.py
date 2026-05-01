import os
from dotenv import load_dotenv

print("📦 Loading config.py and environment variables...")

load_dotenv()

ACCESS_KEY = os.getenv("UPBIT_OPEN_API_ACCESS_KEY")
SECRET_KEY = os.getenv("UPBIT_OPEN_API_SECRET_KEY")
SERVER_URL = os.getenv("UPBIT_OPEN_API_SERVER_URL", "https://api.upbit.com")

if not ACCESS_KEY or not SECRET_KEY:
    raise ValueError("API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")
