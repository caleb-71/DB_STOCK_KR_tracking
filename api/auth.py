# api/auth.py
# -*- coding: utf-8 -*-

import os
import json
import time
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------
# ✅ DB증권 국내주식 OAuth 토큰(24시간) 발급/폐기 + 로컬 캐시
# - 토큰 발급은 1분 1건 제한 → 반드시 캐시 재사용
# - 만료 전 강제 재발급 시, 반드시 기존 토큰 폐기(Revoke) 선행
# ---------------------------------------------------------

load_dotenv()

DB_APP_KEY = os.getenv("DB_APP_KEY")
DB_APP_SECRET = os.getenv("DB_APP_SECRET")

# 도메인은 API 문서 기준
DB_BASE_URL = os.getenv("DB_BASE_URL", "https://openapi.dbsec.co.kr:8443")
TOKEN_PATH = "/oauth2/token"
REVOKE_PATH = "/oauth2/revoke"

# 토큰 캐시 파일
TOKEN_CACHE_PATH = os.getenv("DB_TOKEN_CACHE_PATH", os.path.join("data", "db_access_token.json"))

# 만료 몇 분 전부터 재발급할지(안전마진 5분)
REFRESH_MARGIN_SECONDS = int(os.getenv("DB_TOKEN_REFRESH_MARGIN", "300"))

# 토큰 발급 API: 1분 1건 제한
MIN_TOKEN_CALL_INTERVAL = 60


def _ensure_cache_dir():
    cache_dir = os.path.dirname(TOKEN_CACHE_PATH)
    if cache_dir and not os.path.exists(cache_dir):
        os.makedirs(cache_dir, exist_ok=True)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _read_token_cache() -> dict:
    if not os.path.exists(TOKEN_CACHE_PATH):
        return {}
    try:
        with open(TOKEN_CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception as e:
        print(f"[auth.py] ⚠ 토큰 캐시 읽기 실패: {e}")
        return {}


def _write_token_cache(data: dict):
    try:
        _ensure_cache_dir()
        with open(TOKEN_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[auth.py] ⚠ 토큰 캐시 저장 실패: {e}")


def _is_token_valid(cache: dict) -> bool:
    token = cache.get("access_token")
    expires_at_str = cache.get("expires_at")
    if not token or not expires_at_str:
        return False

    try:
        expires_at = datetime.fromisoformat(expires_at_str)
    except Exception:
        return False

    # 만료 안전마진 적용
    now = _utc_now()
    if now >= (expires_at - timedelta(seconds=REFRESH_MARGIN_SECONDS)):
        return False

    return True


def _respect_rate_limit(cache: dict):
    """
    토큰 발급은 1분에 1건 제한.
    직전 발급시도 시간이 60초 이내면 남은 시간만큼 대기.
    """
    last_issued_at_str = cache.get("last_issued_at")
    if not last_issued_at_str:
        return

    try:
        last_issued_at = datetime.fromisoformat(last_issued_at_str)
    except Exception:
        return

    now = _utc_now()
    elapsed = (now - last_issued_at).total_seconds()
    if elapsed < MIN_TOKEN_CALL_INTERVAL:
        wait_sec = int(MIN_TOKEN_CALL_INTERVAL - elapsed) + 1
        print(f"[auth.py] ⏳ 토큰 API 유량 제한 보호: {wait_sec}초 대기 후 진행")
        time.sleep(wait_sec)


def revoke_access_token(token: str):
    """
    기존에 발급받은 토큰을 폐기(Revoke)합니다.
    """
    if not DB_APP_KEY or not DB_APP_SECRET:
        print("[auth.py] ⚠ 앱키/시크릿이 없어 토큰 폐기를 건너뜁니다.")
        return

    print("[auth.py] 🗑 기존 토큰 폐기를 요청합니다.")
    url = f"{DB_BASE_URL}{REVOKE_PATH}"
    headers = {"content-type": "application/x-www-form-urlencoded"}
    data = {
        "appkey": DB_APP_KEY,
        "appsecretkey": DB_APP_SECRET,
        "token_type_hint": "access_token",
        "token": token,
    }

    try:
        resp = requests.post(url, headers=headers, data=data, timeout=10)
        js = resp.json()
        if resp.status_code == 200 and js.get("code") == 200:
            print(f"[auth.py] ✅ 기존 토큰 폐기 성공: {js.get('message')}")
        else:
            print(f"[auth.py] ⚠ 토큰 폐기 실패: 응답코드 {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"[auth.py] ⚠ 토큰 폐기 중 네트워크 오류 발생: {e}")


def request_new_access_token() -> str:
    """
    DB증권 OAuth2 토큰 신규 발급.
    """
    print("[auth.py] 🔐 신규 Access Token 발급 요청 진행")

    if not DB_APP_KEY or not DB_APP_SECRET:
        raise ValueError("DB_APP_KEY / DB_APP_SECRET 이 설정되지 않았습니다.")

    url = f"{DB_BASE_URL}{TOKEN_PATH}"
    headers = {"content-type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "appkey": DB_APP_KEY,
        "appsecretkey": DB_APP_SECRET,
        "scope": "oob",
    }

    cache = _read_token_cache()
    _respect_rate_limit(cache)

    resp = requests.post(url, headers=headers, data=data, timeout=10)

    if resp.status_code != 200:
        raise Exception(f"[DB OAuth] 토큰 발급 실패: {resp.status_code} - {resp.text}")

    js = resp.json()
    token = js.get("access_token")
    expires_in = int(js.get("expires_in", 86400))

    if not token:
        raise Exception(f"[DB OAuth] 응답에 access_token이 없습니다: {js}")

    now = _utc_now()
    expires_at = now + timedelta(seconds=expires_in)

    new_cache = {
        "access_token": token,
        "expires_at": expires_at.isoformat(),
        "last_issued_at": now.isoformat(),
        "token_type": js.get("token_type", "Bearer"),
        "scope": js.get("scope", "oob"),
        "expires_in": expires_in,
    }
    _write_token_cache(new_cache)

    print(f"[auth.py] ✅ 새 Access Token 발급 및 로컬 캐시 저장 성공 (유효기간: {expires_in}초)")
    return token


def get_access_token(force_refresh: bool = False) -> str:
    """
    - 기본: 캐시가 유효하면 캐시 토큰 사용
    - force_refresh=True 이거나 만료 임박 시: (기존 토큰 폐기 후) 신규 발급
    """
    cache = _read_token_cache()

    if (not force_refresh) and _is_token_valid(cache):
        return cache["access_token"]

    print("[auth.py] ♻ Access Token 갱신이 필요합니다.")

    # 💡 강제 갱신이거나 기존 토큰이 남아있다면 반드시 폐기(Revoke) 후 재발급해야 새 토큰이 나옵니다.
    old_token = cache.get("access_token")
    if old_token:
        revoke_access_token(old_token)
        # 폐기 직후 바로 발급하면 서버 과부하 우려가 있으므로 짧게 대기
        time.sleep(1)

    return request_new_access_token()


def get_auth_headers(extra_headers: dict | None = None) -> dict:
    """
    DB API 호출 시 공통 Authorization 헤더 생성.
    """
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
    }
    if extra_headers:
        headers.update(extra_headers)
    return headers


def force_issue_new_token():
    """
    매일 아침 또는 장 시작 전, 기존 토큰을 강제로 폐기하고 새 토큰을 받아오는 초기화 함수
    """
    print("[auth.py] 🔄 국내주식 서버에 새로운 API 토큰을 강제 갱신합니다.")
    try:
        get_access_token(force_refresh=True)
        print("[auth.py] ✅ 토큰 강제 갱신 완료")
    except Exception as e:
        print(f"[auth.py] ❌ 강제 재발급 중 오류: {e}")