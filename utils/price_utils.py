# utils/price_utils.py
# -*- coding: utf-8 -*-

from decimal import Decimal, ROUND_HALF_UP, ROUND_FLOOR, ROUND_CEILING
from typing import Literal


def get_tick_size(price: float, market: str = "KRX", ticker: str = "") -> float:
    """
    한국거래소(KRX) 주식 정규시장 호가단위를 반환합니다.
    (2023년 개정 기준 코스피/코스닥 공통 적용)

    [KRX 주식 호가단위]
      500,000원 이상                 → 1,000원
      200,000원 이상 ~ 500,000원 미만 → 500원
      50,000원 이상 ~ 200,000원 미만  → 100원
      20,000원 이상 ~ 50,000원 미만   → 50원
      5,000원 이상 ~ 20,000원 미만    → 10원
      2,000원 이상 ~ 5,000원 미만     → 5원
      2,000원 미만                   → 1원

    ※ ETF의 경우 가격 상관없이 일괄 5원 단위가 적용되기도 하지만,
       일반적인 주식 종합 매매 봇을 위해 표준 주식 호가를 최우선으로 적용합니다.
    """
    p = float(price)

    if p <= 0:
        # 비정상 가격이면 틱 1.0 반환
        return 1.0

    if p >= 500_000:
        return 1000.0
    elif p >= 200_000:
        return 500.0
    elif p >= 50_000:
        return 100.0
    elif p >= 20_000:
        return 50.0
    elif p >= 5_000:
        return 10.0
    elif p >= 2_000:
        return 5.0
    else:
        return 1.0


def adjust_price_to_tick(
        price: float,
        market: str = "KRX",
        ticker: str = "",
        mode: Literal["round", "floor", "ceil"] = "round",
) -> float:
    """
    계산된 이론상 가격(예: 현재가 - 4%)을 KRX 실제 호가 단위에 맞게 보정합니다.
    국내 주식은 소수점이 허용되지 않으므로, 유효한 정수형 호가로 강제 변환됩니다.

    - price: 이론상 가격
    - mode:
        - "round": 가장 가까운 호가로 반올림 (일반적인 타점 계산 시)
        - "floor": 현재 가격 이하에서 가장 가까운 호가 (보수적인 매수/패닉셀 투척 시 유리)
        - "ceil" : 현재 가격 이상에서 가장 가까운 호가 (안전한 매도 시 유리)
    """
    tick = get_tick_size(price, market=market, ticker=ticker)
    if tick <= 0:
        return float(price)

    tick_dec = Decimal(str(tick))
    price_dec = Decimal(str(price))

    # price / tick → 몇 틱인가?
    units = price_dec / tick_dec

    if mode == "floor":
        units_adj = units.to_integral_value(rounding=ROUND_FLOOR)
    elif mode == "ceil":
        units_adj = units.to_integral_value(rounding=ROUND_CEILING)
    else:
        # 기본: 가장 가까운 호가로 반올림
        units_adj = units.quantize(Decimal("1"), rounding=ROUND_HALF_UP)

    adjusted = units_adj * tick_dec

    # 국내 주식 호가는 무조건 정수이므로 int 형변환을 거쳐 깔끔하게 float로 반환
    return float(int(adjusted))


def is_min_order_satisfied(price: float, volume: float, market: str = "KRX") -> bool:
    """
    '최소 주문 조건'을 만족하는지 체크합니다.
    - 코인(업비트)은 최소 주문 '금액(5,000원)' 기준이 있었지만,
    - 국내 주식은 소수점 거래가 아닌 이상 '최소 1주' 이상이면 정상 주문이 가능합니다.
    """
    return int(volume) >= 1