# tests/test_generate_buy_orders.py

import pandas as pd
from strategy.casino_strategy import generate_buy_orders


def run_generate_buy_orders_test():
    print("[TEST] generate_buy_orders 테스트 시작")

    # -------- 1. setting.csv 시뮬레이션 --------
    setting_df = pd.DataFrame([
        {
            "market": "KRW-AAA",
            "unit_size": 10000,
            "small_flow_pct": 0.05,
            "small_flow_units": 2,
            "large_flow_pct": 0.10,
            "large_flow_units": 3,
            "take_profit_pct": 0.10
        },
        {
            "market": "KRW-BBB",
            "unit_size": 5000,
            "small_flow_pct": 0.02,
            "small_flow_units": 1,
            "large_flow_pct": 0.05,
            "large_flow_units": 2,
            "take_profit_pct": 0.08
        },
        {
            "market": "KRW-CCC",
            "unit_size": 8000,
            "small_flow_pct": 0.03,
            "small_flow_units": 1,
            "large_flow_pct": 0.07,
            "large_flow_units": 2,
            "take_profit_pct": 0.12
        }
    ])

    # -------- 2. buy_log.csv 시뮬레이션 --------
    buy_log_df = pd.DataFrame([
        # situation2: initial done, flow 주문 대기 중
        {"time": "2025-04-10", "market": "KRW-BBB", "target_price": 1000, "buy_amount": 5000, "buy_units": 1,
         "buy_type": "initial", "buy_uuid": "uuid1", "filled": "done"},
        {"time": "2025-04-10", "market": "KRW-BBB", "target_price": 980, "buy_amount": 5000, "buy_units": 1,
         "buy_type": "small_flow", "buy_uuid": "uuid2", "filled": ""},
        {"time": "2025-04-10", "market": "KRW-BBB", "target_price": 950, "buy_amount": 10000, "buy_units": 2,
         "buy_type": "large_flow", "buy_uuid": "uuid3", "filled": "wait"},

        # situation3: small_flow & large_flow 둘 다 체결됨
        {"time": "2025-04-11", "market": "KRW-CCC", "target_price": 700, "buy_amount": 8000, "buy_units": 1,
         "buy_type": "initial", "buy_uuid": "uuid4", "filled": "done"},
        {"time": "2025-04-11", "market": "KRW-CCC", "target_price": 680, "buy_amount": 8000, "buy_units": 1,
         "buy_type": "small_flow", "buy_uuid": "uuid5", "filled": "done"},
        {"time": "2025-04-11", "market": "KRW-CCC", "target_price": 660, "buy_amount": 16000, "buy_units": 2,
         "buy_type": "large_flow", "buy_uuid": "uuid6", "filled": "done"}
    ])

    # -------- 3. 현재 가격 --------
    current_prices = {
        "KRW-AAA": 1000,  # situation1
        "KRW-BBB": 1025,  # situation2 → small_flow/large_flow 가격 조정 필요
        "KRW-CCC": 700    # situation3 → next 단계 매수 준비
    }

    # -------- 4. 테스트 실행 --------
    updated_df = generate_buy_orders(setting_df, buy_log_df, current_prices)

    # -------- 5. 결과 검토 --------
    print("\n[TEST] generate_buy_orders 결과:")
    print(updated_df[["market", "buy_type", "target_price", "buy_amount", "buy_units", "filled"]])

    # -------- 6. 간단한 assert 테스트 --------
    print("\n[ASSERT] 상황별 결과 확인")

    assert any((updated_df["market"] == "KRW-AAA") & (updated_df["buy_type"] == "initial")), "❌ AAA initial 없음"
    assert any((updated_df["market"] == "KRW-BBB") & (updated_df["filled"] == "update")), "❌ BBB 갱신 없음"
    assert any((updated_df["market"] == "KRW-CCC") & (updated_df["filled"] == "update")), "❌ CCC 후속 주문 없음"

    print("✅ 모든 상황 통과")