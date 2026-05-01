# tests/test_generate_sell_orders.py

import pandas as pd
from strategy.casino_strategy import generate_sell_orders

def run_generate_sell_orders_test():
    print("[TEST] generate_sell_orders 테스트 시작")

    # 1. setting.csv 시뮬레이션
    setting_df = pd.DataFrame([
        {"market": "KRW-AAA", "take_profit_pct": 0.01},
        {"market": "KRW-BBB", "take_profit_pct": 0.05},
        {"market": "KRW-CCC", "take_profit_pct": 0.10}
    ])

    # 2. holdings 시뮬레이션
    holdings = {
        "KRW-AAA": {"avg_price": 1000, "balance": 10},    # ❗ 기존과 다름
        "KRW-BBB": {"avg_price": 2000, "balance": 2},     # ❗ 기존과 다름 + uuid 존재
        "KRW-CCC": {"avg_price": 1500, "balance": 5}      # ✅ 기존과 동일
    }

    # 3. 기존 sell_log_df
    sell_log_df = pd.DataFrame([
        # AAA: 바뀌어야 함
        {
            "market": "KRW-AAA", "avg_buy_price": 900, "quantity": 9,
            "target_sell_price": 950, "sell_uuid": None, "filled": "wait"
        },
        # BBB: 바뀌어야 함 (uuid 유지)
        {
            "market": "KRW-BBB", "avg_buy_price": 1500, "quantity": 3,
            "target_sell_price": 1600, "sell_uuid": "uuid-bbb-123", "filled": "done"
        },
        # CCC: 완전히 동일 (변경 없음)
        {
            "market": "KRW-CCC", "avg_buy_price": 1500, "quantity": 5,
            "target_sell_price": 1650, "sell_uuid": "uuid-ccc-456", "filled": "wait"
        }
    ])

    # 4. 테스트 실행
    updated_df = generate_sell_orders(setting_df, holdings, sell_log_df)

    # 5. 출력
    print("\n[TEST RESULT] updated sell_log_df:")
    print(updated_df)

    # 6. 검증
    # AAA → 업데이트됨, uuid 없음
    row_aaa = updated_df[updated_df["market"] == "KRW-AAA"].iloc[0]
    assert row_aaa["avg_buy_price"] == 1000
    assert row_aaa["quantity"] == 10
    assert row_aaa["target_sell_price"] == 1010
    assert pd.isna(row_aaa["sell_uuid"])
    assert row_aaa["filled"] == "update"

    # BBB → 업데이트됨, 기존 uuid 유지
    row_bbb = updated_df[updated_df["market"] == "KRW-BBB"].iloc[0]
    assert row_bbb["avg_buy_price"] == 2000
    assert row_bbb["quantity"] == 2
    assert row_bbb["target_sell_price"] == 2100
    assert row_bbb["sell_uuid"] == "uuid-bbb-123"
    assert row_bbb["filled"] == "update"

    # CCC → 변경 없음, 기존 내용 유지
    row_ccc = updated_df[updated_df["market"] == "KRW-CCC"].iloc[0]
    assert row_ccc["avg_buy_price"] == 1500
    assert row_ccc["quantity"] == 5
    assert row_ccc["target_sell_price"] == 1650
    assert row_ccc["sell_uuid"] == "uuid-ccc-456"
    assert row_ccc["filled"] == "wait"

    print("✅ generate_sell_orders 테스트 통과")