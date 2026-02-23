import app
from models import Deal
from services.metrics import compute_deal_metrics

with app.app.app_context():
    deals = Deal.query.all()
    print(f"Found {len(deals)} deals")

    total_value_created = 0
    sum_rev = sum_margin = sum_mult = sum_lev = sum_other = 0

    for d in deals:
        m = compute_deal_metrics(d)
        total_value_created += m.get("value_created") or 0

        bridge = m.get("bridge_additive_fund", {})
        if bridge.get("ready"):
            drv = bridge.get("drivers_dollar", {})
            sum_rev += drv.get("revenue") or 0
            sum_margin += drv.get("margin") or 0
            sum_mult += drv.get("multiple") or 0
            sum_lev += drv.get("leverage") or 0
            sum_other += drv.get("other") or 0

    print(f"Total Actual Value Created: {total_value_created}")
    print(f"Sum Bridge: {sum_rev + sum_margin + sum_mult + sum_lev + sum_other}")
    print(f"Driver Revenue: {sum_rev}")
    print(f"Driver Margin: {sum_margin}")
    print(f"Driver Multiple: {sum_mult}")
    print(f"Driver Leverage: {sum_lev}")
    print(f"Driver Other: {sum_other}")
