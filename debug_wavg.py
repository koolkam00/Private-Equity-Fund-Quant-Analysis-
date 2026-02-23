import app
from models import Deal
from services.metrics import compute_bridge_aggregate, compute_deal_metrics, compute_portfolio_analytics

with app.app.app_context():
    deals = Deal.query.all()
    metrics = {d.id: compute_deal_metrics(d) for d in deals}

    for i, d in enumerate(deals, start=1):
        m = metrics[d.id]
        print(f"Deal {i}: {d.company_name}")
        print(f"  MOIC: {m.get('moic')}")
        print(f"  Value Created: {m.get('value_created')}")

    p = compute_portfolio_analytics(deals, metrics_by_id=metrics)
    print("Portfolio Returns:")
    print(p["returns"])

    bridge = compute_bridge_aggregate(deals, model="additive", basis="fund")
    print("Aggregate Bridge (MOIC):")
    print(bridge["drivers"]["moic"])
