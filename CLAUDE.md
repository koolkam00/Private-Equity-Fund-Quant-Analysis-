# PE Portfolio Lab

## Design System
Always read DESIGN.md before making any visual or UI decisions.
All font choices, colors, spacing, and aesthetic direction are defined there.
Do not deviate without explicit user approval.
In QA mode, flag any code that doesn't match DESIGN.md.

### "Do Not Touch" Zones
The benchmark IC print system (~lines 1690-1910 in style.css) and VCA EBITDA print system (~lines 3460-3660) have their own scoped design with `--bench-*` and `--vca-*` variables. Do NOT modify these during visual updates.

### Chart Colors
Chart.js colors are centralized in the `CHART_COLORS` object at the top of `static/js/app.js`. Template-level `FIRM_COLORS` arrays in `analysis_fund_comparison.html` and `analysis_deal_comparison.html` must stay in sync with the design system palette.
