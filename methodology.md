# Methodology: Deal-Only Value Creation Analytics

## Scope
This platform computes portfolio analytics exclusively from deal-level investment and operating data.
No time-series contribution/distribution data is used.

## Return Construction
- Gross MOIC and value created are derived dynamically from realized/unrealized values and invested equity.
- Implied IRR is computed from MOIC and hold period when mathematically valid.

## Value Creation Models
One decomposition view is provided:
1. **Additive Sequential Decomposition**

The model supports:
- Fund-pro-rata and company-level basis
- Dollar, MOIC, and % display units
- Residual reconciliation to ensure totals match observed value created

## Ownership Policy
- Explicit ownership override is used when provided.
- Otherwise, ownership is derived from entry equity value.
- If entry equity is non-positive, a conservative fallback and warning are applied.

## Quality and Diagnostics
- Ownership-sensitivity diagnostics show +/-10% ownership impact on non-residual bridge components.

## Template Compatibility
- `Geography` and `Year Invested` are supported explicitly.
- If missing, geography defaults to `Unknown` and vintage derives from `Investment Date`.
