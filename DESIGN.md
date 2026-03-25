# Design System — PE Portfolio Lab

## Product Context
- **What this is:** Deal-level analytics platform for private equity fund analysis
- **Who it's for:** PE investment professionals, LP due diligence teams, portfolio analysts
- **Space/industry:** Private equity analytics (peers: Addepar, Preqin, Carta, eFront)
- **Project type:** Web app (Flask + Jinja2, server-rendered)

## Aesthetic Direction
- **Direction:** Refined Institutional — a well-typeset private bank report that happens to be interactive
- **Decoration level:** Intentional — shadows for depth instead of borders. Flat backgrounds, no gradients
- **Mood:** Quiet confidence. Data as editorial content worth reading. The user should feel "this is unusually well-made"
- **Reference sites:** Addepar (clean, generous whitespace), Linear (border-less cards, subtle backgrounds)

## Typography
- **Display/Hero:** Instrument Serif (Regular 400) — serif headlines signal sophistication. Used ONLY for page titles and panel headings (h3)
- **Body/UI:** Instrument Sans (400, 500, 600, 700) — clean sans-serif for all UI text, labels, navigation
- **Data/Tables:** Geist Mono (400, 500) — refined monospace for financial figures, tabular-nums enabled
- **Loading:** Self-hosted WOFF2 from `/static/fonts/`. All use `font-display: swap`
- **Fallback stacks:**
  - Display: `'Instrument Serif', Georgia, 'Times New Roman', serif`
  - Body: `'Instrument Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`
  - Mono: `'Geist Mono', 'SF Mono', 'Cascadia Code', monospace`
- **Scale:**
  - display: 32-36px / Instrument Serif 400 / -0.02em tracking / 1.1 line-height (page titles)
  - h3: 18px / Instrument Serif 400 / -0.01em / 1.3 (panel headings)
  - lg: 16px / Instrument Sans 600 / 0 / 1.4 (section labels, nav active)
  - base: 14px / Instrument Sans 400 / 0 / 1.55 (body text)
  - sm: 13px / Instrument Sans 400 / 0 / 1.5 (secondary text, context values)
  - label: 12px / Instrument Sans 500 / 0.01em / 1.4 (form labels, metadata)
  - xs: 11px / Instrument Sans 500 / 0.02em / 1.4 (context strip labels, faint metadata)
  - mono: 13px / Geist Mono 400 / -0.01em / 1.4 (table data, inline numbers)
  - kpi: 28px / Geist Mono 400 / -0.02em / 1.0 (KPI card values)
  - kpi-hero: 48px / Geist Mono 400 / -0.03em / 1.0 (dashboard top-line metrics ONLY)
- **Weight rules:** Max weight is 700 (bold), used only for nav section labels and emphasis. 800 is banned. Labels use 500. Body uses 400.
- **Case rules:** Sentence case everywhere. No `text-transform: uppercase` except `.nav-section` sidebar labels. Benchmark IC and VCA EBITDA print systems are exempt (they have intentional uppercase for data density).

## Color
- **Approach:** Restrained — teal + gold on warm paper. Color is rare and meaningful.
- **Background:** `#FAF9F6` — warm off-white paper
- **Surface (cards):** `#FFFFFF` — pure white, elevated by shadow
- **Surface alt:** `#F5F3EF` — warm gray for table headers, alternating rows, secondary panels
- **Ink:** `#1A1A1A` — near-black primary text
- **Ink muted:** `#6B6B63` — labels, secondary text, metadata (5.10:1 contrast on #FAF9F6, passes WCAG AA)
- **Ink faint:** `#B5B3AE` — placeholder text, disabled states
- **Line:** `#EEECEA` — borders on tables, form inputs, dividers
- **Primary (accent):** `#0A6B58` — refined teal. CTAs, success, active states
- **Primary hover:** `#085A4A` — darker teal for interactive hover
- **Primary light:** `#E8F5F1` — teal tint for success alerts, badges
- **Secondary (accent-2):** `#C9982E` — warmed gold. Warnings, premium indicators
- **Secondary light:** `#FDF6E7` — gold tint for warning alerts
- **Tertiary (accent-3):** `#085A4A` — dark teal for emphasis
- **Danger:** `#B83C4A` — losses, errors, negative values
- **Danger light:** `#FDF0F1` — danger tint for error alerts
- **Info:** `#2563EB` — informational, links
- **Info light:** `#EFF6FF` — info tint for info alerts
- **Success:** `#0A6B58` — same as primary accent (teal = success in this system)
- **Sidebar bg:** `#111714` — near-black with green undertone
- **Sidebar text:** `#9AADA5` — muted sage on dark
- **Sidebar active:** `#FFFFFF` — white text for active nav
- **Sidebar active indicator:** `2px solid #0A6B58` — left border accent
- **Dark mode:** Future — not implemented. Aspirational tokens:
  - bg: `#0F1210`, surface: `#1A1E1C`, surface-alt: `#232826`, ink: `#E8E6E1`

## Shadows
- **sm:** `0 1px 2px rgba(0,0,0,0.04), 0 1px 3px rgba(0,0,0,0.06)` — cards, panels
- **md:** `0 2px 4px rgba(0,0,0,0.04), 0 4px 12px rgba(0,0,0,0.06)` — hover states, modals
- **lg:** `0 4px 8px rgba(0,0,0,0.04), 0 8px 24px rgba(0,0,0,0.08)` — dropdowns, popovers

## Spacing
- **Base unit:** 4px
- **Density:** Comfortable — generous enough for readability, dense enough for data
- **Scale:** 4 / 8 / 12 / 16 / 24 / 32 / 48 / 64 — no other values permitted
- **Density modes:**
  - Executive: panel padding 24px, hero KPIs at 48px, secondary content hidden
  - Analyst: panel padding 16px, KPIs at 28px, all content visible

## Layout
- **Approach:** Grid-disciplined — card-based, sidebar + main content
- **Sidebar:** 250px fixed left, near-black
- **Max content width:** no hard max — fills available space
- **Grid:** `auto-fit, minmax(220px, 1fr)` for KPI grids
- **Border radius:** sm: 4px (inputs), md: 8px (cards, buttons), lg: 12px (modals, hero cards), full: 999px (pills, badges)

## Border Rules
- **Decorative borders:** REMOVED from panels, cards, banners, filter panels. Replaced with shadows.
- **Structural borders KEPT:**
  - Table rows: `border-bottom: 1px solid var(--line)`
  - Form inputs/selects: `1px solid var(--line)`
  - Density toggle pill: `1px solid var(--line)`
  - Coverage badges/pills: `1px solid [semantic-color]`
  - Empty states: `1px dashed var(--line)` (intentional visual pattern)
- **Flash messages:** `border-left: 4px solid [semantic-color]` — left accent bar replaces full border
- **Active nav items:** `border-left: 2px solid var(--accent)` — sidebar indicator

## States
- **Loading:** Skeleton placeholder uses `background: var(--surface-alt)` with shimmer to `var(--line)`
- **Error:** Flash messages with left-border accent + light tint background
- **Empty:** Dashed border `1px dashed var(--line)` with muted text — intentional exception to border removal
- **Success:** Same as primary accent (teal)
- **Hover:** Cards get `box-shadow: var(--shadow-md)`. Buttons darken. Nav items get `rgba(255,255,255,0.08)` overlay.

## Motion
- **Approach:** Minimal-functional — no expressive animations
- **Easing:** enter: ease-out, exit: ease-in, move: ease-in-out
- **Duration:** micro: 100ms (hover), short: 150ms (button press), medium: 250ms (transitions)

## Context Strip
- **Position:** Static (not sticky) — demoted from primary to secondary reference
- **Background:** `var(--surface-alt)` — subtle warm gray, no shadow
- **Labels:** 11px / 400 / `var(--ink-faint)`
- **Values:** 13px / 500 / `var(--ink)`

## "Do Not Touch" Zones
These subsystems have their own scoped design and must NOT be modified:
1. **Benchmark IC print system** (style.css ~lines 1690-1910) — scoped `--bench-*` vars, blue-grey palette
2. **VCA EBITDA print system** (style.css ~lines 3460-3660) — scoped `--vca-*` vars
3. **@media print blocks** — preserve as-is, verify they still render correctly

## Decisions Log
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-03-25 | Initial design system created | Created by /design-consultation with competitive research (Addepar, Preqin, Linear) |
| 2026-03-25 | Refined teal #0A6B58 + gold #C9982E | Distinctive in PE space (competitors use navy/purple). Teal = institutional finance. |
| 2026-03-25 | Instrument Serif + Sans + Geist Mono | Serif for luxury differentiation, sans for UI clarity, mono for financial precision |
| 2026-03-25 | Self-hosted fonts | PE corporate firewalls block external CDNs. WOFF2 in /static/fonts/. |
| 2026-03-25 | Shadow-based elevation | Borders removed from panels/cards. Shadows create hierarchy without visual clutter. |
| 2026-03-25 | Context strip demoted to static | Sticky strip fights with page title for attention in "quiet luxury" direction |
| 2026-03-25 | Benchmark IC + VCA EBITDA exempt | These print subsystems have own scoped design. Don't break PDF output. |
