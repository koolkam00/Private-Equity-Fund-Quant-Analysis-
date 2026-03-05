let dashboardBridgePayload = null;
let dashboardBridgeChart = null;
let dashboardMoicChart = null;
let dashboardMoicHoldChart = null;
let dashboardValueMixChart = null;
let dashboardValueMixPayload = null;
let dashboardExposureChart = null;
let dashboardExitTypeChart = null;
let icMemoCharts = [];
let dealsRollupDetailsPayload = null;
let firmPickerState = null;

const DRIVER_LABELS = {
    revenue: 'Revenue Growth',
    ebitda_growth: 'EBITDA Growth',
    margin: 'Margin Expansion',
    multiple: 'Multiple Expansion',
    leverage: 'Leverage / Debt Paydown',
    other: 'Residual / Other',
};
const LEGACY_BRIDGE_DRIVER_KEYS = ['revenue', 'margin', 'multiple', 'leverage', 'other'];

const AXIS_GRID = 'rgba(20, 35, 33, 0.10)';
const AXIS_TICK = '#435854';
let currencyMetaCache = null;

function getCsrfToken() {
    const meta = document.querySelector('meta[name="csrf-token"]');
    return meta ? String(meta.content || '').trim() : '';
}

function jsonHeaders(extra = {}) {
    const headers = { ...extra };
    const csrfToken = getCsrfToken();
    if (csrfToken) headers['X-CSRFToken'] = csrfToken;
    return headers;
}

function getCurrencyMeta() {
    if (currencyMetaCache) return currencyMetaCache;
    const body = document.body;
    const code = ((body?.dataset?.currencyCode ?? 'USD') || 'USD').trim().toUpperCase();
    const rawSymbol = body?.dataset?.currencySymbol;
    const symbol = rawSymbol === undefined ? '$' : String(rawSymbol).trim();
    const rawUnit = body?.dataset?.currencyUnitLabel;
    const unitLabel = rawUnit === undefined ? `${code} ${symbol}M` : String(rawUnit).trim();
    currencyMetaCache = { code, symbol, unitLabel };
    return currencyMetaCache;
}

function formatCurrencyCore(v) {
    const amount = Number(v);
    if (!Number.isFinite(amount)) return null;
    const { symbol } = getCurrencyMeta();
    const sign = amount < 0 ? '-' : '';
    const absAmount = Math.abs(amount);
    if (symbol) return `${sign}${symbol}${absAmount.toFixed(1)}M`;
    return `${sign}${absAmount.toFixed(1)}M`;
}

function chartBaseOptions() {
    return {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 550, easing: 'easeOutQuart' },
        plugins: {
            legend: {
                labels: {
                    color: AXIS_TICK,
                    font: { family: 'Manrope', weight: '700', size: 11 },
                    boxWidth: 10,
                },
            },
            tooltip: {
                backgroundColor: '#16352f',
                titleColor: '#e9f7f3',
                bodyColor: '#e9f7f3',
                borderColor: 'rgba(233, 180, 76, 0.4)',
                borderWidth: 1,
                padding: 10,
            },
        },
        scales: {
            x: {
                grid: { display: false },
                ticks: { color: AXIS_TICK, font: { family: 'Manrope', size: 11, weight: '600' } },
            },
            y: {
                grid: { color: AXIS_GRID, borderDash: [4, 4] },
                ticks: { color: AXIS_TICK, font: { family: 'JetBrains Mono', size: 10 } },
            },
        },
    };
}

function toggleDetail(btn) {
    const row = btn.closest('tr');
    const dealId = row.dataset.dealId;
    const detailRow = document.getElementById('detail-' + dealId);
    const icon = btn.querySelector('i');
    const open = detailRow.style.display === 'table-row';

    detailRow.style.display = open ? 'none' : 'table-row';
    icon.classList.toggle('bi-chevron-right', open);
    icon.classList.toggle('bi-chevron-down', !open);

    if (!open) {
        loadDealBridge(dealId);
    }
}

function getDealBridgeControls(dealId) {
    const unitEl = document.querySelector(`.deal-bridge-unit[data-deal-id='${dealId}']`);
    return {
        unit: unitEl ? unitEl.value : 'moic',
        basis: 'fund',
    };
}

function getRollupBridgeControls(rowKey) {
    const unitEl = document.querySelector(`.rollup-bridge-unit[data-rollup-key='${rowKey}']`);
    return {
        unit: unitEl ? unitEl.value : 'moic',
        basis: 'fund',
    };
}

function getRollupDetailsPayload() {
    if (dealsRollupDetailsPayload) return dealsRollupDetailsPayload;
    dealsRollupDetailsPayload = getJsonScriptPayload('deals-rollup-details-payload') || {};
    return dealsRollupDetailsPayload;
}

function loadDealBridge(dealId) {
    const canvas = document.getElementById(`deal-waterfall-${dealId}`);
    if (!canvas) return;

    const controls = getDealBridgeControls(dealId);
    const url = `${canvas.dataset.url}?unit=${encodeURIComponent(controls.unit)}`;

    fetch(url)
        .then((r) => r.json())
        .then((payload) => renderDealBridge(canvas, payload, controls));
}

function loadRollupBridge(rowKey) {
    const detailsPayload = getRollupDetailsPayload();
    const rollupPayload = detailsPayload ? detailsPayload[rowKey] : null;
    const bridgePayload = rollupPayload ? rollupPayload.bridge : null;
    const canvas = document.getElementById(`rollup-waterfall-${rowKey}`);
    const diagnosticsEl = document.getElementById(`rollup-bridge-diagnostics-${rowKey}`);
    const tableBodyEl = document.getElementById(`rollup-bridge-table-${rowKey}`);
    if (!canvas || !bridgePayload) {
        if (diagnosticsEl) diagnosticsEl.textContent = 'ADDITIVE | FUND PRO-RATA';
        renderBridgeLeverTable(tableBodyEl, null, { isAggregate: true });
        return;
    }
    const controls = getRollupBridgeControls(rowKey);
    renderBridgeWaterfall(canvas, bridgePayload, controls, diagnosticsEl, {
        isAggregate: true,
        tableBodyEl,
    });
}

function renderDealBridge(canvas, payload, controls) {
    const diagnosticsEl = document.getElementById(`deal-bridge-diagnostics-${payload.deal_id}`);
    const tableBodyEl = document.getElementById(`deal-bridge-table-${payload.deal_id}`);
    renderBridgeWaterfall(canvas, payload, controls, diagnosticsEl, {
        isAggregate: false,
        tableBodyEl,
    });
}

function resolveBridgeSeries(payload, unit, isAggregate) {
    if (isAggregate) {
        const valuesMap = payload?.drivers?.[unit] || {};
        const startValue = payload?.start_end?.[unit]?.start;
        const endValue = payload?.start_end?.[unit]?.end;
        return { valuesMap, startValue, endValue };
    }
    const valuesMap = payload?.drivers || {};
    return {
        valuesMap,
        startValue: payload?.start_value,
        endValue: payload?.end_value,
    };
}

function renderBridgeWaterfall(canvas, payload, controls, diagnosticsEl, options = {}) {
    const isAggregate = Boolean(options.isAggregate);
    const { startValue, endValue } = resolveBridgeSeries(payload, controls.unit, isAggregate);
    const displayRows = getBridgeDisplayRows(payload, isAggregate);
    const leverLabels = displayRows.map((row) => row.label);
    const leverValues = displayRows.map((row) => row[controls.unit]);
    const wf = buildWaterfallSeries(controls.unit, leverLabels, leverValues, startValue, endValue);
    const colors = wf.kinds.map((kind, i) => {
        if (kind === 'start') return '#2f5d50';
        if (kind === 'end') return '#e9b44c';
        const range = wf.ranges[i];
        if (!Array.isArray(range)) return '#9aa8a4';
        return (range[1] - range[0]) >= 0 ? '#0e7c66' : '#b83c4a';
    });

    if (canvas._chart) {
        canvas._chart.destroy();
    }

    const baseOpts = chartBaseOptions();

    canvas._chart = new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: {
            labels: wf.labels,
            datasets: [{
                data: wf.ranges,
                backgroundColor: colors,
                borderRadius: 4,
                maxBarThickness: 42,
                borderSkipped: false,
            }],
        },
        options: {
            ...baseOpts,
            layout: {
                padding: {
                    top: 2,
                    right: 8,
                    bottom: 2,
                    left: 10,
                },
            },
            plugins: {
                ...baseOpts.plugins,
                legend: { display: false },
                tooltip: {
                    ...baseOpts.plugins.tooltip,
                    callbacks: {
                        label: (ctx) => {
                            const kind = wf.kinds[ctx.dataIndex];
                            const range = ctx.raw;
                            if (!Array.isArray(range)) return '—';
                            const delta = range[1] - range[0];
                            const cumulative = range[1];
                            if (kind === 'start') return `Start ${formatBridgeValue(cumulative, controls.unit)}`;
                            if (kind === 'end') return `End ${formatBridgeValue(cumulative, controls.unit)}`;
                            return `${formatBridgeValue(delta, controls.unit)} (to ${formatBridgeValue(cumulative, controls.unit)})`;
                        },
                    },
                },
            },
            scales: {
                ...baseOpts.scales,
                x: {
                    ...baseOpts.scales.x,
                    grid: { display: false },
                    ticks: {
                        ...baseOpts.scales.x.ticks,
                        color: '#234350',
                        font: { family: 'Manrope', size: 11, weight: '700' },
                        autoSkip: false,
                        maxRotation: 0,
                        minRotation: 0,
                        padding: 6,
                        callback(value) {
                            return shortBridgeAxisLabel(this.getLabelForValue(value));
                        },
                    },
                },
                y: {
                    ...baseOpts.scales.y,
                    grid: {
                        ...(baseOpts.scales.y.grid || {}),
                        color: 'rgba(20, 35, 33, 0.16)',
                        borderDash: [3, 3],
                    },
                    ticks: {
                        ...baseOpts.scales.y.ticks,
                        color: '#234350',
                        font: { family: 'JetBrains Mono', size: 12, weight: '700' },
                        padding: 8,
                        maxTicksLimit: 6,
                        callback: (v) => formatBridgeAxisTick(v, controls.unit),
                    },
                },
            },
        },
    });

    if (diagnosticsEl) {
        let txt = `ADDITIVE | FUND PRO-RATA | ${controls.unit.toUpperCase()}`;
        if (isAggregate) {
            const readyCount = payload?.ready_count;
            txt += ` | Ready deals: ${Number.isFinite(Number(readyCount)) ? Number(readyCount) : 0}`;
            const fallbackReadyCount = Number(payload?.fallback_ready_count);
            if (Number.isFinite(fallbackReadyCount) && fallbackReadyCount > 0) {
                txt += ` | Fallback deals: ${fallbackReadyCount}`;
            }
        } else {
            if (payload?.calculation_method === 'revenue_multiple_fallback') {
                txt += ' | FALLBACK: REVENUE-MULTIPLE METHOD (NEG EBITDA)';
            } else if (payload?.calculation_method === 'ebitda_multiple_fallback') {
                txt += ' | FALLBACK: EBITDA-MULTIPLE METHOD (MISSING REVENUE)';
            }
            if (payload.ownership_pct !== null && payload.ownership_pct !== undefined) {
                txt += ` | Ownership ${(payload.ownership_pct * 100).toFixed(1)}%`;
            }
        }
        diagnosticsEl.textContent = txt;
    }

    renderBridgeLeverTable(options.tableBodyEl, payload, { isAggregate });
}

function shortBridgeAxisLabel(label) {
    const raw = String(label || '');
    if (raw.startsWith('Start:')) return 'Start';
    if (raw.startsWith('End:')) return 'End';
    if (raw === 'Revenue Growth') return ['Revenue', 'Growth'];
    if (raw === 'EBITDA Growth') return ['EBITDA', 'Growth'];
    if (raw === 'Margin Expansion') return ['Margin', 'Expansion'];
    if (raw === 'Multiple Expansion') return ['Multiple', 'Expansion'];
    if (raw === 'Leverage / Debt Paydown') return ['Leverage', 'Debt Paydown'];
    if (raw === 'Residual / Other') return ['Residual', 'Other'];
    return raw;
}

function toggleRollupDetail(btn) {
    const rowKey = btn.dataset.rollupKey;
    if (!rowKey) return;

    const detailRow = document.getElementById(`rollup-detail-${rowKey}`);
    if (!detailRow) return;

    const icon = btn.querySelector('i');
    const open = detailRow.style.display === 'table-row';

    detailRow.style.display = open ? 'none' : 'table-row';
    if (icon) {
        icon.classList.toggle('bi-chevron-right', open);
        icon.classList.toggle('bi-chevron-down', !open);
    }

    if (!open) {
        loadRollupBridge(rowKey);
    }
}

function formatBridgeValue(v, unit) {
    if (v === null || v === undefined || Number.isNaN(v)) return '—';
    if (unit === 'dollar') {
        const { code } = getCurrencyMeta();
        const core = formatCurrencyCore(v);
        return core ? `${code} ${core}` : '—';
    }
    if (unit === 'moic') return `${v >= 0 ? '+' : ''}${v.toFixed(2)}x`;
    if (unit === 'pct') return `${v >= 0 ? '+' : ''}${(v * 100).toFixed(1)}%`;
    return String(v);
}

function formatBridgeAxisTick(v, unit) {
    if (v === null || v === undefined || Number.isNaN(v)) return '—';
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    if (unit === 'dollar') {
        const { code, symbol } = getCurrencyMeta();
        const sign = n < 0 ? '-' : '';
        const absAmount = Math.abs(Math.round(n));
        const core = symbol ? `${sign}${symbol}${absAmount}M` : `${sign}${absAmount}M`;
        return `${code} ${core}`;
    }
    if (unit === 'moic') return `${n.toFixed(1)}x`;
    if (unit === 'pct') return `${Math.round(n * 100)}%`;
    return String(n);
}

function formatCurrencyMillions(v) {
    if (v === null || v === undefined || Number.isNaN(v)) return '—';
    const { code } = getCurrencyMeta();
    const core = formatCurrencyCore(v);
    return core ? `${code} ${core}` : '—';
}

function formatPct(v) {
    if (v === null || v === undefined || Number.isNaN(v)) return '—';
    return `${(Number(v) * 100).toFixed(1)}%`;
}

function formatPercentPoints(v) {
    if (v === null || v === undefined || Number.isNaN(v)) return '—';
    return `${Number(v).toFixed(1)}%`;
}

function toOptionalNumber(value) {
    if (value === null || value === undefined || value === '') return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function safeBridgeRatio(numerator, denominator) {
    const n = toOptionalNumber(numerator);
    const d = toOptionalNumber(denominator);
    if (n === null || d === null || Math.abs(d) < 1e-9) return null;
    return n / d;
}

function normalizeBridgeTriples(payload, isAggregate) {
    const dollarSource = isAggregate ? (payload?.drivers?.dollar || {}) : (payload?.drivers_dollar || {});
    const dollar = {};
    const moic = {};
    const pct = {};

    if (isAggregate) {
        const moicSource = payload?.drivers?.moic || {};
        const pctSource = payload?.drivers?.pct || {};
        LEGACY_BRIDGE_DRIVER_KEYS.forEach((key) => {
            dollar[key] = toOptionalNumber(dollarSource[key]);
            moic[key] = toOptionalNumber(moicSource[key]);
            pct[key] = toOptionalNumber(pctSource[key]);
        });
        return { dollar, moic, pct };
    }

    const equity = toOptionalNumber(payload?.equity_invested);
    const valueCreated = toOptionalNumber(payload?.value_created);
    LEGACY_BRIDGE_DRIVER_KEYS.forEach((key) => {
        dollar[key] = toOptionalNumber(dollarSource[key]);
        moic[key] = safeBridgeRatio(dollar[key], equity);
        pct[key] = safeBridgeRatio(dollar[key], valueCreated);
    });
    return { dollar, moic, pct };
}

function getBridgeDisplayRows(payload, isAggregate) {
    if (Array.isArray(payload?.display_drivers) && payload.display_drivers.length) {
        return payload.display_drivers.map((row) => {
            const key = String(row?.key || '').trim() || 'other';
            return {
                key,
                label: row?.label || DRIVER_LABELS[key] || key,
                dollar: toOptionalNumber(row?.dollar),
                moic: toOptionalNumber(row?.moic),
                pct: toOptionalNumber(row?.pct),
            };
        });
    }

    const triples = normalizeBridgeTriples(payload, isAggregate);
    return LEGACY_BRIDGE_DRIVER_KEYS.map((key) => ({
        key,
        label: DRIVER_LABELS[key] || key,
        dollar: triples.dollar[key],
        moic: triples.moic[key],
        pct: triples.pct[key],
    }));
}

function renderBridgeLeverTable(tableBodyEl, payload, options = {}) {
    if (!tableBodyEl) return;
    const isAggregate = Boolean(options.isAggregate);

    if (!payload) {
        tableBodyEl.innerHTML = '<tr><td colspan="4" class="num">No bridge data available.</td></tr>';
        return;
    }

    const displayRows = getBridgeDisplayRows(payload, isAggregate);
    tableBodyEl.innerHTML = '';

    displayRows.forEach((row) => {
        const tr = document.createElement('tr');
        const label = document.createElement('td');
        label.textContent = row.label;
        tr.appendChild(label);

        const dollar = document.createElement('td');
        dollar.className = 'num';
        dollar.textContent = formatBridgeValue(row.dollar, 'dollar');
        tr.appendChild(dollar);

        const moic = document.createElement('td');
        moic.className = 'num';
        moic.textContent = formatBridgeValue(row.moic, 'moic');
        tr.appendChild(moic);

        const pct = document.createElement('td');
        pct.className = 'num';
        pct.textContent = formatBridgeValue(row.pct, 'pct');
        tr.appendChild(pct);

        tableBodyEl.appendChild(tr);
    });
}

function bridgeStartLabel(unit) {
    if (unit === 'dollar') return 'Start: Equity Invested';
    if (unit === 'moic') return 'Start: 1.00x';
    return 'Start: 0%';
}

function bridgeEndLabel(unit) {
    if (unit === 'dollar') return 'End: Total Value';
    if (unit === 'moic') return 'End: Total MOIC';
    return 'End: 100%';
}

function toFiniteNumber(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function buildWaterfallSeries(unit, leverLabels, leverValues, startValue, endValue) {
    const labels = [bridgeStartLabel(unit), ...leverLabels, bridgeEndLabel(unit)];
    const ranges = [];
    const kinds = [];

    const start = toFiniteNumber(startValue);
    if (start === null) {
        return {
            labels,
            ranges: labels.map(() => null),
            kinds: ['start', ...leverLabels.map(() => 'lever'), 'end'],
        };
    }

    ranges.push([0, start]);
    kinds.push('start');

    let running = start;
    for (const rawDelta of leverValues) {
        const delta = toFiniteNumber(rawDelta) ?? 0;
        const next = running + delta;
        ranges.push([running, next]);
        kinds.push('lever');
        running = next;
    }

    const end = toFiniteNumber(endValue) ?? running;
    ranges.push([0, end]);
    kinds.push('end');

    return { labels, ranges, kinds };
}

function attachTableSorting() {
    document.querySelectorAll('.sortable th[data-sort]').forEach((header) => {
        header.addEventListener('click', () => {
            const table = header.closest('table');
            const tbody = table.querySelector('tbody');
            const colIndex = Array.from(header.parentNode.children).indexOf(header);
            const sortType = header.dataset.sort;
            const dir = header.dataset.dir === 'asc' ? 'desc' : 'asc';
            header.dataset.dir = dir;

            const rows = Array.from(tbody.querySelectorAll('tr.deal-row'));
            rows.sort((a, b) => {
                const av = a.children[colIndex].textContent.trim();
                const bv = b.children[colIndex].textContent.trim();
                if (sortType === 'number') {
                    const an = parseFloat(av.replace(/[^0-9.-]/g, '')) || 0;
                    const bn = parseFloat(bv.replace(/[^0-9.-]/g, '')) || 0;
                    return dir === 'asc' ? an - bn : bn - an;
                }
                return dir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av);
            });

            rows.forEach((row) => {
                tbody.appendChild(row);
                const detail = document.getElementById(`detail-${row.dataset.dealId}`);
                if (detail) tbody.appendChild(detail);
            });
        });
    });
}

function attachDropZone() {
    document.querySelectorAll('.drop-zone').forEach((zone) => {
        const input = zone.querySelector('.drop-input');
        const name = zone.querySelector('.file-name');
        if (!input || !name) return;
        input.addEventListener('change', () => {
            if (input.files.length > 0) name.textContent = input.files[0].name;
        });
    });
}

function attachFlashDismiss() {
    document.querySelectorAll('.flash').forEach((el) => {
        if (el.classList.contains('flash-danger')) return;
        setTimeout(() => el.remove(), 10000);
    });
}

function attachDealBridgeControls() {
    document.querySelectorAll('.deal-bridge-unit').forEach((el) => {
        el.addEventListener('change', (evt) => {
            const dealId = evt.target.dataset.dealId;
            loadDealBridge(dealId);
        });
    });
}

function attachRollupBridgeControls() {
    document.querySelectorAll('.rollup-bridge-unit').forEach((el) => {
        if (el.dataset.bound === '1') return;
        el.dataset.bound = '1';
        el.addEventListener('change', (evt) => {
            const rowKey = evt.target.dataset.rollupKey;
            if (rowKey) loadRollupBridge(rowKey);
        });
    });
}

function renderDashboardCharts(payload) {
    dashboardBridgePayload = payload.bridge_aggregate || null;
    dashboardValueMixPayload = payload.value_creation_mix || {};

    renderMoicDistribution(payload.moic_distribution || []);
    renderMoicHoldScatter(payload.moic_hold_scatter || []);
    renderValueCreationMix();
    renderRealizedUnrealizedExposure(payload.realized_unrealized_exposure || {});
    renderExitTypePerformance(payload.exit_type_performance || {});
    renderLossHeatmap(payload.loss_concentration_heatmap || {});
    renderLeadPartnerScorecard(payload.lead_partner_scorecard || []);
    renderBridgeAggregate();
}

function renderMoicDistribution(data) {
    const el = document.getElementById('moicDistChart');
    if (!el) return;
    if (dashboardMoicChart) dashboardMoicChart.destroy();

    dashboardMoicChart = new Chart(el, {
        type: 'bar',
        data: {
            labels: data.map((d) => d.label),
            datasets: [{ data: data.map((d) => d.count), backgroundColor: '#0e7c66', borderRadius: 5, maxBarThickness: 48 }],
        },
        options: {
            ...chartBaseOptions(),
            plugins: { ...chartBaseOptions().plugins, legend: { display: false } },
        },
    });
}

function renderMoicHoldScatter(data) {
    const el = document.getElementById('moicHoldScatterChart');
    if (!el) return;
    if (dashboardMoicHoldChart) dashboardMoicHoldChart.destroy();

    const statusMeta = {
        'Fully Realized': {
            color: 'rgba(14, 124, 102, 0.40)',
            border: '#0e7c66',
        },
        'Partially Realized': {
            color: 'rgba(233, 180, 76, 0.40)',
            border: '#c78c17',
        },
        Unrealized: {
            color: 'rgba(47, 93, 80, 0.42)',
            border: '#2f5d50',
        },
        Other: {
            color: 'rgba(120, 130, 142, 0.35)',
            border: '#6f7a86',
        },
    };

    const normalizeStatus = (raw) => {
        const s = String(raw || '').trim().toLowerCase();
        if (!s) return 'Unrealized';
        if (s.includes('partial') && s.includes('realized')) return 'Partially Realized';
        if (s.includes('fully') && s.includes('realized')) return 'Fully Realized';
        if (s === 'realized' || (s.includes('realized') && !s.includes('unrealized'))) return 'Fully Realized';
        if (s.includes('unrealized')) return 'Unrealized';
        return 'Other';
    };

    const statusOrder = ['Fully Realized', 'Partially Realized', 'Unrealized', 'Other'];
    const pointsByStatus = { 'Fully Realized': [], 'Partially Realized': [], Unrealized: [], Other: [] };
    data.forEach((d) => {
        const status = normalizeStatus(d.status);
        pointsByStatus[status].push({
            x: d.x,
            y: d.y,
            r: d.r,
            company: d.company,
            status,
            equity: d.equity,
        });
    });

    const datasets = statusOrder
        .filter((status) => pointsByStatus[status].length > 0)
        .map((status) => ({
            label: status,
            data: pointsByStatus[status],
            borderColor: statusMeta[status].border,
            backgroundColor: statusMeta[status].color,
            borderWidth: 1.2,
        }));

    dashboardMoicHoldChart = new Chart(el, {
        type: 'bubble',
        data: {
            datasets,
        },
        options: {
            ...chartBaseOptions(),
            plugins: {
                ...chartBaseOptions().plugins,
                legend: {
                    display: true,
                    position: 'bottom',
                    labels: {
                        ...chartBaseOptions().plugins.legend.labels,
                        usePointStyle: true,
                        pointStyle: 'circle',
                    },
                },
                tooltip: {
                    ...chartBaseOptions().plugins.tooltip,
                    callbacks: {
                        title: (items) => items?.[0]?.raw?.company || 'Deal',
                        label: (ctx) => {
                            const p = ctx.raw || {};
                            return [
                                `MOIC ${formatBridgeValue(p.y, 'moic')}`,
                                `Hold ${Number(p.x || 0).toFixed(1)} yrs`,
                                `Equity ${formatCurrencyMillions(p.equity)}`,
                                `Status ${p.status || 'Unknown'}`,
                            ];
                        },
                    },
                },
            },
            scales: {
                ...chartBaseOptions().scales,
                x: {
                    ...chartBaseOptions().scales.x,
                    title: { display: true, text: 'Hold Period (Years)', color: AXIS_TICK, font: { family: 'Manrope', size: 11, weight: '700' } },
                },
                y: {
                    ...chartBaseOptions().scales.y,
                    title: { display: true, text: 'Gross MOIC', color: AXIS_TICK, font: { family: 'Manrope', size: 11, weight: '700' } },
                    ticks: {
                        ...chartBaseOptions().scales.y.ticks,
                        callback: (v) => formatBridgeValue(Number(v), 'moic'),
                    },
                },
            },
        },
    });
}

function renderValueCreationMix(payload) {
    const el = document.getElementById('valueCreationMixChart');
    if (!el) return;
    if (dashboardValueMixChart) dashboardValueMixChart.destroy();

    if (payload) {
        dashboardValueMixPayload = payload;
    }

    const rawPayload = dashboardValueMixPayload || {};
    const groupControl = document.getElementById('valueCreationMixGroup');
    const titleEl = document.getElementById('valueCreationMixTitle');
    const titleByGroup = {
        fund: 'Value Creation Mix by Fund (100%)',
        sector: 'Value Creation Mix by Sector (100%)',
        exit_type: 'Value Creation Mix by Exit Type (100%)',
    };

    let group = 'fund';
    let series = rawPayload;
    if (rawPayload && rawPayload.series) {
        const selected = groupControl ? groupControl.value : null;
        group = selected || rawPayload.current || 'fund';
        if (!rawPayload.series[group]) {
            group = 'fund';
        }
        series = rawPayload.series[group] || { labels: [], drivers: {}, totals_dollar: [] };
    } else if (groupControl) {
        groupControl.value = 'fund';
    }

    if (groupControl && groupControl.value !== group) {
        groupControl.value = group;
    }
    if (titleEl) {
        titleEl.textContent = titleByGroup[group] || titleByGroup.fund;
    }

    const labels = series.labels || [];
    const drivers = series.drivers || {};
    const palette = {
        revenue: '#0e7c66',
        margin: '#2f5d50',
        multiple: '#e9b44c',
        leverage: '#5e8f80',
        other: '#b83c4a',
    };
    const keys = ['revenue', 'margin', 'multiple', 'leverage', 'other'];

    dashboardValueMixChart = new Chart(el, {
        type: 'bar',
        data: {
            labels,
            datasets: keys.map((k) => ({
                label: DRIVER_LABELS[k],
                data: labels.map((_, i) => (drivers[k] || [])[i]),
                backgroundColor: palette[k],
                stack: 'mix',
                borderRadius: 3,
                maxBarThickness: 36,
            })),
        },
        options: {
            ...chartBaseOptions(),
            scales: {
                ...chartBaseOptions().scales,
                x: { ...chartBaseOptions().scales.x, stacked: true },
                y: {
                    ...chartBaseOptions().scales.y,
                    stacked: true,
                    ticks: {
                        ...chartBaseOptions().scales.y.ticks,
                        callback: (v) => formatPct(v),
                    },
                },
            },
            plugins: {
                ...chartBaseOptions().plugins,
                tooltip: {
                    ...chartBaseOptions().plugins.tooltip,
                    callbacks: {
                        label: (ctx) => `${ctx.dataset.label}: ${formatPct(ctx.raw)}`,
                    },
                },
            },
        },
    });
}

function attachValueCreationMixControls() {
    const group = document.getElementById('valueCreationMixGroup');
    if (!group || group.dataset.bound === '1') return;
    group.dataset.bound = '1';
    group.addEventListener('change', () => renderValueCreationMix());
}

function renderRealizedUnrealizedExposure(payload) {
    const el = document.getElementById('realizedUnrealizedChart');
    if (!el) return;
    if (dashboardExposureChart) dashboardExposureChart.destroy();

    const labels = payload.labels || [];
    const realized = payload.realized || [];
    const unrealized = payload.unrealized || [];

    dashboardExposureChart = new Chart(el, {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'Realized Value',
                    data: realized,
                    backgroundColor: '#0e7c66',
                    stack: 'exposure',
                    borderRadius: 4,
                    maxBarThickness: 40,
                },
                {
                    label: 'Unrealized Value',
                    data: unrealized,
                    backgroundColor: '#e9b44c',
                    stack: 'exposure',
                    borderRadius: 4,
                    maxBarThickness: 40,
                },
            ],
        },
        options: {
            ...chartBaseOptions(),
            scales: {
                ...chartBaseOptions().scales,
                x: { ...chartBaseOptions().scales.x, stacked: true },
                y: {
                    ...chartBaseOptions().scales.y,
                    stacked: true,
                    ticks: {
                        ...chartBaseOptions().scales.y.ticks,
                        callback: (v) => formatCurrencyMillions(v),
                    },
                },
            },
            plugins: {
                ...chartBaseOptions().plugins,
                tooltip: {
                    ...chartBaseOptions().plugins.tooltip,
                    callbacks: {
                        label: (ctx) => `${ctx.dataset.label}: ${formatCurrencyMillions(ctx.raw)}`,
                    },
                },
            },
        },
    });
}

function renderExitTypePerformance(payload) {
    const el = document.getElementById('exitTypePerformanceChart');
    if (!el) return;
    if (dashboardExitTypeChart) dashboardExitTypeChart.destroy();

    const labels = payload.labels || [];
    const calculatedMoic = payload.calculated_moic || [];
    const dealCount = payload.deal_count || [];

    dashboardExitTypeChart = new Chart(el, {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'Calculated MOIC',
                    data: calculatedMoic,
                    backgroundColor: '#0e7c66',
                    yAxisID: 'y',
                    borderRadius: 4,
                    maxBarThickness: 36,
                },
            ],
        },
        options: {
            ...chartBaseOptions(),
            scales: {
                ...chartBaseOptions().scales,
                y: {
                    ...chartBaseOptions().scales.y,
                    ticks: {
                        ...chartBaseOptions().scales.y.ticks,
                        callback: (v) => formatBridgeValue(Number(v), 'moic'),
                    },
                },
            },
            plugins: {
                ...chartBaseOptions().plugins,
                tooltip: {
                    ...chartBaseOptions().plugins.tooltip,
                    callbacks: {
                        label: (ctx) => `${ctx.dataset.label}: ${formatBridgeValue(ctx.raw, 'moic')}`,
                        afterLabel: (ctx) => {
                            const n = Number(dealCount[ctx.dataIndex] || 0);
                            return `Deals: ${n}`;
                        },
                    },
                },
            },
        },
    });
}

function renderLossHeatmap(payload) {
    const host = document.getElementById('lossHeatmap');
    if (!host) return;

    const sectors = payload.sectors || [];
    const geographies = payload.geographies || [];
    const values = payload.values || [];
    const maxVal = payload.max_value || 0;

    host.innerHTML = '';
    if (!sectors.length || !geographies.length) {
        const p = document.createElement('p');
        p.className = 'tiny';
        p.textContent = 'No loss concentration in current filter set.';
        host.appendChild(p);
        return;
    }

    const table = document.createElement('table');
    table.className = 'heatmap-table';

    const thead = document.createElement('thead');
    const trHead = document.createElement('tr');
    const th0 = document.createElement('th');
    th0.textContent = 'Sector \\ Geography';
    trHead.appendChild(th0);
    geographies.forEach((g) => {
        const th = document.createElement('th');
        th.textContent = g;
        trHead.appendChild(th);
    });
    thead.appendChild(trHead);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    sectors.forEach((sector, i) => {
        const tr = document.createElement('tr');
        const label = document.createElement('th');
        label.textContent = sector;
        tr.appendChild(label);
        geographies.forEach((_, j) => {
            const td = document.createElement('td');
            const v = Number(values[i]?.[j] || 0);
            td.textContent = formatCurrencyMillions(v);
            td.className = 'heatmap-cell';
            const intensity = maxVal > 0 ? Math.min(1, v / maxVal) : 0;
            td.style.background = `rgba(184, 60, 74, ${0.08 + (0.62 * intensity)})`;
            td.style.color = intensity > 0.55 ? '#fff' : '#4a1f25';
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    host.appendChild(table);
}

function renderLeadPartnerScorecard(rows) {
    const body = document.getElementById('leadPartnerScorecardBody');
    if (!body) return;
    body.innerHTML = '';

    if (!rows || !rows.length) {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = 6;
        td.className = 'num';
        td.textContent = 'No lead partner data in current filter set.';
        tr.appendChild(td);
        body.appendChild(tr);
        return;
    }

    rows.forEach((r) => {
        const tr = document.createElement('tr');
        const vals = [
            r.lead_partner || 'Unassigned',
            String(r.deal_count ?? 0),
            formatCurrencyMillions(r.capital_deployed),
            formatBridgeValue(r.weighted_moic, 'moic'),
            formatPct(r.hit_rate),
            formatPct(r.loss_ratio),
        ];

        vals.forEach((v, idx) => {
            const td = document.createElement('td');
            td.textContent = v;
            if (idx > 0) td.className = 'num';
            tr.appendChild(td);
        });
        body.appendChild(tr);
    });
}

function renderBridgeAggregate() {
    if (!dashboardBridgePayload) return;

    const unit = document.getElementById('bridgeUnit')?.value || 'moic';
    const displayRows = getBridgeDisplayRows(dashboardBridgePayload, true);
    const startEnd = ((dashboardBridgePayload.start_end || {})[unit]) || {};

    const leverLabels = displayRows.map((row) => row.label);
    const leverValues = displayRows.map((row) => row[unit]);
    const wf = buildWaterfallSeries(unit, leverLabels, leverValues, startEnd.start, startEnd.end);
    const colors = wf.kinds.map((kind, i) => {
        if (kind === 'start') return '#2f5d50';
        if (kind === 'end') return '#e9b44c';
        const range = wf.ranges[i];
        if (!Array.isArray(range)) return '#9aa8a4';
        return (range[1] - range[0]) >= 0 ? '#0e7c66' : '#b83c4a';
    });

    const el = document.getElementById('bridgeChart');
    if (!el) return;
    if (dashboardBridgeChart) dashboardBridgeChart.destroy();

    dashboardBridgeChart = new Chart(el, {
        type: 'bar',
        data: {
            labels: wf.labels,
            datasets: [{ data: wf.ranges, backgroundColor: colors, borderRadius: 5, maxBarThickness: 48, borderSkipped: false }],
        },
        options: {
            ...chartBaseOptions(),
            plugins: {
                ...chartBaseOptions().plugins,
                legend: { display: false },
                tooltip: {
                    ...chartBaseOptions().plugins.tooltip,
                    callbacks: {
                        label: (ctx) => {
                            const kind = wf.kinds[ctx.dataIndex];
                            const range = ctx.raw;
                            if (!Array.isArray(range)) return '—';
                            const delta = range[1] - range[0];
                            const cumulative = range[1];
                            if (kind === 'start') return `Start ${formatBridgeValue(cumulative, unit)}`;
                            if (kind === 'end') return `End ${formatBridgeValue(cumulative, unit)}`;
                            return `${formatBridgeValue(delta, unit)} (to ${formatBridgeValue(cumulative, unit)})`;
                        },
                    },
                },
            },
            scales: {
                ...chartBaseOptions().scales,
                y: {
                    ...chartBaseOptions().scales.y,
                    ticks: {
                        ...chartBaseOptions().scales.y.ticks,
                        callback: (v) => formatBridgeValue(v, unit),
                    },
                },
            },
        },
    });

    const diag = document.getElementById('bridgeDiagnostics');
    if (diag) {
        diag.textContent = `ADDITIVE model | Ready deals: ${dashboardBridgePayload.ready_count || 0}`;
    }
    renderBridgeLeverTable(document.getElementById('bridge-lever-table-body'), dashboardBridgePayload, {
        isAggregate: true,
    });
}

function attachDashboardBridgeControls() {
    const unit = document.getElementById('bridgeUnit');
    if (unit) unit.addEventListener('change', renderBridgeAggregate);
}

function attachDashboardFilterAutoApply() {
    const form = document.querySelector('.filter-form');
    if (!form) return;
    form.querySelectorAll('select').forEach((el) => {
        el.addEventListener('change', () => form.submit());
    });
}

function getJsonScriptPayload(id) {
    const el = document.getElementById(id);
    if (!el) return null;
    try {
        return JSON.parse(el.textContent || '{}');
    } catch (err) {
        return null;
    }
}

function registerIcMemoChart(chart) {
    if (chart) icMemoCharts.push(chart);
}

function renderIcMemoBridgeChart(payload) {
    const el = document.getElementById('icBridgeChart');
    if (!el) return;

    const displayRows = getBridgeDisplayRows(payload.bridge || {}, true);
    const startEnd = payload.bridge?.start_end?.moic || {};
    const labels = displayRows.map((row) => row.label);
    const values = displayRows.map((row) => row.moic);
    const wf = buildWaterfallSeries('moic', labels, values, startEnd.start, startEnd.end);
    const colors = wf.kinds.map((kind, i) => {
        if (kind === 'start') return '#2f5d50';
        if (kind === 'end') return '#e9b44c';
        const range = wf.ranges[i];
        if (!Array.isArray(range)) return '#9aa8a4';
        return (range[1] - range[0]) >= 0 ? '#0e7c66' : '#b83c4a';
    });

    const chart = new Chart(el, {
        type: 'bar',
        data: {
            labels: wf.labels,
            datasets: [{ data: wf.ranges, backgroundColor: colors, borderRadius: 5, maxBarThickness: 42, borderSkipped: false }],
        },
        options: {
            ...chartBaseOptions(),
            plugins: {
                ...chartBaseOptions().plugins,
                legend: { display: false },
                tooltip: {
                    ...chartBaseOptions().plugins.tooltip,
                    callbacks: {
                        label: (ctx) => {
                            const kind = wf.kinds[ctx.dataIndex];
                            const range = ctx.raw;
                            if (!Array.isArray(range)) return '—';
                            const delta = range[1] - range[0];
                            const cumulative = range[1];
                            if (kind === 'start') return `Start ${formatBridgeValue(cumulative, 'moic')}`;
                            if (kind === 'end') return `End ${formatBridgeValue(cumulative, 'moic')}`;
                            return `${formatBridgeValue(delta, 'moic')} (to ${formatBridgeValue(cumulative, 'moic')})`;
                        },
                    },
                },
            },
            scales: {
                ...chartBaseOptions().scales,
                y: {
                    ...chartBaseOptions().scales.y,
                    ticks: {
                        ...chartBaseOptions().scales.y.ticks,
                        callback: (v) => formatBridgeValue(v, 'moic'),
                    },
                },
            },
        },
    });
    registerIcMemoChart(chart);
}

function renderIcMemoLeverageCharts(payload) {
    const lev = payload.risk?.leverage_entry_exit || {};
    const ndEbitda = lev.net_debt_ebitda || {};
    const ndTev = lev.net_debt_tev || {};

    const ebitdaEl = document.getElementById('icLeverageEbitdaChart');
    if (ebitdaEl) {
        const chart = new Chart(ebitdaEl, {
            type: 'bar',
            data: {
                labels: ['Average', 'Weighted'],
                datasets: [
                    {
                        label: 'Entry',
                        data: [ndEbitda.entry_avg, ndEbitda.entry_wtd],
                        backgroundColor: '#2f5d50',
                        borderRadius: 4,
                    },
                    {
                        label: 'Exit',
                        data: [ndEbitda.exit_avg, ndEbitda.exit_wtd],
                        backgroundColor: '#0e7c66',
                        borderRadius: 4,
                    },
                ],
            },
            options: {
                ...chartBaseOptions(),
                scales: {
                    ...chartBaseOptions().scales,
                    y: {
                        ...chartBaseOptions().scales.y,
                        ticks: {
                            ...chartBaseOptions().scales.y.ticks,
                            callback: (v) => formatBridgeValue(Number(v), 'moic'),
                        },
                    },
                },
            },
        });
        registerIcMemoChart(chart);
    }

    const tevEl = document.getElementById('icLeverageTevChart');
    if (tevEl) {
        const chart = new Chart(tevEl, {
            type: 'bar',
            data: {
                labels: ['Average', 'Weighted'],
                datasets: [
                    {
                        label: 'Entry',
                        data: [ndTev.entry_avg, ndTev.entry_wtd],
                        backgroundColor: '#e9b44c',
                        borderRadius: 4,
                    },
                    {
                        label: 'Exit',
                        data: [ndTev.exit_avg, ndTev.exit_wtd],
                        backgroundColor: '#b83c4a',
                        borderRadius: 4,
                    },
                ],
            },
            options: {
                ...chartBaseOptions(),
                scales: {
                    ...chartBaseOptions().scales,
                    y: {
                        ...chartBaseOptions().scales.y,
                        ticks: {
                            ...chartBaseOptions().scales.y.ticks,
                            callback: (v) => formatPct(Number(v)),
                        },
                    },
                },
                plugins: {
                    ...chartBaseOptions().plugins,
                    tooltip: {
                        ...chartBaseOptions().plugins.tooltip,
                        callbacks: {
                            label: (ctx) => `${ctx.dataset.label}: ${formatPct(ctx.raw)}`,
                        },
                    },
                },
            },
        });
        registerIcMemoChart(chart);
    }
}

function renderIcMemoOperatingCharts(payload) {
    const operating = payload.operating || {};
    const multiples = operating.multiples || {};
    const margin = operating.margin || {};
    const growth = operating.growth || {};

    const multiplesEl = document.getElementById('icOperatingMultiplesChart');
    if (multiplesEl) {
        const chart = new Chart(multiplesEl, {
            type: 'bar',
            data: {
                labels: ['TEV / EBITDA', 'TEV / Revenue'],
                datasets: [
                    {
                        label: 'Entry Avg',
                        data: [multiples.tev_ebitda?.entry_avg, multiples.tev_revenue?.entry_avg],
                        backgroundColor: '#2f5d50',
                        borderRadius: 4,
                    },
                    {
                        label: 'Exit Avg',
                        data: [multiples.tev_ebitda?.exit_avg, multiples.tev_revenue?.exit_avg],
                        backgroundColor: '#0e7c66',
                        borderRadius: 4,
                    },
                    {
                        label: 'Entry Wtd',
                        data: [multiples.tev_ebitda?.entry_wtd, multiples.tev_revenue?.entry_wtd],
                        backgroundColor: '#5e8f80',
                        borderRadius: 4,
                    },
                    {
                        label: 'Exit Wtd',
                        data: [multiples.tev_ebitda?.exit_wtd, multiples.tev_revenue?.exit_wtd],
                        backgroundColor: '#e9b44c',
                        borderRadius: 4,
                    },
                ],
            },
            options: {
                ...chartBaseOptions(),
                scales: {
                    ...chartBaseOptions().scales,
                    y: {
                        ...chartBaseOptions().scales.y,
                        ticks: {
                            ...chartBaseOptions().scales.y.ticks,
                            callback: (v) => formatBridgeValue(Number(v), 'moic'),
                        },
                    },
                },
            },
        });
        registerIcMemoChart(chart);
    }

    const marginEl = document.getElementById('icOperatingMarginChart');
    if (marginEl) {
        const m = margin.ebitda_margin || {};
        const chart = new Chart(marginEl, {
            type: 'bar',
            data: {
                labels: ['Average', 'Weighted'],
                datasets: [
                    {
                        label: 'Entry',
                        data: [m.entry_avg, m.entry_wtd],
                        backgroundColor: '#2f5d50',
                        borderRadius: 4,
                    },
                    {
                        label: 'Exit',
                        data: [m.exit_avg, m.exit_wtd],
                        backgroundColor: '#0e7c66',
                        borderRadius: 4,
                    },
                ],
            },
            options: {
                ...chartBaseOptions(),
                scales: {
                    ...chartBaseOptions().scales,
                    y: {
                        ...chartBaseOptions().scales.y,
                        ticks: {
                            ...chartBaseOptions().scales.y.ticks,
                            callback: (v) => formatPercentPoints(Number(v)),
                        },
                    },
                },
                plugins: {
                    ...chartBaseOptions().plugins,
                    tooltip: {
                        ...chartBaseOptions().plugins.tooltip,
                        callbacks: {
                            label: (ctx) => `${ctx.dataset.label}: ${formatPercentPoints(ctx.raw)}`,
                        },
                    },
                },
            },
        });
        registerIcMemoChart(chart);
    }

    const growthEl = document.getElementById('icGrowthChart');
    if (growthEl) {
        const chart = new Chart(growthEl, {
            type: 'bar',
            data: {
                labels: ['Revenue Growth', 'EBITDA Growth', 'Revenue CAGR', 'EBITDA CAGR'],
                datasets: [
                    {
                        label: 'Average',
                        data: [
                            growth.revenue_growth?.avg,
                            growth.ebitda_growth?.avg,
                            growth.revenue_cagr?.avg,
                            growth.ebitda_cagr?.avg,
                        ],
                        backgroundColor: '#5e8f80',
                        borderRadius: 4,
                    },
                    {
                        label: 'Weighted',
                        data: [
                            growth.revenue_growth?.wavg,
                            growth.ebitda_growth?.wavg,
                            growth.revenue_cagr?.wavg,
                            growth.ebitda_cagr?.wavg,
                        ],
                        backgroundColor: '#0e7c66',
                        borderRadius: 4,
                    },
                ],
            },
            options: {
                ...chartBaseOptions(),
                scales: {
                    ...chartBaseOptions().scales,
                    y: {
                        ...chartBaseOptions().scales.y,
                        ticks: {
                            ...chartBaseOptions().scales.y.ticks,
                            callback: (v) => formatPercentPoints(Number(v)),
                        },
                    },
                },
                plugins: {
                    ...chartBaseOptions().plugins,
                    tooltip: {
                        ...chartBaseOptions().plugins.tooltip,
                        callbacks: {
                            label: (ctx) => `${ctx.dataset.label}: ${formatPercentPoints(ctx.raw)}`,
                        },
                    },
                },
            },
        });
        registerIcMemoChart(chart);
    }
}

function renderIcMemoSliceChart(canvasId, groups) {
    const el = document.getElementById(canvasId);
    if (!el) return;
    const rows = (groups || [])
        .filter((row) => row.weighted_moic !== null && row.weighted_moic !== undefined)
        .slice(0, 10);

    const chart = new Chart(el, {
        type: 'bar',
        data: {
            labels: rows.map((row) => row.label),
            datasets: [
                {
                    label: 'Weighted MOIC',
                    data: rows.map((row) => row.weighted_moic),
                    backgroundColor: '#0e7c66',
                    borderRadius: 4,
                    maxBarThickness: 34,
                },
            ],
        },
        options: {
            ...chartBaseOptions(),
            plugins: {
                ...chartBaseOptions().plugins,
                legend: { display: false },
            },
            scales: {
                ...chartBaseOptions().scales,
                y: {
                    ...chartBaseOptions().scales.y,
                    ticks: {
                        ...chartBaseOptions().scales.y.ticks,
                        callback: (v) => formatBridgeValue(Number(v), 'moic'),
                    },
                },
            },
        },
    });
    registerIcMemoChart(chart);
}

function renderIcMemoSliceCharts(payload) {
    const dims = payload.slicing?.dimensions || {};
    renderIcMemoSliceChart('icVintageChart', dims.vintage_year?.groups);
    renderIcMemoSliceChart('icSectorChart', dims.sector?.groups);
    renderIcMemoSliceChart('icGeographyChart', dims.geography?.groups);
}

function bindIcMemoPrintResize() {
    if (!icMemoCharts.length) return;
    const resizeAll = () => {
        icMemoCharts.forEach((chart) => {
            if (chart && typeof chart.resize === 'function') chart.resize();
        });
    };
    window.addEventListener('beforeprint', resizeAll);
    window.addEventListener('afterprint', resizeAll);
}

function renderIcMemoCharts(payload) {
    icMemoCharts = [];
    renderIcMemoBridgeChart(payload);
    renderIcMemoLeverageCharts(payload);
    renderIcMemoOperatingCharts(payload);
    renderIcMemoSliceCharts(payload);
    bindIcMemoPrintResize();
}

function revealMethodologyHashTarget() {
    const hash = decodeURIComponent(window.location.hash || '');
    if (!hash || hash === '#') return;

    const target = document.getElementById(hash.slice(1));
    if (!target) return;

    const details = target.closest('details');
    if (details && !details.open) {
        details.open = true;
    }

    target.classList.remove('method-highlight');
    // restart the highlight animation when re-targeting
    void target.offsetWidth;
    target.classList.add('method-highlight');
    target.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function bindMethodologyAnchors() {
    if (!document.querySelector('.methodology-root')) return;

    const onHash = () => revealMethodologyHashTarget();
    window.addEventListener('hashchange', onHash);

    if (window.location.hash) {
        setTimeout(onHash, 80);
    }
}

let chartBuilderState = null;
const chartBuilderCharts = new Map();
let chartBuilderCardCounter = 1;

function chartBuilderConfigDefaults(source) {
    return {
        config_version: 1,
        source: source || 'deals',
        chart_type: 'auto',
        x: { field: '', bucket: '' },
        y: [],
        series: { field: '' },
        size: { field: '', agg: 'sum' },
        filters: [],
        sort: { by: 'x', direction: 'asc' },
        limit: 200,
    };
}

function chartBuilderSourceMap() {
    const catalog = chartBuilderState?.catalog || {};
    const sources = catalog.sources || [];
    const map = {};
    sources.forEach((source) => {
        map[source.key] = source;
    });
    return map;
}

function chartBuilderFindFieldMeta(source, field) {
    if (!source || !field) return null;
    const sourceDef = chartBuilderSourceMap()[source];
    if (!sourceDef) return null;
    const fields = [...(sourceDef.dimensions || []), ...(sourceDef.measures || [])];
    return fields.find((item) => item.field === field) || null;
}

function chartBuilderDefaultYField(source) {
    const sourceDef = chartBuilderSourceMap()[source];
    const measure = (sourceDef?.measures || [])[0];
    if (!measure) return null;
    return {
        field: measure.field,
        agg: 'avg',
        label: measure.label || measure.field,
        color: '',
        weight_field: '',
    };
}

function chartBuilderCreateCard(seed = {}) {
    const source = seed.source || chartBuilderState?.catalog?.default_source || 'deals';
    const defaults = chartBuilderConfigDefaults(source);
    const merged = {
        id: `cb-card-${chartBuilderCardCounter++}`,
        source,
        chart_type: seed.chart_type || defaults.chart_type,
        x: seed.x || defaults.x,
        y: Array.isArray(seed.y) && seed.y.length ? seed.y : [],
        series: seed.series || defaults.series,
        size: seed.size || defaults.size,
        filters: Array.isArray(seed.filters) ? seed.filters : [],
        sort: seed.sort || defaults.sort,
        limit: Number(seed.limit || defaults.limit) || defaults.limit,
        show_table: seed.show_table !== false,
        result: seed.result || null,
    };
    if (!merged.y.length) {
        const firstY = chartBuilderDefaultYField(source);
        if (firstY) merged.y = [firstY];
    }
    return merged;
}

function chartBuilderDestroyCharts() {
    chartBuilderCharts.forEach((chart) => {
        if (chart && typeof chart.destroy === 'function') {
            chart.destroy();
        }
    });
    chartBuilderCharts.clear();
}

function chartBuilderCardById(cardId) {
    return chartBuilderState.cards.find((card) => card.id === cardId) || null;
}

function chartBuilderAddCard(seed) {
    const card = chartBuilderCreateCard(seed || {});
    chartBuilderState.cards.push(card);
    chartBuilderState.activeCardId = card.id;
    chartBuilderState.activeWell = 'x';
    chartBuilderRenderBoard();
}

function chartBuilderRemoveCard(cardId) {
    chartBuilderState.cards = chartBuilderState.cards.filter((card) => card.id !== cardId);
    if (!chartBuilderState.cards.length) {
        chartBuilderAddCard({ source: chartBuilderState.catalog.default_source || 'deals' });
        return;
    }
    if (chartBuilderState.activeCardId === cardId) {
        chartBuilderState.activeCardId = chartBuilderState.cards[0].id;
    }
    chartBuilderRenderBoard();
}

function chartBuilderDuplicateCard(cardId) {
    const original = chartBuilderCardById(cardId);
    if (!original) return;
    const seed = JSON.parse(JSON.stringify(original));
    seed.result = null;
    chartBuilderAddCard(seed);
}

function chartBuilderResetCardForSource(card, source) {
    card.source = source;
    card.x = { field: '', bucket: '' };
    const yField = chartBuilderDefaultYField(source);
    card.y = yField ? [yField] : [];
    card.series = { field: '' };
    card.size = { field: '', agg: 'sum' };
    card.filters = [];
    card.result = null;
}

function chartBuilderFormatValue(value, valueType) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
    if (valueType === 'currency') return formatCurrencyMillions(Number(value));
    if (valueType === 'multiple') return `${Number(value).toFixed(2)}x`;
    if (valueType === 'percent') {
        const v = Number(value);
        if (!Number.isFinite(v)) return '—';
        if (Math.abs(v) <= 2) return formatPct(v);
        return formatPercentPoints(v);
    }
    if (valueType === 'year') return `${Math.round(Number(value))}`;
    if (valueType === 'number') {
        const v = Number(value);
        if (!Number.isFinite(v)) return String(value);
        return Number.isInteger(v) ? `${v}` : v.toFixed(2);
    }
    return String(value);
}

function chartBuilderPaletteMarkup(sourceDef, query) {
    const q = (query || '').trim().toLowerCase();
    const filterFields = (items) =>
        items.filter((item) => {
            if (!q) return true;
            return String(item.label || item.field).toLowerCase().includes(q) || String(item.field).toLowerCase().includes(q);
        });

    const dimensions = filterFields(sourceDef?.dimensions || []);
    const measures = filterFields(sourceDef?.measures || []);

    const renderFieldChip = (field, role) => `
        <button
            type="button"
            class="chart-builder-field-chip"
            draggable="true"
            data-cb-field="${field.field}"
            data-cb-source="${sourceDef.key}"
            data-cb-role="${role}"
            title="${field.label}"
        >
            <span>${field.label}</span>
            <small>${field.type}</small>
        </button>
    `;

    return {
        dimensions: dimensions.map((field) => renderFieldChip(field, 'dimension')).join('') || '<p class="tiny">No matching dimensions.</p>',
        measures: measures.map((field) => renderFieldChip(field, 'measure')).join('') || '<p class="tiny">No matching measures.</p>',
    };
}

function chartBuilderYRowsMarkup(card) {
    if (!card.y.length) return '<div class="chart-builder-empty">Drop measure fields here</div>';
    return card.y
        .map((row, idx) => {
            const fieldMeta = chartBuilderFindFieldMeta(card.source, row.field);
            const fieldLabel = fieldMeta?.label || row.field;
            return `
                <div class="chart-builder-y-row">
                    <span class="chart-builder-assigned">${fieldLabel}</span>
                    <select class="analysis-input analysis-input-sm" data-cb-card-id="${card.id}" data-cb-y-index="${idx}" data-cb-y-prop="agg">
                        <option value="count" ${row.agg === 'count' ? 'selected' : ''}>count</option>
                        <option value="count_distinct" ${row.agg === 'count_distinct' ? 'selected' : ''}>count_distinct</option>
                        <option value="sum" ${row.agg === 'sum' ? 'selected' : ''}>sum</option>
                        <option value="avg" ${row.agg === 'avg' ? 'selected' : ''}>avg</option>
                        <option value="wavg" ${row.agg === 'wavg' ? 'selected' : ''}>wavg</option>
                        <option value="min" ${row.agg === 'min' ? 'selected' : ''}>min</option>
                        <option value="max" ${row.agg === 'max' ? 'selected' : ''}>max</option>
                    </select>
                    <button type="button" class="btn-link" data-cb-action="remove-y" data-cb-card-id="${card.id}" data-cb-y-index="${idx}">Remove</button>
                </div>
            `;
        })
        .join('');
}

function chartBuilderFilterRowsMarkup(card) {
    if (!card.filters.length) {
        return '<div class="chart-builder-empty">No local filters</div>';
    }
    const sourceDef = chartBuilderSourceMap()[card.source];
    const availableFields = [...(sourceDef?.dimensions || []), ...(sourceDef?.measures || [])];
    return card.filters
        .map((filterRow, idx) => {
            const selectedField = (filterRow.field || '').trim();
            const fieldOptions = [
                `<option value="" ${selectedField === '' ? 'selected' : ''}>Select field</option>`,
                ...availableFields.map(
                    (field) => `<option value="${field.field}" ${selectedField === field.field ? 'selected' : ''}>${field.label}</option>`
                ),
            ].join('');
            return `
            <div class="chart-builder-filter-row">
                <select class="analysis-input analysis-input-sm" data-cb-card-id="${card.id}" data-cb-filter-index="${idx}" data-cb-filter-prop="field">
                    ${fieldOptions}
                </select>
                <select class="analysis-input analysis-input-sm" data-cb-card-id="${card.id}" data-cb-filter-index="${idx}" data-cb-filter-prop="op">
                    <option value="eq" ${filterRow.op === 'eq' ? 'selected' : ''}>=</option>
                    <option value="neq" ${filterRow.op === 'neq' ? 'selected' : ''}>!=</option>
                    <option value="contains" ${filterRow.op === 'contains' ? 'selected' : ''}>contains</option>
                    <option value="gt" ${filterRow.op === 'gt' ? 'selected' : ''}>></option>
                    <option value="gte" ${filterRow.op === 'gte' ? 'selected' : ''}>>=</option>
                    <option value="lt" ${filterRow.op === 'lt' ? 'selected' : ''}>&lt;</option>
                    <option value="lte" ${filterRow.op === 'lte' ? 'selected' : ''}>&lt;=</option>
                </select>
                <input class="analysis-input analysis-input-sm" data-cb-card-id="${card.id}" data-cb-filter-index="${idx}" data-cb-filter-prop="value" value="${filterRow.value || ''}">
                <button type="button" class="btn-link" data-cb-action="remove-filter" data-cb-card-id="${card.id}" data-cb-filter-index="${idx}">Remove</button>
            </div>
        `;
        })
        .join('');
}

function chartBuilderSourceOptionsHtml(selected) {
    return (chartBuilderState.catalog.sources || [])
        .map((source) => `<option value="${source.key}" ${source.key === selected ? 'selected' : ''}>${source.label}</option>`)
        .join('');
}

function chartBuilderCardMarkup(card) {
    const xMeta = chartBuilderFindFieldMeta(card.source, card.x.field);
    const seriesMeta = chartBuilderFindFieldMeta(card.source, card.series?.field);
    const sizeMeta = chartBuilderFindFieldMeta(card.source, card.size?.field);
    const resultMeta = card.result?.meta || {};
    return `
        <article class="panel chart-builder-card" data-cb-card="${card.id}">
            <header class="chart-builder-card-head">
                <div class="chart-builder-card-title">
                    <strong>Chart</strong>
                    <small>${card.source}</small>
                </div>
                <div class="chart-builder-card-actions">
                    <button type="button" class="btn-upload btn-secondary btn-sm" data-cb-action="run" data-cb-card-id="${card.id}">Run</button>
                    <button type="button" class="btn-upload btn-sm" data-cb-action="save-template" data-cb-card-id="${card.id}">Save</button>
                    <button type="button" class="btn-upload btn-secondary btn-sm" data-cb-action="duplicate" data-cb-card-id="${card.id}">Duplicate</button>
                    <button type="button" class="btn-upload btn-secondary btn-sm" data-cb-action="toggle-table" data-cb-card-id="${card.id}">${card.show_table ? 'Hide Table' : 'Show Table'}</button>
                    <button type="button" class="btn-upload btn-secondary btn-sm" data-cb-action="export-png" data-cb-card-id="${card.id}">Export PNG</button>
                    <button type="button" class="btn-upload btn-secondary btn-sm" data-cb-action="export-csv" data-cb-card-id="${card.id}">Export CSV</button>
                    <button type="button" class="btn-upload btn-danger-soft btn-sm" data-cb-action="remove" data-cb-card-id="${card.id}">Remove</button>
                </div>
            </header>
            <div class="chart-builder-card-config">
                <label class="filter-field">
                    <span>Source</span>
                    <select class="analysis-input" data-cb-card-id="${card.id}" data-cb-prop="source">
                        ${chartBuilderSourceOptionsHtml(card.source)}
                    </select>
                </label>
                <label class="filter-field">
                    <span>Chart Type</span>
                    <select class="analysis-input" data-cb-card-id="${card.id}" data-cb-prop="chart_type">
                        <option value="auto" ${card.chart_type === 'auto' ? 'selected' : ''}>auto</option>
                        <option value="bar" ${card.chart_type === 'bar' ? 'selected' : ''}>bar</option>
                        <option value="line" ${card.chart_type === 'line' ? 'selected' : ''}>line</option>
                        <option value="area" ${card.chart_type === 'area' ? 'selected' : ''}>area</option>
                        <option value="scatter" ${card.chart_type === 'scatter' ? 'selected' : ''}>scatter</option>
                        <option value="bubble" ${card.chart_type === 'bubble' ? 'selected' : ''}>bubble</option>
                        <option value="donut" ${card.chart_type === 'donut' ? 'selected' : ''}>donut</option>
                    </select>
                </label>
                <label class="filter-field">
                    <span>Limit</span>
                    <input class="analysis-input num-input" type="number" min="1" max="5000" data-cb-card-id="${card.id}" data-cb-prop="limit" value="${card.limit || 200}">
                </label>
            </div>

            <div class="chart-builder-wells">
                <div class="chart-builder-well" data-cb-card-id="${card.id}" data-cb-well="x">
                    <h5>X</h5>
                    <div class="chart-builder-assigned">${xMeta ? xMeta.label : 'Drop field'}</div>
                    <select class="analysis-input analysis-input-sm" data-cb-card-id="${card.id}" data-cb-prop="x_bucket">
                        <option value="" ${(card.x?.bucket || '') === '' ? 'selected' : ''}>No bucket</option>
                        <option value="year" ${(card.x?.bucket || '') === 'year' ? 'selected' : ''}>Year</option>
                        <option value="quarter" ${(card.x?.bucket || '') === 'quarter' ? 'selected' : ''}>Quarter</option>
                        <option value="month" ${(card.x?.bucket || '') === 'month' ? 'selected' : ''}>Month</option>
                    </select>
                </div>
                <div class="chart-builder-well" data-cb-card-id="${card.id}" data-cb-well="y">
                    <h5>Y</h5>
                    ${chartBuilderYRowsMarkup(card)}
                </div>
                <div class="chart-builder-well" data-cb-card-id="${card.id}" data-cb-well="series">
                    <h5>Series</h5>
                    <div class="chart-builder-assigned">${seriesMeta ? seriesMeta.label : 'Drop field (optional)'}</div>
                    <button type="button" class="btn-link" data-cb-action="clear-series" data-cb-card-id="${card.id}" ${seriesMeta ? '' : 'disabled'}>Clear</button>
                </div>
                <div class="chart-builder-well" data-cb-card-id="${card.id}" data-cb-well="size">
                    <h5>Size</h5>
                    <div class="chart-builder-assigned">${sizeMeta ? sizeMeta.label : 'Drop measure (bubble)'}</div>
                    <select class="analysis-input analysis-input-sm" data-cb-card-id="${card.id}" data-cb-prop="size_agg">
                        <option value="sum" ${(card.size?.agg || 'sum') === 'sum' ? 'selected' : ''}>sum</option>
                        <option value="avg" ${(card.size?.agg || '') === 'avg' ? 'selected' : ''}>avg</option>
                        <option value="min" ${(card.size?.agg || '') === 'min' ? 'selected' : ''}>min</option>
                        <option value="max" ${(card.size?.agg || '') === 'max' ? 'selected' : ''}>max</option>
                    </select>
                </div>
            </div>

            <div class="chart-builder-local-filters">
                <div class="chart-builder-subhead">
                    <h5>Local Filters</h5>
                    <button type="button" class="btn-upload btn-secondary btn-sm" data-cb-action="add-filter" data-cb-card-id="${card.id}">Add Filter</button>
                </div>
                <div class="chart-builder-filter-list">${chartBuilderFilterRowsMarkup(card)}</div>
            </div>

            <div class="chart-builder-chart-panel">
                <canvas id="chart-builder-canvas-${card.id}" class="chart-builder-canvas"></canvas>
                <div class="tiny chart-builder-meta" id="chart-builder-meta-${card.id}">
                    ${resultMeta.warnings && resultMeta.warnings.length ? resultMeta.warnings.join(' | ') : ''}
                </div>
            </div>

            <div class="chart-builder-table-wrap ${card.show_table ? '' : 'is-hidden'}" id="chart-builder-table-wrap-${card.id}">
                <table class="data-table compact chart-builder-table">
                    <thead id="chart-builder-table-head-${card.id}"></thead>
                    <tbody id="chart-builder-table-body-${card.id}"><tr><td class="num">Run query to populate table.</td></tr></tbody>
                </table>
            </div>
        </article>
    `;
}

function chartBuilderRenderBoard() {
    const board = chartBuilderState.root.querySelector('#chart-builder-board');
    if (!board) return;
    chartBuilderDestroyCharts();
    board.innerHTML = chartBuilderState.cards.map((card) => chartBuilderCardMarkup(card)).join('');
    chartBuilderBindBoardInteractions();
    chartBuilderRenderCardOutputs();
}

function chartBuilderRenderCardOutputs() {
    chartBuilderState.cards.forEach((card) => {
        const canvas = document.getElementById(`chart-builder-canvas-${card.id}`);
        if (!canvas) return;
        const result = card.result;
        if (!result) return;

        const resolvedType = result.chart_type_resolved || 'bar';
        const chartType = resolvedType === 'area' ? 'line' : (resolvedType === 'donut' ? 'doughnut' : resolvedType);
        const datasets = (result.datasets || []).map((dataset) => {
            const copy = { ...dataset };
            if (resolvedType === 'area') {
                copy.fill = true;
                copy.tension = 0.28;
                copy.pointRadius = 2;
            }
            return copy;
        });

        const options = chartBaseOptions();
        if (resolvedType === 'donut') {
            options.scales = {};
        }
        if (resolvedType === 'scatter' || resolvedType === 'bubble') {
            options.scales = {
                x: {
                    ...chartBaseOptions().scales.x,
                    ticks: {
                        ...chartBaseOptions().scales.x.ticks,
                        callback: (v) => `${v}`,
                    },
                },
                y: {
                    ...chartBaseOptions().scales.y,
                    ticks: {
                        ...chartBaseOptions().scales.y.ticks,
                        callback: (v) => `${v}`,
                    },
                },
            };
        } else if (result.meta?.stacked) {
            options.scales = {
                x: { ...chartBaseOptions().scales.x, stacked: true },
                y: { ...chartBaseOptions().scales.y, stacked: true },
            };
        } else {
            options.scales = {
                ...chartBaseOptions().scales,
            };
        }

        const chart = new Chart(canvas, {
            type: chartType,
            data: {
                labels: result.labels || [],
                datasets,
            },
            options,
        });
        chartBuilderCharts.set(card.id, chart);

        const headEl = document.getElementById(`chart-builder-table-head-${card.id}`);
        const bodyEl = document.getElementById(`chart-builder-table-body-${card.id}`);
        if (headEl && bodyEl) {
            const columns = result.table_columns || [];
            const rows = result.table_rows || [];
            headEl.innerHTML = `<tr>${columns.map((col) => `<th>${col.label}</th>`).join('')}</tr>`;
            if (!rows.length) {
                bodyEl.innerHTML = `<tr><td colspan="${Math.max(1, columns.length)}" class="num">No rows returned.</td></tr>`;
            } else {
                bodyEl.innerHTML = rows
                    .map((row) => {
                        const tds = columns
                            .map((col) => `<td class="${col.type === 'string' || col.type === 'enum' ? '' : 'num'}">${chartBuilderFormatValue(row[col.key], col.type)}</td>`)
                            .join('');
                        return `<tr>${tds}</tr>`;
                    })
                    .join('');
            }
        }

        const metaEl = document.getElementById(`chart-builder-meta-${card.id}`);
        if (metaEl) {
            const warnings = result.meta?.warnings || [];
            const base = `Rows: ${result.meta?.row_count ?? 0}${result.meta?.truncated ? ' (truncated)' : ''}`;
            metaEl.textContent = warnings.length ? `${base} | ${warnings.join(' | ')}` : base;
        }
    });
}

function chartBuilderGlobalFiltersFromUrl() {
    const params = new URLSearchParams(window.location.search);
    const out = {};
    [
        'fund',
        'status',
        'sector',
        'geography',
        'vintage',
        'exit_type',
        'lead_partner',
        'security_type',
        'deal_type',
        'entry_channel',
        'benchmark_asset_class',
    ].forEach((key) => {
        out[key] = (params.get(key) || '').trim();
    });
    return out;
}

function chartBuilderBuildSpec(card) {
    return {
        source: card.source,
        chart_type: card.chart_type,
        x: card.x || {},
        y: card.y || [],
        series: card.series || {},
        size: card.size || {},
        filters: card.filters || [],
        sort: card.sort || { by: 'x', direction: 'asc' },
        limit: card.limit || 200,
        global_filters: chartBuilderGlobalFiltersFromUrl(),
    };
}

function chartBuilderRunCard(cardId) {
    const card = chartBuilderCardById(cardId);
    if (!card) return Promise.resolve();
    const spec = chartBuilderBuildSpec(card);
    return fetch(chartBuilderState.endpoints.queryUrl, {
        method: 'POST',
        headers: jsonHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify(spec),
    })
        .then((response) => response.json().then((body) => ({ ok: response.ok, body })))
        .then(({ ok, body }) => {
            if (!ok) {
                throw new Error(body?.error || 'Chart query failed.');
            }
            card.result = body;
            chartBuilderRenderBoard();
        })
        .catch((err) => {
            const metaEl = document.getElementById(`chart-builder-meta-${cardId}`);
            if (metaEl) metaEl.textContent = err.message;
        });
}

function chartBuilderExportCsv(cardId) {
    const card = chartBuilderCardById(cardId);
    if (!card || !card.result) return;
    const columns = card.result.table_columns || [];
    const rows = card.result.table_rows || [];
    const esc = (value) => {
        const raw = value === null || value === undefined ? '' : String(value);
        return `"${raw.replace(/"/g, '""')}"`;
    };
    const lines = [];
    lines.push(columns.map((col) => esc(col.label)).join(','));
    rows.forEach((row) => {
        lines.push(columns.map((col) => esc(row[col.key])).join(','));
    });
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${card.source}-${card.id}.csv`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
}

function chartBuilderExportPng(cardId) {
    const canvas = document.getElementById(`chart-builder-canvas-${cardId}`);
    if (!canvas) return;
    const link = document.createElement('a');
    link.href = canvas.toDataURL('image/png');
    link.download = `chart-${cardId}.png`;
    document.body.appendChild(link);
    link.click();
    link.remove();
}

function chartBuilderSerializeBoard() {
    return {
        config_version: 1,
        cards: chartBuilderState.cards.map((card) => ({
            source: card.source,
            chart_type: card.chart_type,
            x: card.x || { field: '', bucket: '' },
            y: card.y || [],
            series: card.series || { field: '' },
            size: card.size || { field: '', agg: 'sum' },
            filters: card.filters || [],
            sort: card.sort || { by: 'x', direction: 'asc' },
            limit: card.limit || 200,
            show_table: card.show_table !== false,
        })),
    };
}

function chartBuilderLoadFromTemplate(templateId) {
    const selected = (chartBuilderState.templates || []).find((item) => String(item.id) === String(templateId));
    if (!selected) return;
    const config = selected.config || {};
    const cards = Array.isArray(config.cards) ? config.cards : [];
    chartBuilderState.cards = cards.map((seed) => chartBuilderCreateCard(seed));
    if (!chartBuilderState.cards.length) {
        chartBuilderState.cards = [chartBuilderCreateCard({ source: chartBuilderState.catalog.default_source || 'deals' })];
    }
    chartBuilderState.currentTemplateId = selected.id;
    chartBuilderRenderBoard();
}

function chartBuilderRefreshTemplateSelect() {
    const select = document.getElementById('chart-builder-template-select');
    if (!select) return;
    const options = ['<option value="">Select template</option>']
        .concat(
            (chartBuilderState.templates || []).map(
                (row) => `<option value="${row.id}" ${String(chartBuilderState.currentTemplateId || '') === String(row.id) ? 'selected' : ''}>${row.name}</option>`
            )
        )
        .join('');
    select.innerHTML = options;
}

function chartBuilderLoadTemplates() {
    return fetch(chartBuilderState.endpoints.templatesUrl)
        .then((r) => r.json())
        .then((payload) => {
            chartBuilderState.templates = payload.templates || [];
            chartBuilderRefreshTemplateSelect();
        });
}

function chartBuilderSaveTemplate(isUpdate, preferredName) {
    const templateId = chartBuilderState.currentTemplateId;
    if (isUpdate && !templateId) {
        return;
    }
    let name = preferredName;
    if (!name) {
        if (isUpdate) {
            const current = (chartBuilderState.templates || []).find((t) => t.id === templateId);
            name = window.prompt('Template name', current?.name || '');
        } else {
            name = window.prompt('Template name', '');
        }
    }
    if (!name) return;
    const payload = {
        name: String(name).trim(),
        source: chartBuilderState.cards[0]?.source || 'deals',
        config: chartBuilderSerializeBoard(),
    };
    if (!payload.name) return;

    const url = isUpdate
        ? `${chartBuilderState.endpoints.templatesUrl}/${encodeURIComponent(templateId)}`
        : chartBuilderState.endpoints.templatesUrl;
    const method = isUpdate ? 'PUT' : 'POST';

    fetch(url, {
        method,
        headers: jsonHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify(payload),
    })
        .then((response) => response.json().then((body) => ({ ok: response.ok, body })))
        .then(({ ok, body }) => {
            if (!ok) throw new Error(body?.error || 'Template save failed.');
            chartBuilderState.currentTemplateId = body.id;
            return chartBuilderLoadTemplates();
        })
        .catch((err) => {
            window.alert(err.message);
        });
}

function chartBuilderDeleteTemplate() {
    const templateId = chartBuilderState.currentTemplateId;
    if (!templateId) return;
    if (!window.confirm('Delete this template?')) return;
    fetch(`${chartBuilderState.endpoints.templatesUrl}/${encodeURIComponent(templateId)}`, {
        method: 'DELETE',
        headers: jsonHeaders(),
    })
        .then((r) => r.json())
        .then(() => {
            chartBuilderState.currentTemplateId = null;
            return chartBuilderLoadTemplates();
        });
}

function chartBuilderApplyFieldToWell(cardId, well, field, source, role) {
    const card = chartBuilderCardById(cardId);
    if (!card) return;

    if (card.source !== source) {
        chartBuilderResetCardForSource(card, source);
    }

    if (well === 'x') {
        card.x = { ...(card.x || {}), field };
    } else if (well === 'series') {
        card.series = { ...(card.series || {}), field };
    } else if (well === 'size') {
        if (role !== 'measure') return;
        card.size = { ...(card.size || { agg: 'sum' }), field };
    } else {
        if (role !== 'measure') return;
        const exists = card.y.some((row) => row.field === field);
        if (!exists) {
            const meta = chartBuilderFindFieldMeta(card.source, field);
            card.y.push({
                field,
                agg: 'avg',
                label: meta?.label || field,
                color: '',
                weight_field: '',
            });
        }
    }
    card.result = null;
    chartBuilderRenderBoard();
}

function chartBuilderBindPaletteInteractions() {
    const root = chartBuilderState.root;
    const palette = root.querySelector('.chart-builder-palette');
    if (!palette) return;

    palette.querySelectorAll('.chart-builder-field-chip').forEach((chip) => {
        chip.addEventListener('dragstart', (evt) => {
            const payload = {
                field: chip.dataset.cbField,
                source: chip.dataset.cbSource,
                role: chip.dataset.cbRole,
            };
            evt.dataTransfer.setData('application/json', JSON.stringify(payload));
            evt.dataTransfer.effectAllowed = 'copy';
        });
        chip.addEventListener('click', () => {
            const cardId = chartBuilderState.activeCardId || chartBuilderState.cards[0]?.id;
            if (!cardId) return;
            const well = chartBuilderState.activeWell || 'x';
            chartBuilderApplyFieldToWell(cardId, well, chip.dataset.cbField, chip.dataset.cbSource, chip.dataset.cbRole);
        });
    });
}

function chartBuilderBindBoardInteractions() {
    const board = chartBuilderState.root.querySelector('#chart-builder-board');
    if (!board) return;

    board.querySelectorAll('.chart-builder-well').forEach((wellEl) => {
        wellEl.addEventListener('click', () => {
            chartBuilderState.activeCardId = wellEl.dataset.cbCardId;
            chartBuilderState.activeWell = wellEl.dataset.cbWell;
            board.querySelectorAll('.chart-builder-well').forEach((node) => node.classList.remove('is-active'));
            wellEl.classList.add('is-active');
        });
        wellEl.addEventListener('dragover', (evt) => {
            evt.preventDefault();
            evt.dataTransfer.dropEffect = 'copy';
        });
        wellEl.addEventListener('drop', (evt) => {
            evt.preventDefault();
            try {
                const payload = JSON.parse(evt.dataTransfer.getData('application/json') || '{}');
                chartBuilderApplyFieldToWell(
                    wellEl.dataset.cbCardId,
                    wellEl.dataset.cbWell,
                    payload.field,
                    payload.source,
                    payload.role
                );
            } catch (err) {
                // ignore bad payload
            }
        });
    });

    board.querySelectorAll('[data-cb-action]').forEach((btn) => {
        btn.addEventListener('click', () => {
            const action = btn.dataset.cbAction;
            const cardId = btn.dataset.cbCardId;
            if (action === 'run') chartBuilderRunCard(cardId);
            if (action === 'duplicate') chartBuilderDuplicateCard(cardId);
            if (action === 'remove') chartBuilderRemoveCard(cardId);
            if (action === 'export-csv') chartBuilderExportCsv(cardId);
            if (action === 'export-png') chartBuilderExportPng(cardId);
            if (action === 'toggle-table') {
                const card = chartBuilderCardById(cardId);
                if (!card) return;
                card.show_table = !card.show_table;
                chartBuilderRenderBoard();
            }
            if (action === 'add-filter') {
                const card = chartBuilderCardById(cardId);
                if (!card) return;
                card.filters.push({ field: '', op: 'eq', value: '' });
                chartBuilderRenderBoard();
            }
            if (action === 'clear-series') {
                const card = chartBuilderCardById(cardId);
                if (!card) return;
                card.series = { field: '' };
                card.result = null;
                chartBuilderRenderBoard();
            }
            if (action === 'remove-filter') {
                const card = chartBuilderCardById(cardId);
                if (!card) return;
                const idx = Number(btn.dataset.cbFilterIndex);
                card.filters.splice(idx, 1);
                chartBuilderRenderBoard();
            }
            if (action === 'remove-y') {
                const card = chartBuilderCardById(cardId);
                if (!card) return;
                const idx = Number(btn.dataset.cbYIndex);
                card.y.splice(idx, 1);
                chartBuilderRenderBoard();
            }
            if (action === 'save-template') {
                chartBuilderSaveTemplate(false);
            }
        });
    });

    board.querySelectorAll('[data-cb-prop]').forEach((el) => {
        el.addEventListener('change', () => {
            const card = chartBuilderCardById(el.dataset.cbCardId);
            if (!card) return;
            const prop = el.dataset.cbProp;
            if (prop === 'source') {
                chartBuilderResetCardForSource(card, el.value);
            } else if (prop === 'chart_type') {
                card.chart_type = el.value;
            } else if (prop === 'limit') {
                card.limit = Number(el.value) || 200;
            } else if (prop === 'x_bucket') {
                card.x = { ...(card.x || {}), bucket: el.value };
            } else if (prop === 'size_agg') {
                card.size = { ...(card.size || {}), agg: el.value };
            }
            card.result = null;
            chartBuilderRenderBoard();
        });
    });

    board.querySelectorAll('[data-cb-y-prop]').forEach((el) => {
        el.addEventListener('change', () => {
            const card = chartBuilderCardById(el.dataset.cbCardId);
            if (!card) return;
            const idx = Number(el.dataset.cbYIndex);
            if (!card.y[idx]) return;
            const prop = el.dataset.cbYProp;
            card.y[idx][prop] = el.value;
            card.result = null;
        });
    });

    board.querySelectorAll('[data-cb-filter-prop]').forEach((el) => {
        const evtName = el.tagName === 'INPUT' ? 'input' : 'change';
        el.addEventListener(evtName, () => {
            const card = chartBuilderCardById(el.dataset.cbCardId);
            if (!card) return;
            const idx = Number(el.dataset.cbFilterIndex);
            if (!card.filters[idx]) return;
            const prop = el.dataset.cbFilterProp;
            card.filters[idx][prop] = el.value;
            card.result = null;
        });
    });
}

function chartBuilderRenderPalette() {
    const sourceSelect = document.getElementById('chart-builder-source-select');
    const searchInput = document.getElementById('chart-builder-field-search');
    const dimEl = document.getElementById('chart-builder-dimension-fields');
    const measureEl = document.getElementById('chart-builder-measure-fields');
    if (!sourceSelect || !dimEl || !measureEl) return;

    const selectedSource = sourceSelect.value || chartBuilderState.catalog.default_source || 'deals';
    const sourceDef = chartBuilderSourceMap()[selectedSource];
    if (!sourceDef) return;
    const palette = chartBuilderPaletteMarkup(sourceDef, searchInput?.value || '');
    dimEl.innerHTML = palette.dimensions;
    measureEl.innerHTML = palette.measures;
    chartBuilderBindPaletteInteractions();
}

function initChartBuilder() {
    const root = document.getElementById('chart-builder-root');
    if (!root) return;
    const catalog = getJsonScriptPayload('chart-builder-catalog-payload');
    if (!catalog || !Array.isArray(catalog.sources)) return;

    chartBuilderState = {
        root,
        catalog,
        cards: [],
        templates: [],
        activeCardId: null,
        activeWell: 'x',
        currentTemplateId: null,
        endpoints: {
            catalogUrl: root.dataset.catalogUrl,
            queryUrl: root.dataset.queryUrl,
            templatesUrl: root.dataset.templatesUrl,
        },
    };

    const sourceSelect = document.getElementById('chart-builder-source-select');
    if (sourceSelect) {
        sourceSelect.innerHTML = (catalog.sources || [])
            .map((source) => `<option value="${source.key}">${source.label} (${source.row_count})</option>`)
            .join('');
        sourceSelect.value = catalog.default_source || 'deals';
        sourceSelect.addEventListener('change', () => chartBuilderRenderPalette());
    }
    const searchInput = document.getElementById('chart-builder-field-search');
    if (searchInput) searchInput.addEventListener('input', () => chartBuilderRenderPalette());

    const addCardBtn = document.getElementById('chart-builder-add-card');
    if (addCardBtn) addCardBtn.addEventListener('click', () => chartBuilderAddCard({ source: sourceSelect?.value || catalog.default_source || 'deals' }));

    const templateSelect = document.getElementById('chart-builder-template-select');
    if (templateSelect) {
        templateSelect.addEventListener('change', () => {
            chartBuilderState.currentTemplateId = templateSelect.value ? Number(templateSelect.value) : null;
        });
    }
    const loadBtn = document.getElementById('chart-builder-template-load');
    if (loadBtn) loadBtn.addEventListener('click', () => chartBuilderLoadFromTemplate(chartBuilderState.currentTemplateId));
    const saveBtn = document.getElementById('chart-builder-template-save');
    if (saveBtn) saveBtn.addEventListener('click', () => chartBuilderSaveTemplate(false));
    const updateBtn = document.getElementById('chart-builder-template-update');
    if (updateBtn) updateBtn.addEventListener('click', () => chartBuilderSaveTemplate(true));
    const deleteBtn = document.getElementById('chart-builder-template-delete');
    if (deleteBtn) deleteBtn.addEventListener('click', () => chartBuilderDeleteTemplate());

    chartBuilderRenderPalette();
    chartBuilderAddCard({ source: catalog.default_source || 'deals' });
    chartBuilderLoadTemplates();
}

function firmPickerNormalizeIds(values, allowedIds) {
    const out = [];
    const seen = new Set();
    const rows = Array.isArray(values) ? values : [];
    rows.forEach((value) => {
        const id = Number(value);
        if (!Number.isInteger(id) || !allowedIds.has(id) || seen.has(id)) return;
        seen.add(id);
        out.push(id);
    });
    return out;
}

function firmPickerStorageKey(userId, teamId) {
    const userScope = Number.isInteger(Number(userId)) ? Number(userId) : 'anon';
    const teamScope = Number.isInteger(Number(teamId)) ? Number(teamId) : 'none';
    return `firm_picker:v1:user_${userScope}:team_${teamScope}`;
}

function loadFirmPickerPreferences(storageKey) {
    try {
        const raw = window.localStorage.getItem(storageKey);
        if (!raw) return { pinnedFirmIds: [], recentFirmIds: [] };
        const parsed = JSON.parse(raw);
        return {
            pinnedFirmIds: Array.isArray(parsed?.pinnedFirmIds) ? parsed.pinnedFirmIds : [],
            recentFirmIds: Array.isArray(parsed?.recentFirmIds) ? parsed.recentFirmIds : [],
        };
    } catch (err) {
        return { pinnedFirmIds: [], recentFirmIds: [] };
    }
}

function saveFirmPickerPreferences() {
    if (!firmPickerState) return;
    const payload = {
        pinnedFirmIds: firmPickerState.pinnedFirmIds,
        recentFirmIds: firmPickerState.recentFirmIds.slice(0, 10),
    };
    try {
        window.localStorage.setItem(firmPickerState.storageKey, JSON.stringify(payload));
    } catch (err) {
        // Ignore storage failures (private mode/quota).
    }
}

function firmPickerRankFirms(firms, query) {
    const sorted = [...firms].sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: 'base' }));
    const q = String(query || '').trim().toLowerCase();
    if (!q) return sorted;
    const starts = [];
    const contains = [];
    sorted.forEach((firm) => {
        const name = firm.name.toLowerCase();
        if (name.startsWith(q)) {
            starts.push(firm);
        } else if (name.includes(q)) {
            contains.push(firm);
        }
    });
    return starts.concat(contains);
}

function firmPickerTogglePin(firmId) {
    if (!firmPickerState) return;
    const idx = firmPickerState.pinnedFirmIds.indexOf(firmId);
    if (idx >= 0) {
        firmPickerState.pinnedFirmIds.splice(idx, 1);
    } else {
        firmPickerState.pinnedFirmIds.push(firmId);
    }
    saveFirmPickerPreferences();
    renderFirmPicker();
}

function firmPickerRecordRecent(firmId) {
    if (!firmPickerState) return;
    const next = firmPickerState.recentFirmIds.filter((id) => id !== firmId);
    next.unshift(firmId);
    firmPickerState.recentFirmIds = next.slice(0, 10);
}

function firmPickerSubmitSelection(firmId) {
    if (!firmPickerState || !Number.isInteger(firmId)) return;
    firmPickerRecordRecent(firmId);
    saveFirmPickerPreferences();
    firmPickerState.submitForm.action = `/firms/${encodeURIComponent(String(firmId))}/select`;
    firmPickerState.submitForm.submit();
}

function firmPickerMoveHighlight(step) {
    if (!firmPickerState) return;
    const ids = firmPickerState.renderedFirmIds;
    if (!ids.length) return;
    let currentIdx = ids.indexOf(firmPickerState.highlightedFirmId);
    if (currentIdx < 0) currentIdx = 0;
    const nextIdx = (currentIdx + step + ids.length) % ids.length;
    firmPickerState.highlightedFirmId = ids[nextIdx];
    renderFirmPicker();
}

function renderFirmPickerSection(title, firms) {
    const section = document.createElement('section');
    section.className = 'firm-picker-section';
    const heading = document.createElement('h3');
    heading.className = 'firm-picker-section-title';
    heading.textContent = title;
    section.appendChild(heading);

    const list = document.createElement('div');
    list.className = 'firm-picker-list';

    firms.forEach((firm) => {
        const item = document.createElement('div');
        item.className = 'firm-picker-row-item';

        const row = document.createElement('button');
        row.type = 'button';
        row.className = 'firm-picker-row';
        row.dataset.firmSelect = String(firm.id);
        if (firm.id === firmPickerState.activeFirmId) row.classList.add('is-active');
        if (firm.id === firmPickerState.highlightedFirmId) row.classList.add('is-highlighted');
        row.addEventListener('click', () => firmPickerSubmitSelection(firm.id));

        const name = document.createElement('span');
        name.className = 'firm-picker-row-name';
        name.textContent = firm.name;
        row.appendChild(name);

        if (firm.id === firmPickerState.activeFirmId) {
            const active = document.createElement('span');
            active.className = 'firm-picker-row-status';
            active.textContent = 'Active';
            row.appendChild(active);
        }

        const pinButton = document.createElement('button');
        pinButton.type = 'button';
        pinButton.className = 'firm-picker-pin';
        pinButton.dataset.firmPin = String(firm.id);
        const isPinned = firmPickerState.pinnedFirmIds.includes(firm.id);
        pinButton.setAttribute('aria-pressed', isPinned ? 'true' : 'false');
        pinButton.title = isPinned ? 'Unpin firm' : 'Pin firm';
        pinButton.innerHTML = `<i class="bi ${isPinned ? 'bi-star-fill' : 'bi-star'}"></i>`;
        pinButton.addEventListener('click', (event) => {
            event.preventDefault();
            event.stopPropagation();
            firmPickerTogglePin(firm.id);
        });

        item.appendChild(row);
        item.appendChild(pinButton);
        list.appendChild(item);
    });

    section.appendChild(list);
    return section;
}

function renderFirmPicker() {
    if (!firmPickerState) return;
    const query = firmPickerState.searchInput.value;
    const ranked = firmPickerRankFirms(firmPickerState.firms, query);
    const pinnedSet = new Set(firmPickerState.pinnedFirmIds);
    const recentSet = new Set(firmPickerState.recentFirmIds);

    const pinned = ranked.filter((firm) => pinnedSet.has(firm.id));
    const recent = ranked.filter((firm) => !pinnedSet.has(firm.id) && recentSet.has(firm.id));
    const all = ranked.filter((firm) => !pinnedSet.has(firm.id) && !recentSet.has(firm.id));

    const sections = [];
    if (pinned.length) sections.push({ title: 'Pinned', firms: pinned });
    if (recent.length) sections.push({ title: 'Recent', firms: recent });
    if (all.length) sections.push({ title: 'All Firms (A-Z)', firms: all });

    firmPickerState.sections.innerHTML = '';
    if (!sections.length) {
        firmPickerState.empty.hidden = false;
        if (firmPickerState.firms.length) {
            firmPickerState.empty.textContent = 'No firms match your search.';
        } else {
            firmPickerState.empty.textContent = 'No firms are available for your team.';
        }
        firmPickerState.renderedFirmIds = [];
        return;
    }

    firmPickerState.empty.hidden = true;
    sections.forEach((section) => {
        firmPickerState.sections.appendChild(renderFirmPickerSection(section.title, section.firms));
    });

    const renderedIds = sections.flatMap((section) => section.firms.map((firm) => firm.id));
    firmPickerState.renderedFirmIds = renderedIds;
    if (!renderedIds.includes(firmPickerState.highlightedFirmId)) {
        firmPickerState.highlightedFirmId = renderedIds[0];
        renderFirmPicker();
    }
}

function openFirmPicker() {
    if (!firmPickerState || !firmPickerState.firms.length) return;
    firmPickerState.modal.hidden = false;
    firmPickerState.modal.setAttribute('aria-hidden', 'false');
    firmPickerState.trigger.setAttribute('aria-expanded', 'true');
    document.body.classList.add('firm-picker-open');
    firmPickerState.searchInput.value = '';
    firmPickerState.highlightedFirmId = firmPickerState.activeFirmId || firmPickerState.firms[0]?.id || null;
    renderFirmPicker();
    window.requestAnimationFrame(() => {
        firmPickerState.searchInput.focus();
        firmPickerState.searchInput.select();
    });
}

function closeFirmPicker() {
    if (!firmPickerState) return;
    firmPickerState.modal.hidden = true;
    firmPickerState.modal.setAttribute('aria-hidden', 'true');
    firmPickerState.trigger.setAttribute('aria-expanded', 'false');
    document.body.classList.remove('firm-picker-open');
}

function initFirmPicker() {
    const payload = getJsonScriptPayload('firm-picker-data-payload');
    const modal = document.getElementById('firm-picker-modal');
    const trigger = document.getElementById('firm-picker-trigger');
    const searchInput = document.getElementById('firm-picker-search');
    const sections = document.getElementById('firm-picker-sections');
    const empty = document.getElementById('firm-picker-empty');
    const submitForm = document.getElementById('firm-picker-submit-form');
    if (!payload || !modal || !trigger || !searchInput || !sections || !empty || !submitForm) return;

    const firms = Array.isArray(payload.firms)
        ? payload.firms
            .map((firm) => ({ id: Number(firm?.id), name: String(firm?.name || '').trim() }))
            .filter((firm) => Number.isInteger(firm.id) && firm.name.length > 0)
        : [];
    const allowedIds = new Set(firms.map((firm) => firm.id));
    const storageKey = firmPickerStorageKey(payload.userId, payload.teamId);
    const stored = loadFirmPickerPreferences(storageKey);
    const pinnedFirmIds = firmPickerNormalizeIds(stored.pinnedFirmIds, allowedIds);
    const recentFirmIds = firmPickerNormalizeIds(stored.recentFirmIds, allowedIds).slice(0, 10);

    firmPickerState = {
        modal,
        trigger,
        searchInput,
        sections,
        empty,
        submitForm,
        firms,
        activeFirmId: Number.isInteger(Number(payload.activeFirmId)) ? Number(payload.activeFirmId) : null,
        storageKey,
        pinnedFirmIds,
        recentFirmIds,
        highlightedFirmId: null,
        renderedFirmIds: [],
    };
    saveFirmPickerPreferences();

    trigger.addEventListener('click', () => openFirmPicker());
    searchInput.addEventListener('input', () => renderFirmPicker());
    modal.querySelectorAll('[data-firm-picker-close]').forEach((btn) => {
        btn.addEventListener('click', () => closeFirmPicker());
    });

    document.addEventListener('keydown', (event) => {
        if (!firmPickerState) return;
        const key = event.key || '';
        const isShortcut = (event.metaKey || event.ctrlKey) && key.toLowerCase() === 'k';
        if (isShortcut) {
            event.preventDefault();
            openFirmPicker();
            return;
        }

        const isOpen = !firmPickerState.modal.hidden;
        if (!isOpen) return;

        if (key === 'Escape') {
            event.preventDefault();
            closeFirmPicker();
            return;
        }
        if (key === 'ArrowDown') {
            event.preventDefault();
            firmPickerMoveHighlight(1);
            return;
        }
        if (key === 'ArrowUp') {
            event.preventDefault();
            firmPickerMoveHighlight(-1);
            return;
        }
        if (key === 'Enter') {
            event.preventDefault();
            if (Number.isInteger(firmPickerState.highlightedFirmId)) {
                firmPickerSubmitSelection(firmPickerState.highlightedFirmId);
            }
        }
    });
}

document.addEventListener('DOMContentLoaded', () => {
    attachTableSorting();
    attachDropZone();
    attachFlashDismiss();
    attachDealBridgeControls();
    attachRollupBridgeControls();
    attachDashboardFilterAutoApply();
    attachValueCreationMixControls();

    const endpoint = document.getElementById('dashboard-series-endpoint');
    if (endpoint) {
        const qs = window.location.search || '';
        fetch(`${endpoint.dataset.url}${qs}`)
            .then((r) => r.json())
            .then((payload) => {
                renderDashboardCharts(payload);
                attachDashboardBridgeControls();
                attachValueCreationMixControls();
            });
    }

    const memoPayload = getJsonScriptPayload('ic-memo-payload');
    if (memoPayload) {
        renderIcMemoCharts(memoPayload);
    }

    bindMethodologyAnchors();
    initChartBuilder();
    initFirmPicker();
});
