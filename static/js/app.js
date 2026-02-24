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

const DRIVER_LABELS = {
    revenue: 'Revenue Growth',
    margin: 'Margin Expansion',
    multiple: 'Multiple Expansion',
    leverage: 'Leverage / Debt Paydown',
    other: 'Residual / Other',
};
const BRIDGE_DRIVER_KEYS = ['revenue', 'margin', 'multiple', 'leverage', 'other'];

const AXIS_GRID = 'rgba(20, 35, 33, 0.10)';
const AXIS_TICK = '#435854';

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
    const { valuesMap, startValue, endValue } = resolveBridgeSeries(payload, controls.unit, isAggregate);
    const leverLabels = BRIDGE_DRIVER_KEYS.map((k) => DRIVER_LABELS[k]);
    const leverValues = BRIDGE_DRIVER_KEYS.map((k) => valuesMap[k]);
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
        } else if (payload.ownership_pct !== null && payload.ownership_pct !== undefined) {
            txt += ` | Ownership ${(payload.ownership_pct * 100).toFixed(1)}%`;
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
    if (unit === 'dollar') return `$${v.toFixed(1)}M`;
    if (unit === 'moic') return `${v >= 0 ? '+' : ''}${v.toFixed(2)}x`;
    if (unit === 'pct') return `${v >= 0 ? '+' : ''}${(v * 100).toFixed(1)}%`;
    return String(v);
}

function formatBridgeAxisTick(v, unit) {
    if (v === null || v === undefined || Number.isNaN(v)) return '—';
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    if (unit === 'dollar') return `$${Math.round(n)}M`;
    if (unit === 'moic') return `${n.toFixed(1)}x`;
    if (unit === 'pct') return `${Math.round(n * 100)}%`;
    return String(n);
}

function formatCurrencyMillions(v) {
    if (v === null || v === undefined || Number.isNaN(v)) return '—';
    return `$${Number(v).toFixed(1)}M`;
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
        BRIDGE_DRIVER_KEYS.forEach((key) => {
            dollar[key] = toOptionalNumber(dollarSource[key]);
            moic[key] = toOptionalNumber(moicSource[key]);
            pct[key] = toOptionalNumber(pctSource[key]);
        });
        return { dollar, moic, pct };
    }

    const equity = toOptionalNumber(payload?.equity_invested);
    const valueCreated = toOptionalNumber(payload?.value_created);
    BRIDGE_DRIVER_KEYS.forEach((key) => {
        dollar[key] = toOptionalNumber(dollarSource[key]);
        moic[key] = safeBridgeRatio(dollar[key], equity);
        pct[key] = safeBridgeRatio(dollar[key], valueCreated);
    });
    return { dollar, moic, pct };
}

function renderBridgeLeverTable(tableBodyEl, payload, options = {}) {
    if (!tableBodyEl) return;
    const isAggregate = Boolean(options.isAggregate);

    if (!payload) {
        tableBodyEl.innerHTML = '<tr><td colspan="4" class="num">No bridge data available.</td></tr>';
        return;
    }

    const triples = normalizeBridgeTriples(payload, isAggregate);
    tableBodyEl.innerHTML = '';

    BRIDGE_DRIVER_KEYS.forEach((key) => {
        const tr = document.createElement('tr');
        const label = document.createElement('td');
        label.textContent = DRIVER_LABELS[key];
        tr.appendChild(label);

        const dollar = document.createElement('td');
        dollar.className = 'num';
        dollar.textContent = formatBridgeValue(triples.dollar[key], 'dollar');
        tr.appendChild(dollar);

        const moic = document.createElement('td');
        moic.className = 'num';
        moic.textContent = formatBridgeValue(triples.moic[key], 'moic');
        tr.appendChild(moic);

        const pct = document.createElement('td');
        pct.className = 'num';
        pct.textContent = formatBridgeValue(triples.pct[key], 'pct');
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
    const drivers = ((dashboardBridgePayload.drivers || {})[unit]) || {};
    const startEnd = ((dashboardBridgePayload.start_end || {})[unit]) || {};

    const leverLabels = BRIDGE_DRIVER_KEYS.map((k) => DRIVER_LABELS[k]);
    const leverValues = BRIDGE_DRIVER_KEYS.map((k) => drivers[k]);
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

    const drivers = payload.bridge?.drivers?.moic || {};
    const startEnd = payload.bridge?.start_end?.moic || {};
    const labels = BRIDGE_DRIVER_KEYS.map((k) => DRIVER_LABELS[k]);
    const values = BRIDGE_DRIVER_KEYS.map((k) => drivers[k]);
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
});
