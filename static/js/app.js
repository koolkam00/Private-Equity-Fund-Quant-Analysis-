let dashboardBridgePayload = null;
let dashboardBridgeChart = null;
let dashboardMoicChart = null;
let dashboardVintageChart = null;

const DRIVER_LABELS = {
    revenue: 'Revenue Growth',
    margin: 'Margin Expansion',
    multiple: 'Multiple Expansion',
    leverage: 'Leverage / Debt Paydown',
    other: 'Residual / Other',
};

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
    const modelEl = document.querySelector(`.deal-bridge-model[data-deal-id='${dealId}']`);
    const unitEl = document.querySelector(`.deal-bridge-unit[data-deal-id='${dealId}']`);
    const basisEl = document.querySelector(`.deal-bridge-basis[data-deal-id='${dealId}']`);
    return {
        model: modelEl ? modelEl.value : 'additive',
        unit: unitEl ? unitEl.value : 'moic',
        basis: basisEl ? basisEl.value : 'fund',
    };
}

function loadDealBridge(dealId) {
    const canvas = document.getElementById(`deal-waterfall-${dealId}`);
    if (!canvas) return;

    const controls = getDealBridgeControls(dealId);
    const url = `${canvas.dataset.url}?model=${encodeURIComponent(controls.model)}&unit=${encodeURIComponent(controls.unit)}&basis=${encodeURIComponent(controls.basis)}`;

    fetch(url)
        .then((r) => r.json())
        .then((payload) => renderDealBridge(canvas, payload, controls));
}

function renderDealBridge(canvas, payload, controls) {
    const valuesMap = payload.drivers || {};
    const keys = ['revenue', 'margin', 'multiple', 'leverage', 'other'];
    const labels = keys.map((k) => DRIVER_LABELS[k]);
    const values = keys.map((k) => valuesMap[k] || 0);

    if (canvas._chart) {
        canvas._chart.destroy();
    }

    canvas._chart = new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                data: values,
                backgroundColor: values.map((v) => (v >= 0 ? '#0e7c66' : '#b83c4a')),
                borderRadius: 4,
                maxBarThickness: 42,
            }],
        },
        options: {
            ...chartBaseOptions(),
            plugins: {
                ...chartBaseOptions().plugins,
                legend: { display: false },
                tooltip: {
                    ...chartBaseOptions().plugins.tooltip,
                    callbacks: {
                        label: (ctx) => formatBridgeValue(ctx.raw, controls.unit),
                    },
                },
            },
            scales: {
                ...chartBaseOptions().scales,
                y: {
                    ...chartBaseOptions().scales.y,
                    ticks: {
                        ...chartBaseOptions().scales.y.ticks,
                        callback: (v) => formatBridgeValue(v, controls.unit),
                    },
                },
            },
        },
    });

    const diag = document.getElementById(`deal-bridge-diagnostics-${payload.deal_id}`);
    if (diag) {
        let txt = `${controls.model.toUpperCase()} | ${controls.basis.toUpperCase()} | ${controls.unit.toUpperCase()}`;
        if (payload.low_confidence_bridge) txt += ' | Low-confidence bridge';
        if (payload.ownership_pct !== null && payload.ownership_pct !== undefined) {
            txt += ` | Ownership ${(payload.ownership_pct * 100).toFixed(1)}%`;
        }
        diag.textContent = txt;
    }
}

function formatBridgeValue(v, unit) {
    if (v === null || v === undefined || Number.isNaN(v)) return '—';
    if (unit === 'dollar') return `$${v.toFixed(1)}M`;
    if (unit === 'moic') return `${v >= 0 ? '+' : ''}${v.toFixed(2)}x`;
    if (unit === 'pct') return `${v >= 0 ? '+' : ''}${(v * 100).toFixed(1)}%`;
    return String(v);
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
    document.querySelectorAll('.deal-bridge-model, .deal-bridge-unit, .deal-bridge-basis').forEach((el) => {
        el.addEventListener('change', (evt) => {
            const dealId = evt.target.dataset.dealId;
            loadDealBridge(dealId);
        });
    });
}

function renderDashboardCharts(payload) {
    dashboardBridgePayload = payload.bridge_aggregate || null;

    renderMoicDistribution(payload.moic_distribution || []);
    renderVintage(payload.vintage_series || []);
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

function renderVintage(data) {
    const el = document.getElementById('vintageChart');
    if (!el) return;
    if (dashboardVintageChart) dashboardVintageChart.destroy();

    dashboardVintageChart = new Chart(el, {
        type: 'line',
        data: {
            labels: data.map((d) => d.year),
            datasets: [{
                label: 'Average MOIC',
                data: data.map((d) => d.avg_moic),
                borderColor: '#0e7c66',
                backgroundColor: 'rgba(14, 124, 102, 0.16)',
                fill: true,
                tension: 0.3,
                pointRadius: 3,
                pointHoverRadius: 5,
                pointBackgroundColor: '#0e7c66',
            }],
        },
        options: {
            ...chartBaseOptions(),
            scales: {
                ...chartBaseOptions().scales,
                y: {
                    ...chartBaseOptions().scales.y,
                    ticks: {
                        ...chartBaseOptions().scales.y.ticks,
                        callback: (v) => `${Number(v).toFixed(2)}x`,
                    },
                },
            },
        },
    });
}

function renderBridgeAggregate() {
    if (!dashboardBridgePayload) return;

    const model = document.getElementById('bridgeModel')?.value || 'additive';
    const unit = document.getElementById('bridgeUnit')?.value || 'moic';

    const modelPayload = dashboardBridgePayload[model] || {};
    const drivers = ((modelPayload.drivers || {})[unit]) || {};

    const keys = ['revenue', 'margin', 'multiple', 'leverage', 'other'];
    const labels = keys.map((k) => DRIVER_LABELS[k]);
    const values = keys.map((k) => drivers[k] || 0);

    const el = document.getElementById('bridgeChart');
    if (!el) return;
    if (dashboardBridgeChart) dashboardBridgeChart.destroy();

    dashboardBridgeChart = new Chart(el, {
        type: 'bar',
        data: {
            labels,
            datasets: [{ data: values, backgroundColor: values.map((v) => (v >= 0 ? '#0e7c66' : '#b83c4a')), borderRadius: 5, maxBarThickness: 48 }],
        },
        options: {
            ...chartBaseOptions(),
            plugins: {
                ...chartBaseOptions().plugins,
                legend: { display: false },
                tooltip: {
                    ...chartBaseOptions().plugins.tooltip,
                    callbacks: { label: (ctx) => formatBridgeValue(ctx.raw, unit) },
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
        const details = dashboardBridgePayload.diagnostics || {};
        diag.textContent = `${model.toUpperCase()} model | Ready deals: ${modelPayload.ready_count || 0} | Low-confidence multiplicative bridges: ${details.low_confidence_count || 0}`;
    }
}

function attachDashboardBridgeControls() {
    const model = document.getElementById('bridgeModel');
    const unit = document.getElementById('bridgeUnit');
    if (model) model.addEventListener('change', renderBridgeAggregate);
    if (unit) unit.addEventListener('change', renderBridgeAggregate);
}

document.addEventListener('DOMContentLoaded', () => {
    attachTableSorting();
    attachDropZone();
    attachFlashDismiss();
    attachDealBridgeControls();

    const endpoint = document.getElementById('dashboard-series-endpoint');
    if (endpoint) {
        fetch(endpoint.dataset.url)
            .then((r) => r.json())
            .then((payload) => {
                renderDashboardCharts(payload);
                attachDashboardBridgeControls();
            });
    }
});
