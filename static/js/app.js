/* ============================================
   PE Fund Tracker — JavaScript
   ============================================ */

/* --- Expand/Collapse Deal Detail Rows --- */
function toggleDetail(btn) {
    var row = btn.closest('tr');
    var dealId = row.dataset.dealId;
    var detailRow = document.getElementById('detail-' + dealId);
    var icon = btn.querySelector('i');

    if (detailRow.style.display === 'none' || detailRow.style.display === '') {
        detailRow.style.display = 'table-row';
        icon.classList.remove('bi-chevron-right');
        icon.classList.add('bi-chevron-down');
        row.classList.add('expanded');
    } else {
        detailRow.style.display = 'none';
        icon.classList.remove('bi-chevron-down');
        icon.classList.add('bi-chevron-right');
        row.classList.remove('expanded');
    }
}

/* --- Table Column Sorting --- */
document.addEventListener('DOMContentLoaded', function () {
    document.querySelectorAll('.sortable th[data-sort]').forEach(function (header) {
        header.addEventListener('click', function () {
            var table = header.closest('table');
            var tbody = table.querySelector('tbody');
            var colIndex = Array.from(header.parentNode.children).indexOf(header);
            var sortType = header.dataset.sort;
            var currentDir = header.dataset.dir || 'asc';
            var newDir = currentDir === 'asc' ? 'desc' : 'asc';

            // Reset all header indicators
            table.querySelectorAll('th[data-sort]').forEach(function (th) {
                th.dataset.dir = '';
                th.classList.remove('sort-asc', 'sort-desc');
            });

            header.dataset.dir = newDir;
            header.classList.add('sort-' + newDir);

            // Collect primary rows (not detail rows)
            var rows = Array.from(tbody.querySelectorAll('tr.deal-row'));

            rows.sort(function (a, b) {
                var aCell = a.children[colIndex];
                var bCell = b.children[colIndex];
                var aVal, bVal;

                if (sortType === 'number') {
                    aVal = parseFloat(aCell.dataset.value || aCell.textContent.replace(/[$,%xM—]/g, '')) || 0;
                    bVal = parseFloat(bCell.dataset.value || bCell.textContent.replace(/[$,%xM—]/g, '')) || 0;
                } else if (sortType === 'date') {
                    aVal = aCell.dataset.value || aCell.textContent.trim();
                    bVal = bCell.dataset.value || bCell.textContent.trim();
                    aVal = aVal === '—' ? '' : aVal;
                    bVal = bVal === '—' ? '' : bVal;
                } else {
                    aVal = aCell.textContent.trim().toLowerCase();
                    bVal = bCell.textContent.trim().toLowerCase();
                }

                if (aVal < bVal) return newDir === 'asc' ? -1 : 1;
                if (aVal > bVal) return newDir === 'asc' ? 1 : -1;
                return 0;
            });

            // Re-append rows keeping detail rows paired
            rows.forEach(function (row) {
                tbody.appendChild(row);
                var detailId = 'detail-' + row.dataset.dealId;
                var detailRow = document.getElementById(detailId);
                if (detailRow) {
                    tbody.appendChild(detailRow);
                }
            });
        });
    });
});

/* --- Drag-and-Drop File Zones --- */
document.addEventListener('DOMContentLoaded', function () {
    document.querySelectorAll('.drop-zone').forEach(function (zone) {
        var input = zone.querySelector('.drop-input');
        var nameDisplay = zone.querySelector('.file-name');

        ['dragenter', 'dragover'].forEach(function (evt) {
            zone.addEventListener(evt, function (e) {
                e.preventDefault();
                zone.classList.add('drag-over');
            });
        });

        ['dragleave', 'drop'].forEach(function (evt) {
            zone.addEventListener(evt, function (e) {
                e.preventDefault();
                zone.classList.remove('drag-over');
            });
        });

        zone.addEventListener('drop', function (e) {
            if (e.dataTransfer.files.length > 0) {
                input.files = e.dataTransfer.files;
                if (nameDisplay) {
                    nameDisplay.textContent = e.dataTransfer.files[0].name;
                }
            }
        });

        input.addEventListener('change', function () {
            if (input.files.length > 0 && nameDisplay) {
                nameDisplay.textContent = input.files[0].name;
            }
        });
    });
});

/* --- Flash Message Auto-Dismiss --- */
document.addEventListener('DOMContentLoaded', function () {
    document.querySelectorAll('.flash-message').forEach(function (msg) {
        setTimeout(function () {
            msg.style.opacity = '0';
            msg.style.transform = 'translateY(-10px)';
            setTimeout(function () {
                msg.remove();
            }, 300);
        }, 8000);
    });
});
