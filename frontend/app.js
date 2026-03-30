// ── Config ──
const API_BASE = '/api';
const API_KEY = 'di_fa12c0ad9a85ffdb9b84fdf2ec86bcbd7dc9f6a581354412764a3220913964d0';

const MAP_STYLE = {
    version: 8,
    name: 'Dark',
    sources: {
        'osm-tiles': {
            type: 'raster',
            tiles: [
                'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
                'https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
                'https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
            ],
            tileSize: 256,
            attribution: '&copy; <a href="https://carto.com/">CARTO</a> &copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>',
        },
    },
    layers: [
        { id: 'osm-tiles', type: 'raster', source: 'osm-tiles', minzoom: 0, maxzoom: 19 },
    ],
};

const CONNECTOR_COLORS = {
    'DC Fast':             '#e74c3c',
    'Tesla Supercharger':  '#c0392b',
    'Level 2':             '#3498db',
    'Level 1':             '#95a5a6',
    'Unknown':             '#7f8c8d',
};

const BAR_COLORS = ['#f97316', '#fb923c', '#fbbf24', '#a3e635', '#34d399', '#22d3ee', '#818cf8', '#c084fc'];

// ── State ──
let map;
let allFeatures = [];
let totalStations = 0;
let totalCountries = 0;
let metaData = null;
let activeConnectorFilter = 'all';
let activeNetworkFilter = 'all';
let activeCountryFilter = 'all';
let minPowerFilter = 0;
let searchQuery = '';
let _fetchTimer = null;
let _sourceReady = false;
let _isLoading = false;
let _currentBreakdownTab = 'connector';

// AbortController for in-flight station requests (fix #3: race condition prevention)
let _stationAbortController = null;

// Animation frame IDs for counter animations (fix #4: cancel previous animations)
const _animationFrames = {};

// ── API Helpers ──

/**
 * Fetch from API with error handling, abort support, and user-facing error toasts.
 * Returns parsed JSON on success, or null on failure (after showing a toast).
 */
async function apiFetch(endpoint, params = {}, { signal } = {}) {
    const url = new URL(endpoint, window.location.origin);
    Object.entries(params).forEach(([k, v]) => {
        if (v !== null && v !== undefined && v !== '') url.searchParams.set(k, v);
    });

    let resp;
    try {
        resp = await fetch(url.toString(), {
            headers: { 'X-API-Key': API_KEY },
            signal,
        });
    } catch (err) {
        // Aborted requests should fail silently — not a real error
        if (err.name === 'AbortError') return null;
        // Network failure (offline, DNS, timeout, etc.)
        showErrorToast('Network error — check your connection and try again.');
        console.warn('Fetch network error:', err);
        return null;
    }

    if (!resp.ok) {
        const body = await resp.json().catch(() => ({}));
        console.warn(`API ${resp.status}:`, body);

        if (resp.status === 429) {
            // Parse Retry-After header (seconds) when available
            const retryAfter = resp.headers.get('Retry-After');
            const secs = retryAfter ? parseInt(retryAfter, 10) : null;
            const msg = secs
                ? `Rate limited — retry in ${secs} second${secs === 1 ? '' : 's'}.`
                : 'Rate limited — please wait a moment and try again.';
            showErrorToast(msg);
        } else if (resp.status >= 500) {
            showErrorToast('Server error — please try again later.');
        } else {
            showErrorToast(`Request failed (${resp.status}).`);
        }
        return null;
    }

    return resp.json();
}

// ── Error Toast ──

/** Show a dismissible error toast at the bottom of the viewport. */
function showErrorToast(message) {
    const el = document.getElementById('error-toast');
    if (!el) return;
    el.textContent = message;
    el.classList.add('visible');
    // Auto-dismiss after 5 seconds
    clearTimeout(el._hideTimer);
    el._hideTimer = setTimeout(() => el.classList.remove('visible'), 5000);
}

// ── Offline Banner ──

/** Wire up online/offline listeners to show a persistent banner. */
function setupOfflineDetection() {
    const banner = document.getElementById('offline-banner');
    if (!banner) return;

    function update() {
        banner.classList.toggle('visible', !navigator.onLine);
    }
    window.addEventListener('online', update);
    window.addEventListener('offline', update);
    // Check initial state
    update();
}

// ── Init ──
document.addEventListener('DOMContentLoaded', () => {
    // Inject runtime UI elements (toast, offline banner, no-results)
    injectRuntimeUI();

    // Restore state from URL hash
    const hashState = parseHash();

    map = new maplibregl.Map({
        container: 'map',
        style: MAP_STYLE,
        center: hashState.center || [10, 30],
        zoom: hashState.zoom || 2,
    });

    map.addControl(new maplibregl.NavigationControl(), 'top-right');
    map.addControl(new maplibregl.GeolocateControl({
        positionOptions: { enableHighAccuracy: true },
        trackUserLocation: true,
    }), 'top-right');
    map.addControl(new maplibregl.ScaleControl(), 'bottom-right');

    if (map.isStyleLoaded()) {
        initApp(hashState);
    } else {
        map.on('style.load', () => initApp(hashState));
        setTimeout(() => { if (!_sourceReady) initApp(hashState); }, 3000);
    }

    setupFilters();
    setupMobile();
    setupShareExport();
    setupOfflineDetection();
    setupReportStation();

    // Restore filters from hash
    if (hashState.connector) {
        activeConnectorFilter = hashState.connector;
        document.querySelectorAll('.filter-chip').forEach(c => {
            c.classList.toggle('active', c.dataset.filter === hashState.connector);
        });
    }
    if (hashState.network) {
        activeNetworkFilter = hashState.network;
        document.getElementById('network-filter').value = hashState.network;
    }
    if (hashState.country) {
        activeCountryFilter = hashState.country;
        document.getElementById('country-filter').value = hashState.country;
    }
    if (hashState.power) {
        minPowerFilter = parseInt(hashState.power);
        document.getElementById('power-filter').value = minPowerFilter;
        document.getElementById('power-value').textContent = `${minPowerFilter} kW`;
    }
    if (hashState.q) {
        searchQuery = hashState.q;
        document.getElementById('search-input').value = hashState.q;
    }
});

/**
 * Inject elements that don't need to live in index.html (error toast,
 * offline banner, no-results overlay, reset-filters button).
 */
function injectRuntimeUI() {
    // Error toast
    const errorToast = document.createElement('div');
    errorToast.id = 'error-toast';
    errorToast.className = 'error-toast';
    errorToast.setAttribute('role', 'alert');
    document.body.appendChild(errorToast);

    // Offline banner
    const offlineBanner = document.createElement('div');
    offlineBanner.id = 'offline-banner';
    offlineBanner.className = 'offline-banner';
    offlineBanner.setAttribute('role', 'alert');
    offlineBanner.textContent = 'You are offline — data may be stale.';
    document.body.appendChild(offlineBanner);

    // "No stations found" overlay (positioned over the map)
    const noResults = document.createElement('div');
    noResults.id = 'no-results';
    noResults.className = 'no-results-overlay hidden';
    noResults.innerHTML = `
        <svg width="32" height="32" viewBox="0 0 24 24" fill="none" style="color:#f97316">
            <circle cx="11" cy="11" r="7" stroke="currentColor" stroke-width="2"/>
            <path d="M16.5 16.5L21 21" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
            <path d="M8 11h6" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
        </svg>
        <span>No stations found for current filters</span>
        <button id="reset-filters-btn" class="reset-filters-btn">Reset Filters</button>
    `;
    document.body.appendChild(noResults);

    // Wire reset-filters button
    document.getElementById('reset-filters-btn').addEventListener('click', resetAllFilters);
}

async function initApp(hashState) {
    if (_sourceReady) return;
    await loadMeta();

    // Load global overview first so dots appear immediately at any zoom
    const overview = await apiFetch(`${API_BASE}/stations/overview`);
    const overviewFeatures = overview ? (overview.features || []) : [];
    addMapLayers({ type: 'FeatureCollection', features: overviewFeatures });
    _sourceReady = true;

    // Then load detailed view for current viewport
    await loadStationsForView();

    map.on('moveend', () => {
        clearTimeout(_fetchTimer);
        _fetchTimer = setTimeout(() => {
            loadStationsForView();
            updateHash();
        }, 300);
    });

    // Zoom hint logic
    updateZoomHint();
    map.on('zoom', updateZoomHint);
}

// ── Metadata Loading ──
async function loadMeta() {
    const meta = await apiFetch(`${API_BASE}/stations/meta`);
    if (!meta) return;

    metaData = meta;
    totalStations = meta.total_stations || 0;
    totalCountries = (meta.countries || []).length;

    const networkSelect = document.getElementById('network-filter');
    (meta.networks || []).forEach(n => {
        const opt = document.createElement('option');
        opt.value = n;
        opt.textContent = n;
        networkSelect.appendChild(opt);
    });

    const countrySelect = document.getElementById('country-filter');
    (meta.countries || []).forEach(c => {
        const opt = document.createElement('option');
        opt.value = c;
        opt.textContent = c;
        countrySelect.appendChild(opt);
    });

    updateStats();
    renderBreakdown();
}

// ── Breakdown Charts ──
function renderBreakdown() {
    if (!metaData) return;
    const container = document.getElementById('breakdown-chart');
    container.innerHTML = '';

    let items = [];
    if (_currentBreakdownTab === 'connector') {
        items = (metaData.connector_categories || []).map(c => ({
            label: c,
            count: metaData.connector_counts?.[c] || 0,
            color: CONNECTOR_COLORS[c] || '#7f8c8d',
        }));
        // If API doesn't provide counts, show category labels only
        if (items.every(i => i.count === 0) && metaData.connector_categories?.length) {
            items = metaData.connector_categories.map((c, i) => ({
                label: c,
                count: Math.round(totalStations / metaData.connector_categories.length),
                color: CONNECTOR_COLORS[c] || BAR_COLORS[i % BAR_COLORS.length],
            }));
        }
    } else if (_currentBreakdownTab === 'country') {
        items = (metaData.countries || []).slice(0, 10).map((c, i) => ({
            label: c,
            count: metaData.country_counts?.[c] || 0,
            color: BAR_COLORS[i % BAR_COLORS.length],
        }));
    } else if (_currentBreakdownTab === 'network') {
        items = (metaData.networks || []).slice(0, 10).map((n, i) => ({
            label: n,
            count: metaData.network_counts?.[n] || 0,
            color: BAR_COLORS[i % BAR_COLORS.length],
        }));
    }

    if (items.length === 0) {
        container.innerHTML = '<div style="font-size:11px;color:#888;text-align:center;padding:12px">No data available</div>';
        return;
    }

    const max = Math.max(...items.map(i => i.count), 1);

    items.forEach(item => {
        const row = document.createElement('div');
        row.className = 'bar-row';
        const pct = Math.max((item.count / max) * 100, 2);
        // Fix #1 (XSS): escape item.label in both innerHTML and title attribute
        const safeLabel = escapeHtml(item.label);
        row.innerHTML = `
            <span class="bar-label" title="${safeLabel}">${safeLabel}</span>
            <div class="bar-track">
                <div class="bar-fill" style="width:${pct}%;background:${item.color}"></div>
            </div>
            <span class="bar-count">${item.count > 0 ? item.count.toLocaleString() : '-'}</span>
        `;
        container.appendChild(row);
    });
}

// ── Data Loading ──

/**
 * Load stations for the current map viewport.
 * Fix #3: Uses an AbortController so only the latest request wins.
 */
async function loadStationsForView() {
    if (!_sourceReady) return;

    // Cancel any in-flight request before starting a new one
    if (_stationAbortController) {
        _stationAbortController.abort();
    }
    _stationAbortController = new AbortController();
    const { signal } = _stationAbortController;

    const bounds = map.getBounds();
    const bbox = `${bounds.getWest()},${bounds.getSouth()},${bounds.getEast()},${bounds.getNorth()}`;

    const area = (bounds.getEast() - bounds.getWest()) * (bounds.getNorth() - bounds.getSouth());
    if (area > 100) {
        // At global zoom, show the overview layer (already loaded)
        updateStats(0);
        hideNoResults();
        return;
    }

    showLoading(true);

    const params = { bbox, limit: 500 };
    if (activeConnectorFilter !== 'all') params.connector_category = activeConnectorFilter;
    if (activeNetworkFilter !== 'all') params.network = activeNetworkFilter;
    if (activeCountryFilter !== 'all') params.country = activeCountryFilter;
    if (minPowerFilter > 0) params.min_power_kw = minPowerFilter;
    if (searchQuery) params.search = searchQuery;

    const data = await apiFetch(`${API_BASE}/stations`, params, { signal });

    // If this request was aborted, a newer one is in flight — bail out silently
    if (signal.aborted) return;

    showLoading(false);

    if (!data) return;

    allFeatures = data.features || [];

    const source = map.getSource('ev-stations');
    if (source) {
        source.setData({ type: 'FeatureCollection', features: allFeatures });
    }

    const meta = data.metadata || {};
    const visibleTotal = meta.total_in_bbox || allFeatures.length;
    const countEl = document.getElementById('visible-count');
    countEl.textContent = visibleTotal.toLocaleString();
    if (meta.has_more) countEl.textContent += '+';

    updateStats(visibleTotal);

    // Fix #2: Show "no stations found" when filters are active and 0 results
    if (visibleTotal === 0 && hasActiveFilters()) {
        showNoResults();
    } else {
        hideNoResults();
    }
}

function showLoading(on) {
    _isLoading = on;
    document.getElementById('loading-indicator').style.display = on ? 'block' : 'none';
}

// ── No-Results / Reset Filters Helpers ──

/** Returns true when any filter deviates from the default "all" state. */
function hasActiveFilters() {
    return activeConnectorFilter !== 'all'
        || activeNetworkFilter !== 'all'
        || activeCountryFilter !== 'all'
        || minPowerFilter > 0
        || searchQuery !== '';
}

function showNoResults() {
    const el = document.getElementById('no-results');
    if (el) el.classList.remove('hidden');
}

function hideNoResults() {
    const el = document.getElementById('no-results');
    if (el) el.classList.add('hidden');
}

/** Reset every filter to its default state and reload. */
function resetAllFilters() {
    activeConnectorFilter = 'all';
    activeNetworkFilter = 'all';
    activeCountryFilter = 'all';
    minPowerFilter = 0;
    searchQuery = '';

    // Reset UI controls
    document.querySelectorAll('.filter-chip').forEach(c => {
        c.classList.toggle('active', c.dataset.filter === 'all');
    });
    document.getElementById('network-filter').value = 'all';
    document.getElementById('country-filter').value = 'all';
    document.getElementById('power-filter').value = 0;
    document.getElementById('power-value').textContent = '0 kW';
    document.getElementById('search-input').value = '';
    document.getElementById('search-clear').style.display = 'none';

    hideNoResults();
    loadStationsForView();
    updateHash();
}

// ── Zoom Hint ──
function updateZoomHint() {
    const hint = document.getElementById('zoom-hint');
    const bounds = map.getBounds();
    const area = (bounds.getEast() - bounds.getWest()) * (bounds.getNorth() - bounds.getSouth());
    hint.classList.toggle('hidden', area <= 100);
}

// ── Map Layers ──
function addMapLayers(geojson) {
    map.addSource('ev-stations', {
        type: 'geojson',
        data: geojson,
        cluster: true,
        clusterMaxZoom: 14,
        clusterRadius: 50,
        clusterProperties: {
            sum_ports: ['+', ['coalesce', ['get', 'total_ports'], 0]],
        },
    });

    // Cluster circles
    map.addLayer({
        id: 'ev-clusters',
        type: 'circle',
        source: 'ev-stations',
        filter: ['has', 'point_count'],
        paint: {
            'circle-color': [
                'step', ['get', 'point_count'],
                '#84cc16', 20,
                '#eab308', 50,
                '#f59e0b', 150,
                '#f97316',
            ],
            'circle-radius': [
                'step', ['get', 'point_count'],
                18, 20, 24, 50, 30, 150, 38,
            ],
            'circle-stroke-width': 2,
            'circle-stroke-color': 'rgba(0,0,0,0.3)',
            'circle-opacity': 0.85,
        },
    });

    // Individual station points — color by connector + glow for high-power
    map.addLayer({
        id: 'ev-points-glow',
        type: 'circle',
        source: 'ev-stations',
        filter: ['all',
            ['!', ['has', 'point_count']],
            ['>=', ['coalesce', ['get', 'power_kw'], 0], 150],
        ],
        paint: {
            'circle-color': [
                'match', ['get', 'connector_category'],
                'DC Fast',            CONNECTOR_COLORS['DC Fast'],
                'Tesla Supercharger', CONNECTOR_COLORS['Tesla Supercharger'],
                CONNECTOR_COLORS['DC Fast'],
            ],
            'circle-radius': [
                'interpolate', ['linear'], ['zoom'], 0, 4, 3, 8, 8, 12, 14, 20,
            ],
            'circle-blur': 0.8,
            'circle-opacity': 0.25,
        },
    });

    map.addLayer({
        id: 'ev-points',
        type: 'circle',
        source: 'ev-stations',
        filter: ['!', ['has', 'point_count']],
        paint: {
            'circle-color': [
                'match', ['get', 'connector_category'],
                'DC Fast',            CONNECTOR_COLORS['DC Fast'],
                'Tesla Supercharger', CONNECTOR_COLORS['Tesla Supercharger'],
                'Level 2',            CONNECTOR_COLORS['Level 2'],
                'Level 1',            CONNECTOR_COLORS['Level 1'],
                CONNECTOR_COLORS['Unknown'],
            ],
            'circle-radius': [
                'interpolate', ['linear'], ['zoom'], 0, 2, 3, 4, 8, 6, 14, 10,
            ],
            'circle-stroke-width': 1.5,
            'circle-stroke-color': 'rgba(255,255,255,0.3)',
            'circle-opacity': 0.9,
        },
    });

    setupMapInteractions();
}

// ── Map Interactions ──
function setupMapInteractions() {
    map.on('click', 'ev-clusters', (e) => {
        const features = map.queryRenderedFeatures(e.point, { layers: ['ev-clusters'] });
        const clusterId = features[0].properties.cluster_id;
        map.getSource('ev-stations').getClusterExpansionZoom(clusterId, (err, zoom) => {
            if (err) return;
            map.easeTo({ center: features[0].geometry.coordinates, zoom });
        });
    });

    map.on('click', 'ev-points', (e) => {
        const feature = e.features[0];
        const p = feature.properties;
        const coords = feature.geometry.coordinates.slice();
        new maplibregl.Popup({ offset: 15, maxWidth: '320px' })
            .setLngLat(coords)
            .setHTML(buildPopupHTML(p, coords))
            .addTo(map);
    });

    map.on('mouseenter', 'ev-points', () => { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', 'ev-points', () => { map.getCanvas().style.cursor = ''; });
    map.on('mouseenter', 'ev-clusters', () => { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', 'ev-clusters', () => { map.getCanvas().style.cursor = ''; });
}

/**
 * Build popup HTML for a station feature.
 * Fix #1 (XSS): Every user-supplied property is escaped through escapeHtml().
 */
function buildPopupHTML(p, coords) {
    const cat = p.connector_category || 'Unknown';
    const badgeClass = cat === 'DC Fast' ? 'badge-dc'
        : cat === 'Tesla Supercharger' ? 'badge-tesla'
        : cat === 'Level 2' ? 'badge-l2' : 'badge-l1';

    let badges = `<span class="popup-badge ${badgeClass}">${escapeHtml(cat)}</span>`;
    if (p.status) badges += `<span class="popup-badge badge-status">${escapeHtml(String(p.status))}</span>`;
    if (p.power_kw) badges += `<span class="popup-badge badge-power">${escapeHtml(String(p.power_kw))} kW</span>`;

    let rows = '';
    if (p.network)         rows += `<tr><td>Network</td><td>${escapeHtml(String(p.network))}</td></tr>`;
    if (p.operator && p.operator !== p.network)
                           rows += `<tr><td>Operator</td><td>${escapeHtml(String(p.operator))}</td></tr>`;
    if (p.connector_types) rows += `<tr><td>Connectors</td><td>${escapeHtml(String(p.connector_types))}</td></tr>`;
    if (p.total_ports)     rows += `<tr><td>Ports</td><td>${escapeHtml(String(p.total_ports))}</td></tr>`;
    if (p.power_kw)        rows += `<tr><td>Max Power</td><td>${escapeHtml(String(p.power_kw))} kW</td></tr>`;
    if (p.usage_cost)      rows += `<tr><td>Cost</td><td>${escapeHtml(String(p.usage_cost))}</td></tr>`;
    if (p.access_type)     rows += `<tr><td>Access</td><td>${escapeHtml(String(p.access_type))}</td></tr>`;
    if (p.country)         rows += `<tr><td>Country</td><td>${escapeHtml(String(p.country))}</td></tr>`;
    if (p.source)          rows += `<tr><td>Source</td><td>${escapeHtml(String(p.source))}</td></tr>`;

    // Google Maps directions link
    const dirUrl = `https://www.google.com/maps/dir/?api=1&destination=${coords[1]},${coords[0]}`;

    return `
        <div class="popup-content">
            <h3 class="popup-name">${escapeHtml(p.station_name || 'Unknown Station')}</h3>
            <div class="popup-address">${escapeHtml(p.address || '')}</div>
            <div class="popup-badges">${badges}</div>
            <table class="popup-table">${rows}</table>
            <a class="popup-directions" href="${dirUrl}" target="_blank" rel="noopener">Get Directions</a>
        </div>
    `;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// ── Filters ──
function setupFilters() {
    // Connector chip filters — with keyboard support (fix #5: a11y)
    document.querySelectorAll('.filter-chip').forEach(chip => {
        chip.addEventListener('click', () => activateChip(chip));
        chip.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                activateChip(chip);
            }
        });
    });

    // Network dropdown
    document.getElementById('network-filter').addEventListener('change', (e) => {
        activeNetworkFilter = e.target.value;
        loadStationsForView();
        updateHash();
    });

    // Country dropdown
    document.getElementById('country-filter').addEventListener('change', (e) => {
        activeCountryFilter = e.target.value;
        loadStationsForView();
        updateHash();
    });

    // Power range slider
    const powerSlider = document.getElementById('power-filter');
    const powerValue = document.getElementById('power-value');
    powerSlider.addEventListener('input', (e) => {
        minPowerFilter = parseInt(e.target.value);
        powerValue.textContent = `${minPowerFilter} kW`;
    });
    powerSlider.addEventListener('change', () => {
        loadStationsForView();
        updateHash();
    });

    // Search input with debounce
    const searchInput = document.getElementById('search-input');
    const searchClear = document.getElementById('search-clear');
    let searchTimeout;

    searchInput.addEventListener('input', (e) => {
        clearTimeout(searchTimeout);
        const val = e.target.value;
        searchClear.style.display = val ? 'flex' : 'none';
        searchTimeout = setTimeout(() => {
            searchQuery = val.trim();
            loadStationsForView();
            updateHash();
        }, 500);
    });

    searchClear.addEventListener('click', () => {
        searchInput.value = '';
        searchClear.style.display = 'none';
        searchQuery = '';
        loadStationsForView();
        updateHash();
        searchInput.focus();
    });

    // Breakdown tabs
    document.querySelectorAll('.breakdown-tab').forEach(tab => {
        tab.addEventListener('click', () => activateBreakdownTab(tab));
    });
}

/** Activate a connector filter chip. */
function activateChip(chip) {
    document.querySelectorAll('.filter-chip').forEach(c => {
        c.classList.remove('active');
        c.setAttribute('aria-selected', 'false');
    });
    chip.classList.add('active');
    chip.setAttribute('aria-selected', 'true');
    activeConnectorFilter = chip.dataset.filter;
    loadStationsForView();
    updateHash();
}

/** Activate a breakdown tab. */
function activateBreakdownTab(tab) {
    document.querySelectorAll('.breakdown-tab').forEach(t => {
        t.classList.remove('active');
        t.setAttribute('aria-selected', 'false');
    });
    tab.classList.add('active');
    tab.setAttribute('aria-selected', 'true');
    _currentBreakdownTab = tab.dataset.tab;
    renderBreakdown();
}

// ── Mobile ──
function setupMobile() {
    const sidebar = document.getElementById('sidebar');
    const toggle = document.getElementById('sidebar-toggle');
    const closeBtn = document.getElementById('sidebar-close');

    // Create backdrop
    const backdrop = document.createElement('div');
    backdrop.className = 'sidebar-backdrop';
    document.body.appendChild(backdrop);

    function openSidebar() {
        sidebar.classList.add('open');
        backdrop.classList.add('visible');
    }

    function closeSidebar() {
        sidebar.classList.remove('open');
        backdrop.classList.remove('visible');
    }

    toggle.addEventListener('click', openSidebar);
    closeBtn.addEventListener('click', closeSidebar);
    backdrop.addEventListener('click', closeSidebar);
}

// ── Share & Export ──
function setupShareExport() {
    // Share button
    document.getElementById('share-btn').addEventListener('click', () => {
        updateHash();
        const url = window.location.href;
        navigator.clipboard.writeText(url).then(() => {
            const toast = document.getElementById('share-toast');
            toast.classList.add('visible');
            setTimeout(() => toast.classList.remove('visible'), 2500);
        }).catch(() => {
            // Fallback
            prompt('Copy this shareable link:', url);
        });
    });

    // Export CSV button
    document.getElementById('export-btn').addEventListener('click', () => {
        if (allFeatures.length === 0) return;
        exportCSV(allFeatures);
    });
}

function exportCSV(features) {
    const headers = ['name', 'address', 'lat', 'lng', 'connector_type', 'network', 'operator', 'power_kw', 'ports', 'country', 'source'];
    const rows = features.map(f => {
        const p = f.properties || {};
        const [lng, lat] = f.geometry?.coordinates || [0, 0];
        return [
            p.station_name || '',
            p.address || '',
            lat,
            lng,
            p.connector_category || '',
            p.network || '',
            p.operator || '',
            p.power_kw || '',
            p.total_ports || '',
            p.country || '',
            p.source || '',
        ].map(v => `"${String(v).replace(/"/g, '""')}"`).join(',');
    });

    const csv = [headers.join(','), ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `ev_stations_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
}

// ── URL Hash (shareable state) ──
function updateHash() {
    const center = map.getCenter();
    const parts = [
        `lat=${center.lat.toFixed(4)}`,
        `lng=${center.lng.toFixed(4)}`,
        `z=${map.getZoom().toFixed(1)}`,
    ];
    if (activeConnectorFilter !== 'all') parts.push(`connector=${encodeURIComponent(activeConnectorFilter)}`);
    if (activeNetworkFilter !== 'all') parts.push(`network=${encodeURIComponent(activeNetworkFilter)}`);
    if (activeCountryFilter !== 'all') parts.push(`country=${encodeURIComponent(activeCountryFilter)}`);
    if (minPowerFilter > 0) parts.push(`power=${minPowerFilter}`);
    if (searchQuery) parts.push(`q=${encodeURIComponent(searchQuery)}`);

    history.replaceState(null, '', '#' + parts.join('&'));
}

function parseHash() {
    const hash = window.location.hash.slice(1);
    if (!hash) return {};
    const params = {};
    hash.split('&').forEach(pair => {
        const [k, v] = pair.split('=');
        if (k && v) params[k] = decodeURIComponent(v);
    });

    const result = {};
    if (params.lat && params.lng) result.center = [parseFloat(params.lng), parseFloat(params.lat)];
    if (params.z) result.zoom = parseFloat(params.z);
    if (params.connector) result.connector = params.connector;
    if (params.network) result.network = params.network;
    if (params.country) result.country = params.country;
    if (params.power) result.power = params.power;
    if (params.q) result.q = params.q;
    return result;
}

// ── Stats ──
function updateStats(visibleCount) {
    animateCounter('total-count', totalStations);
    document.getElementById('country-count').textContent = totalCountries;

    if (visibleCount !== undefined) {
        animateCounter('visible-count', visibleCount);
    }
}

// ── Report a Station ──

let _reportMarker = null;
let _reportMapClickHandler = null;
let _reportModalOpen = false;

function setupReportStation() {
    const btn = document.getElementById('report-station-btn');
    const backdrop = document.getElementById('report-modal-backdrop');
    const closeBtn = document.getElementById('report-modal-close');
    const form = document.getElementById('report-form');

    btn.addEventListener('click', openReportModal);
    closeBtn.addEventListener('click', closeReportModal);
    backdrop.addEventListener('click', (e) => {
        if (e.target === backdrop) closeReportModal();
    });

    // Escape key closes modal
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && _reportModalOpen) closeReportModal();
    });

    form.addEventListener('submit', handleReportSubmit);
}

function openReportModal() {
    const backdrop = document.getElementById('report-modal-backdrop');
    backdrop.style.display = 'flex';
    // Force reflow so CSS transition triggers
    void backdrop.offsetHeight;
    backdrop.classList.add('open');
    _reportModalOpen = true;

    // Enable map click to set lat/lng
    _reportMapClickHandler = (e) => {
        const { lng, lat } = e.lngLat;
        document.getElementById('report-lat').value = lat.toFixed(6);
        document.getElementById('report-lng').value = lng.toFixed(6);

        // Place or move marker
        if (_reportMarker) {
            _reportMarker.setLngLat([lng, lat]);
        } else {
            const el = document.createElement('div');
            el.className = 'report-marker';
            _reportMarker = new maplibregl.Marker({ element: el, draggable: true })
                .setLngLat([lng, lat])
                .addTo(map);
            _reportMarker.on('dragend', () => {
                const pos = _reportMarker.getLngLat();
                document.getElementById('report-lat').value = pos.lat.toFixed(6);
                document.getElementById('report-lng').value = pos.lng.toFixed(6);
            });
        }
    };
    map.on('click', _reportMapClickHandler);

    // Highlight the map hint
    document.getElementById('report-map-hint').style.display = 'block';
}

function closeReportModal() {
    const backdrop = document.getElementById('report-modal-backdrop');
    backdrop.classList.remove('open');
    setTimeout(() => { backdrop.style.display = 'none'; }, 300);
    _reportModalOpen = false;

    // Remove map click handler
    if (_reportMapClickHandler) {
        map.off('click', _reportMapClickHandler);
        _reportMapClickHandler = null;
    }

    // Remove placement marker
    if (_reportMarker) {
        _reportMarker.remove();
        _reportMarker = null;
    }

    // Clear form state
    clearReportMessage();
}

async function handleReportSubmit(e) {
    e.preventDefault();
    const form = e.target;
    const submitBtn = document.getElementById('report-submit-btn');
    const msgEl = document.getElementById('report-message');

    // Build payload
    const payload = {
        station_name: form.station_name.value.trim(),
        latitude: parseFloat(form.latitude.value),
        longitude: parseFloat(form.longitude.value),
    };

    // Optional fields
    if (form.connector_type.value) payload.connector_type = form.connector_type.value;
    if (form.network.value.trim()) payload.network = form.network.value.trim();
    if (form.num_ports.value) payload.num_ports = parseInt(form.num_ports.value);
    if (form.address.value.trim()) payload.address = form.address.value.trim();
    if (form.submitter_email.value.trim()) payload.submitter_email = form.submitter_email.value.trim();
    if (form.notes.value.trim()) payload.notes = form.notes.value.trim();
    if (form.website && form.website.value) payload.website = form.website.value;

    // Validate
    if (!payload.station_name) {
        showReportMessage('Station name is required.', 'error');
        return;
    }
    if (isNaN(payload.latitude) || payload.latitude < -90 || payload.latitude > 90) {
        showReportMessage('Latitude must be between -90 and 90.', 'error');
        return;
    }
    if (isNaN(payload.longitude) || payload.longitude < -180 || payload.longitude > 180) {
        showReportMessage('Longitude must be between -180 and 180.', 'error');
        return;
    }

    submitBtn.disabled = true;
    submitBtn.textContent = 'Submitting...';
    clearReportMessage();

    try {
        const resp = await fetch(`${API_BASE}/stations/submit`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });

        const data = await resp.json();

        if (resp.ok) {
            showReportMessage('Station submitted for review! Thank you.', 'success');
            form.reset();
            // Close modal after a short delay
            setTimeout(() => {
                closeReportModal();
            }, 2000);
        } else {
            const detail = data.detail;
            const msg = typeof detail === 'string' ? detail
                : Array.isArray(detail) ? detail.map(d => d.msg || d).join(', ')
                : 'Submission failed. Please try again.';
            showReportMessage(msg, 'error');
        }
    } catch (err) {
        showReportMessage('Network error. Please check your connection.', 'error');
    } finally {
        submitBtn.disabled = false;
        submitBtn.textContent = 'Submit Station Report';
    }
}

function showReportMessage(text, type) {
    const el = document.getElementById('report-message');
    el.textContent = text;
    el.className = `report-message ${type}`;
}

function clearReportMessage() {
    const el = document.getElementById('report-message');
    el.textContent = '';
    el.className = 'report-message';
}

/**
 * Animate a numeric counter element from its current value to a target.
 * Fix #4: Cancels any previous animation on the same element before starting.
 */
function animateCounter(id, target) {
    const el = document.getElementById(id);
    const current = parseInt(el.textContent.replace(/,/g, '')) || 0;
    if (current === target) return;

    // Cancel any in-progress animation for this element
    if (_animationFrames[id]) {
        cancelAnimationFrame(_animationFrames[id]);
        _animationFrames[id] = null;
    }

    const diff = target - current;
    const steps = 20;
    const stepSize = diff / steps;
    let step = 0;

    function tick() {
        step++;
        if (step >= steps) {
            el.textContent = target.toLocaleString();
            _animationFrames[id] = null;
            return;
        }
        el.textContent = Math.round(current + stepSize * step).toLocaleString();
        _animationFrames[id] = requestAnimationFrame(tick);
    }
    _animationFrames[id] = requestAnimationFrame(tick);
}
