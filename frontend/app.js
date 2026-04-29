const API_URL  = window.ENV?.API_URL  || 'http://localhost:8000';
const TILE_URL = window.ENV?.TILE_URL || 'http://localhost:3000';

const COLORS = {
  green:  '#22c55e',
  yellow: '#f59e0b',
  red:    '#ef4444',
};

const LABELS = {
  green:  'Good access',
  yellow: 'Moderate',
  red:    'Poor access',
};

let map;
let pathLayer  = null;
let stopLayer  = null;

// ── Init after page + Geoportail loader are both ready ──────────────────────
window.onload = function () {
  map = new lux.Map({
    target:   'map',
    bgLayer:  'basemap_2015_global',
    zoom:     12,
    position: [76771, 72205],  // Luxembourg centre in EPSG:2169
  });

  map.addLayer(buildRoadLayer());
  map.addLayer(buildAccessLayer());
  setupInteractions();
  setupSearch();
};

// ── Road links MVT layer (INSPIRE bus transport network) ─────────────────────
function buildRoadLayer() {
  return new ol.layer.VectorTile({
    source: new ol.source.VectorTile({
      format: new ol.format.MVT(),
      url: `${TILE_URL}/road_links/{z}/{x}/{y}`,
    }),
    style: new ol.style.Style({
      stroke: new ol.style.Stroke({ color: '#3b82f6', width: 1.5, lineDash: [4, 3] }),
    }),
  });
}

// ── Martin MVT layer ─────────────────────────────────────────────────────────
function buildAccessLayer() {
  const styleCache = {};

  return new ol.layer.VectorTile({
    source: new ol.source.VectorTile({
      format: new ol.format.MVT(),
      url:    `${TILE_URL}/residence_accessibility/{z}/{x}/{y}`,
    }),
    style: function (feature) {
      const cls = feature.get('color_class');
      if (!styleCache[cls]) {
        styleCache[cls] = new ol.style.Style({
          image: new ol.style.Circle({
            radius: 5,
            fill:   new ol.style.Fill({ color: COLORS[cls] ?? '#888' }),
            stroke: new ol.style.Stroke({ color: '#fff', width: 0.8 }),
          }),
        });
      }
      return styleCache[cls];
    },
  });
}

// ── Walk path overlay ────────────────────────────────────────────────────────
function clearPath() {
  if (pathLayer) { map.removeLayer(pathLayer); pathLayer = null; }
  if (stopLayer) { map.removeLayer(stopLayer); stopLayer = null; }
}

async function fetchAndDrawPath(addressId) {
  clearPath();
  try {
    const res = await fetch(`${API_URL}/path/${addressId}`);
    if (!res.ok) return;
    drawPath(await res.json());
  } catch { /* non-critical — silently skip */ }
}

function drawPath(data) {
  const proj = map.getView().getProjection();
  const color = COLORS[data.color_class] ?? '#888';

  // Walking path line
  const pathFeature = new ol.format.GeoJSON().readFeature(data.path, {
    dataProjection:    'EPSG:4326',
    featureProjection: proj,
  });
  pathLayer = new ol.layer.Vector({
    source: new ol.source.Vector({ features: [pathFeature] }),
    style: new ol.style.Style({
      stroke: new ol.style.Stroke({ color, width: 4, lineDash: [8, 4] }),
    }),
    zIndex: 10,
  });
  map.addLayer(pathLayer);

  // Stop location marker
  const stopCoords = ol.proj.transform(
    data.stop_location.coordinates, 'EPSG:4326', proj,
  );
  stopLayer = new ol.layer.Vector({
    source: new ol.source.Vector({
      features: [new ol.Feature({ geometry: new ol.geom.Point(stopCoords) })],
    }),
    style: new ol.style.Style({
      image: new ol.style.Circle({
        radius: 8,
        fill:   new ol.style.Fill({ color: '#1d4ed8' }),
        stroke: new ol.style.Stroke({ color: '#fff', width: 2 }),
      }),
    }),
    zIndex: 11,
  });
  map.addLayer(stopLayer);
}

// ── Map interactions ─────────────────────────────────────────────────────────
function setupInteractions() {
  map.on('pointermove', e => {
    map.getTargetElement().style.cursor =
      map.hasFeatureAtPixel(e.pixel) ? 'pointer' : '';
  });

  map.on('click', e => {
    map.forEachFeatureAtPixel(e.pixel, feature => {
      const props = feature.getProperties();
      showInfoPanel(props);
      if (props.id && props.color_class) fetchAndDrawPath(props.id);
      return true;
    });
  });
}

// ── Info panel ───────────────────────────────────────────────────────────────
function showInfoPanel(props) {
  document.getElementById('info-address').textContent  = props.address   ?? '—';
  document.getElementById('info-stop').textContent     = props.stop_name ?? '—';
  document.getElementById('info-distance').textContent =
    props.distance_m != null ? `${props.distance_m} m` : '—';

  const roadEl  = document.getElementById('info-road');
  const roadRow = document.getElementById('info-road-row');
  if (props.road_connected == null) {
    roadRow.hidden = true;
  } else {
    roadRow.hidden    = false;
    roadEl.textContent = props.road_connected ? 'Connected' : 'No overlap detected';
    roadEl.className   = `info-value road-${props.road_connected ? 'ok' : 'warn'}`;
  }

  const badge = document.getElementById('info-badge');
  badge.className   = `info-badge ${props.color_class}`;
  badge.textContent = LABELS[props.color_class] ?? props.color_class;

  document.getElementById('info-panel').hidden = false;
}

document.getElementById('info-close').addEventListener('click', () => {
  document.getElementById('info-panel').hidden = true;
  clearPath();
});

// ── Search ───────────────────────────────────────────────────────────────────
function setupSearch() {
  const input   = document.getElementById('search-input');
  const results = document.getElementById('search-results');
  let debounce;

  input.addEventListener('input', () => {
    clearTimeout(debounce);
    const q = input.value.trim();
    if (q.length < 3) { hideResults(); return; }
    debounce = setTimeout(() => fetchResults(q), 300);
  });

  input.addEventListener('keydown', e => {
    if (e.key === 'Escape') { hideResults(); input.blur(); }
  });

  document.addEventListener('click', e => {
    if (!e.target.closest('#search-box')) hideResults();
  });

  function hideResults() {
    results.hidden = true;
    results.innerHTML = '';
  }

  async function fetchResults(q) {
    try {
      const res = await fetch(
        `${API_URL}/search?q=${encodeURIComponent(q)}&limit=8`
      );
      if (!res.ok) { hideResults(); return; }
      renderResults(await res.json());
    } catch {
      hideResults();
    }
  }

  function renderResults(items) {
    results.innerHTML = '';
    if (!items.length) { hideResults(); return; }
    items.forEach(item => {
      const div = document.createElement('div');
      div.className = 'search-result-item';
      div.innerHTML =
        `<span class="dot ${item.color_class}"></span><span>${item.address}</span>`;
      div.addEventListener('click', () => selectResult(item));
      results.appendChild(div);
    });
    results.hidden = false;
  }

  function selectResult(item) {
    hideResults();
    input.value = item.address;

    map.getView().animate({
      center:   ol.proj.fromLonLat([item.longitude, item.latitude]),
      zoom:     17,
      duration: 700,
    });

    showInfoPanel(item);
    fetchAndDrawPath(item.id);
  }
}
