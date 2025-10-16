async function getJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

function qs() {
  const start = document.getElementById('start').value.trim();
  const end = document.getElementById('end').value.trim();
  const rush = document.getElementById('rush').value.trim();
  const minp = document.getElementById('minp').value.trim();
  const maxp = document.getElementById('maxp').value.trim();

  const parts = [];
  if (start) parts.push(`start=${encodeURIComponent(start)}`);
  if (end) parts.push(`end=${encodeURIComponent(end)}`);
  if (rush) parts.push(`rush=${encodeURIComponent(rush)}`);
  if (minp) parts.push(`min_passengers=${encodeURIComponent(minp)}`);
  if (maxp) parts.push(`max_passengers=${encodeURIComponent(maxp)}`);
  return parts.length ? `?${parts.join('&')}` : '';
}

let hoursChart, durChart, speedChart;

async function refresh() {
  const q = qs();

  // KPIs
  const summary = await getJSON(`/api/summary${q}`);
  document.getElementById('trips').textContent = summary.trips ?? '0';
  const avgMin = summary.avg_duration_s ? (summary.avg_duration_s / 60).toFixed(1) : '0.0';
  document.getElementById('avgDur').textContent = avgMin;
  document.getElementById('avgKm').textContent = summary.avg_km ? Number(summary.avg_km).toFixed(2) : '0.00';
  document.getElementById('avgKmh').textContent = summary.avg_kmh ? Number(summary.avg_kmh).toFixed(1) : '0.0';

  // Busiest hours
  const top = await getJSON(`/api/busiest_hours${q ? (q.includes('?') ? '&' : '?') + q.slice(1) : ''}`);
  const hoursLabels = top.top.map(x => `${x.hour}:00`);
  const hoursData = top.top.map(x => x.trips);
  const ctxH = document.getElementById('hoursChart').getContext('2d');
  if (hoursChart) hoursChart.destroy();
  hoursChart = new Chart(ctxH, {
    type: 'bar',
    data: { labels: hoursLabels, datasets: [{ label: 'Trips', data: hoursData }] },
    options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } }
  });

  // Duration mix
  const dist = await getJSON(`/api/distribution${q}`);
  const durLabels = Object.keys(dist);
  const durData = Object.values(dist);
  const ctxD = document.getElementById('durChart').getContext('2d');
  if (durChart) durChart.destroy();
  durChart = new Chart(ctxD, {
    type: 'pie',
    data: { labels: durLabels, datasets: [{ data: durData }] },
    options: { responsive: true, plugins: { legend: { position: 'bottom' } } }
  });

  // Speed histogram (bin size fixed to 5 km/h; tweak by adding UI if desired)
  const hist = await getJSON(`/api/speeds_hist${q ? (q.includes('?') ? '&' : '?') + q.slice(1) : ''}`);
  const spLabels = hist.bins.map(b => b.label);
  const spData = hist.bins.map(b => b.count);
  const ctxS = document.getElementById('speedChart').getContext('2d');
  if (speedChart) speedChart.destroy();
  speedChart = new Chart(ctxS, {
    type: 'bar',
    data: { labels: spLabels, datasets: [{ label: 'Trips', data: spData }] },
    options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } }
  });
}

document.getElementById('refresh').addEventListener('click', () => refresh());
window.addEventListener('load', () => refresh());
