const path = require('path');
const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const DB_PATH = process.env.FDS_DB_PATH || path.join(__dirname, '..', 'fds_bot.db');
const MONITOR_DB_PATH = process.env.MONITOR_DB_PATH || path.join(__dirname, '..', 'kick_monitor.sqlite3');

const db = new sqlite3.Database(DB_PATH, (err) => {
  if (err) console.error('Erro abrindo fds DB:', err.message);
});
const monitorDb = new sqlite3.Database(MONITOR_DB_PATH, sqlite3.OPEN_READONLY, (err) => {
  if (err) console.error('Erro abrindo monitor DB:', err.message);
});
const DEBUG = process.env.WEB_DASH_DEBUG === '1';
const fs = require('fs');

const SCREENSHOT_DIR = process.env.SCREENSHOT_DIR || path.join('/', 'data', 'screenshots');
const SCREENSHOT_DB_PATH = path.join(SCREENSHOT_DIR, 'screenshots.db');

function runAsync(dbInstance, sql, params=[]) {
  return new Promise((resolve, reject) => {
    dbInstance.run(sql, params, function(err) {
      if (err) return reject(err);
      resolve({ lastID: this.lastID, changes: this.changes });
    });
  });
}

function allAsync(dbInstance, sql, params=[]) {
  return new Promise((resolve, reject) => {
    dbInstance.all(sql, params, (err, rows) => {
      if (err) return reject(err);
      resolve(rows);
    });
  });
}

// initialize channels table
db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS channels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
  )`);
});

// ensure monitor tables exist (no-op if already present)
monitorDb.serialize(() => {
  monitorDb.run(`CREATE TABLE IF NOT EXISTS samples (id INTEGER PRIMARY KEY, channel TEXT, ts INTEGER, viewers INTEGER, is_live INTEGER, raw_json TEXT, session_id INTEGER)`);
  monitorDb.run(`CREATE TABLE IF NOT EXISTS sessions (id INTEGER PRIMARY KEY, channel TEXT, livestream_id TEXT, title TEXT, start_ts INTEGER, end_ts INTEGER, avg_viewers REAL, max_viewers INTEGER, sample_count INTEGER)`);
  monitorDb.run(`CREATE TABLE IF NOT EXISTS peaks (channel TEXT PRIMARY KEY, peak_overall INTEGER, peak_overall_ts INTEGER, peak_daily INTEGER, peak_daily_date TEXT, peak_weekly INTEGER, peak_week_start TEXT, peak_monthly INTEGER, peak_month TEXT)`);
});

app.get('/api/channels', async (req, res) => {
  try {
    const rows = await allAsync(db, 'SELECT id, name FROM channels ORDER BY name');
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/channels', async (req, res) => {
  const { name } = req.body;
  if (!name) return res.status(400).json({ error: 'name required' });
  try {
    const info = await runAsync(db, 'INSERT INTO channels (name) VALUES (?)', [name]);
    res.json({ id: info.lastID, name });
  } catch (err) {
    res.status(409).json({ error: 'exists' });
  }
});

app.put('/api/channels/:id', async (req, res) => {
  const id = req.params.id;
  const { name } = req.body;
  if (!name) return res.status(400).json({ error: 'name required' });
  try {
    const info = await runAsync(db, 'UPDATE channels SET name=? WHERE id=?', [name, id]);
    if (info.changes === 0) return res.status(404).json({ error: 'not found' });
    res.json({ id: Number(id), name });
  } catch (err) {
    res.status(409).json({ error: 'conflict' });
  }
});

app.delete('/api/channels/:id', async (req, res) => {
  const id = req.params.id;
  try {
    const info = await runAsync(db, 'DELETE FROM channels WHERE id=?', [id]);
    if (info.changes === 0) return res.status(404).json({ error: 'not found' });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// legacy monitor endpoints
app.get('/api/samples', async (req, res) => {
  try {
    const channel = req.query.channel;
    const limit = parseInt(req.query.limit || '200', 10);
    const since = req.query.since ? parseInt(req.query.since, 10) : 0;
    // if channel + since: return samples since timestamp, ordered ascending for timeline
    if (channel) {
      if (since > 0) {
        const rows = await allAsync(monitorDb, 'SELECT * FROM samples WHERE channel=? AND ts>=? ORDER BY ts ASC LIMIT ?', [channel, since, limit]);
  if (DEBUG) console.log(`[api/samples] channel=${channel} since=${since} limit=${limit} rows=${rows.length}`);
        return res.json(rows);
      }
      const rows = await allAsync(monitorDb, 'SELECT * FROM samples WHERE channel=? ORDER BY ts DESC LIMIT ?', [channel, limit]);
  if (DEBUG) console.log(`[api/samples] channel=${channel} since=0 limit=${limit} rows=${rows.length}`);
      return res.json(rows);
    }
    // no channel filter
    if (since > 0) {
      const rows = await allAsync(monitorDb, 'SELECT * FROM samples WHERE ts>=? ORDER BY ts ASC LIMIT ?', [since, limit]);
  if (DEBUG) console.log(`[api/samples] channel=ALL since=${since} limit=${limit} rows=${rows.length}`);
      return res.json(rows);
    }
    const rows = await allAsync(monitorDb, 'SELECT * FROM samples ORDER BY ts DESC LIMIT ?', [limit]);
  if (DEBUG) console.log(`[api/samples] channel=ALL since=0 limit=${limit} rows=${rows.length}`);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// diagnostic/meta for samples per channel
app.get('/api/samples/meta', async (req, res) => {
  try {
    const channel = req.query.channel;
    if (!channel) return res.status(400).json({ error: 'channel required' });
    const rows = await allAsync(monitorDb, 'SELECT COUNT(*) as cnt, MIN(ts) as min_ts, MAX(ts) as max_ts FROM samples WHERE channel=?', [channel]);
    const meta = rows && rows[0] ? rows[0] : { cnt:0, min_ts:null, max_ts:null };
    res.json(meta);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/sessions', async (req, res) => {
  try {
    const channel = req.query.channel;
    const limit = parseInt(req.query.limit || '100', 10);
    if (channel) {
      const rows = await allAsync(monitorDb, 'SELECT * FROM sessions WHERE channel=? ORDER BY start_ts DESC LIMIT ?', [channel, limit]);
      return res.json(rows);
    }
    const rows = await allAsync(monitorDb, 'SELECT * FROM sessions ORDER BY start_ts DESC LIMIT ?', [limit]);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/session/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const rows = await allAsync(monitorDb, 'SELECT * FROM sessions WHERE id=?', [id]);
    if (!rows || rows.length === 0) return res.status(404).json({ error: 'not found' });
    res.json(rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/peaks', async (req, res) => {
  try {
    const channel = req.query.channel;
    if (channel) {
      const rows = await allAsync(monitorDb, 'SELECT * FROM peaks WHERE channel=?', [channel]);
      return res.json(rows);
    }
    const rows = await allAsync(monitorDb, 'SELECT * FROM peaks');
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// live summary: latest sample per channel where is_live=1
app.get('/api/live-summary', async (req, res) => {
  try {
    const sql = `
      SELECT s.channel, s.viewers, s.ts
      FROM samples s
      JOIN (
        SELECT channel, MAX(ts) AS maxts FROM samples GROUP BY channel
      ) m ON s.channel = m.channel AND s.ts = m.maxts
      WHERE s.is_live = 1 AND s.viewers IS NOT NULL
      ORDER BY s.viewers DESC
    `;
    const rows = await allAsync(monitorDb, sql, []);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// timeseries aggregated per channel into fixed resolution buckets
app.get('/api/timeseries', async (req, res) => {
  try {
    const since = req.query.since ? parseInt(req.query.since, 10) : Math.floor(Date.now()/1000) - 10800; // default 3h
    const resolution = req.query.resolution ? parseInt(req.query.resolution, 10) : 60; // seconds
    const start = Math.floor(since / resolution) * resolution;
    const end = Math.floor(Date.now()/1000 / resolution) * resolution;
    const buckets = [];
    for (let t = start; t <= end; t += resolution) buckets.push(t);

    // fetch relevant samples
    const rows = await allAsync(monitorDb, 'SELECT channel, ts, viewers, is_live FROM samples WHERE ts>=? ORDER BY ts ASC', [start]);

    // build map channel -> bucket -> last sample
    const channelMap = new Map();
    for (const r of rows) {
      const ch = r.channel;
      const b = Math.floor(r.ts / resolution) * resolution;
      if (!channelMap.has(ch)) channelMap.set(ch, new Map());
      // overwrite so last sample in bucket remains
      channelMap.get(ch).set(b, { viewers: r.viewers, is_live: r.is_live });
    }

    const datasets = [];
    for (const [ch, map] of channelMap.entries()) {
      const data = buckets.map(b => {
        const s = map.get(b);
        if (!s) return null;
        return (s.is_live && Number(s.is_live) > 0) ? s.viewers : 0;
      });
      datasets.push({ channel: ch, data });
    }

    // labels as localized strings
    const labels = buckets.map(b => new Date(b * 1000).toLocaleString());
    res.json({ labels, datasets });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// channel-level aggregated metrics (samples within since_ts)
app.get('/api/channel/metrics', async (req, res) => {
  try {
    const channel = req.query.channel;
    if (!channel) return res.status(400).json({ error: 'channel required' });
    const since = parseInt(req.query.since || '0', 10);
    // aggregate samples
    const params = since > 0 ? [channel, since] : [channel];
    const sampleSql = since > 0 ? 'SELECT COUNT(*) as count, AVG(viewers) as avg_viewers, MAX(viewers) as max_viewers, MAX(ts) as last_ts FROM samples WHERE channel=? AND ts>=?' : 'SELECT COUNT(*) as count, AVG(viewers) as avg_viewers, MAX(viewers) as max_viewers, MAX(ts) as last_ts FROM samples WHERE channel=?';
    const sRows = await allAsync(monitorDb, sampleSql, params);
    const samplesAgg = sRows && sRows[0] ? sRows[0] : { count:0, avg_viewers:null, max_viewers:null, last_ts:null };
    // sessions count and details
    const sessParams = since > 0 ? [channel, since] : [channel];
    const sessSql = since > 0 ? 'SELECT COUNT(*) as sessions_count FROM sessions WHERE channel=? AND start_ts>=?' : 'SELECT COUNT(*) as sessions_count FROM sessions WHERE channel=?';
    const sessRows = await allAsync(monitorDb, sessSql, sessParams);
    const sessionsCount = sessRows && sessRows[0] ? sessRows[0].sessions_count : 0;
    // peaks
    const peaks = await allAsync(monitorDb, 'SELECT * FROM peaks WHERE channel=?', [channel]);
    res.json({ channel, samples: samplesAgg, sessions_count: sessionsCount, peaks: peaks });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// streamer trend metrics endpoint
app.get('/api/streamer/metrics', async (req, res) => {
  try {
    const channel = req.query.channel;
    const minutes = parseInt(req.query.minutes || '10', 10);
    if (!channel) return res.status(400).json({ error: 'channel required' });

    // find latest ts for channel
    const rows = await allAsync(monitorDb, 'SELECT MAX(ts) as maxts FROM samples WHERE channel=?', [channel]);
    const maxts = rows && rows[0] ? rows[0].maxts : null;
    if (!maxts) return res.json({ channel, error: 'no samples' });
    const latest = Number(maxts);
    const window = minutes * 60;
    const recent_since = latest - window;
    const prev_since = recent_since - window;
    const prev_until = recent_since - 1;

    const recent = await allAsync(monitorDb, 'SELECT ts, viewers, is_live FROM samples WHERE channel=? AND ts>=? AND ts<=? ORDER BY ts ASC', [channel, recent_since, latest]);
    const prev = await allAsync(monitorDb, 'SELECT ts, viewers, is_live FROM samples WHERE channel=? AND ts>=? AND ts<=? ORDER BY ts ASC', [channel, prev_since, prev_until]);

    function avgPeak(rows){
      const vs = rows.map(r=>Number(r.viewers)||0).filter(x=>x>=0);
      if (!vs.length) return {avg:0, peak:0, median:0, sd:0};
      const avg = vs.reduce((a,b)=>a+b,0)/vs.length;
      const peak = Math.max(...vs);
      vs.sort((a,b)=>a-b);
      const median = vs[Math.floor(vs.length/2)];
      const sd = vs.length>1? Math.sqrt(vs.map(x=>Math.pow(x-avg,2)).reduce((a,b)=>a+b,0)/(vs.length-1)):0;
      return {avg, peak, median, sd};
    }

    function slopePerMin(rows){
      if (rows.length<2) return 0;
      const xs = rows.map(r=>(r.ts - rows[0].ts)/60.0);
      const ys = rows.map(r=>Number(r.viewers)||0);
      const meanX = xs.reduce((a,b)=>a+b,0)/xs.length;
      const meanY = ys.reduce((a,b)=>a+b,0)/ys.length;
      const num = xs.map((x,i)=> (x-meanX)*(ys[i]-meanY)).reduce((a,b)=>a+b,0);
      const den = xs.map(x=>Math.pow(x-meanX,2)).reduce((a,b)=>a+b,0);
      if (den===0) return 0;
      return num/den;
    }

    const recentAgg = avgPeak(recent);
    const prevAgg = avgPeak(prev);
    const pct = prevAgg.avg===0 ? (recentAgg.avg>0? 100 : 0) : (recentAgg.avg - prevAgg.avg)/prevAgg.avg*100;
    const slope = slopePerMin(recent);
    const uptime = recent.length? (recent.filter(r=>Number(r.is_live)).length / recent.length) : 0;

    res.json({ channel, minutes, latest, recent_count: recent.length, recent_avg: recentAgg.avg, recent_peak: recentAgg.peak, prev_count: prev.length, prev_avg: prevAgg.avg, pct_change: pct, slope_per_min: slope, uptime_ratio: uptime });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// Serve screenshots metadata from the screenshot worker volume
app.get('/screenshots', async (req, res) => {
  try {
    // prefer the screenshots DB if present
    if (fs.existsSync(SCREENSHOT_DB_PATH)) {
      const sdb = new sqlite3.Database(SCREENSHOT_DB_PATH, sqlite3.OPEN_READONLY);
      sdb.all('SELECT id, channel, path, ts, width, height, status FROM screenshots ORDER BY ts DESC LIMIT 500', [], (err, rows) => {
        sdb.close();
        if (err) return res.status(500).json({ error: err.message });
        // map path to URL under /screenshots/files
        const mapped = rows.map(r => ({ ...r, url: `/screenshots/files/${encodeURIComponent(path.basename(r.path))}` }));
        res.json(mapped);
      });
      return;
    }

    // fallback: list files from directory
    if (!fs.existsSync(SCREENSHOT_DIR)) return res.json([]);
    const files = fs.readdirSync(SCREENSHOT_DIR).filter(f => f.match(/\.jpe?g$|\.png$/i)).sort().reverse();
    const mapped = files.slice(0,500).map(f => ({ file: f, url: `/screenshots/files/${encodeURIComponent(f)}` }));
    res.json(mapped);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// static serve the screenshots files from the mounted volume (if available)
if (fs.existsSync(SCREENSHOT_DIR)) {
  app.use('/screenshots/files', express.static(SCREENSHOT_DIR, { index: false, dotfiles: 'ignore' }));
} else {
  // create route that attempts to serve from /data/screenshots by resolving at request time
  app.use('/screenshots/files', (req, res, next) => {
    const file = decodeURIComponent(req.path.replace(/^\//, ''));
    const full = path.join(SCREENSHOT_DIR, file);
    if (fs.existsSync(full)) return res.sendFile(full);
    res.status(404).end();
  });
}

const port = process.env.PORT || 3000;
const host = process.env.HOST || '0.0.0.0';
app.listen(port, host, () => console.log(`Server listening on http://${host}:${port}`));
