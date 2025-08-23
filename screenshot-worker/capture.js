#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer');
const Database = require('better-sqlite3');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const argv = yargs(hideBin(process.argv)).option('channels', {
  alias: 'c',
  type: 'string',
  description: 'Comma-separated list of URLs or channel slugs to capture',
}).argv;

const SCREENSHOT_DIR = process.env.SCREENSHOT_DIR || '/data/screenshots';
const DB_PATH = path.join(SCREENSHOT_DIR, 'screenshots.db');

if (!fs.existsSync(SCREENSHOT_DIR)) fs.mkdirSync(SCREENSHOT_DIR, { recursive: true });

const channels = (argv.channels || process.env.CHANNELS || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

function initDb() {
  const db = new Database(DB_PATH);
  db.exec(`
    CREATE TABLE IF NOT EXISTS screenshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      channel TEXT,
      path TEXT NOT NULL,
      ts INTEGER NOT NULL,
      width INTEGER,
      height INTEGER,
      status INTEGER
    );
  `);
  return db;
}

async function takeOne(browser, url, outPath) {
  const page = await browser.newPage();
  try {
    await page.setViewport({ width: 1280, height: 720 });
    const resp = await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
    await page.waitForTimeout(1000); // allow animations
    await page.screenshot({ path: outPath, type: 'jpeg', quality: 75, fullPage: false });
    const vp = await page.viewport();
    return { status: resp ? resp.status() : null, width: vp.width, height: vp.height };
  } catch (err) {
    console.error('capture error', err.message);
    return { status: null };
  } finally {
    try { await page.close(); } catch (e) {}
  }
}

async function runOnce(db, browser) {
  const insert = db.prepare('INSERT INTO screenshots (channel, path, ts, width, height, status) VALUES (?, ?, ?, ?, ?, ?)');
  const ts = Date.now();
  for (const c of channels) {
    const safe = c.replace(/[^a-z0-9_-]/gi, '_');
    const fname = `${safe}_${new Date(ts).toISOString().replace(/[:.]/g,'-')}.jpg`;
    const out = path.join(SCREENSHOT_DIR, fname);
    const url = c.startsWith('http') ? c : `https://kick.com/${c}`;
    console.log('capturing', url);
    const res = await takeOne(browser, url, out);
    insert.run(c, out, ts, res.width || null, res.height || null, res.status || null);
    console.log('saved', out);
  }
}

async function main() {
  if (channels.length === 0) {
    console.error('No channels configured. Use --channels or CHANNELS env var.');
    process.exit(2);
  }
  const db = initDb();
  const browser = await puppeteer.launch({ args: ['--no-sandbox', '--disable-setuid-sandbox'] });
  try {
    await runOnce(db, browser);
  } catch (err) {
    console.error('run error', err);
  } finally {
    await browser.close();
    db.close();
  }
}

if (require.main === module) {
  main();
}
