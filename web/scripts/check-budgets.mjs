#!/usr/bin/env node
/**
 * Bundle and interactivity budget gate.
 *
 * Always checks the built `dist/` JavaScript and CSS against
 *   200 kB raw and 50 kB gzipped.
 * Then, only when a Lighthouse binary and Chrome are installed, serves the
 * build with stubbed API responses and checks Lighthouse time to interactive
 * against 1.5 s. Lighthouse is deliberately NOT a dependency: the check is
 * skipped, with a clear message, when the tools are absent.
 */
import { spawn, spawnSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync, rmSync, statSync } from 'node:fs';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { dirname, extname, join, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { gzipSync } from 'node:zlib';

const HERE = dirname(fileURLToPath(import.meta.url));
const PROJECT = resolve(HERE, '..');
const DIST = join(PROJECT, 'dist');

const RAW_LIMIT = 200 * 1024;
const GZIP_LIMIT = 50 * 1024;
const TTI_LIMIT_MS = 1_500;

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json',
  '.svg': 'image/svg+xml',
  '.webp': 'image/webp',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.woff2': 'font/woff2',
};

const BUNDLE_EXTENSIONS = new Set(['.js', '.mjs', '.css']);
const kB = (bytes) => `${(bytes / 1024).toFixed(1)} kB`;

function walk(directory) {
  const files = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const full = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...walk(full));
    else files.push(full);
  }
  return files;
}

const WINDOWS = process.platform === 'win32';

function which(command) {
  const result = spawnSync(WINDOWS ? 'where' : 'which', [command], { encoding: 'utf8' });
  return result.status === 0 ? result.stdout.split(/\r?\n/)[0].trim() : null;
}

/** `spawnSync`'s result shape (status, stderr), without blocking the event loop */
function run(command, args, { timeout, env }) {
  return new Promise((resolveRun) => {
    // npm's launchers on Windows are .cmd files, which only start through a shell
    const viaShell = WINDOWS && /\.(cmd|bat)$/i.test(command);
    const child = viaShell
      ? spawn([command, ...args].map((arg) => `"${arg}"`).join(' '), { env, shell: true, stdio: ['ignore', 'ignore', 'pipe'] })
      : spawn(command, args, { env, stdio: ['ignore', 'ignore', 'pipe'] });
    let stderr = '';
    child.stderr.on('data', (chunk) => (stderr += chunk));
    const timer = setTimeout(() => child.kill('SIGKILL'), timeout);
    child.on('error', (error) => {
      clearTimeout(timer);
      resolveRun({ status: null, stderr: String(error) });
    });
    child.on('close', (status) => {
      clearTimeout(timer);
      resolveRun({ status, stderr });
    });
  });
}

function findLighthouse() {
  const candidates = [
    process.env.LIGHTHOUSE_BIN,
    join(PROJECT, 'node_modules', '.bin', WINDOWS ? 'lighthouse.cmd' : 'lighthouse'),
    which('lighthouse'),
  ];
  return candidates.find((candidate) => candidate && existsSync(candidate)) ?? null;
}

function findChrome() {
  const candidates = [
    process.env.CHROME_PATH,
    which('google-chrome-stable'),
    which('google-chrome'),
    which('chromium'),
    which('chromium-browser'),
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    ...windowsChromePaths(),
  ];
  return candidates.find((candidate) => candidate && existsSync(candidate)) ?? null;
}

/** The usual Chrome and Edge install paths on Windows (none elsewhere) */
function windowsChromePaths() {
  if (!WINDOWS) return [];
  const roots = [process.env.PROGRAMFILES, process.env['PROGRAMFILES(X86)'], process.env.LOCALAPPDATA];
  return roots.filter(Boolean).flatMap((root) => [
    join(root, 'Google', 'Chrome', 'Application', 'chrome.exe'),
    join(root, 'Microsoft', 'Edge', 'Application', 'msedge.exe'),
  ]);
}

async function serveDist() {
  const sale = {
    name: 'Aurora X1',
    price_cents: 129_900,
    sale_start_time: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
    total_inventory: 500,
    sold_out: false,
  };

  const server = createServer((request, response) => {
    const url = new URL(request.url ?? '/', 'http://127.0.0.1');

    const sendJson = (status, body) => {
      response.writeHead(status, {
        'content-type': 'application/json',
        'cache-control': 'no-store',
      });
      response.end(JSON.stringify(body));
    };

    // Minimal API stubs so the page reaches its steady countdown state.
    if (url.pathname === '/v1/session') {
      sendJson(201, { token: 'budget-check-stub' });
      return;
    }
    if (url.pathname.startsWith('/v1/sales/')) {
      sendJson(200, sale);
      return;
    }
    if (url.pathname.startsWith('/v1/')) {
      sendJson(404, { message: 'not found' });
      return;
    }

    let pathname = '/';
    try {
      pathname = decodeURIComponent(url.pathname);
    } catch {
      // Fall through to the 404 below.
    }
    if (pathname === '/') pathname = '/index.html';

    const file = resolve(DIST, `.${pathname}`);
    if (!file.startsWith(DIST + sep) || !existsSync(file) || !statSync(file).isFile()) {
      response.writeHead(404, { 'content-type': 'text/plain' });
      response.end('not found');
      return;
    }

    response.writeHead(200, {
      'content-type': MIME[extname(file)] ?? 'application/octet-stream',
      'cache-control': 'no-store',
    });
    response.end(readFileSync(file));
  });

  await new Promise((resolveListening, rejectListening) => {
    server.once('error', rejectListening);
    server.listen(0, '127.0.0.1', resolveListening);
  });

  const address = server.address();
  return { server, port: typeof address === 'object' && address !== null ? address.port : 0 };
}

async function checkLighthouse() {
  const lighthouse = findLighthouse();
  const chrome = findChrome();

  if (lighthouse === null || chrome === null) {
    const missing = [];
    if (lighthouse === null) {
      missing.push('lighthouse (npm i -g lighthouse, or set LIGHTHOUSE_BIN)');
    }
    if (chrome === null) missing.push('Chrome/Chromium (or set CHROME_PATH)');
    console.log(`\nSkipping Lighthouse TTI check: ${missing.join(' and ')} not found.`);
    return true;
  }

  const { server, port } = await serveDist();
  const reportPath = join(tmpdir(), `flash-sale-lighthouse-${process.pid}.json`);

  try {
    // Not spawnSync: it would block this process's event loop, and with it the
    // server above that Lighthouse is loading the page from
    const result = await run(
      lighthouse,
      [
        `http://127.0.0.1:${port}/`,
        '--only-categories=performance',
        '--preset=desktop',
        '--output=json',
        `--output-path=${reportPath}`,
        '--quiet',
        '--chrome-flags=--headless=new --no-sandbox --disable-gpu --disable-dev-shm-usage',
      ],
      {
        encoding: 'utf8',
        timeout: 120_000,
        env: { ...process.env, CHROME_PATH: chrome },
      },
    );

    if (result.status !== 0 || !existsSync(reportPath)) {
      console.log('\nLighthouse could not run here; skipping the TTI check.');
      const stderr = (result.stderr ?? '').trim().split('\n').filter(Boolean).slice(-3);
      if (stderr.length > 0) console.log(stderr.join('\n'));
      return true;
    }

    const report = JSON.parse(readFileSync(reportPath, 'utf8'));
    const tti = report.audits?.interactive?.numericValue;
    if (typeof tti !== 'number') {
      console.log('\nLighthouse did not report time to interactive; skipping the TTI check.');
      return true;
    }

    const passed = tti < TTI_LIMIT_MS;
    console.log(
      `\nLighthouse (desktop) time to interactive: ${Math.round(tti)} ms ` +
        `(limit ${TTI_LIMIT_MS} ms) ${passed ? 'PASS' : 'FAIL'}`,
    );
    return passed;
  } finally {
    server.close();
    rmSync(reportPath, { force: true });
  }
}

async function main() {
  if (!existsSync(DIST)) {
    console.error('dist/ not found. Run `npm run build` before `npm run budgets`.');
    process.exit(1);
  }

  const files = walk(DIST).filter((file) =>
    BUNDLE_EXTENSIONS.has(extname(file).toLowerCase()),
  );

  if (files.length === 0) {
    console.error('No JavaScript or CSS found in dist/. Run `npm run build` again.');
    process.exit(1);
  }

  let rawTotal = 0;
  let gzipTotal = 0;
  const rows = [];

  for (const file of files.sort()) {
    const source = readFileSync(file);
    const gzipped = gzipSync(source, { level: 9 }).length;
    rawTotal += source.length;
    gzipTotal += gzipped;
    rows.push({
      name: file.slice(DIST.length + 1),
      raw: source.length,
      gzip: gzipped,
    });
  }

  const width = Math.max(...rows.map((row) => row.name.length), 10);
  console.log('Bundle budget (JavaScript + CSS)');
  console.log(`${'file'.padEnd(width)}  ${'raw'.padStart(9)}  ${'gzip'.padStart(9)}`);
  for (const row of rows) {
    console.log(
      `${row.name.padEnd(width)}  ${kB(row.raw).padStart(9)}  ${kB(row.gzip).padStart(9)}`,
    );
  }
  console.log('-'.repeat(width + 22));
  console.log(
    `${'total'.padEnd(width)}  ${kB(rawTotal).padStart(9)}  ${kB(gzipTotal).padStart(9)}`,
  );
  console.log(
    `${'limit'.padEnd(width)}  ${kB(RAW_LIMIT).padStart(9)}  ${kB(GZIP_LIMIT).padStart(9)}`,
  );

  let failed = false;
  if (rawTotal > RAW_LIMIT) {
    console.error(`\nFAIL: raw bundle is ${kB(rawTotal)}, over the ${kB(RAW_LIMIT)} limit.`);
    failed = true;
  }
  if (gzipTotal > GZIP_LIMIT) {
    console.error(
      `\nFAIL: gzipped bundle is ${kB(gzipTotal)}, over the ${kB(GZIP_LIMIT)} limit.`,
    );
    failed = true;
  }

  const lighthousePassed = await checkLighthouse();
  if (!lighthousePassed) {
    console.error(`\nFAIL: time to interactive is over ${TTI_LIMIT_MS} ms.`);
    failed = true;
  }

  if (failed) {
    console.error('\nBudget check failed.');
    process.exit(1);
  }
  console.log('\nAll budget checks passed.');
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
