let dotenv = null;
try {
  dotenv = await import('dotenv');
  dotenv = dotenv.default || dotenv;
  dotenv.config();
} catch {}

import os from 'os';
import { logger } from './logger.js';

const toBool = (v, def = false) => {
  if (v === undefined || v === null || v === '') return def;
  if (typeof v === 'boolean') return v;
  const s = String(v).toLowerCase();
  return ['1', 'true', 'yes', 'y', 'on'].includes(s);
};

const parseIntDef = (v, def) => {
  const n = parseInt(v, 10);
  return Number.isFinite(n) && n > 0 ? n : def;
};

export function loadConfig() {
  const dryRun = toBool(process.env.DRY_RUN, false);
  const cfg = {
    dryRun,
    concurrency: parseIntDef(process.env.CONCURRENCY, Math.min(4, Math.max(2, os.cpus()?.length || 2))),
    schemaConcurrency: parseIntDef(process.env.SCHEMA_CONCURRENCY, 1),
    batchSize: parseIntDef(process.env.BATCH_SIZE, 1000),
    ddlTimeoutMs: parseIntDef(process.env.DDL_TIMEOUT_MS, 120000),
    flushTimeoutMs: parseIntDef(process.env.FLUSH_TIMEOUT_MS, 300000),
    stripFK: toBool(process.env.STRIP_FK ?? '1', true),
    restoreFK: toBool(process.env.RESTORE_FK ?? '1', true),
    failOnError: toBool(process.env.FAIL_ON_ERROR ?? '1', true),
    src: {
      host: process.env.SRC_HOST || 'localhost',
      port: parseIntDef(process.env.SRC_PORT, 3306),
      user: process.env.SRC_USER,
      password: process.env.SRC_PASSWORD,
      database: process.env.SRC_DATABASE,
    },
    dst: {
      host: process.env.DST_HOST || 'localhost',
      port: parseIntDef(process.env.DST_PORT, 3306),
      user: process.env.DST_USER,
      password: process.env.DST_PASSWORD,
      database: process.env.DST_DATABASE,
    },
    include: (process.env.INCLUDE_TABLES || '').split(',').map(s => s.trim()).filter(Boolean),
    exclude: (process.env.EXCLUDE_TABLES || '').split(',').map(s => s.trim()).filter(Boolean),
    skipConnect: toBool(process.env.SKIP_CONNECT, dryRun),
    verify: toBool(process.env.VERIFY ?? process.env.VERIFY_COUNTS, true),
  };

  const required = [
    ['SRC_USER', cfg.src.user],
    ['SRC_PASSWORD', cfg.src.password],
    ['SRC_DATABASE', cfg.src.database],
    ['DST_USER', cfg.dst.user],
    ['DST_PASSWORD', cfg.dst.password],
    ['DST_DATABASE', cfg.dst.database],
  ];

  const missing = required.filter(([_, v]) => !v).map(([k]) => k);
  if (missing.length) {
    logger.warn('Variáveis ausentes no .env:', missing.join(', '));
  }

  if (cfg.include.length && cfg.exclude.length) {
    logger.warn('INCLUDE_TABLES e EXCLUDE_TABLES definidos. INCLUDE terá prioridade.');
  }

  return cfg;
}

export default loadConfig;
