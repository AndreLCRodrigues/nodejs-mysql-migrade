#!/usr/bin/env node
import { loadConfig } from './config.js';
import { migrate, restoreAllForeignKeys } from './migrate.js';
import { logger } from './logger.js';

function parseArgs(argv) {
  const args = new Map();
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    const [k, v] = a.includes('=') ? a.slice(2).split('=') : [a.slice(2), 'true'];
    args.set(k, v);
  }
  return args;
}

async function preflight(cfg) {
  if (cfg.dryRun || cfg.skipConnect) return;
  const mysql = await import('mysql2/promise');
  // Source
  try {
    const src = await mysql.createConnection({
      host: cfg.src.host,
      port: cfg.src.port,
      user: cfg.src.user,
      password: cfg.src.password,
      database: cfg.src.database,
    });
    await src.query('SELECT 1');
    await src.end();
  } catch (e) {
    throw new Error(`Falha ao conectar na origem (${cfg.src.host}:${cfg.src.port}/${cfg.src.database}): ${e.message}`);
  }
  // Destination
  try {
    const dst = await mysql.createConnection({
      host: cfg.dst.host,
      port: cfg.dst.port,
      user: cfg.dst.user,
      password: cfg.dst.password,
      database: cfg.dst.database,
      multipleStatements: true,
    });
    await dst.query('SELECT 1');
    await dst.end();
  } catch (e) {
    throw new Error(`Falha ao conectar no destino (${cfg.dst.host}:${cfg.dst.port}/${cfg.dst.database}): ${e.message}`);
  }
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.has('help') || args.has('h')) {
    console.log(`mysql-migrate\n\n` +
      `Modo: streaming + INSERT em batches\n` +
      `Env vars (ou .env):\n` +
      `  SRC_HOST, SRC_PORT, SRC_USER, SRC_PASSWORD, SRC_DATABASE\n` +
      `  DST_HOST, DST_PORT, DST_USER, DST_PASSWORD, DST_DATABASE\n` +
      `  CONCURRENCY=N (padrão: CPUs)\n` +
      `  SCHEMA_CONCURRENCY=N (padrão: 1; limita DDL paralelos)\n` +
      `  BATCH_SIZE=N (padrão: 1000 linhas por INSERT)\n` +
      `  DDL_TIMEOUT_MS (padrão: 120000), FLUSH_TIMEOUT_MS (padrão: 300000)\n` +
      `  STRIP_FK=1/0 (padrão: 1; remove FKs do CREATE durante import)\n` +
      `  RESTORE_FK=1/0 (padrão: 1; recria FKs após importar dados)\n` +
      `  RESTORE_FK_ONLY=1 (apenas recria FKs; não copia dados)\n` +
      `  FAIL_ON_ERROR=1/0 (padrão: 1; encerra com erro se tabelas falharem)\n` +
      `  INCLUDE_TABLES=t1,t2,t3 | EXCLUDE_TABLES=t4,t5\n` +
      `  VERIFY=1 para verificar contagem src/dst por tabela (padrão: 1)\n` +
      `  DRY_RUN=1 para simular | SKIP_CONNECT=1 (implicado em DRY_RUN)\n` +
      `CLI flags (sobrepõem .env se aplicável):\n` +
      `  --concurrency=N --include=t1,t2 --exclude=t3 --dry\n`);
    process.exit(0);
  }

  const cfg = loadConfig();
  if (args.has('concurrency')) cfg.concurrency = parseInt(args.get('concurrency'), 10) || cfg.concurrency;
  if (args.has('include')) cfg.include = String(args.get('include')).split(',').map(s => s.trim()).filter(Boolean);
  if (args.has('exclude')) cfg.exclude = String(args.get('exclude')).split(',').map(s => s.trim()).filter(Boolean);
  if (args.has('dry')) { cfg.dryRun = true; cfg.skipConnect = true; }

  const startedAt = Date.now();
  try {
    if (process.env.RESTORE_FK_ONLY && ['1','true','yes','on'].includes(String(process.env.RESTORE_FK_ONLY).toLowerCase())) {
      await preflight(cfg);
      await restoreAllForeignKeys(cfg);
      const ms = Date.now() - startedAt;
      logger.success(`FKs restauradas em ${(ms/1000).toFixed(1)}s`);
      return;
    }

    await preflight(cfg);
    await migrate(cfg);
    const ms = Date.now() - startedAt;
    logger.success(`Finalizado em ${(ms/1000).toFixed(1)}s`);
  } catch (e) {
    logger.error('Falha na migração:', e.stack || e.message || e);
    process.exitCode = 1;
  }
}

main();
