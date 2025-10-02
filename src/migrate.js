import { createPools, listTables } from './db.js';
import createProgress from './progress.js';
import { logger } from './logger.js';

// Helper: fetch DDL from source
async function getCreateTableDDL(srcPool, table) {
  const [ddlRows] = await srcPool.query(`SHOW CREATE TABLE \`${table}\``);
  const ddl = ddlRows?.[0]?.['Create Table'] || ddlRows?.[0]?.Create || Object.values(ddlRows?.[0] || {})[1];
  if (!ddl) throw new Error(`Falha ao obter DDL de ${table}`);
  return ddl;
}

async function getPLimit(concurrency) {
  try {
    const mod = await import('p-limit');
    const pl = mod.default || mod;
    return pl(concurrency);
  } catch {
    let active = 0;
    const q = [];
    const runNext = () => {
      if (!q.length) return;
      if (active >= concurrency) return;
      const { fn, resolve, reject } = q.shift();
      active++;
      Promise.resolve().then(fn)
        .then((v) => resolve(v))
        .catch(reject)
        .finally(() => { active--; runNext(); });
    };
    return (fn) => new Promise((resolve, reject) => { q.push({ fn, resolve, reject }); runNext(); });
  }
}

async function estimateRowCount(srcPool, dbName, table) {
  try {
    const [rows] = await srcPool.query(
      'SELECT TABLE_ROWS AS r FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?',
      [dbName, table]
    );
    const est = rows?.[0]?.r;
    if (Number.isFinite(est) && est > 0) return est;
  } catch {}
  return null; // unknown
}

function withTimeout(promise, ms, label) {
  return new Promise((resolve, reject) => {
    const to = setTimeout(() => reject(new Error(`timeout ${label} após ${ms}ms`)), ms);
    promise.then((v)=>{ clearTimeout(to); resolve(v); }, (e)=>{ clearTimeout(to); reject(e); });
  });
}

function extractAndStripFKs(sql) {
  try {
    const open = sql.indexOf('(');
    const close = sql.lastIndexOf(')');
    if (open === -1 || close === -1 || close <= open) return { ddl: sql, fks: [] };
    const pre = sql.slice(0, open + 1);
    const body = sql.slice(open + 1, close);
    const post = sql.slice(close);
    const lines = body.split('\n');
    const fks = [];
    const kept = [];
    for (let line of lines) {
      if (/foreign\s+key/i.test(line)) {
        // remove trailing comma
        let clause = line.trim().replace(/,\s*$/, '');
        // Normalize leading comma fragments
        clause = clause.replace(/^,\s*/, '');
        // Build ADD clause: supports either starting with CONSTRAINT ... FOREIGN KEY ... or FOREIGN KEY ...
        if (!/^ADD\s+/i.test(clause)) clause = 'ADD ' + clause;
        fks.push(clause);
      } else {
        kept.push(line);
      }
    }
    let newBody = kept.join('\n');
    newBody = newBody.replace(/,\s*\n\s*$/m, '\n');
    let ddl = pre + newBody + post;
    ddl = ddl.replace(/,\s*\)/g, ')');
    return { ddl, fks };
  } catch {
    return { ddl: sql, fks: [] };
  }
}

async function copyTableStreaming(cfg, pools, table, onPhase, progress, schemaLimit) {
  let srcRaw = null;
  let dstConn = null;
  let fkClauses = [];
  try {
    onPhase('preparando schema (aguardando)');

    await schemaLimit(async () => {
      onPhase('preparando schema (executando)');
      const [ddlRows] = await withTimeout(
        pools.src.query(`SHOW CREATE TABLE \`${table}\``),
        cfg.ddlTimeoutMs,
        `SHOW CREATE TABLE ${table}`
      );
      let createSql = ddlRows?.[0]?.['Create Table'] || ddlRows?.[0]?.Create || Object.values(ddlRows?.[0] || {})[1];
      if (!createSql) throw new Error(`Falha ao obter DDL de ${table}`);
      if (cfg.stripFK) {
        const out = extractAndStripFKs(createSql);
        createSql = out.ddl;
        fkClauses = out.fks;
      }

      // Use short-lived destination connection for DDL
      const ddlConn = await pools.dst.getConnection();
      try {
        // Set session lock timeouts
        try {
          const lockSec = Math.max(1, Math.floor((cfg.ddlTimeoutMs || 120000) / 1000));
          await ddlConn.query(`SET SESSION lock_wait_timeout=${lockSec}`);
          await ddlConn.query(`SET SESSION innodb_lock_wait_timeout=${lockSec}`);
        } catch {}
        // Disable checks for DDL session to tolerate FK order
        try {
          await ddlConn.query('SET FOREIGN_KEY_CHECKS=0');
          await ddlConn.query('SET UNIQUE_CHECKS=0');
        } catch {}

        await withTimeout(ddlConn.query(`DROP TABLE IF EXISTS \`${table}\``), cfg.ddlTimeoutMs, `DROP ${table}`);
        await withTimeout(ddlConn.query(createSql), cfg.ddlTimeoutMs, `CREATE ${table}`);
      } finally {
        ddlConn.release();
      }
    });

    // Estimate total for progress
    const totalEst = await estimateRowCount(pools.src, cfg.src.database, table);
    if (totalEst && progress?.setTotal) progress.setTotal(table, totalEst);

    onPhase('copiando dados');

    // Open destination connection for DML
    dstConn = await pools.dst.getConnection();
    try {
      await dstConn.query('SET FOREIGN_KEY_CHECKS=0');
      await dstConn.query('SET UNIQUE_CHECKS=0');
      // Set DML lock timeout too (best-effort)
      try {
        const lockSec = Math.max(1, Math.floor((cfg.flushTimeoutMs || 300000) / 1000));
        await dstConn.query(`SET SESSION lock_wait_timeout=${lockSec}`);
        await dstConn.query(`SET SESSION innodb_lock_wait_timeout=${lockSec}`);
      } catch {}

      // Open a raw source connection for streaming
      const mysqlRaw = await import('mysql2');
      srcRaw = mysqlRaw.createConnection({
        host: cfg.src.host,
        port: cfg.src.port,
        user: cfg.src.user,
        password: cfg.src.password,
        database: cfg.src.database,
      });

      const query = srcRaw.query({ sql: `SELECT * FROM \`${table}\``, rowsAsArray: false });
      const stream = query.stream({ objectMode: true, highWaterMark: cfg.batchSize });

      const BATCH_SIZE = cfg.batchSize;
      const rows = [];
      let inserted = 0;
      let columns = null;

      const flush = async () => {
        if (!rows.length) return;
        if (!columns) columns = Object.keys(rows[0]);
        const colList = columns.map(c => `\`${c}\``).join(',');
        const placeholdersRow = `(${columns.map(() => '?').join(',')})`;
        const placeholders = new Array(rows.length).fill(placeholdersRow).join(',');
        const sql = `INSERT INTO \`${table}\` (${colList}) VALUES ${placeholders}`;
        const params = [];
        for (const r of rows) {
          for (const c of columns) params.push(r[c]);
        }
        const count = rows.length;
        await withTimeout(dstConn.query(sql, params), cfg.flushTimeoutMs, `INSERT batch em ${table}`);
        inserted += count;
        rows.length = 0;
        if (progress?.update) progress.update(table, inserted, `copiando dados (${inserted}${totalEst?`/${totalEst}`:''})`);
      };

      await dstConn.beginTransaction();

      await new Promise((resolve, reject) => {
        let pending = Promise.resolve();
        stream.on('data', (row) => {
          rows.push(row);
          if (rows.length >= BATCH_SIZE) {
            stream.pause();
            pending = pending.then(flush)
              .then(() => { stream.resume(); })
              .catch(reject);
          }
        });
        stream.on('end', () => {
          pending.then(flush).then(resolve).catch(reject);
        });
        stream.on('error', reject);
      });

      await dstConn.commit();
    } finally {
      try { await dstConn.query('SET UNIQUE_CHECKS=1'); } catch {}
      try { await dstConn.query('SET FOREIGN_KEY_CHECKS=1'); } catch {}
      dstConn.release();
    }

    return { fks: fkClauses };
  } catch (e) {
    try { dstConn && await dstConn.rollback(); } catch {}
    throw e;
  } finally {
    try { srcRaw && srcRaw.end(); } catch {}
  }
}

async function verifyTableCounts(srcPool, dstPool, table) {
  const [[srcCountRow]] = await srcPool.query(`SELECT COUNT(*) AS c FROM \`${table}\``);
  const [[dstCountRow]] = await dstPool.query(`SELECT COUNT(*) AS c FROM \`${table}\``);
  return { src: srcCountRow.c ?? 0, dst: dstCountRow.c ?? 0 };
}

async function restoreForeignKeys(cfg, dstPool, fkMap, schemaLimit) {
  const entries = Object.entries(fkMap).filter(([, arr]) => arr?.length);
  if (!entries.length) return { added: 0, failed: [] };
  let pending = entries.flatMap(([table, arr]) => arr.map(cl => ({ table, clause: cl })));
  const maxPasses = 5;
  let added = 0;
  const failedSet = new Set();
  for (let pass = 1; pass <= maxPasses && pending.length; pass++) {
    logger.info(`[fk] Passo ${pass}: ${pending.length} constraints a aplicar`);
    const next = [];
    for (const item of pending) {
      try {
        await schemaLimit(async () => {
          const conn = await dstPool.getConnection();
          try {
            await conn.query('SET FOREIGN_KEY_CHECKS=0');
            const sql = `ALTER TABLE \`${item.table}\` ${item.clause}`;
            await withTimeout(conn.query(sql), cfg.ddlTimeoutMs, `ADD FK ${item.table}`);
            added++;
          } finally {
            conn.release();
          }
        });
      } catch (e) {
        // Keep for next pass
        next.push(item);
        failedSet.add(`${item.table}::${item.clause}`);
      }
    }
    if (next.length === pending.length) break; // no progress
    pending = next;
  }
  const failed = pending.map(i => ({ table: i.table, clause: i.clause }));
  return { added, failed };
}

export async function migrate(cfg) {
  const limit = await getPLimit(Math.max(1, cfg.concurrency));
  const schemaLimit = await getPLimit(Math.max(1, cfg.schemaConcurrency));
  const { src, dst, end } = await createPools(cfg);
  const verifySummary = [];
  const fkMap = {};
  try {
    let tables = [];
    if (cfg.include?.length) {
      tables = cfg.include.slice();
    } else if (!cfg.skipConnect) {
      tables = await listTables(src, cfg.src.database);
    }
    if (cfg.exclude?.length) {
      const ex = new Set(cfg.exclude);
      tables = tables.filter(t => !ex.has(t));
    }
    tables.sort((a, b) => a.localeCompare(b));

    if (!tables.length) {
      logger.warn('Nenhuma tabela para migrar. Verifique .env INCLUDE/EXCLUDE ou a conexão.');
      return;
    }

    const progress = await createProgress();

    logger.info(`Migrando ${tables.length} tabelas com concorrência=${cfg.concurrency}, batchSize=${cfg.batchSize}...`);

    const tasks = tables.map((table) => limit(async () => {
      const bar = progress.addTable(table);
      try {
        progress.update?.(table, 0, 'iniciando');
        if (cfg.dryRun) {
          progress.update?.(table, 1, 'simulando dump');
          progress.update?.(table, 2, 'simulando restore');
          progress.done(table);
          return;
        }

        // Streaming copy with schema-limited DDL
        progress.update?.(table, 0, 'dump/stream (batches)');
        const res = await copyTableStreaming(cfg, { src, dst }, table, (phase) => {
          progress.update?.(table, undefined, phase);
        }, progress, schemaLimit);
        if (cfg.stripFK && cfg.restoreFK && res?.fks?.length) {
          fkMap[table] = res.fks.slice();
        }

        if (cfg.verify) {
          progress.update?.(table, undefined, 'verificando');
          const { src: cSrc, dst: cDst } = await verifyTableCounts(src, dst, table);
          if (cSrc !== cDst) {
            logger.warn(`[verify] ${table}: origem=${cSrc} destino=${cDst} (diferença)`);
            verifySummary.push({ table, src: cSrc, dst: cDst, ok: false });
          } else {
            logger.info(`[verify] ${table}: ${cDst}/${cSrc} OK`);
            verifySummary.push({ table, src: cSrc, dst: cDst, ok: true });
          }
        }

        progress.update?.(table, undefined, 'finalizando');
        progress.done(table);
      } catch (e) {
        progress.update?.(table, undefined, 'erro');
        const msg = e?.sqlMessage || e?.message || String(e);
        logger.error(`Tabela ${table}:`, msg);
        throw e;
      } finally {
        bar?.stop?.();
      }
    }));

    const results = await Promise.allSettled(tasks);
    const failedTables = results
      .map((r, i) => (r.status === 'rejected' ? tables[i] : null))
      .filter(Boolean);
    progress.stop();

    if (cfg.restoreFK && Object.keys(fkMap).length) {
      logger.info('[fk] Restaurando chaves estrangeiras...');
      const { added, failed } = await restoreForeignKeys(cfg, dst, fkMap, schemaLimit);
      logger.info(`[fk] Adicionadas: ${added}, Falharam: ${failed.length}`);
      if (failed.length) {
        failed.slice(0, 10).forEach(f => logger.warn(`[fk] Falhou: ${f.table} :: ${f.clause}`));
        if (failed.length > 10) logger.warn(`[fk] +${failed.length - 10} falhas não listadas`);
      }
    }

    if (cfg.verify && verifySummary.length) {
      const lines = verifySummary.sort((a,b)=>a.table.localeCompare(b.table)).map(r => ` - ${r.table}: ${r.dst}/${r.src} ${r.ok?'OK':'MISMATCH'}`);
      logger.info('Resumo de verificação por tabela:\n' + lines.join('\n'));
    }

    if (failedTables.length) {
      const msg = `${failedTables.length} tabela(s) falharam: ${failedTables.join(', ')}`;
      if (cfg.failOnError) throw new Error(msg);
      logger.warn(msg);
    }

    logger.success('Migração concluída com sucesso.');
  } finally {
    await end();
  }
}

export async function restoreAllForeignKeys(cfg) {
  const schemaLimit = await getPLimit(Math.max(1, cfg.schemaConcurrency));
  const { src, dst, end } = await createPools(cfg);
  try {
    let tables = [];
    if (cfg.include?.length) tables = cfg.include.slice();
    else tables = await listTables(src, cfg.src.database);
    if (cfg.exclude?.length) tables = tables.filter(t => !new Set(cfg.exclude).has(t));
    tables.sort((a,b)=>a.localeCompare(b));

    const fkMap = {};
    for (const table of tables) {
      const [ddlRows] = await withTimeout(
        src.query(`SHOW CREATE TABLE \`${table}\``),
        cfg.ddlTimeoutMs,
        `SHOW CREATE TABLE ${table}`
      );
      const createSql = ddlRows?.[0]?.['Create Table'] || ddlRows?.[0]?.Create || Object.values(ddlRows?.[0] || {})[1];
      if (!createSql) continue;
      const out = extractAndStripFKs(createSql);
      if (out.fks?.length) fkMap[table] = out.fks;
    }

    if (!Object.keys(fkMap).length) {
      logger.warn('[fk] Nenhuma FK encontrada para restaurar.');
      return;
    }

    logger.info('[fk] Restaurando chaves estrangeiras (modo only)...');
    const { added, failed } = await restoreForeignKeys(cfg, dst, fkMap, schemaLimit);
    logger.info(`[fk] Adicionadas: ${added}, Falharam: ${failed.length}`);
    if (failed.length) {
      failed.slice(0, 10).forEach(f => logger.warn(`[fk] Falhou: ${f.table} :: ${f.clause}`));
      if (failed.length > 10) logger.warn(`[fk] +${failed.length - 10} falhas não listadas`);
    }
  } finally {
    await end();
  }
}

export default migrate;
