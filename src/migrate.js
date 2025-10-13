import { createPools, listTables } from './db.js';
import createProgress from './progress.js';
import { logger } from './logger.js';
import readline from 'readline';

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

async function getSourceAutoIncrement(srcPool, dbName, table) {
  const [rows] = await srcPool.query(
    'SELECT AUTO_INCREMENT AS ai FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?',
    [dbName, table]
  );
  const ai = rows?.[0]?.ai;
  return Number.isFinite(ai) ? ai : null;
}

async function copyTableStreaming(cfg, pools, table, onPhase, progress, schemaLimit, options = { doDDL: true, doData: true }) {
  let srcRaw = null;
  let dstConn = null;
  let fkClauses = [];
  try {
    if (options.doDDL) {
      onPhase('preparando schema (aguardando)');

      await schemaLimit(async () => {
        onPhase('preparando schema (executando)');
        const [ddlRows] = await withTimeout(
          pools.src.query(`SHOW CREATE TABLE \`${table}\``),
          cfg.ddlTimeoutMs,
          `SHOW CREATE TABLE ${table}`
        );
        let createSql = ddlRows?.[0]?.['Create Table'] || ddlRows?.[0]?.['Create View'] || ddlRows?.[0]?.Create || Object.values(ddlRows?.[0] || {})[1];
        if (!createSql) throw new Error(`Falha ao obter DDL de ${table}`);
        const isView = createSql.trim().toUpperCase().startsWith('CREATE VIEW');
        if (isView) {
          options.doData = false; // Views don't have data to copy
        }
        if (cfg.stripFK) {
          const out = extractAndStripFKs(createSql);
          createSql = out.ddl;
          fkClauses = out.fks;
        }

        const ddlConn = await pools.dst.getConnection();
        try {
          try {
            const lockSec = Math.max(1, Math.floor((cfg.ddlTimeoutMs || 120000) / 1000));
            await ddlConn.query(`SET SESSION lock_wait_timeout=${lockSec}`);
            await ddlConn.query(`SET SESSION innodb_lock_wait_timeout=${lockSec}`);
          } catch {}
          try {
            await ddlConn.query('SET FOREIGN_KEY_CHECKS=0');
            await ddlConn.query('SET UNIQUE_CHECKS=0');
          } catch {}

          await withTimeout(ddlConn.query(`DROP TABLE IF EXISTS \`${table}\``), cfg.ddlTimeoutMs, `DROP ${table}`);
          try {
            await withTimeout(ddlConn.query(createSql), cfg.ddlTimeoutMs, `CREATE ${table}`);
          } catch (ddlError) {
            if (isView) {
              logger.warn(`DDL falhou para view ${table}, pulando: ${ddlError.message}`);
              return { fks: [] }; // Skip the rest for views
            } else {
              throw ddlError;
            }
          }

          // Set AUTO_INCREMENT from source so new registros não conflitem com dados adiados
          if (!isView) {
            const ai = await getSourceAutoIncrement(pools.src, cfg.src.database, table);
            if (ai) {
              await withTimeout(ddlConn.query(`ALTER TABLE \`${table}\` AUTO_INCREMENT = ${ai}`), cfg.ddlTimeoutMs, `AUTO_INCREMENT ${table}`);
            }
          }
        } finally {
          ddlConn.release();
        }
      });
    }

    if (!options.doData) {
      return { fks: fkClauses };
    }

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

      // Truncate table if dataOnly and truncate enabled
      if (cfg.dataOnly && cfg.dataOnlyTruncate) {
        await withTimeout(dstConn.query(`TRUNCATE TABLE \`${table}\``), cfg.ddlTimeoutMs, `TRUNCATE ${table}`);
      }

      // Open a raw source connection for streaming
      const mysqlRaw = await import('mysql2');
      srcRaw = mysqlRaw.createConnection({
        host: cfg.src.host,
        port: cfg.src.port,
        user: cfg.src.user,
        password: cfg.src.password,
        database: cfg.src.database,
        maxAllowedPacket: 67108864,
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

      // After copy, ensure AUTO_INCREMENT matches source (optional, usually unchanged)
      const ai = await getSourceAutoIncrement(pools.src, cfg.src.database, table);
      if (ai) {
        await withTimeout(dstConn.query(`ALTER TABLE \`${table}\` AUTO_INCREMENT = ${ai}`), cfg.ddlTimeoutMs, `AUTO_INCREMENT ${table}`);
      }
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

    const noDataSet = new Set(cfg.noDataTables || []);
    const deferSet = new Set(cfg.deferDataTables || []);

    const progress = await createProgress();

    logger.info(`Migrando ${tables.length} tabelas com concorrência=${cfg.concurrency}, batchSize=${cfg.batchSize}...`);

    const tasks = tables.map((table) => limit(async () => {
      const bar = progress.addTable(table);
      try {
        progress.update?.(table, 0, 'iniciando');
        if (cfg.dryRun) {
          progress.update?.(table, 1, 'simulando schema');
          progress.update?.(table, 2, 'simulando dados');
          progress.done(table);
          return;
        }

        const doDDL = !cfg.dataOnly;
        const doData = cfg.dataOnly || !(noDataSet.has(table) || deferSet.has(table));
        progress.update?.(table, 0, doData ? 'dump/stream (batches)' : 'apenas schema');
        const res = await copyTableStreaming(cfg, { src, dst }, table, (phase) => {
          progress.update?.(table, undefined, phase);
        }, progress, schemaLimit, { doDDL, doData });

        if (cfg.stripFK && cfg.restoreFK && res?.fks?.length) {
          fkMap[table] = (fkMap[table] || []).concat(res.fks);
        }

        if (doData && cfg.verify) {
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
        const msg = e.stack || e.message || String(e);
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

    // Stop the first multibar before any interactive prompt
    try { progress.stop(); } catch {}

    // If there are deferred tables, optionally wait for user approval to start copying them
    if (deferSet.size) {
      const deferred = tables.filter(t => deferSet.has(t));
      if (!cfg.deferAutoStart) {
        if (process.stdin.isTTY) {
          const msg = `Foram adiadas ${deferred.length} tabela(s): ${deferred.join(', ')}\nPressione ENTER para iniciar a importação das tabelas adiadas...`;
          await new Promise((resolve) => {
            const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
            rl.question(msg + '\n', () => { rl.close(); resolve(); });
          });
        } else {
          logger.info(`Foram adiadas ${deferred.length} tabela(s): ${deferred.join(', ')}`);
          logger.info('Iniciando fase adiada automaticamente (STDIN não interativo).');
        }
      }

      logger.info('Iniciando importação das tabelas adiadas...');
      const deferredProgress = await createProgress();
      const deferredTasks = deferred.map((table) => limit(async () => {
        const bar = deferredProgress.addTable(table);
        try {
          // Accurate total and stable cutoff when PK integer exists
          const pkInfo = await getPrimaryKeyColumn(src, cfg.src.database, table);
          if (pkInfo && isIntegerDataType(pkInfo.dataType)) {
            const cutoffId = await getMaxPk(src, table, pkInfo.name);
            const total = await countUpToPk(src, table, pkInfo.name, cutoffId);
            if (total && deferredProgress?.setTotal) deferredProgress.setTotal(table, total);
            await copyTableDeferredWithCutoff(cfg, { src, dst }, table, pkInfo.name, cutoffId, deferredProgress);
            if (cfg.verify) {
              const srcCount = await countUpToPk(src, table, pkInfo.name, cutoffId);
              const [dstRows] = await dst.query(`SELECT COUNT(*) AS c FROM \`${table}\``);
              const dstCount = dstRows?.[0]?.c ?? 0;
              if (srcCount !== dstCount) {
                logger.warn(`[verify] ${table}: origem<=cutoff=${srcCount} destino=${dstCount} (diferença)`);
              } else {
                logger.info(`[verify] ${table}: ${dstCount}/${srcCount} OK`);
              }
            }
          } else {
            // Fallback: reuse generic streaming (might show approximate progress)
            deferredProgress.update?.(table, 0, 'copiando dados (adiadas)');
            const res = await copyTableStreaming(cfg, { src, dst }, table, (phase) => {
              deferredProgress.update?.(table, undefined, phase);
            }, deferredProgress, schemaLimit, { doDDL: false, doData: true });
            if (cfg.stripFK && cfg.restoreFK && res?.fks?.length) {
              fkMap[table] = (fkMap[table] || []).concat(res.fks);
            }
            if (cfg.verify) {
              const [sRows] = await src.query(`SELECT COUNT(*) AS c FROM \`${table}\``);
              const [dRows] = await dst.query(`SELECT COUNT(*) AS c FROM \`${table}\``);
              const sc = sRows?.[0]?.c ?? 0;
              const dc = dRows?.[0]?.c ?? 0;
              if (sc !== dc) logger.warn(`[verify] ${table}: origem=${sc} destino=${dc} (diferença possível)`);
              else logger.info(`[verify] ${table}: ${dc}/${sc} OK`);
            }
          }
          deferredProgress.done(table);
        } catch (e) {
          deferredProgress.update?.(table, undefined, 'erro');
          const msg = e?.sqlMessage || e?.message || String(e);
          logger.error(`Tabela (adiada) ${table}:`, msg);
          throw e;
        } finally {
          bar?.stop?.();
        }
      }));

      const deferredResults = await Promise.allSettled(deferredTasks);
      const deferredFailed = deferredResults
        .map((r, i) => (r.status === 'rejected' ? deferred[i] : null))
        .filter(Boolean);
      try { deferredProgress.stop(); } catch {}
      failedTables.push(...deferredFailed);
    }

    // Restore FKs at the end (after deferred too)
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

    // Final explicit success after all phases
    logger.success('Migração concluída com sucesso.');
  } finally {
    await end();
  }
}

async function restoreAllForeignKeys(cfg) {
  const schemaLimit = await getPLimit(Math.max(1, cfg.schemaConcurrency));
  const { src, dst, end } = await createPools(cfg);
  try {
    let tables = [];
    if (cfg.include?.length) {
      tables = cfg.include.slice();
    } else {
      tables = await listTables(src, cfg.src.database);
    }
    if (cfg.exclude?.length) {
      const ex = new Set(cfg.exclude);
      tables = tables.filter(t => !ex.has(t));
    }
    tables.sort((a,b)=>a.localeCompare(b));

    const fkMap = {};
    for (const table of tables) {
      const [ddlRows] = await withTimeout(
        src.query(`SHOW CREATE TABLE \`${table}\``),
        cfg.ddlTimeoutMs,
        `SHOW CREATE TABLE ${table}`
      );
      const createSql = ddlRows?.[0]?.['Create Table'] || ddlRows?.[0]?.['Create View'] || ddlRows?.[0]?.Create || Object.values(ddlRows?.[0] || {})[1];
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

export { restoreAllForeignKeys };

export default migrate;

async function getPrimaryKeyColumn(srcPool, dbName, table) {
  const [rows] = await srcPool.query(
    `SELECT k.COLUMN_NAME, c.DATA_TYPE
     FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
     JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
       ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME
      AND t.TABLE_SCHEMA = k.TABLE_SCHEMA
      AND t.TABLE_NAME = k.TABLE_NAME
     JOIN INFORMATION_SCHEMA.COLUMNS c
       ON c.TABLE_SCHEMA = k.TABLE_SCHEMA
      AND c.TABLE_NAME = k.TABLE_NAME
      AND c.COLUMN_NAME = k.COLUMN_NAME
     WHERE t.TABLE_SCHEMA = ?
       AND t.TABLE_NAME = ?
       AND t.CONSTRAINT_TYPE = 'PRIMARY KEY'
     ORDER BY k.ORDINAL_POSITION
     LIMIT 1`, [dbName, table]
  );
  if (!rows?.length) return null;
  return { name: rows[0].COLUMN_NAME, dataType: String(rows[0].DATA_TYPE || '').toLowerCase() };
}

function isIntegerDataType(dt) {
  return ['tinyint','smallint','mediumint','int','integer','bigint'].includes(dt);
}

async function getMaxPk(srcPool, table, pk) {
  const [rows] = await srcPool.query(`SELECT MAX(\`${pk}\`) AS maxId FROM \`${table}\``);
  return rows?.[0]?.maxId ?? null;
}

async function countUpToPk(srcPool, table, pk, cutoff) {
  if (cutoff == null) return 0;
  const [rows] = await srcPool.query(`SELECT COUNT(*) AS c FROM \`${table}\` WHERE \`${pk}\` <= ?`, [cutoff]);
  return rows?.[0]?.c ?? 0;
}

async function copyTableDeferredWithCutoff(cfg, pools, table, pk, cutoffId, progress) {
  const srcConn = await pools.src.getConnection();
  const dstConn = await pools.dst.getConnection();
  let srcRaw = null;
  try {
    const mysqlRaw = await import('mysql2');
    srcRaw = mysqlRaw.createConnection({
      host: cfg.src.host,
      port: cfg.src.port,
      user: cfg.src.user,
      password: cfg.src.password,
      database: cfg.src.database,
      maxAllowedPacket: 67108864,
    });

    await dstConn.query('SET FOREIGN_KEY_CHECKS=0');
    await dstConn.query('SET UNIQUE_CHECKS=0');

    // Optional lock timeouts
    try {
      const lockSec = Math.max(1, Math.floor((cfg.flushTimeoutMs || 300000) / 1000));
      await dstConn.query(`SET SESSION lock_wait_timeout=${lockSec}`);
      await dstConn.query(`SET SESSION innodb_lock_wait_timeout=${lockSec}`);
    } catch {}

    const BATCH_SIZE = cfg.batchSize;
    let last = null;
    let copied = 0;

    await dstConn.beginTransaction();

    for (;;) {
      const [rows] = await srcConn.query(
        `SELECT * FROM \`${table}\`
         WHERE ${last==null? '1=1' : `\`${pk}\` > ?`} AND \`${pk}\` <= ?
         ORDER BY \`${pk}\`
         LIMIT ?`,
        last==null ? [cutoffId, BATCH_SIZE] : [last, cutoffId, BATCH_SIZE]
      );

      if (!rows.length) break;

      const columns = Object.keys(rows[0]);
      const colList = columns.map(c => `\`${c}\``).join(',');
      const placeholdersRow = `(${columns.map(() => '?').join(',')})`;
      const placeholders = new Array(rows.length).fill(placeholdersRow).join(',');
      const sql = `INSERT INTO \`${table}\` (${colList}) VALUES ${placeholders}`;
      const params = [];
      for (const r of rows) {
        for (const c of columns) params.push(r[c]);
      }
      await withTimeout(dstConn.query(sql, params), cfg.flushTimeoutMs, `INSERT batch em ${table}`);
      copied += rows.length;
      if (progress?.update) progress.update(table, copied, `copiando dados (${copied})`);
      last = rows[rows.length - 1][pk];
    }

    await dstConn.commit();
  } catch (e) {
    try { await dstConn.rollback(); } catch {}
    throw e;
  } finally {
    try { await dstConn.query('SET UNIQUE_CHECKS=1'); } catch {}
    try { await dstConn.query('SET FOREIGN_KEY_CHECKS=1'); } catch {}
    srcConn.release();
    dstConn.release();
    try { srcRaw && srcRaw.end(); } catch {}
  }
}
