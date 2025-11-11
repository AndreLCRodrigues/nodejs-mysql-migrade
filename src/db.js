import { logger } from './logger.js';

export async function createPools(cfg) {
  if (cfg.skipConnect) {
    logger.info('SKIP_CONNECT ativado: conexões com o banco serão puladas.');
    return {
      src: null,
      dst: null,
      end: async () => {},
    };
  }
  const mysql = await import('mysql2/promise');

  const src = await mysql.createPool({
    host: cfg.src.host,
    port: cfg.src.port,
    user: cfg.src.user,
    password: cfg.src.password,
    database: cfg.src.database,
    connectionLimit: Math.max(2, cfg.concurrency),
    decimalNumbers: true,
    supportBigNumbers: true,
    dateStrings: true,
  });

  const dst = await mysql.createPool({
    host: cfg.dst.host,
    port: cfg.dst.port,
    user: cfg.dst.user,
    password: cfg.dst.password,
    database: cfg.dst.database,
    connectionLimit: Math.max(2, cfg.concurrency),
    multipleStatements: true,
    decimalNumbers: true,
    supportBigNumbers: true,
    dateStrings: true,
  });

  return {
    src,
    dst,
    end: async () => {
      try { await src.end(); } catch {}
      try { await dst.end(); } catch {}
    },
  };
}

export async function listTables(pool, database) {
  if (!pool) return [];
  const sql = `SELECT TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'`;
  const [rows] = await pool.query(sql, [database]);
  return rows.map(r => r.name);
}

export default { createPools, listTables };
