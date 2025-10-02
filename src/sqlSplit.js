// Lightweight SQL splitter for MySQL dumps: respects quotes and escapes; ignores semicolons inside strings.
// Not a full parser; suitable for table schema+INSERT dumps (no procedures/triggers).

export function createSqlAccumulator(onStatement) {
  let buf = '';
  let inSingle = false;
  let inDouble = false;
  let inBacktick = false;
  let inLineComment = false;
  let inBlockComment = false;
  let prev = '';

  function feed(chunk) {
    for (let i = 0; i < chunk.length; i++) {
      const ch = chunk[i];
      const next = i + 1 < chunk.length ? chunk[i + 1] : '';

      // Handle line comments -- ...\n
      if (!inSingle && !inDouble && !inBacktick && !inBlockComment) {
        if (!inLineComment && ch === '-' && next === '-') {
          inLineComment = true;
          i++; // skip next '-'
          continue;
        }
      }
      if (inLineComment) {
        if (ch === '\n') inLineComment = false;
        continue;
      }

      // Handle block comments /* ... */
      if (!inSingle && !inDouble && !inBacktick && !inLineComment) {
        if (!inBlockComment && ch === '/' && next === '*') {
          inBlockComment = true; i++; continue;
        }
        if (inBlockComment && ch === '*' && next === '/') {
          inBlockComment = false; i++; continue;
        }
        if (inBlockComment) continue;
      }

      // Strings / identifiers
      if (!inDouble && !inBacktick && ch === "'" && prev !== '\\') inSingle = !inSingle;
      else if (!inSingle && !inBacktick && ch === '"' && prev !== '\\') inDouble = !inDouble;
      else if (!inSingle && !inDouble && ch === '`' && prev !== '\\') inBacktick = !inBacktick;

      buf += ch;

      if (!inSingle && !inDouble && !inBacktick && ch === ';') {
        const stmt = buf.trim();
        buf = '';
        if (stmt) onStatement(stmt);
      }

      prev = ch;
    }
  }

  function end() {
    const stmt = buf.trim();
    buf = '';
    if (stmt) onStatement(stmt);
  }

  return { feed, end };
}

export default createSqlAccumulator;

