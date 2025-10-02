let chalk = null;
try {
  // Lazy optional import; may fail if dependencies not installed (dry-run)
  const mod = await import('chalk');
  chalk = mod.default || mod;
} catch (_) {
  // no chalk; fallback to no-color
}

const tag = (label, color) => {
  const text = `[${label}]`;
  if (!chalk) return text;
  try { return chalk[color] ? chalk[color](text) : text; } catch { return text; }
};

const colorize = (s, color) => {
  if (!chalk) return s;
  try { return chalk[color] ? chalk[color](s) : s; } catch { return s; }
};

export const logger = {
  info: (...args) => console.log(tag('info', 'cyan'), ...args),
  warn: (...args) => console.warn(tag('warn', 'yellow'), ...args),
  error: (...args) => console.error(tag('error', 'red'), ...args),
  success: (...args) => console.log(tag('ok', 'green'), ...args),
  dim: (...args) => console.log(colorize(args.join(' '), 'gray')),
};

export default logger;
