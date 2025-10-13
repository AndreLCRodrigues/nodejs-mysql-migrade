import fs from 'fs';
import path from 'path';

// Ensure logs directory exists
const logsDir = path.join(process.cwd(), 'logs');
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir, { recursive: true });
}

// Create write stream for log file
const logStream = fs.createWriteStream(path.join(logsDir, 'migration.log'), { flags: 'a' });

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

// Helper to write to file
const writeToFile = (message) => {
  logStream.write(message + '\n');
};

export const logger = {
  info: (...args) => {
    const message = [tag('info', 'cyan'), ...args].join(' ');
    console.log(message);
    writeToFile(message);
  },
  warn: (...args) => {
    const message = [tag('warn', 'yellow'), ...args].join(' ');
    console.warn(message);
    writeToFile(message);
  },
  error: (...args) => {
    const message = [tag('error', 'red'), ...args].join(' ');
    console.error(message);
    writeToFile(message);
  },
  success: (...args) => {
    const message = [tag('ok', 'green'), ...args].join(' ');
    console.log(message);
    writeToFile(message);
  },
  dim: (...args) => {
    const message = colorize(args.join(' '), 'gray');
    console.log(message);
    writeToFile(message);
  },
};

export default logger;
