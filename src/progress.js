export async function createProgress() {
  let cliProgress = null;
  let chalk = null;
  try {
    const mod = await import('cli-progress');
    cliProgress = mod.default || mod;
  } catch {}
  try {
    const mod = await import('chalk');
    chalk = mod.default || mod;
  } catch {}

  const isTTY = !!process.stdout.isTTY;

  // Console fallback (or when not TTY): print only on meaningful change (>=1% or phase change)
  if (!cliProgress || !isTTY) {
    const state = new Map(); // table -> { value, total, phase, lastPct, lastPhase }
    function addTable(table) {
      state.set(table, { value: 0, total: 100, phase: 'iniciando', lastPct: -1, lastPhase: '' });
      console.log(`[progress] ${table} iniciando`);
      return { update: () => {}, stop: () => {} };
    }
    function setTotal(table, total) {
      const s = state.get(table) || { value: 0, phase: '', lastPct: -1, lastPhase: '' };
      s.total = Math.max(1, total|0);
      state.set(table, s);
    }
    function maybePrint(table, s) {
      const pct = Math.floor((s.value / s.total) * 100);
      const phase = s.phase || '';
      if (pct !== s.lastPct || phase !== s.lastPhase || pct === 100) {
        console.log(`[progress] ${table} ${pct.toFixed(0)}% ${phase}`.trim());
        s.lastPct = pct; s.lastPhase = phase;
      }
    }
    function tick(table, delta = 1, phase) {
      const s = state.get(table) || { value: 0, total: 100, phase: '', lastPct: -1, lastPhase: '' };
      s.value = Math.min(s.total, s.value + (delta|0));
      if (phase) s.phase = phase;
      state.set(table, s);
      maybePrint(table, s);
    }
    function update(table, value, phase) {
      const s = state.get(table) || { value: 0, total: 100, phase: '', lastPct: -1, lastPhase: '' };
      if (typeof value === 'number' && Number.isFinite(value)) {
        s.value = Math.max(0, Math.min(s.total, value|0));
      }
      if (phase) s.phase = phase;
      state.set(table, s);
      maybePrint(table, s);
    }
    function done(table) {
      const s = state.get(table) || { total: 100, value: 0, phase: '', lastPct: -1, lastPhase: '' };
      s.value = s.total;
      s.phase = 'concluída';
      state.set(table, s);
      maybePrint(table, s);
      state.delete(table);
    }
    function stop() {}
    return { addTable, setTotal, tick, update, done, stop };
  }

  // TTY + cli-progress: granular bars per table
  const fmt = `${chalk ? chalk.gray('{bar}') : '{bar}'} {percentage}% | {value}/{total} | {table} {phase}`;
  const multibar = new cliProgress.MultiBar({
    clearOnComplete: true,
    hideCursor: true,
    format: fmt,
    barsize: 30,
    autopadding: true,
    noTTYOutput: false,
    forceRedraw: true,
  }, cliProgress.Presets.shades_classic);

  const bars = new Map();

  function addTable(table) {
    const bar = multibar.create(100, 0, { table, phase: (chalk ? chalk.gray('(iniciando)') : '(iniciando)') });
    bars.set(table, bar);
    return bar;
  }

  function setTotal(table, total) {
    const bar = bars.get(table);
    if (!bar) return;
    const t = Math.max(1, total|0);
    bar.setTotal(t);
  }

  function tick(table, delta = 1, phase) {
    const bar = bars.get(table);
    if (!bar) return;
    if (phase) bar.update(bar.value, { table, phase });
    bar.increment(Math.max(1, delta|0));
  }

  function update(table, value, phase) {
    const bar = bars.get(table);
    if (!bar) return;
    const val = (typeof value === 'number' && Number.isFinite(value)) ? value : bar.value;
    bar.update(val, { table, phase });
  }

  function done(table) {
    const bar = bars.get(table);
    if (!bar) return;
    const phaseText = chalk ? chalk.green('(concluída)') : '(concluída)';
    bar.update(bar.getTotal(), { table, phase: phaseText });
    bar.stop();
    bars.delete(table);
  }

  function stop() {
    try { multibar.stop(); } catch {}
  }

  return { addTable, setTotal, tick, update, done, stop };
}

export default createProgress;
