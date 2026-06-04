// Test helpers for exercising the (script-tag-global, non-module) frontend
// helpers in dtrack/web/static/*.js without a browser/DOM.
//
// The page scripts run top-level DOM code, so we can't import them wholesale.
// Instead we extract a single named function / const by brace-matching and
// eval just that snippet in a fresh vm context. Intentionally simple — it
// targets small pure helpers whose bodies have balanced braces (template
// `${...}` pairs are balanced, so the counter stays correct).

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import vm from 'node:vm';

const HERE = dirname(fileURLToPath(import.meta.url));
export const STATIC_DIR = resolve(HERE, '../../../dtrack/web/static');

export function readStatic(file) {
  return readFileSync(resolve(STATIC_DIR, file), 'utf8');
}

// Extract `function NAME(...) {...}` or `const NAME = ...;` from `src`.
export function extract(src, name) {
  const decl = new RegExp(`(?:function\\s+${name}\\s*\\(|const\\s+${name}\\s*=)`);
  const m = decl.exec(src);
  if (!m) throw new Error(`declaration not found: ${name}`);
  const start = m.index;
  let i = src.indexOf('{', start);
  if (i < 0) throw new Error(`no opening brace for ${name}`);
  let depth = 0;
  for (; i < src.length; i++) {
    const c = src[i];
    if (c === '{') depth++;
    else if (c === '}') { depth--; if (depth === 0) { i++; break; } }
  }
  if (src[i] === ';') i++;
  return src.slice(start, i).trim();
}

// Eval the given snippets (joined) in a fresh context and return that context.
// Function declarations become properties on the context global, so callers do
// `ctx.fnName(...)`. `globals` seeds extra bindings (stubs, etc.).
export function sandbox(snippets, globals = {}) {
  const ctx = vm.createContext({ ...globals });
  vm.runInContext(snippets.join('\n'), ctx);
  return ctx;
}
