// Unit tests for the row-compare display helpers (row_compare.js).
// These are the pure formatters the reconciliation UI uses to render counts,
// diffs, date ranges, and HTML-escaped labels.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readStatic, extract, sandbox } from './helpers.mjs';

const src = readStatic('row_compare.js');
const ctx = sandbox([
  extract(src, 'fmtCount'),
  extract(src, 'fmtDiff'),
  extract(src, 'fmtRange'),
  extract(src, 'esc'),
]);

test('fmtCount: null -> em-dash, numbers -> locale string', () => {
  assert.equal(ctx.fmtCount(null), '—');
  assert.equal(ctx.fmtCount(undefined), '—');
  assert.equal(ctx.fmtCount(1000), (1000).toLocaleString());
});

test('fmtDiff: zero, positive gets +, negative keeps -', () => {
  assert.equal(ctx.fmtDiff(0), '0');
  assert.equal(ctx.fmtDiff(5), '+' + (5).toLocaleString());
  assert.equal(ctx.fmtDiff(-5), (-5).toLocaleString());
});

test('fmtRange: renders "a → b", em-dash when empty', () => {
  assert.equal(ctx.fmtRange(null), '—');
  assert.equal(ctx.fmtRange([null, null]), '—');
  assert.equal(ctx.fmtRange(['2026-01-01', '2026-04-30']),
    '2026-01-01 → 2026-04-30');
});

test('esc: escapes <, >, " and treats falsy as empty', () => {
  assert.equal(ctx.esc(null), '');
  assert.equal(ctx.esc(''), '');
  assert.equal(ctx.esc('a<b>"c'), 'a&lt;b&gt;&quot;c');
});
