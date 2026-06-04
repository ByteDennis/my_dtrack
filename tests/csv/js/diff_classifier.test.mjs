// Unit tests for the column-compare mismatch classifier (col_compare.js) and
// the csv-compare HTML escaper (csv_compare.js).
//
// `_compHasDiff(comp)` decides whether a per-(column, date) comparison row is
// a mismatch given the user's active "Compare fields" selection — the core
// reconciliation predicate on the frontend. We stub `_getCompareFields()`
// (normally localStorage-backed) to drive the active set directly.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readStatic, extract, sandbox } from './helpers.mjs';

const colCompare = readStatic('col_compare.js');
const csvCompare = readStatic('csv_compare.js');

function classifierWithActiveFields(active) {
  return sandbox([
    `function _getCompareFields(){ return ${JSON.stringify(active)}; }`,
    extract(colCompare, '_DIFF_FNS'),
    extract(colCompare, '_compHasDiff'),
  ]);
}

test('_compHasDiff: flags an active count field that differs', () => {
  const ctx = classifierWithActiveFields(['n_total', 'min']);
  assert.equal(ctx._compHasDiff({ n_total_diff: 1 }), true);
  assert.equal(ctx._compHasDiff({ min_match: false }), true);
});

test('_compHasDiff: ignores diffs in INACTIVE fields', () => {
  const ctx = classifierWithActiveFields(['max']); // only max is active
  // n_total and min differ, but neither is selected -> not a mismatch
  assert.equal(ctx._compHasDiff({ n_total_diff: 5, min_match: false }), false);
  assert.equal(ctx._compHasDiff({ max_match: false }), true);
});

test('_compHasDiff: a fully-matching row is not a mismatch', () => {
  const ctx = classifierWithActiveFields(
    ['n_total', 'n_missing', 'n_unique', 'min', 'max', 'hash_sum']);
  assert.equal(ctx._compHasDiff({
    n_total_diff: 0, n_missing_diff: 0, n_unique_diff: 0,
    min_match: true, max_match: true, hash_sum_match: true,
  }), false);
});

test('_compHasDiff: hash_sum drift surfaces when active', () => {
  const active = classifierWithActiveFields(['hash_sum']);
  assert.equal(active._compHasDiff({ hash_sum_match: false }), true);
  const inactive = classifierWithActiveFields(['n_total']);
  assert.equal(inactive._compHasDiff({ hash_sum_match: false }), false);
});

const cvCtx = sandbox([extract(csvCompare, '_cvEsc')]);

test('_cvEsc: escapes &, <, > and maps null/undefined to empty', () => {
  assert.equal(cvCtx._cvEsc(null), '');
  assert.equal(cvCtx._cvEsc(undefined), '');
  assert.equal(cvCtx._cvEsc('a & b < c > d'), 'a &amp; b &lt; c &gt; d');
  assert.equal(cvCtx._cvEsc(42), '42');
});
