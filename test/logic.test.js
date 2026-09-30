const test = require('node:test');
const assert = require('node:assert/strict');
const { canTransition } = require('../src/common/status');
const { pickKitchen, validateOrder } = require('../src/common/routing');

test('status moves forward one step at a time', () => {
  assert.ok(canTransition('PLACED', 'ACCEPTED'));
  assert.ok(canTransition('READY', 'OUT_FOR_DELIVERY'));
});
test('rejects duplicates, skips and going backwards', () => {
  assert.ok(!canTransition('ACCEPTED', 'ACCEPTED'));
  assert.ok(!canTransition('PLACED', 'READY'));
  assert.ok(!canTransition('DELIVERED', 'PREPARING'));
});
test('cancel only before preparation starts', () => {
  assert.ok(canTransition('PLACED', 'CANCELLED'));
  assert.ok(!canTransition('PREPARING', 'CANCELLED'));
});
test('kitchen routing is deterministic and spreads load', () => {
  const ks = ['kitchen-1', 'kitchen-2'];
  assert.equal(pickKitchen('abc', ks), pickKitchen('abc', ks));
  const seen = new Set(Array.from({ length: 50 }, (_, i) => pickKitchen(`order-${i}`, ks)));
  assert.equal(seen.size, 2);
});
test('validation catches bad payloads', () => {
  assert.equal(validateOrder({ customer: 'A', items: [{ name: 'x', qty: 1 }] }).length, 0);
  assert.ok(validateOrder({ customer: '', items: [] }).length >= 2);
  assert.equal(validateOrder({ customer: 'A', items: [{ name: 'x', qty: 0 }] }).length, 1);
});
