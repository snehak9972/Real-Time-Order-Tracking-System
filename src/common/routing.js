const crypto = require('crypto');

// Deterministic kitchen assignment: same order id always maps to the same kitchen.
function pickKitchen(orderId, kitchenIds) {
  if (!kitchenIds.length) throw new Error('no kitchens configured');
  const n = crypto.createHash('sha1').update(orderId).digest().readUInt32BE(0);
  return kitchenIds[n % kitchenIds.length];
}

function validateOrder(body) {
  const errors = [];
  if (!body || typeof body.customer !== 'string' || !body.customer.trim()) errors.push('customer is required');
  if (!Array.isArray(body?.items) || body.items.length === 0) errors.push('items must be a non-empty array');
  else body.items.forEach((it, i) => {
    if (typeof it.name !== 'string' || !it.name) errors.push(`items[${i}].name is required`);
    if (!Number.isInteger(it.qty) || it.qty < 1) errors.push(`items[${i}].qty must be a positive integer`);
  });
  return errors;
}

module.exports = { pickKitchen, validateOrder };
