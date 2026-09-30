// Order lifecycle as a small state machine. Keeping it pure makes it easy to unit test
// and makes consumers idempotent: duplicate / out-of-order events are simply rejected.
const FLOW = ['PLACED', 'ACCEPTED', 'PREPARING', 'READY', 'OUT_FOR_DELIVERY', 'DELIVERED'];
const CANCELLABLE = new Set(['PLACED', 'ACCEPTED']);

function canTransition(from, to) {
  if (to === 'CANCELLED') return CANCELLABLE.has(from);
  const i = FLOW.indexOf(from);
  const j = FLOW.indexOf(to);
  return i !== -1 && j === i + 1;
}

module.exports = { FLOW, canTransition };
