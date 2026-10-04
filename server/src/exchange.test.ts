import test from 'node:test'; import assert from 'node:assert/strict';
import { containers, voyages, tasks, appointments, updateContainer } from './domain.js';
import { exportBatch, getBatch, importBatch, retryBatch } from './exchange.js';

const baseContainer = { voyageRef: 'VOY-AX7', size: '40', iso: '45G1', flow: 'IMP', ctrStatus: 'YARD', grossWeightKg: 21000, reefer: 'N', customsStatus: 'RELEASED', priorityCode: 'N', position: 'C03-R09-T1' };

test('import maps external container shape into domain model with normalized key, units and enums', () => {
  const { batch } = importBatch('TOS', { messageId: 'MSG-T1', containers: [{ ...baseContainer, cntrNo: 'TCLU5550001' }] });
  assert.equal(batch.status, 'completed');
  const container = containers.find(item => item.id === 'TCLU-5550001');
  assert.ok(container);
  assert.equal(container.weightT, 21);
  assert.equal(container.direction, 'import');
  assert.equal(container.customs, 'released');
  assert.equal(container.lastEvent.includes('TOS'), true);
});

test('duplicate messageId is idempotent; same id with different content is rejected', () => {
  const payload = { messageId: 'MSG-T2', containers: [{ ...baseContainer, cntrNo: 'TCLU5550002' }] };
  const first = importBatch('TOS', payload);
  const second = importBatch('TOS', payload);
  assert.equal(second.duplicate, true);
  assert.equal(second.batch.id, first.batch.id);
  assert.equal(containers.filter(item => item.id === 'TCLU-5550002').length, 1);
  assert.throws(() => importBatch('TOS', { ...payload, containers: [{ ...baseContainer, cntrNo: 'TCLU5550003' }] }), /内容不一致/);
});

test('unknown enum values are quarantined and never touch the domain', () => {
  const { batch } = importBatch('TOS', { messageId: 'MSG-T3', containers: [{ ...baseContainer, cntrNo: 'TCLU5550004', customsStatus: 'X-RAY' }] });
  const record = batch.records[0];
  assert.equal(record.status, 'quarantined');
  assert.equal(record.retryable, true);
  assert.match(record.rejectReason ?? '', /未知枚举/);
  assert.equal(containers.some(item => item.id === 'TCLU-5550004'), false);
});

test('local manual edits win over external updates until a dispatcher resolves the conflict', () => {
  importBatch('TOS', { messageId: 'MSG-T4A', containers: [{ ...baseContainer, cntrNo: 'TCLU5550005' }] });
  updateContainer('TCLU-5550005', { customs: 'hold' });
  const { batch } = importBatch('TOS', { messageId: 'MSG-T4B', containers: [{ ...baseContainer, cntrNo: 'TCLU5550005', customsStatus: 'RELEASED', grossWeightKg: 23000 }] });
  const record = batch.records[0];
  assert.equal(record.status, 'conflict');
  assert.ok(record.diff.some(item => item.field === 'customs'));
  assert.equal(containers.find(item => item.id === 'TCLU-5550005')?.customs, 'hold');
  retryBatch(batch.id, undefined, { [record.id]: 'keep-local' });
  assert.equal(record.status, 'resolved-local');
  assert.equal(containers.find(item => item.id === 'TCLU-5550005')?.customs, 'hold');
  const again = importBatch('TOS', { messageId: 'MSG-T4C', containers: [{ ...baseContainer, cntrNo: 'TCLU5550005', customsStatus: 'RELEASED', grossWeightKg: 23000 }] });
  const conflicted = again.batch.records[0];
  assert.equal(conflicted.status, 'conflict');
  retryBatch(again.batch.id, undefined, { [conflicted.id]: 'accept-external' });
  assert.equal(conflicted.status, 'resolved-external');
  assert.equal(containers.find(item => item.id === 'TCLU-5550005')?.customs, 'released');
});

test('partial import failure rolls back only the failing record', () => {
  const { batch } = importBatch('TOS', {
    messageId: 'MSG-T5',
    containers: [
      { ...baseContainer, cntrNo: 'TCLU5550006' },
      { ...baseContainer, cntrNo: 'TCLU5550007', position: 'Z99-R01-T1' }
    ]
  });
  assert.equal(batch.status, 'partial');
  assert.equal(containers.some(item => item.id === 'TCLU-5550006'), true);
  assert.equal(containers.some(item => item.id === 'TCLU-5550007'), false);
  const failed = batch.records.find(record => record.externalKey === 'TCLU-5550007');
  assert.equal(failed?.status, 'rejected');
  assert.match(failed?.rejectReason ?? '', /区块不存在/);
});

test('delete semantics guard active links and tombstone prevents silent resurrection', () => {
  importBatch('TOS', { messageId: 'MSG-T6A', containers: [{ ...baseContainer, cntrNo: 'TCLU5550008' }] });
  const blocked = importBatch('TOS', { messageId: 'MSG-T6B', containers: [{ cntrNo: 'FCIU9920310', deleted: true }] });
  assert.equal(blocked.batch.records[0].status, 'rejected');
  assert.match(blocked.batch.records[0].rejectReason ?? '', /预约/);
  assert.ok(containers.some(item => item.id === 'FCIU-9920310'));
  const gone = importBatch('TOS', { messageId: 'MSG-T6C', containers: [{ cntrNo: 'TCLU5550008', deleted: true }] });
  assert.equal(gone.batch.records[0].status, 'deleted');
  assert.equal(containers.some(item => item.id === 'TCLU-5550008'), false);
  const recreate = importBatch('TOS', { messageId: 'MSG-T6D', containers: [{ ...baseContainer, cntrNo: 'TCLU5550008' }] });
  assert.equal(recreate.batch.records[0].status, 'created');
  assert.match(recreate.batch.records[0].note ?? '', /重新建档/);
});

test('events are idempotent by eventId and stale yard positions are skipped', () => {
  importBatch('TOS', { messageId: 'MSG-T7A', containers: [{ ...baseContainer, cntrNo: 'TCLU5550009' }] });
  const first = importBatch('TOS', { messageId: 'MSG-T7B', events: [{ eventId: 'EVT-T7', cntrNo: 'TCLU5550009', eventType: 'GATE_IN', eventTime: '2026-10-04 09:00' }] });
  assert.equal(first.batch.records[0].status, 'updated');
  assert.equal(containers.find(item => item.id === 'TCLU-5550009')?.status, 'at-gate');
  const second = importBatch('TOS', { messageId: 'MSG-T7C', events: [{ eventId: 'EVT-T7', cntrNo: 'TCLU5550009', eventType: 'GATE_IN', eventTime: '2026-10-04 09:00' }] });
  assert.equal(second.batch.records[0].status, 'skipped-duplicate');
  const stale = importBatch('TOS', { messageId: 'MSG-T7D', yardPositions: [{ cntrNo: 'TCLU5550009', position: 'C03-R02-T2', eventTime: '2026-10-04 08:00' }] });
  const fresh = importBatch('TOS', { messageId: 'MSG-T7E', yardPositions: [{ cntrNo: 'TCLU5550009', position: 'C03-R03-T3', eventTime: '2026-10-04 10:00' }] });
  const older = importBatch('TOS', { messageId: 'MSG-T7F', yardPositions: [{ cntrNo: 'TCLU5550009', position: 'C03-R04-T4', eventTime: '2026-10-04 09:30' }] });
  assert.equal(stale.batch.records[0].status, 'updated');
  assert.equal(fresh.batch.records[0].status, 'updated');
  assert.equal(older.batch.records[0].status, 'skipped-stale');
  assert.equal(containers.find(item => item.id === 'TCLU-5550009')?.yardSlot, 'C03-R03-T3');
});

test('inspect events create yard tasks; export emits external task and gate shapes', () => {
  const before = tasks.length;
  importBatch('TOS', { messageId: 'MSG-T8A', events: [{ eventId: 'EVT-T8', cntrNo: 'MSCU4829017', eventType: 'INSPECT', eventTime: '2026-10-04 11:00' }] });
  assert.equal(tasks.length, before + 1);
  assert.equal(tasks[0].kind, 'inspect');
  const batch = exportBatch({});
  assert.equal(batch.direction, 'export');
  assert.ok(batch.records.some(record => record.entity === 'task'));
  assert.ok(batch.records.some(record => record.entity === 'gate-status'));
  const payload = batch.payload as { tasks: { taskStatus: string }[]; gateStatus: { bookingStatus: string; lane: string }[] };
  assert.ok(payload.tasks.every(item => ['QUEUED', 'ASSIGNED', 'WORKING', 'BLOCKED', 'DONE'].includes(item.taskStatus)));
  assert.ok(payload.gateStatus.every(item => ['N', 'S', 'R'].includes(item.lane)));
  assert.ok(getBatch(batch.id));
});

test('voyage import conflicts with local replan audit and resolves explicitly', () => {
  const { batch } = importBatch('TOS', { messageId: 'MSG-T9', voyages: [{ voyageRef: 'VOY-AX7', voyageStatus: 'WORKING', completedMoves: 900, plannedMoves: 1240 }] });
  const record = batch.records[0];
  assert.equal(record.status, 'conflict');
  assert.equal(voyages.find(item => item.id === 'VOY-AX7')?.status, 'berthed');
  retryBatch(batch.id, undefined, { [record.id]: 'accept-external' });
  assert.equal(voyages.find(item => item.id === 'VOY-AX7')?.status, 'loading');
  assert.equal(appointments.length > 0, true);
});
