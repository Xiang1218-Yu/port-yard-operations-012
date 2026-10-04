import test from 'node:test';
import assert from 'node:assert/strict';
import { containers, createYardTask, updateContainer, voyages } from './domain.js';
import { batchDetail, exportTasksBatch, importBatch, resolveConflict, retryBatchItems } from './exchange.js';

const later = (minutes: number) => new Date(Date.now() + minutes * 60000).toISOString();

test('TOS 导入完成字段映射：外部枚举与重量单位换算为领域模型', () => {
  const result = importBatch({
    batchId: 'TOS-T1-MAP', source: 'TOS-TEST',
    voyages: [{ voyage: 'VOY-T1', vesselName: 'Test Vessel', imo: 'IMO0000001', service: 'TS1', eta: later(60), etd: later(600), voyageStatus: 'ARRIVING', berth: 'B01', plannedMoves: 100, doneMoves: 0 }],
    containers: [{ cntrNo: 'tstc-0000001', voyage: 'VOY-T1', size: '40GP', iso: '45G1', flow: 'I', ctrStatus: 'YARD', weightKg: 21500, reefer: 'N', customs: 'R', slot: 'C03-R11-T1' }]
  });
  assert.equal(result.duplicate, false);
  assert.equal(result.batch.summary.applied, 2);
  const container = containers.find(c => c.id === 'TSTC-0000001');
  assert.ok(container);
  assert.equal(container.direction, 'import');
  assert.equal(container.status, 'in-yard');
  assert.equal(container.customs, 'released');
  assert.equal(container.weightT, 21.5);
  assert.equal(container.size, '40');
  const voyage = voyages.find(v => v.id === 'VOY-T1');
  assert.equal(voyage?.status, 'approaching');
});

test('重复批次按 batchId 幂等返回首次结果，不重复落账', () => {
  const payload = {
    batchId: 'TOS-T2-DUP',
    containers: [{ cntrNo: 'TSTC-0000002', voyage: 'VOY-T1', size: '20', iso: '22G1', flow: 'E', ctrStatus: 'YARD', weightKg: 10000, reefer: 'N', customs: 'R', slot: 'C03-R12-T1' }]
  };
  const first = importBatch(payload);
  const count = containers.filter(c => c.id === 'TSTC-0000002').length;
  const second = importBatch(payload);
  assert.equal(second.duplicate, true);
  assert.equal(second.batch.id, first.batch.id);
  assert.equal(containers.filter(c => c.id === 'TSTC-0000002').length, count);
});

test('未知枚举被隔离：记录拒绝原因并保留原始报文，不污染领域模型', () => {
  const result = importBatch({
    batchId: 'TOS-T3-ENUM',
    containers: [{ cntrNo: 'TSTC-0000003', voyage: 'VOY-T1', size: '20', iso: '22G1', flow: 'I', ctrStatus: 'YARD', weightKg: 9000, reefer: 'N', customs: 'X', slot: 'C03-R13-T1' }]
  });
  assert.equal(result.batch.status, 'rejected');
  const item = result.batch.items[0];
  assert.equal(item.status, 'rejected');
  assert.match(item.reason ?? '', /未知枚举 customs='X'/);
  assert.ok(item.rawPayload);
  assert.equal(containers.some(c => c.id === 'TSTC-0000003'), false);
});

test('本地人工修改优先：外部更新挂起为冲突，裁决接受外部后才覆盖', () => {
  importBatch({
    batchId: 'TOS-T4-SEED',
    containers: [{ cntrNo: 'TSTC-0000004', voyage: 'VOY-T1', size: '20', iso: '22G1', flow: 'I', ctrStatus: 'YARD', weightKg: 10000, reefer: 'N', customs: 'R', slot: 'C03-R14-T1' }]
  });
  updateContainer('TSTC-0000004', { weightT: 18 }); // 调度员人工改重量
  const result = importBatch({
    batchId: 'TOS-T4-CONFLICT',
    containers: [{ cntrNo: 'TSTC-0000004', voyage: 'VOY-T1', size: '20', iso: '22G1', flow: 'I', ctrStatus: 'YARD', weightKg: 12000, reefer: 'N', customs: 'R', slot: 'C03-R14-T1' }]
  });
  const item = result.batch.items[0];
  assert.equal(item.status, 'conflict');
  assert.equal(containers.find(c => c.id === 'TSTC-0000004')?.weightT, 18); // 本地值未被覆盖
  const weightDiff = item.diff?.find(d => d.field === 'weightT');
  assert.equal(weightDiff?.local, 18);
  assert.equal(weightDiff?.external, 12);
  const resolved = resolveConflict(result.batch.id, item.key, 'accept-external');
  assert.equal(resolved?.items[0].status, 'applied');
  assert.equal(containers.find(c => c.id === 'TSTC-0000004')?.weightT, 12);
});

test('裁决保留本地：冲突项关闭且本地值不变', () => {
  importBatch({
    batchId: 'TOS-T5-SEED',
    containers: [{ cntrNo: 'TSTC-0000005', voyage: 'VOY-T1', size: '20', iso: '22G1', flow: 'I', ctrStatus: 'YARD', weightKg: 10000, reefer: 'N', customs: 'R', slot: 'C03-R15-T1' }]
  });
  updateContainer('TSTC-0000005', { weightT: 17 });
  const result = importBatch({
    batchId: 'TOS-T5-CONFLICT',
    containers: [{ cntrNo: 'TSTC-0000005', voyage: 'VOY-T1', size: '20', iso: '22G1', flow: 'I', ctrStatus: 'YARD', weightKg: 11000, reefer: 'N', customs: 'R', slot: 'C03-R15-T1' }]
  });
  const resolved = resolveConflict(result.batch.id, result.batch.items[0].key, 'keep-local');
  assert.equal(resolved?.items[0].status, 'skipped');
  assert.equal(containers.find(c => c.id === 'TSTC-0000005')?.weightT, 17);
});

test('原子批次任一失败即整体回滚，已成功项不落账', () => {
  const result = importBatch({
    batchId: 'TOS-T6-ATOMIC', mode: 'atomic',
    containers: [
      { cntrNo: 'TSTC-0000006', voyage: 'VOY-T1', size: '20', iso: '22G1', flow: 'I', ctrStatus: 'YARD', weightKg: 8000, reefer: 'N', customs: 'R', slot: 'C03-R16-T1' },
      { cntrNo: 'TSTC-0000007', voyage: 'VOY-T1', size: '20', iso: '22G1', flow: 'I', ctrStatus: 'YARD', weightKg: 8000, reefer: 'N', customs: 'BAD', slot: 'C03-R17-T1' }
    ]
  });
  assert.equal(result.batch.status, 'rolled-back');
  assert.equal(containers.some(c => c.id === 'TSTC-0000006'), false);
  assert.equal(containers.some(c => c.id === 'TSTC-0000007'), false);
  assert.ok(result.batch.items.every(item => item.status === 'rolled-back' || item.status === 'rejected'));
});

test('删除语义：有进行中业务的箱拒绝删除，无关联的箱可删除', () => {
  importBatch({
    batchId: 'TOS-T7-SEED',
    containers: [
      { cntrNo: 'TSTC-0000008', voyage: 'VOY-T1', size: '20', iso: '22G1', flow: 'I', ctrStatus: 'YARD', weightKg: 8000, reefer: 'N', customs: 'R', slot: 'C03-R18-T1' },
      { cntrNo: 'TSTC-0000009', voyage: 'VOY-T1', size: '20', iso: '22G1', flow: 'I', ctrStatus: 'YARD', weightKg: 8000, reefer: 'N', customs: 'R', slot: 'C03-R19-T1' }
    ]
  });
  createYardTask({ kind: 'deliver', containerId: 'TSTC-0000008', from: 'C03-R18-T1', to: '南闸口', priority: 50, scheduledAt: later(30), estimatedMinutes: 15 });
  const result = importBatch({
    batchId: 'TOS-T7-DELETE',
    containers: [
      { cntrNo: 'TSTC-0000008', voyage: 'VOY-T1', size: '20', iso: '22G1', flow: 'I', ctrStatus: 'YARD', weightKg: 8000, reefer: 'N', customs: 'R', slot: 'C03-R18-T1', op: 'delete' },
      { cntrNo: 'TSTC-0000009', voyage: 'VOY-T1', size: '20', iso: '22G1', flow: 'I', ctrStatus: 'YARD', weightKg: 8000, reefer: 'N', customs: 'R', slot: 'C03-R19-T1', op: 'delete' }
    ]
  });
  const [blocked, removed] = result.batch.items;
  assert.equal(blocked.status, 'rejected');
  assert.match(blocked.reason ?? '', /拒绝外部删除/);
  assert.equal(containers.some(c => c.id === 'TSTC-0000008'), true);
  assert.equal(removed.status, 'applied');
  assert.equal(containers.some(c => c.id === 'TSTC-0000009'), false);
});

test('作业事件按 eventId 幂等去重，过期事件不重放', () => {
  importBatch({
    batchId: 'TOS-T8-SEED',
    containers: [{ cntrNo: 'TSTC-0000010', voyage: 'VOY-T1', size: '20', iso: '22G1', flow: 'I', ctrStatus: 'YARD', weightKg: 8000, reefer: 'N', customs: 'R', slot: 'C03-R20-T1' }]
  });
  const first = importBatch({ batchId: 'TOS-T8-EV1', workEvents: [{ eventId: 'EV-T8-1', cntrNo: 'TSTC-0000010', eventType: 'CUSTOMS_HOLD', eventTime: later(10) }] });
  assert.equal(first.batch.items[0].status, 'applied');
  assert.equal(containers.find(c => c.id === 'TSTC-0000010')?.customs, 'hold');
  const dup = importBatch({ batchId: 'TOS-T8-EV2', workEvents: [{ eventId: 'EV-T8-1', cntrNo: 'TSTC-0000010', eventType: 'CUSTOMS_HOLD', eventTime: later(10) }] });
  assert.equal(dup.batch.items[0].status, 'skipped');
  const stale = importBatch({ batchId: 'TOS-T8-EV3', workEvents: [{ eventId: 'EV-T8-2', cntrNo: 'TSTC-0000010', eventType: 'CUSTOMS_RELEASE', eventTime: later(-50) }] });
  assert.equal(stale.batch.items[0].status, 'skipped');
  assert.equal(containers.find(c => c.id === 'TSTC-0000010')?.customs, 'hold');
});

test('只重试失败项：依赖修复后重试成功，已成功项不受影响', () => {
  const result = importBatch({
    batchId: 'TOS-T9-RETRY',
    containers: [
      { cntrNo: 'TSTC-0000011', voyage: 'VOY-T9-MISSING', size: '20', iso: '22G1', flow: 'I', ctrStatus: 'YARD', weightKg: 8000, reefer: 'N', customs: 'R', slot: 'C03-R01-T2' },
      { cntrNo: 'TSTC-0000012', voyage: 'VOY-T1', size: '20', iso: '22G1', flow: 'I', ctrStatus: 'YARD', weightKg: 8000, reefer: 'N', customs: 'R', slot: 'C03-R02-T2' }
    ]
  });
  assert.equal(result.batch.items[0].status, 'rejected');
  assert.equal(result.batch.items[1].status, 'applied');
  importBatch({ batchId: 'TOS-T9-VOYAGE', voyages: [{ voyage: 'VOY-T9-MISSING', vesselName: 'Late Vessel', imo: '', service: 'TS2', eta: later(120), etd: later(900), voyageStatus: 'ARRIVING', berth: 'B10', plannedMoves: 50, doneMoves: 0 }] });
  const retried = retryBatchItems(result.batch.id);
  assert.equal(retried?.items[0].status, 'applied');
  assert.equal(retried?.items[0].attempts, 2);
  assert.equal(retried?.items[1].status, 'applied');
  assert.equal(retried?.items[1].attempts, 1); // 成功项未被重放
  assert.equal(containers.some(c => c.id === 'TSTC-0000011'), true);
});

test('导出作业任务为 TOS 报文格式并落导出批次', () => {
  const batch = exportTasksBatch();
  assert.equal(batch.direction, 'export');
  assert.ok(batch.items.length > 0);
  const record = batch.items[0].rawPayload as Record<string, unknown>;
  assert.ok(record.taskId && record.cntrNo && record.taskStatus && record.schedTime);
  assert.ok(batchDetail(batch.id));
});
