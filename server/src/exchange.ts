import { createHash } from 'node:crypto';
import { appointments, auditEvents, blocks, containers, tasks, voyages } from './domain.js';
import type { Container, GateAppointment, Voyage, YardTask } from './domain.js';

// ---- 交换批次与记录模型 ----
export type ExchangeEntity = 'voyage' | 'container' | 'yard-position' | 'event' | 'task' | 'gate-status';
export type RecordStatus = 'created' | 'updated' | 'deleted' | 'exported' | 'skipped-duplicate' | 'skipped-stale' | 'conflict' | 'rejected' | 'quarantined' | 'resolved-external' | 'resolved-local';
export interface FieldDiff { field: string; local: unknown; external: unknown }
export interface ExchangeRecord {
  id: string; entity: ExchangeEntity; externalKey: string; op: 'create' | 'update' | 'delete' | 'export';
  status: RecordStatus; diff: FieldDiff[]; rejectReason?: string; retryable: boolean; note?: string;
  raw: unknown; attempts: { at: string; status: RecordStatus; note?: string }[];
}
export interface ExchangeBatch {
  id: string; direction: 'import' | 'export'; source: string; externalId?: string; payloadHash: string;
  receivedAt: string; status: 'completed' | 'partial' | 'failed';
  summary: { total: number; applied: number; skipped: number; conflicts: number; rejected: number };
  records: ExchangeRecord[]; payload?: unknown;
}

// ---- TOS 外部报文形状（字段名、枚举、时间格式均与领域模型不同）----
interface TosVoyage { voyageRef: string; vesselName?: string; imo?: string; serviceCode?: string; eta?: string; etd?: string; voyageStatus?: string; berthCode?: string; plannedMoves?: number; completedMoves?: number; delayRisk?: string; deleted?: boolean; updatedAt?: string }
interface TosContainer { cntrNo: string; voyageRef?: string; size?: string | number; iso?: string; flow?: string; ctrStatus?: string; grossWeightKg?: number; reefer?: string | boolean; setTemp?: number; imoClass?: string; customsStatus?: string; priorityCode?: string; position?: string; deleted?: boolean; updatedAt?: string }
interface TosYardPosition { cntrNo: string; position: string; eventTime?: string }
interface TosEvent { eventId: string; cntrNo: string; eventType: string; eventTime?: string; position?: string; voyageRef?: string }
export interface TosPayload { messageId?: string; sentAt?: string; voyages?: TosVoyage[]; containers?: TosContainer[]; yardPositions?: TosYardPosition[]; events?: TosEvent[] }

const TOS_ACTOR = 'TOS-ADAPTER';
export const exchangeBatches: ExchangeBatch[] = [];
let batchSeq = 1000; let recordSeq = 1000;
const lastExternalApply = new Map<string, string>();   // `${entity}:${id}` -> 最近一次外部应用时间
const appliedHashes = new Map<string, string>();       // `${entity}:${id}` -> 已应用的规范化内容哈希，用于记录级幂等
const appliedEventIds = new Set<string>();             // 事件级幂等
const tombstones = new Map<string, string>();          // `${entity}:${id}` -> 删除时间，防止旧报文复活已删记录

const hash = (value: unknown) => createHash('sha1').update(JSON.stringify(value)).digest('hex');
const now = () => new Date().toISOString();
const syncKey = (entity: string, id: string) => `${entity}:${id}`;

// ---- 枚举映射表：未知值不猜测、不落地，整条记录进入隔离区 ----
const voyageStatusMap: Record<string, Voyage['status']> = { ARRIVING: 'approaching', BERTHED: 'berthed', WORKING: 'loading', SAILED: 'departed' };
const delayRiskMap: Record<string, Voyage['lateRisk']> = { LOW: 'low', NORMAL: 'medium', SEVERE: 'high' };
const flowMap: Record<string, Container['direction']> = { IMP: 'import', EXP: 'export', TRS: 'transshipment' };
const ctrStatusMap: Record<string, Container['status']> = { SAILING: 'in-transit', GATE_IN: 'at-gate', YARD: 'in-yard', QUAY: 'on-quay', DELIVERED: 'released', HELD: 'hold' };
const customsMap: Record<string, Container['customs']> = { RELEASED: 'released', INSPECT: 'inspection', HELD: 'hold' };
const priorityMap: Record<string, Container['priority']> = { N: 'normal', H: 'hot', R: 'reefer', D: 'hazmat' };
const eventTypeMap: Record<string, string> = { DISCHARGE: 'discharge', LOAD: 'load', GATE_IN: 'receive', GATE_OUT: 'deliver', MOVE: 'rehandle', INSPECT: 'inspect' };
const taskStatusExportMap: Record<string, string> = { queued: 'QUEUED', assigned: 'ASSIGNED', executing: 'WORKING', blocked: 'BLOCKED', done: 'DONE' };
const appointmentStatusExportMap: Record<string, string> = { booked: 'BOOKED', 'checked-in': 'ARRIVED', completed: 'DONE', cancelled: 'CANCELLED', exception: 'EXCEPTION' };
const laneExportMap: Record<string, string> = { north: 'N', south: 'S', rail: 'R' };

function mapEnum<T>(table: Record<string, T>, value: unknown, field: string, issues: string[]): T | undefined {
  if (value === undefined || value === null || value === '') return undefined;
  const key = String(value).trim().toUpperCase();
  if (key in table) return table[key];
  issues.push(`字段 ${field} 出现未知枚举值「${String(value)}」，已隔离未落地`);
  return undefined;
}
function parseExternalTime(value: string | undefined, field: string, issues: string[]): string | undefined {
  if (!value) return undefined;
  const parsed = new Date(value.includes('T') ? value : value.replace(' ', 'T'));
  if (Number.isNaN(parsed.getTime())) { issues.push(`字段 ${field} 时间格式无法解析「${value}」`); return undefined; }
  return parsed.toISOString();
}
const formatExternalTime = (iso: string) => { const d = new Date(iso); const p = (n: number) => String(n).padStart(2, '0'); return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`; };
function normalizeContainerNo(raw: string): string {
  const compact = raw.trim().toUpperCase().replace(/[\s-]/g, '');
  return /^[A-Z]{4}\d/.test(compact) ? `${compact.slice(0, 4)}-${compact.slice(4)}` : compact;
}
const parseSlot = (slot: string) => slot.match(/^([A-Z]\d\d)-/)?.[1];

// ---- 冲突判定：本地人工操作（非适配器写入的审计）优先于外部更新 ----
function hasLocalChanges(entity: string, id: string): boolean {
  const since = lastExternalApply.get(syncKey(entity, id)) ?? '';
  return auditEvents.some(event => event.entity === entity && event.entityId === id && event.actor !== TOS_ACTOR && event.at > since);
}
function noteAudit(action: string, entity: string, entityId: string, details: string) {
  auditEvents.unshift({ id: `AUD-${Date.now()}-${Math.floor(Math.random() * 1000)}`, actor: TOS_ACTOR, action, entity, entityId, at: now(), details });
}

// ---- 局部回滚：每条记录应用前快照，失败仅恢复本条影响 ----
function snapshotState() {
  return {
    voyages: voyages.map(item => ({ ...item })),
    containers: containers.map(item => ({ ...item })),
    appointments: appointments.map(item => ({ ...item })),
    tasks: tasks.map(item => ({ ...item, blockingReasons: [...item.blockingReasons] }))
  };
}
function restoreState(snap: ReturnType<typeof snapshotState>) {
  voyages.splice(0, voyages.length, ...snap.voyages);
  containers.splice(0, containers.length, ...snap.containers);
  appointments.splice(0, appointments.length, ...snap.appointments);
  tasks.splice(0, tasks.length, ...snap.tasks);
}

class RejectError extends Error { retryable: boolean; constructor(message: string, retryable: boolean) { super(message); this.retryable = retryable; } }

function newRecord(entity: ExchangeEntity, externalKey: string, op: ExchangeRecord['op'], raw: unknown): ExchangeRecord {
  return { id: `REC-${recordSeq++}`, entity, externalKey, op, status: 'rejected', diff: [], retryable: false, raw, attempts: [] };
}
function finishRecord(record: ExchangeRecord, status: RecordStatus, note?: string) {
  record.status = status; if (note) record.note = note;
  record.attempts.push({ at: now(), status, note });
}
function markApplied(entity: ExchangeEntity, id: string, mapped: unknown) {
  lastExternalApply.set(syncKey(entity === 'yard-position' || entity === 'event' ? 'container' : entity, id), now());
  appliedHashes.set(syncKey(entity, id), hash(mapped));
}

// ---- 航次映射与落库 ----
function mapVoyage(raw: TosVoyage) {
  const issues: string[] = [];
  const mapped: Partial<Voyage> = {
    vessel: raw.vesselName, imo: raw.imo, service: raw.serviceCode, berth: raw.berthCode,
    movesPlanned: raw.plannedMoves, movesDone: raw.completedMoves,
    eta: parseExternalTime(raw.eta, 'eta', issues), etd: parseExternalTime(raw.etd, 'etd', issues),
    status: mapEnum(voyageStatusMap, raw.voyageStatus, 'voyageStatus', issues),
    lateRisk: mapEnum(delayRiskMap, raw.delayRisk, 'delayRisk', issues)
  };
  Object.keys(mapped).forEach(key => (mapped as Record<string, unknown>)[key] === undefined && delete (mapped as Record<string, unknown>)[key]);
  return { mapped, issues };
}
function applyVoyage(record: ExchangeRecord, raw: TosVoyage, force: boolean) {
  const id = raw.voyageRef?.trim().toUpperCase();
  if (!id) throw new RejectError('缺少 voyageRef，无法定位航次', false);
  record.externalKey = id;
  if (raw.deleted) {
    record.op = 'delete';
    const index = voyages.findIndex(item => item.id === id);
    if (index < 0) { finishRecord(record, 'skipped-duplicate', '航次不存在，删除视为已生效'); return; }
    if (containers.some(item => item.voyageId === id)) throw new RejectError(`航次 ${id} 下仍有 ${containers.filter(item => item.voyageId === id).length} 条箱务记录，拒绝删除`, true);
    if (hasLocalChanges('voyage', id) && !force) { record.diff = [{ field: '_delete', local: '存在', external: '删除' }]; finishRecord(record, 'conflict', '本地有人工操作，删除需调度员裁决'); return; }
    voyages.splice(index, 1); tombstones.set(syncKey('voyage', id), now());
    noteAudit('TOS_DELETE', 'voyage', id, 'TOS 报文删除航次'); markApplied('voyage', id, { deleted: true });
    finishRecord(record, 'deleted'); return;
  }
  const { mapped, issues } = mapVoyage(raw);
  if (issues.length) { finishRecord(record, 'quarantined', issues.join('；')); record.retryable = true; record.rejectReason = issues.join('；'); return; }
  const contentHash = hash(mapped);
  if (appliedHashes.get(syncKey('voyage', id)) === contentHash) { finishRecord(record, 'skipped-duplicate', '内容与上次应用一致'); return; }
  const existing = voyages.find(item => item.id === id);
  if (raw.updatedAt && existing) {
    const externalTime = parseExternalTime(raw.updatedAt, 'updatedAt', []);
    if (externalTime && (lastExternalApply.get(syncKey('voyage', id)) ?? '') > externalTime) { finishRecord(record, 'skipped-stale', '报文版本早于已应用版本'); return; }
  }
  if (!existing) {
    const required = ['vessel', 'service', 'eta', 'etd', 'status', 'berth'] as const;
    const missing = required.filter(key => mapped[key] === undefined);
    if (missing.length) throw new RejectError(`新建航次缺少必填字段：${missing.join('、')}`, false);
    const wasTombstoned = tombstones.delete(syncKey('voyage', id));
    voyages.unshift({ id, movesPlanned: 0, movesDone: 0, lateRisk: 'low', ...mapped } as Voyage);
    noteAudit('TOS_CREATE', 'voyage', id, 'TOS 报文新建航次'); markApplied('voyage', id, mapped);
    finishRecord(record, 'created', wasTombstoned ? '删除后重建' : undefined); return;
  }
  record.op = 'update';
  record.diff = Object.entries(mapped).filter(([key, value]) => (existing as unknown as Record<string, unknown>)[key] !== value).map(([field, external]) => ({ field, local: (existing as unknown as Record<string, unknown>)[field], external }));
  if (!record.diff.length) { finishRecord(record, 'skipped-duplicate', '映射后与本地一致'); markApplied('voyage', id, mapped); return; }
  if (hasLocalChanges('voyage', id) && !force) { finishRecord(record, 'conflict', '本地存在人工修改，外部更新挂起待裁决'); return; }
  Object.assign(existing, mapped);
  noteAudit('TOS_UPDATE', 'voyage', id, `TOS 更新字段：${record.diff.map(item => item.field).join('、')}`); markApplied('voyage', id, mapped);
  finishRecord(record, force ? 'resolved-external' : 'updated');
}

// ---- 箱务映射与落库 ----
function mapContainer(raw: TosContainer) {
  const issues: string[] = [];
  const reefer = raw.reefer === undefined ? undefined : (raw.reefer === true || String(raw.reefer).toUpperCase() === 'Y');
  const size = raw.size === undefined ? undefined : String(raw.size) as Container['size'];
  if (size && !['20', '40', '45'].includes(size)) issues.push(`字段 size 出现未知枚举值「${String(raw.size)}」，已隔离未落地`);
  const mapped: Partial<Container> = {
    voyageId: raw.voyageRef?.trim().toUpperCase(), isoType: raw.iso, size, reefer,
    temperature: raw.setTemp, hazmatClass: raw.imoClass || undefined, yardSlot: raw.position?.trim().toUpperCase(),
    weightT: raw.grossWeightKg === undefined ? undefined : Math.round(raw.grossWeightKg / 100) / 10,
    direction: mapEnum(flowMap, raw.flow, 'flow', issues),
    status: mapEnum(ctrStatusMap, raw.ctrStatus, 'ctrStatus', issues),
    customs: mapEnum(customsMap, raw.customsStatus, 'customsStatus', issues),
    priority: mapEnum(priorityMap, raw.priorityCode, 'priorityCode', issues)
  };
  Object.keys(mapped).forEach(key => (mapped as Record<string, unknown>)[key] === undefined && delete (mapped as Record<string, unknown>)[key]);
  return { mapped, issues };
}
function validateSlotConstraints(mapped: Partial<Container>, base?: Container) {
  const merged = { ...base, ...mapped } as Container;
  const blockId = merged.yardSlot ? parseSlot(merged.yardSlot) : undefined;
  if (blockId && !blocks.some(block => block.id === blockId)) throw new RejectError(`堆位 ${merged.yardSlot} 所属区块不存在`, false);
  const block = blocks.find(item => item.id === blockId);
  if (merged.hazmatClass && block && !block.hazmatAllowed) throw new RejectError(`危险品 ${merged.hazmatClass} 类不能进入区块 ${blockId}`, false);
  if (merged.reefer && block && block.reeferPlugs <= block.usedPlugs && base?.yardSlot !== merged.yardSlot) throw new RejectError(`区块 ${blockId} 没有可用冷藏插座`, true);
}
function applyContainer(record: ExchangeRecord, raw: TosContainer, force: boolean) {
  const id = normalizeContainerNo(raw.cntrNo ?? '');
  if (!id) throw new RejectError('缺少 cntrNo，无法定位箱', false);
  record.externalKey = id;
  if (raw.deleted) {
    record.op = 'delete';
    const index = containers.findIndex(item => item.id === id);
    if (index < 0) { finishRecord(record, 'skipped-duplicate', '箱不存在，删除视为已生效'); return; }
    const activeAppointments = appointments.filter(item => item.containerId === id && (item.status === 'booked' || item.status === 'checked-in'));
    const openTasks = tasks.filter(item => item.containerId === id && item.status !== 'done');
    if (activeAppointments.length || openTasks.length) throw new RejectError(`箱 ${id} 存在进行中的预约 ${activeAppointments.length} 条、未完成任务 ${openTasks.length} 条，删除被拒绝`, true);
    if (hasLocalChanges('container', id) && !force) { record.diff = [{ field: '_delete', local: '存在', external: '删除' }]; finishRecord(record, 'conflict', '本地有人工操作，删除需调度员裁决'); return; }
    containers.splice(index, 1);
    appointments.filter(item => item.containerId === id).forEach(item => appointments.splice(appointments.indexOf(item), 1));
    tombstones.set(syncKey('container', id), now());
    noteAudit('TOS_DELETE', 'container', id, 'TOS 报文删除箱务记录（保留墓碑防复活）'); markApplied('container', id, { deleted: true });
    finishRecord(record, 'deleted'); return;
  }
  const { mapped, issues } = mapContainer(raw);
  if (issues.length) { finishRecord(record, 'quarantined', issues.join('；')); record.retryable = true; record.rejectReason = issues.join('；'); return; }
  const contentHash = hash(mapped);
  if (appliedHashes.get(syncKey('container', id)) === contentHash) { finishRecord(record, 'skipped-duplicate', '内容与上次应用一致'); return; }
  const existing = containers.find(item => item.id === id);
  if (raw.updatedAt && existing) {
    const externalTime = parseExternalTime(raw.updatedAt, 'updatedAt', []);
    if (externalTime && (lastExternalApply.get(syncKey('container', id)) ?? '') > externalTime) { finishRecord(record, 'skipped-stale', '报文版本早于已应用版本'); return; }
  }
  if (!existing) {
    if (!mapped.voyageId || !mapped.status || !mapped.direction || mapped.weightT === undefined || !mapped.yardSlot || !mapped.size || !mapped.isoType) throw new RejectError('新建箱缺少必填字段（航次/状态/流向/重量/堆位/尺寸/箱型）', false);
    if (!voyages.some(item => item.id === mapped.voyageId)) throw new RejectError(`关联航次 ${mapped.voyageId} 不存在`, true);
    validateSlotConstraints(mapped);
    const wasTombstoned = tombstones.delete(syncKey('container', id));
    containers.unshift({ customs: 'released', priority: 'normal', reefer: false, ...mapped, id, lastEvent: `TOS 建档 ${now()}` } as Container);
    noteAudit('TOS_CREATE', 'container', id, 'TOS 报文新建箱务记录'); markApplied('container', id, mapped);
    finishRecord(record, 'created', wasTombstoned ? '该箱曾被删除，本次为重新建档' : undefined); return;
  }
  record.op = 'update';
  validateSlotConstraints(mapped, existing);
  record.diff = Object.entries(mapped).filter(([key, value]) => (existing as unknown as Record<string, unknown>)[key] !== value).map(([field, external]) => ({ field, local: (existing as unknown as Record<string, unknown>)[field], external }));
  if (!record.diff.length) { finishRecord(record, 'skipped-duplicate', '映射后与本地一致'); markApplied('container', id, mapped); return; }
  if (hasLocalChanges('container', id) && !force) { finishRecord(record, 'conflict', '本地存在人工修改（含海关状态/预约关联），外部更新挂起待裁决'); return; }
  Object.assign(existing, mapped, { lastEvent: `TOS 同步 ${now()}` });
  noteAudit('TOS_UPDATE', 'container', id, `TOS 更新字段：${record.diff.map(item => item.field).join('、')}`); markApplied('container', id, mapped);
  finishRecord(record, force ? 'resolved-external' : 'updated');
}

// ---- 堆位回传 ----
function applyYardPosition(record: ExchangeRecord, raw: TosYardPosition) {
  const id = normalizeContainerNo(raw.cntrNo ?? '');
  const container = containers.find(item => item.id === id);
  if (!container) throw new RejectError(`箱 ${id} 不存在，堆位回传暂挂`, true);
  record.externalKey = id;
  const position = raw.position?.trim().toUpperCase();
  if (!position) throw new RejectError('缺少 position 字段', false);
  const issues: string[] = [];
  const eventTime = parseExternalTime(raw.eventTime, 'eventTime', issues) ?? now();
  if (issues.length) { finishRecord(record, 'quarantined', issues.join('；')); record.retryable = true; record.rejectReason = issues.join('；'); return; }
  if ((lastExternalApply.get(syncKey('yard-position', id)) ?? '') > eventTime) { finishRecord(record, 'skipped-stale', '堆位事件时间早于已应用版本'); return; }
  if (container.yardSlot === position) { finishRecord(record, 'skipped-duplicate', '堆位与本地一致'); lastExternalApply.set(syncKey('yard-position', id), eventTime); return; }
  validateSlotConstraints({ yardSlot: position }, container);
  record.diff = [{ field: 'yardSlot', local: container.yardSlot, external: position }];
  container.yardSlot = position; container.lastEvent = `TOS 堆位回传 ${eventTime}`;
  noteAudit('TOS_MOVE', 'container', id, `堆位 ${record.diff[0].local} -> ${position}`);
  lastExternalApply.set(syncKey('yard-position', id), eventTime);
  finishRecord(record, 'updated');
}

// ---- 作业事件：按 eventId 幂等，状态推进 + 必要时生成场内任务；不触碰海关状态 ----
function applyEvent(record: ExchangeRecord, raw: TosEvent) {
  if (!raw.eventId) throw new RejectError('缺少 eventId', false);
  record.externalKey = raw.eventId;
  if (appliedEventIds.has(raw.eventId)) { finishRecord(record, 'skipped-duplicate', '事件已应用过'); return; }
  const id = normalizeContainerNo(raw.cntrNo ?? '');
  const container = containers.find(item => item.id === id);
  if (!container) throw new RejectError(`箱 ${id} 不存在，事件暂挂`, true);
  const issues: string[] = [];
  const kind = mapEnum(eventTypeMap, raw.eventType, 'eventType', issues);
  const eventTime = parseExternalTime(raw.eventTime, 'eventTime', issues) ?? now();
  if (issues.length || !kind) { finishRecord(record, 'quarantined', issues.join('；') || '未知事件类型'); record.retryable = true; record.rejectReason = issues.join('；'); return; }
  const statusByEvent: Record<string, Container['status'] | undefined> = { DISCHARGE: 'on-quay', LOAD: 'on-quay', GATE_IN: 'at-gate', GATE_OUT: 'released', MOVE: undefined, INSPECT: undefined };
  const nextStatus = statusByEvent[raw.eventType.toUpperCase()];
  if (raw.eventType.toUpperCase() === 'MOVE') {
    if (!raw.position) throw new RejectError('MOVE 事件缺少 position', false);
    validateSlotConstraints({ yardSlot: raw.position.trim().toUpperCase() }, container);
    record.diff.push({ field: 'yardSlot', local: container.yardSlot, external: raw.position.trim().toUpperCase() });
    container.yardSlot = raw.position.trim().toUpperCase();
  }
  if (nextStatus && container.status !== nextStatus) { record.diff.push({ field: 'status', local: container.status, external: nextStatus }); container.status = nextStatus; }
  if (raw.eventType.toUpperCase() === 'INSPECT') {
    const task: YardTask = { id: `TASK-${Date.now()}-${record.id}`, kind: 'inspect', containerId: id, from: container.yardSlot, to: container.yardSlot, priority: 90, status: 'queued', scheduledAt: eventTime, blockingReasons: [], estimatedMinutes: 30 };
    tasks.unshift(task); record.note = `生成查验任务 ${task.id}`;
  }
  container.lastEvent = `TOS 事件 ${raw.eventType} ${eventTime}`;
  appliedEventIds.add(raw.eventId);
  noteAudit('TOS_EVENT', 'container', id, `事件 ${raw.eventId}（${raw.eventType}）已应用`);
  finishRecord(record, record.diff.length || record.note ? 'updated' : 'skipped-duplicate', record.note);
}

// ---- 导入主流程：批次级幂等 + 逐记录隔离应用 ----
export function importBatch(source: string, payload: TosPayload): { batch: ExchangeBatch; duplicate: boolean } {
  if (!payload || typeof payload !== 'object') throw new RejectError('报文体为空或不是 JSON 对象', false);
  const externalId = payload.messageId?.trim();
  const payloadHash = hash(payload);
  if (externalId) {
    const existing = exchangeBatches.find(batch => batch.direction === 'import' && batch.source === source && batch.externalId === externalId);
    if (existing && existing.payloadHash === payloadHash) return { batch: existing, duplicate: true };
    if (existing) throw new RejectError(`报文 ${externalId} 已接收但内容不一致，拒绝重放（防篡改冲突）`, false);
  }
  const batch: ExchangeBatch = { id: `IBATCH-${batchSeq++}`, direction: 'import', source, externalId, payloadHash, receivedAt: now(), status: 'completed', summary: { total: 0, applied: 0, skipped: 0, conflicts: 0, rejected: 0 }, records: [] };
  const sections: { entity: ExchangeEntity; items: unknown[]; apply: (record: ExchangeRecord, raw: never, force: boolean) => void }[] = [
    { entity: 'voyage', items: payload.voyages ?? [], apply: applyVoyage },
    { entity: 'container', items: payload.containers ?? [], apply: applyContainer },
    { entity: 'yard-position', items: payload.yardPositions ?? [], apply: (record, raw: TosYardPosition) => applyYardPosition(record, raw) },
    { entity: 'event', items: payload.events ?? [], apply: (record, raw: TosEvent) => applyEvent(record, raw) }
  ];
  for (const section of sections) {
    for (const raw of section.items) {
      const rawObj = raw as Record<string, unknown>;
      const record = newRecord(section.entity, String(rawObj.voyageRef ?? rawObj.cntrNo ?? rawObj.eventId ?? '?'), rawObj.deleted ? 'delete' : 'create', raw);
      const snap = snapshotState();
      try { section.apply(record, raw as never, false); }
      catch (error) {
        restoreState(snap);
        const retryable = error instanceof RejectError ? error.retryable : false;
        finishRecord(record, 'rejected', error instanceof Error ? error.message : '应用失败');
        record.retryable = retryable; record.rejectReason = error instanceof Error ? error.message : '应用失败';
      }
      batch.records.push(record);
    }
  }
  refreshBatch(batch);
  exchangeBatches.unshift(batch);
  return { batch, duplicate: false };
}

function refreshBatch(batch: ExchangeBatch) {
  const summary = { total: batch.records.length, applied: 0, skipped: 0, conflicts: 0, rejected: 0 };
  for (const record of batch.records) {
    if (['created', 'updated', 'deleted', 'exported', 'resolved-external'].includes(record.status)) summary.applied++;
    else if (record.status === 'conflict') summary.conflicts++;
    else if (record.status === 'rejected' || record.status === 'quarantined') summary.rejected++;
    else summary.skipped++;
  }
  batch.summary = summary;
  batch.status = summary.conflicts + summary.rejected === 0 ? 'completed' : summary.applied + summary.skipped > 0 ? 'partial' : 'failed';
}

// ---- 重试：只重放失败/隔离/冲突记录；冲突必须携带裁决 ----
export function retryBatch(batchId: string, recordIds?: string[], resolutions?: Record<string, 'accept-external' | 'keep-local'>): ExchangeBatch {
  const batch = exchangeBatches.find(item => item.id === batchId);
  if (!batch) throw new RejectError('批次不存在', false);
  if (batch.direction !== 'import') throw new RejectError('导出批次不支持重试', false);
  const targets = batch.records.filter(record => recordIds ? recordIds.includes(record.id) : ['rejected', 'quarantined', 'conflict'].includes(record.status));
  for (const record of targets) {
    if (record.status === 'conflict') {
      const resolution = resolutions?.[record.id];
      if (!resolution) continue;
      if (resolution === 'keep-local') { finishRecord(record, 'resolved-local', '调度员裁决：保留本地'); continue; }
      const snap = snapshotState();
      try {
        if (record.entity === 'voyage') applyVoyage(record, record.raw as TosVoyage, true);
        else if (record.entity === 'container') applyContainer(record, record.raw as TosContainer, true);
      } catch (error) {
        restoreState(snap);
        finishRecord(record, 'rejected', error instanceof Error ? error.message : '应用失败');
        record.rejectReason = error instanceof Error ? error.message : '应用失败';
      }
      continue;
    }
    if (record.status !== 'rejected' && record.status !== 'quarantined') continue;
    const snap = snapshotState();
    try {
      if (record.entity === 'voyage') applyVoyage(record, record.raw as TosVoyage, false);
      else if (record.entity === 'container') applyContainer(record, record.raw as TosContainer, false);
      else if (record.entity === 'yard-position') applyYardPosition(record, record.raw as TosYardPosition);
      else if (record.entity === 'event') applyEvent(record, record.raw as TosEvent);
    } catch (error) {
      restoreState(snap);
      finishRecord(record, 'rejected', error instanceof Error ? error.message : '应用失败');
      record.retryable = error instanceof RejectError ? error.retryable : false;
      record.rejectReason = error instanceof Error ? error.message : '应用失败';
    }
  }
  refreshBatch(batch);
  return batch;
}

// ---- 导出：本系统任务与闸口状态 -> TOS 外部形状 ----
export function exportBatch(scope: { tasks?: boolean; gate?: boolean }): ExchangeBatch {
  const includeTasks = scope.tasks !== false; const includeGate = scope.gate !== false;
  const taskPayload = includeTasks ? tasks.map(task => ({
    taskRef: task.id, cntrNo: task.containerId.replace('-', ''), workType: task.kind.toUpperCase(),
    fromPosition: task.from, toPosition: task.to, equipmentRef: task.equipmentId ?? null,
    priority: task.priority, taskStatus: taskStatusExportMap[task.status] ?? task.status.toUpperCase(),
    scheduledTime: formatExternalTime(task.scheduledAt), estMinutes: task.estimatedMinutes,
    blockedBy: task.blockingReasons.length ? task.blockingReasons : undefined
  })) : [];
  const gatePayload = includeGate ? appointments.map(item => ({
    bookingRef: item.id, cntrNo: item.containerId.replace('-', ''), truck: item.truckNo, driver: item.driver,
    moveType: item.type === 'pickup' ? 'PICKUP' : 'DROPOFF',
    windowFrom: formatExternalTime(item.windowStart), windowTo: formatExternalTime(item.windowEnd),
    lane: laneExportMap[item.lane] ?? item.lane, bookingStatus: appointmentStatusExportMap[item.status] ?? item.status.toUpperCase(),
    sealOk: item.sealVerified, weightOk: item.weightVerified, exception: item.exception
  })) : [];
  const payload = { messageId: `EXP-${Date.now()}`, sentAt: formatExternalTime(now()), tasks: taskPayload, gateStatus: gatePayload };
  const batch: ExchangeBatch = { id: `XBATCH-${batchSeq++}`, direction: 'export', source: 'HarborGrid', payloadHash: hash(payload), receivedAt: now(), status: 'completed', summary: { total: 0, applied: 0, skipped: 0, conflicts: 0, rejected: 0 }, records: [], payload };
  const toRecord = (entity: ExchangeEntity, key: string, raw: unknown): ExchangeRecord => {
    const record = newRecord(entity, key, 'export', raw);
    finishRecord(record, 'exported');
    return record;
  };
  batch.records.push(...taskPayload.map(item => toRecord('task', item.taskRef, item)));
  batch.records.push(...gatePayload.map(item => toRecord('gate-status', item.bookingRef, item)));
  refreshBatch(batch);
  exchangeBatches.unshift(batch);
  noteAudit('TOS_EXPORT', 'exchange', batch.id, `导出 ${taskPayload.length} 条任务、${gatePayload.length} 条闸口状态`);
  return batch;
}

export interface BatchSummaryView extends Omit<ExchangeBatch, 'records' | 'payload'> { records: number }
export function listBatches(direction?: string): BatchSummaryView[] {
  return exchangeBatches.filter(batch => !direction || batch.direction === direction).map(batch => ({ ...batch, records: batch.records.length, payload: undefined }));
}
export function getBatch(id: string) { return exchangeBatches.find(batch => batch.id === id); }
