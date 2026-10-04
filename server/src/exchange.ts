import { appointments, auditEvents, blocks, containers, localMutationListeners, parseSlot, tasks, voyages } from './domain.js';
import type { Container, ContainerStatus, GateAppointment, Voyage } from './domain.js';

/**
 * TOS 交换适配层
 *
 * 外部 TOS 的箱号、状态、时间字段与本系统领域模型不同，适配层负责：
 *  - 导入：航次 / 箱务 / 堆位 / 作业事件 四类报文 → 字段映射 → 校验 → 应用
 *  - 导出：本系统作业任务、闸口状态 → TOS 报文格式
 *  - 每次交换落批次台账：映射结果、逐字段差异、拒绝原因、可重试记录
 *
 * 关键语义决策：
 *  1. 冲突优先级：默认 local-wins —— 上次外部同步之后发生过本地人工修改的实体，
 *     外部更新不直接覆盖，记为 conflict 等调度员逐条裁决；批次可显式声明 external-wins。
 *  2. 未知枚举隔离：映射失败的记录不进入领域模型，原始报文留存在批次项中（隔离），
 *     映射表扩展或数据修正后可通过 retry 重新处理。
 *  3. 幂等：batchId 是幂等键，重复批次直接返回首次结果；作业事件按 eventId 去重；
 *     与本地状态一致的外部更新记为 skipped，不产生写操作。
 *  4. 局部失败回滚：默认 best-effort —— 每条记录自身原子，失败项隔离不影响其他项；
 *     mode=atomic 时任一失败/冲突即整体回滚到批次前快照，批次状态 rolled-back。
 *  5. 删除语义：只认显式 op='delete'，绝不从"报文中缺少某实体"推断删除（防止全量
 *     同步掩盖本地变化）；有进行中预约/任务的箱拒绝外部删除；本地有未同步修改时
 *     删除同样进入冲突裁决。
 */

export type ExchangeEntity = 'voyage' | 'container' | 'yard-position' | 'work-event' | 'yard-task' | 'gate-appointment';
export type ExchangeItemStatus = 'applied' | 'rejected' | 'conflict' | 'skipped' | 'rolled-back';
export type ExchangeAction = 'create' | 'update' | 'delete' | 'export' | 'skip';
export type ConflictPolicy = 'local-wins' | 'external-wins';
export type ImportMode = 'best-effort' | 'atomic';

export interface FieldDiff { field: string; local: unknown; external: unknown; applied: boolean; }
export interface ExchangeItem {
  key: string;                 // entity:externalId，重试与裁决的定位键
  entity: ExchangeEntity;
  externalId: string;
  action: ExchangeAction;
  status: ExchangeItemStatus;
  reason?: string;             // 拒绝 / 冲突 / 跳过原因
  diff?: FieldDiff[];          // 逐字段 本地值 → 外部值
  rawPayload?: unknown;        // 原始报文，供重试与审计
  attempts: number;
}
export interface ExchangeBatch {
  id: string;
  externalBatchId: string;
  direction: 'import' | 'export';
  source: string;
  mode: ImportMode;
  conflictPolicy: ConflictPolicy;
  status: 'applied' | 'partial' | 'rejected' | 'rolled-back';
  receivedAt: string;
  items: ExchangeItem[];
  summary: { total: number; applied: number; rejected: number; conflicts: number; skipped: number };
}

export const exchangeBatches: ExchangeBatch[] = [];

// ---------- 实体同步元数据：本地修改 vs 外部同步的时间线 ----------
interface EntityMeta { origin: 'seed' | 'local' | 'tos'; lastLocalEditAt?: string; lastExternalSyncAt?: string; lastEventAt?: string; }
const entityMeta = new Map<string, EntityMeta>();
const processedEventIds = new Set<string>();

localMutationListeners.push((entity, entityId) => {
  const key = `${entity}:${entityId}`;
  const meta = entityMeta.get(key) ?? { origin: 'local' as const };
  meta.lastLocalEditAt = new Date().toISOString();
  entityMeta.set(key, meta);
});

/** 上次外部同步之后是否存在本地人工修改 —— 有则外部更新不能直接覆盖 */
function hasLocalOverride(entity: string, id: string): boolean {
  const meta = entityMeta.get(`${entity}:${id}`);
  if (!meta?.lastLocalEditAt) return false;
  return !meta.lastExternalSyncAt || meta.lastLocalEditAt >= meta.lastExternalSyncAt;
}
function markExternalSync(entity: string, id: string, at: string) {
  const key = `${entity}:${id}`;
  const meta = entityMeta.get(key) ?? { origin: 'tos' as const };
  meta.lastExternalSyncAt = at;
  meta.lastLocalEditAt = undefined; // 外部值已落账，此前的本地修改视为已被调和（被覆盖或本就不冲突）
  if (meta.origin === 'seed') meta.origin = 'tos';
  entityMeta.set(key, meta);
}

function exchangeAudit(action: string, entityId: string, details: string) {
  auditEvents.unshift({ id: `AUD-${Date.now()}-${Math.floor(Math.random() * 1000)}`, actor: 'TOS-ADAPTER', action, entity: 'exchange', entityId, at: new Date().toISOString(), details });
}

// ---------- 外部枚举 → 领域枚举 映射表（未知值一律隔离，不猜、不默认） ----------
const FLOW_MAP: Record<string, Container['direction']> = { I: 'import', E: 'export', T: 'transshipment' };
const CTR_STATUS_MAP: Record<string, ContainerStatus> = { AT_SEA: 'in-transit', GATE: 'at-gate', YARD: 'in-yard', QUAY: 'on-quay', OUT: 'released', HOLD: 'hold' };
const CUSTOMS_MAP: Record<string, Container['customs']> = { R: 'released', I: 'inspection', H: 'hold' };
const VOYAGE_STATUS_MAP: Record<string, Voyage['status']> = { ARRIVING: 'approaching', BERTHED: 'berthed', WORKING: 'loading', SAILED: 'departed' };
const LANE_MAP: Record<string, GateAppointment['lane']> = { N: 'north', S: 'south', R: 'rail' };
const LANE_REVERSE: Record<GateAppointment['lane'], string> = { north: 'N', south: 'S', rail: 'R' };
const EVENT_TYPES = ['GATE_IN', 'GATE_OUT', 'LOAD', 'DISCHARGE', 'SHIFT', 'CUSTOMS_HOLD', 'CUSTOMS_RELEASE', 'CUSTOMS_INSPECT'] as const;
type EventType = typeof EVENT_TYPES[number];

function mapEnum<T>(table: Record<string, T>, value: unknown, field: string, errors: string[]): T | undefined {
  const raw = String(value ?? '').trim().toUpperCase();
  if (!raw) { errors.push(`缺少必填字段 ${field}`); return undefined; }
  const mapped = table[raw];
  if (mapped === undefined) { errors.push(`未知枚举 ${field}='${raw}'，已隔离等待映射表扩展`); return undefined; }
  return mapped;
}
function mapSize(value: unknown, errors: string[]): Container['size'] | undefined {
  const digits = String(value ?? '').replace(/[^0-9]/g, '');
  if (digits === '20' || digits === '40' || digits === '45') return digits;
  errors.push(`未知箱尺寸 size='${String(value ?? '')}'，已隔离`);
  return undefined;
}
function mapTime(value: unknown, field: string, errors: string[]): string | undefined {
  const time = new Date(String(value ?? ''));
  if (Number.isNaN(time.getTime())) { errors.push(`字段 ${field} 时间无法解析：'${String(value ?? '')}'`); return undefined; }
  return time.toISOString();
}

// ---------- 报文 → 领域模型 映射 ----------
interface MappedContainer {
  id: string; voyageId: string; size: Container['size']; isoType: string; direction: Container['direction'];
  status: ContainerStatus; weightT: number; reefer: boolean; temperature?: number; hazmatClass?: string;
  customs: Container['customs']; yardSlot: string; op: 'upsert' | 'delete';
}
function mapTosContainer(raw: unknown): { data?: MappedContainer; errors: string[] } {
  const errors: string[] = [];
  const source = (raw ?? {}) as Record<string, unknown>;
  const id = String(source.cntrNo ?? '').trim().toUpperCase();
  if (!id) errors.push('缺少箱号 cntrNo');
  const voyageId = String(source.voyage ?? '').trim().toUpperCase();
  if (!voyageId) errors.push('缺少航次 voyage');
  const size = mapSize(source.size, errors);
  const direction = mapEnum(FLOW_MAP, source.flow, 'flow', errors);
  const status = mapEnum(CTR_STATUS_MAP, source.ctrStatus, 'ctrStatus', errors);
  const customs = mapEnum(CUSTOMS_MAP, source.customs, 'customs', errors);
  const weightKg = Number(source.weightKg);
  if (!Number.isFinite(weightKg) || weightKg <= 0) errors.push(`重量 weightKg 非法：'${String(source.weightKg ?? '')}'`);
  const reefer = source.reefer === 'Y' || source.reefer === true;
  let temperature: number | undefined;
  if (reefer) {
    temperature = Number(source.setTemp);
    if (!Number.isFinite(temperature)) { errors.push(`冷藏箱缺少有效设定温度 setTemp`); temperature = undefined; }
  }
  const op = String(source.op ?? 'upsert').toLowerCase() === 'delete' ? 'delete' as const : 'upsert' as const;
  if (errors.length) return { errors };
  const hazmat = String(source.imdg ?? '').trim();
  return {
    errors,
    data: {
      id, voyageId, size: size!, isoType: String(source.iso ?? '').trim() || '45G1', direction: direction!,
      status: status!, weightT: Math.round(weightKg / 100) / 10, reefer, temperature,
      hazmatClass: hazmat || undefined, customs: customs!, yardSlot: String(source.slot ?? '').trim().toUpperCase(), op
    }
  };
}

interface MappedVoyage { id: string; vessel: string; imo: string; service: string; eta: string; etd: string; status: Voyage['status']; berth: string; movesPlanned: number; movesDone: number; op: 'upsert' | 'delete'; }
function mapTosVoyage(raw: unknown): { data?: MappedVoyage; errors: string[] } {
  const errors: string[] = [];
  const source = (raw ?? {}) as Record<string, unknown>;
  const id = String(source.voyage ?? '').trim().toUpperCase();
  if (!id) errors.push('缺少航次号 voyage');
  const vessel = String(source.vesselName ?? '').trim();
  if (!vessel) errors.push('缺少船名 vesselName');
  const eta = mapTime(source.eta, 'eta', errors);
  const etd = mapTime(source.etd, 'etd', errors);
  const status = mapEnum(VOYAGE_STATUS_MAP, source.voyageStatus, 'voyageStatus', errors);
  const movesPlanned = Number(source.plannedMoves ?? 0);
  const movesDone = Number(source.doneMoves ?? 0);
  if (!Number.isFinite(movesPlanned) || movesPlanned < 0) errors.push('plannedMoves 非法');
  if (!Number.isFinite(movesDone) || movesDone < 0) errors.push('doneMoves 非法');
  const op = String(source.op ?? 'upsert').toLowerCase() === 'delete' ? 'delete' as const : 'upsert' as const;
  if (errors.length) return { errors };
  return { errors, data: { id, vessel, imo: String(source.imo ?? ''), service: String(source.service ?? ''), eta: eta!, etd: etd!, status: status!, berth: String(source.berth ?? ''), movesPlanned, movesDone, op } };
}

// ---------- 应用上下文 ----------
interface ImportContext { batchId: string; now: string; conflictPolicy: ConflictPolicy; forcedPolicy?: ConflictPolicy; }

function rejectItem(entity: ExchangeEntity, externalId: string, raw: unknown, reason: string, action: ExchangeAction = 'skip'): ExchangeItem {
  return { key: `${entity}:${externalId || 'UNKNOWN'}`, entity, externalId, action, status: 'rejected', reason, rawPayload: raw, attempts: 1 };
}
function skipItem(entity: ExchangeEntity, externalId: string, raw: unknown, reason: string): ExchangeItem {
  return { key: `${entity}:${externalId}`, entity, externalId, action: 'skip', status: 'skipped', reason, rawPayload: raw, attempts: 1 };
}
function conflictItem(entity: ExchangeEntity, externalId: string, action: ExchangeAction, raw: unknown, diff: FieldDiff[] | undefined, reason: string): ExchangeItem {
  return { key: `${entity}:${externalId}`, entity, externalId, action, status: 'conflict', reason, diff, rawPayload: raw, attempts: 1 };
}
function appliedItem(entity: ExchangeEntity, externalId: string, action: ExchangeAction, raw: unknown, diff?: FieldDiff[]): ExchangeItem {
  return { key: `${entity}:${externalId}`, entity, externalId, action, status: 'applied', diff, rawPayload: raw, attempts: 1 };
}

function diffFields(local: Record<string, unknown>, external: Record<string, unknown>): FieldDiff[] {
  return Object.entries(external)
    .map(([field, value]) => ({ field, local: local[field] ?? null, external: value ?? null, applied: false }))
    .filter(entry => String(entry.local ?? '') !== String(entry.external ?? ''));
}

/** 本地优先策略下，有未同步人工修改 → 冲突挂起；否则应用 */
function guardConflict(ctx: ImportContext, entity: ExchangeEntity, id: string, action: ExchangeAction, raw: unknown, diff: FieldDiff[] | undefined, auditNote: string): ExchangeItem | undefined {
  const policy = ctx.forcedPolicy ?? ctx.conflictPolicy;
  if (policy === 'local-wins' && hasLocalOverride(entity === 'yard-position' || entity === 'work-event' ? 'container' : entity, id)) {
    return conflictItem(entity, id, action, raw, diff, `本地人工修改优先：该记录在上次同步后被调度员修改，${auditNote}等待人工裁决`);
  }
  return undefined;
}

// ---------- 航次 ----------
function applyVoyage(ctx: ImportContext, raw: unknown): ExchangeItem {
  const { data, errors } = mapTosVoyage(raw);
  const externalId = String((raw as Record<string, unknown> | undefined)?.voyage ?? 'UNKNOWN').toUpperCase();
  if (!data) return rejectItem('voyage', externalId, raw, errors.join('；'));
  if (data.op === 'delete') {
    const index = voyages.findIndex(v => v.id === data.id);
    if (index < 0) return skipItem('voyage', data.id, raw, '删除目标已不存在（幂等跳过）');
    if (containers.some(c => c.voyageId === data.id)) return rejectItem('voyage', data.id, raw, `航次下仍有 ${containers.filter(c => c.voyageId === data.id).length} 条箱务记录，拒绝外部删除`, 'delete');
    const conflict = guardConflict(ctx, 'voyage', data.id, 'delete', raw, undefined, '删除操作');
    if (conflict) return conflict;
    voyages.splice(index, 1);
    markExternalSync('voyage', data.id, ctx.now);
    exchangeAudit('TOS_DELETE', data.id, `批次 ${ctx.batchId} 删除航次`);
    return appliedItem('voyage', data.id, 'delete', raw);
  }
  const existing = voyages.find(v => v.id === data.id);
  const mapped: Record<string, unknown> = { vessel: data.vessel, imo: data.imo, service: data.service, eta: data.eta, etd: data.etd, status: data.status, berth: data.berth, movesPlanned: data.movesPlanned, movesDone: data.movesDone };
  if (!existing) {
    voyages.push({ id: data.id, lateRisk: 'low', ...(mapped as unknown as Omit<Voyage, 'id' | 'lateRisk'>) });
    markExternalSync('voyage', data.id, ctx.now);
    exchangeAudit('TOS_CREATE', data.id, `批次 ${ctx.batchId} 创建航次 ${data.vessel}`);
    return appliedItem('voyage', data.id, 'create', raw);
  }
  const diff = diffFields(existing as unknown as Record<string, unknown>, mapped);
  if (!diff.length) return skipItem('voyage', data.id, raw, '外部数据与本地一致，无写操作');
  const conflict = guardConflict(ctx, 'voyage', data.id, 'update', raw, diff, `差异字段 ${diff.map(d => d.field).join('/')} `);
  if (conflict) return conflict;
  Object.assign(existing, mapped);
  markExternalSync('voyage', data.id, ctx.now);
  exchangeAudit('TOS_UPDATE', data.id, `批次 ${ctx.batchId} 更新航次字段：${diff.map(d => d.field).join('、')}`);
  return appliedItem('voyage', data.id, 'update', raw, diff.map(d => ({ ...d, applied: true })));
}

// ---------- 箱务 ----------
function validateContainerSlot(yardSlot: string, hazmatClass: string | undefined, reefer: boolean): string | undefined {
  const blockId = parseSlot(yardSlot);
  if (!blockId) return `堆位 '${yardSlot}' 格式无法识别`;
  const block = blocks.find(item => item.id === blockId);
  if (!block) return `堆位所属区块 ${blockId} 不存在`;
  if (hazmatClass && !block.hazmatAllowed) return `危险品不能进入区块 ${blockId}`;
  if (reefer && block.reeferPlugs <= block.usedPlugs) return `区块 ${blockId} 没有可用冷藏插座`;
  return undefined;
}

function applyContainer(ctx: ImportContext, raw: unknown): ExchangeItem {
  const { data, errors } = mapTosContainer(raw);
  const externalId = String((raw as Record<string, unknown> | undefined)?.cntrNo ?? 'UNKNOWN').toUpperCase();
  if (!data) return rejectItem('container', externalId, raw, errors.join('；'));
  if (data.op === 'delete') {
    const index = containers.findIndex(c => c.id === data.id);
    if (index < 0) return skipItem('container', data.id, raw, '删除目标已不存在（幂等跳过）');
    const openAppointments = appointments.filter(a => a.containerId === data.id && a.status !== 'completed' && a.status !== 'cancelled');
    const openTasks = tasks.filter(t => t.containerId === data.id && t.status !== 'done');
    if (openAppointments.length || openTasks.length) {
      return rejectItem('container', data.id, raw, `存在 ${openAppointments.length} 条进行中闸口预约、${openTasks.length} 条未完成场内任务，拒绝外部删除`, 'delete');
    }
    const conflict = guardConflict(ctx, 'container', data.id, 'delete', raw, undefined, '删除操作');
    if (conflict) return conflict;
    containers.splice(index, 1);
    markExternalSync('container', data.id, ctx.now);
    exchangeAudit('TOS_DELETE', data.id, `批次 ${ctx.batchId} 删除箱务记录（无进行中业务）`);
    return appliedItem('container', data.id, 'delete', raw);
  }
  const slotError = validateContainerSlot(data.yardSlot, data.hazmatClass, data.reefer);
  if (slotError) return rejectItem('container', data.id, raw, slotError);
  const existing = containers.find(c => c.id === data.id);
  const priority: Container['priority'] = data.hazmatClass ? 'hazmat' : data.reefer ? 'reefer' : 'normal';
  if (!existing) {
    if (!voyages.some(v => v.id === data.voyageId)) return rejectItem('container', data.id, raw, `关联航次 ${data.voyageId} 不存在（本批次航次未成功导入或本地缺失）`);
    const item: Container = {
      id: data.id, voyageId: data.voyageId, size: data.size, isoType: data.isoType, direction: data.direction,
      status: data.status, weightT: data.weightT, reefer: data.reefer, temperature: data.temperature,
      hazmatClass: data.hazmatClass, customs: data.customs, priority, yardSlot: data.yardSlot,
      lastEvent: `TOS 同步建档 ${ctx.batchId}`
    };
    containers.unshift(item);
    markExternalSync('container', data.id, ctx.now);
    exchangeAudit('TOS_CREATE', data.id, `批次 ${ctx.batchId} 创建箱务记录`);
    return appliedItem('container', data.id, 'create', raw);
  }
  if (!voyages.some(v => v.id === data.voyageId)) return rejectItem('container', data.id, raw, `关联航次 ${data.voyageId} 不存在`);
  const mapped: Record<string, unknown> = {
    voyageId: data.voyageId, size: data.size, isoType: data.isoType, direction: data.direction, status: data.status,
    weightT: data.weightT, reefer: data.reefer, temperature: data.temperature, hazmatClass: data.hazmatClass,
    customs: data.customs, yardSlot: data.yardSlot
  };
  const diff = diffFields(existing as unknown as Record<string, unknown>, mapped);
  if (!diff.length) return skipItem('container', data.id, raw, '外部数据与本地一致，无写操作');
  const conflict = guardConflict(ctx, 'container', data.id, 'update', raw, diff, `差异字段 ${diff.map(d => d.field).join('/')} `);
  if (conflict) return conflict;
  Object.assign(existing, mapped, { lastEvent: `TOS 同步更新 ${ctx.batchId}` });
  markExternalSync('container', data.id, ctx.now);
  exchangeAudit('TOS_UPDATE', data.id, `批次 ${ctx.batchId} 更新箱务字段：${diff.map(d => d.field).join('、')}`);
  return appliedItem('container', data.id, 'update', raw, diff.map(d => ({ ...d, applied: true })));
}

// ---------- 堆位 ----------
function applyYardPosition(ctx: ImportContext, raw: unknown): ExchangeItem {
  const source = (raw ?? {}) as Record<string, unknown>;
  const cntrNo = String(source.cntrNo ?? '').trim().toUpperCase();
  const slot = String(source.slot ?? '').trim().toUpperCase();
  if (!cntrNo || !slot) return rejectItem('yard-position', cntrNo || 'UNKNOWN', raw, '堆位报文缺少 cntrNo 或 slot');
  const container = containers.find(c => c.id === cntrNo);
  if (!container) return rejectItem('yard-position', cntrNo, raw, `箱号 ${cntrNo} 不存在，堆位无法落账`);
  const slotError = validateContainerSlot(slot, container.hazmatClass, container.reefer);
  if (slotError) return rejectItem('yard-position', cntrNo, raw, slotError);
  const diff = diffFields(container as unknown as Record<string, unknown>, { yardSlot: slot });
  if (!diff.length) return skipItem('yard-position', cntrNo, raw, '堆位与本地一致，无写操作');
  const conflict = guardConflict(ctx, 'yard-position', cntrNo, 'update', raw, diff, '堆位变更');
  if (conflict) return conflict;
  container.yardSlot = slot;
  container.lastEvent = `TOS 堆位确认 ${ctx.batchId}`;
  markExternalSync('container', cntrNo, ctx.now);
  exchangeAudit('TOS_UPDATE', cntrNo, `批次 ${ctx.batchId} 堆位调整 → ${slot}`);
  return appliedItem('yard-position', cntrNo, 'update', raw, diff.map(d => ({ ...d, applied: true })));
}

// ---------- 作业事件 ----------
function applyWorkEvent(ctx: ImportContext, raw: unknown): ExchangeItem {
  const source = (raw ?? {}) as Record<string, unknown>;
  const eventId = String(source.eventId ?? '').trim();
  const cntrNo = String(source.cntrNo ?? '').trim().toUpperCase();
  const externalId = eventId || 'UNKNOWN';
  if (!eventId) return rejectItem('work-event', externalId, raw, '作业事件缺少 eventId（幂等键）');
  if (processedEventIds.has(eventId)) return skipItem('work-event', eventId, raw, `事件 ${eventId} 已处理过，幂等跳过`);
  const errors: string[] = [];
  const eventType = String(source.eventType ?? '').trim().toUpperCase();
  if (!EVENT_TYPES.includes(eventType as EventType)) errors.push(`未知事件类型 eventType='${eventType}'，已隔离`);
  const eventTime = mapTime(source.eventTime, 'eventTime', errors);
  if (!cntrNo) errors.push('作业事件缺少 cntrNo');
  if (errors.length) return rejectItem('work-event', externalId, raw, errors.join('；'));
  const container = containers.find(c => c.id === cntrNo);
  if (!container) return rejectItem('work-event', externalId, raw, `箱号 ${cntrNo} 不存在，事件无法落账`);
  const metaKey = `container:${cntrNo}`;
  const lastEventAt = entityMeta.get(metaKey)?.lastEventAt;
  if (lastEventAt && eventTime! < lastEventAt) {
    processedEventIds.add(eventId);
    return skipItem('work-event', eventId, raw, `事件时间 ${eventTime} 早于已应用的最新事件 ${lastEventAt}，过期事件不重放`);
  }
  const appointment = appointments.find(a => a.id === container.appointmentId);
  const voyage = voyages.find(v => v.id === container.voyageId);
  const lane = source.lane ? mapEnum(LANE_MAP, source.lane, 'lane', []) : undefined;
  const truck = source.truck ? String(source.truck) : undefined;
  let description = '';
  switch (eventType as EventType) {
    case 'GATE_IN':
      container.status = 'at-gate';
      if (appointment) { appointment.status = 'checked-in'; if (lane) appointment.lane = lane; if (truck) appointment.truckNo = truck; }
      description = '闸口进场';
      break;
    case 'GATE_OUT':
      container.status = 'released';
      if (appointment) appointment.status = 'completed';
      description = '闸口出场';
      break;
    case 'LOAD':
      container.status = 'on-quay';
      if (voyage) voyage.movesDone = Math.min(voyage.movesPlanned, voyage.movesDone + 1);
      description = '装船完成';
      break;
    case 'DISCHARGE':
      container.status = 'in-yard';
      if (voyage) voyage.movesDone = Math.min(voyage.movesPlanned, voyage.movesDone + 1);
      description = '卸船完成';
      break;
    case 'SHIFT': {
      const slot = String(source.slot ?? '').trim().toUpperCase();
      if (!slot) return rejectItem('work-event', externalId, raw, 'SHIFT 事件缺少目标堆位 slot');
      const slotError = validateContainerSlot(slot, container.hazmatClass, container.reefer);
      if (slotError) return rejectItem('work-event', externalId, raw, slotError);
      container.yardSlot = slot;
      description = `场内移位 → ${slot}`;
      break;
    }
    case 'CUSTOMS_HOLD': container.customs = 'hold'; description = '海关扣留'; break;
    case 'CUSTOMS_RELEASE': container.customs = 'released'; description = '海关放行'; break;
    case 'CUSTOMS_INSPECT': container.customs = 'inspection'; description = '海关查验'; break;
  }
  container.lastEvent = `TOS ${description} ${eventTime}`;
  processedEventIds.add(eventId);
  markExternalSync('container', cntrNo, ctx.now);
  entityMeta.get(metaKey)!.lastEventAt = eventTime;
  exchangeAudit('TOS_EVENT', cntrNo, `批次 ${ctx.batchId} 事件 ${eventId}：${description}`);
  return appliedItem('work-event', eventId, 'update', raw, [{ field: 'event', local: lastEventAt ?? null, external: `${eventType}@${eventTime}`, applied: true }]);
}

// ---------- 批次编排 ----------
function processItem(ctx: ImportContext, entity: ExchangeEntity, raw: unknown): ExchangeItem {
  try {
    if (entity === 'voyage') return applyVoyage(ctx, raw);
    if (entity === 'container') return applyContainer(ctx, raw);
    if (entity === 'yard-position') return applyYardPosition(ctx, raw);
    return applyWorkEvent(ctx, raw);
  } catch (error) {
    return rejectItem(entity, 'UNKNOWN', raw, `适配层内部错误：${error instanceof Error ? error.message : String(error)}`);
  }
}

function snapshotState() {
  return {
    voyages: structuredClone(voyages), containers: structuredClone(containers),
    appointments: structuredClone(appointments), tasks: structuredClone(tasks),
    auditEvents: structuredClone(auditEvents),
    entityMeta: new Map(entityMeta), processedEventIds: new Set(processedEventIds)
  };
}
function restoreState(snapshot: ReturnType<typeof snapshotState>) {
  voyages.splice(0, voyages.length, ...snapshot.voyages);
  containers.splice(0, containers.length, ...snapshot.containers);
  appointments.splice(0, appointments.length, ...snapshot.appointments);
  tasks.splice(0, tasks.length, ...snapshot.tasks);
  auditEvents.splice(0, auditEvents.length, ...snapshot.auditEvents);
  entityMeta.clear(); snapshot.entityMeta.forEach((value, key) => entityMeta.set(key, value));
  processedEventIds.clear(); snapshot.processedEventIds.forEach(id => processedEventIds.add(id));
}

function recomputeSummary(batch: ExchangeBatch) {
  batch.summary = {
    total: batch.items.length,
    applied: batch.items.filter(i => i.status === 'applied').length,
    rejected: batch.items.filter(i => i.status === 'rejected').length,
    conflicts: batch.items.filter(i => i.status === 'conflict').length,
    skipped: batch.items.filter(i => i.status === 'skipped' || i.status === 'rolled-back').length
  };
  if (batch.status === 'rolled-back') return;
  if (batch.summary.rejected + batch.summary.conflicts === 0) batch.status = 'applied';
  else if (batch.summary.applied === 0 && batch.summary.rejected > 0) batch.status = 'rejected';
  else batch.status = 'partial';
}

export interface ImportResult { batch: ExchangeBatch; duplicate: boolean; }

export function importBatch(payload: unknown): ImportResult {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) throw new Error('批次报文必须是 JSON 对象');
  const source = payload as Record<string, unknown>;
  const externalBatchId = String(source.batchId ?? '').trim();
  if (!externalBatchId) throw new Error('缺少 batchId：批次幂等键必填，防止重复导入');
  const existing = exchangeBatches.find(b => b.direction === 'import' && b.externalBatchId === externalBatchId);
  if (existing) return { batch: existing, duplicate: true };
  const sections: [ExchangeEntity, unknown[]][] = [
    ['voyage', Array.isArray(source.voyages) ? source.voyages : []],
    ['container', Array.isArray(source.containers) ? source.containers : []],
    ['yard-position', Array.isArray(source.yardPositions) ? source.yardPositions : []],
    ['work-event', Array.isArray(source.workEvents) ? source.workEvents : []]
  ];
  if (!sections.some(([, list]) => list.length)) throw new Error('批次报文不包含 voyages / containers / yardPositions / workEvents 任何数据');
  const mode: ImportMode = source.mode === 'atomic' ? 'atomic' : 'best-effort';
  const conflictPolicy: ConflictPolicy = source.conflictPolicy === 'external-wins' ? 'external-wins' : 'local-wins';
  const now = new Date().toISOString();
  const batch: ExchangeBatch = {
    id: `XCH-${Date.now()}-${exchangeBatches.length + 1}`, externalBatchId, direction: 'import',
    source: String(source.source ?? 'TOS'), mode, conflictPolicy, status: 'applied', receivedAt: now,
    items: [], summary: { total: 0, applied: 0, rejected: 0, conflicts: 0, skipped: 0 }
  };
  const ctx: ImportContext = { batchId: batch.id, now, conflictPolicy };
  const snapshot = mode === 'atomic' ? snapshotState() : undefined;
  for (const [entity, list] of sections) for (const raw of list) batch.items.push(processItem(ctx, entity, raw));
  if (snapshot && batch.items.some(item => item.status === 'rejected' || item.status === 'conflict')) {
    restoreState(snapshot);
    batch.status = 'rolled-back';
    batch.items.forEach(item => {
      if (item.status === 'applied' || item.status === 'conflict') {
        item.status = 'rolled-back';
        item.reason = '原子批次存在失败项，整体回滚；修正后可单项重试';
      }
    });
    exchangeAudit('TOS_ROLLBACK', batch.id, `原子批次 ${externalBatchId} 回滚，${batch.items.length} 条记录未落账`);
  }
  recomputeSummary(batch);
  exchangeBatches.unshift(batch);
  exchangeAudit('TOS_IMPORT', batch.id, `批次 ${externalBatchId}：应用 ${batch.summary.applied} / 拒绝 ${batch.summary.rejected} / 冲突 ${batch.summary.conflicts} / 跳过 ${batch.summary.skipped}`);
  return { batch, duplicate: false };
}

/** 只重试失败/被回滚的项：用原始报文在当前状态下重新映射与应用，不触碰已成功项 */
export function retryBatchItems(batchId: string, itemKeys?: string[]): ExchangeBatch | undefined {
  const batch = exchangeBatches.find(b => b.id === batchId);
  if (!batch) return undefined;
  if (batch.direction !== 'import') throw new Error('导出批次不支持重试');
  const targets = batch.items.filter(item =>
    (item.status === 'rejected' || item.status === 'rolled-back') && item.rawPayload !== undefined &&
    (!itemKeys || itemKeys.includes(item.key)));
  if (!targets.length) throw new Error('没有可重试的失败项');
  if (batch.status === 'rolled-back') batch.status = 'applied';
  const ctx: ImportContext = { batchId: batch.id, now: new Date().toISOString(), conflictPolicy: batch.conflictPolicy };
  for (const item of targets) {
    const fresh = processItem(ctx, item.entity, item.rawPayload);
    fresh.attempts = item.attempts + 1;
    Object.assign(item, fresh);
  }
  recomputeSummary(batch);
  exchangeAudit('TOS_RETRY', batch.id, `重试 ${targets.length} 条失败项，结果：应用 ${batch.summary.applied} / 拒绝 ${batch.summary.rejected}`);
  return batch;
}

/** 冲突裁决：接受外部值（强制应用原始报文）或保留本地值（跳过该项） */
export function resolveConflict(batchId: string, itemKey: string, resolution: 'accept-external' | 'keep-local'): ExchangeBatch | undefined {
  const batch = exchangeBatches.find(b => b.id === batchId);
  if (!batch) return undefined;
  const item = batch.items.find(entry => entry.key === itemKey);
  if (!item || item.status !== 'conflict') throw new Error('目标项不是待裁决冲突');
  if (resolution === 'keep-local') {
    item.status = 'skipped';
    item.reason = '调度员裁决：保留本地人工修改，忽略外部报文';
    item.diff = item.diff?.map(d => ({ ...d, applied: false }));
    item.attempts += 1;
  } else {
    const ctx: ImportContext = { batchId: batch.id, now: new Date().toISOString(), conflictPolicy: batch.conflictPolicy, forcedPolicy: 'external-wins' };
    const fresh = processItem(ctx, item.entity, item.rawPayload);
    fresh.attempts = item.attempts + 1;
    Object.assign(item, fresh);
    if (fresh.status === 'applied') item.reason = '调度员裁决：接受外部值，已覆盖本地修改';
  }
  recomputeSummary(batch);
  exchangeAudit('TOS_RESOLVE', batch.id, `冲突项 ${itemKey} 裁决：${resolution === 'keep-local' ? '保留本地' : '接受外部'}`);
  return batch;
}

export function listBatches(): ExchangeBatch[] { return exchangeBatches; }
export function batchDetail(id: string): ExchangeBatch | undefined { return exchangeBatches.find(b => b.id === id); }

// ---------- 导出：本系统 → TOS 报文 ----------
function exportBatch(kind: 'tasks' | 'gate'): ExchangeBatch {
  const now = new Date().toISOString();
  const isTasks = kind === 'tasks';
  const records = isTasks
    ? tasks.map(task => ({
        taskId: task.id, cntrNo: task.containerId, moveKind: task.kind.toUpperCase(),
        fromLoc: task.from, toLoc: task.to, equip: task.equipmentId ?? null,
        priority: task.priority, taskStatus: task.status.toUpperCase().replace('-', '_'),
        schedTime: task.scheduledAt, estMinutes: task.estimatedMinutes
      }))
    : appointments.map(item => ({
        apptId: item.id, cntrNo: item.containerId, truck: item.truckNo, driver: item.driver,
        dir: item.type === 'pickup' ? 'OUT' : 'IN', slotStart: item.windowStart, slotEnd: item.windowEnd,
        lane: LANE_REVERSE[item.lane], gateStatus: item.status.toUpperCase().replace('-', '_'),
        sealOk: item.sealVerified, weightOk: item.weightVerified, exception: item.exception ?? null
      }));
  const entity: ExchangeEntity = isTasks ? 'yard-task' : 'gate-appointment';
  const batch: ExchangeBatch = {
    id: `XCH-${Date.now()}-${exchangeBatches.length + 1}`, externalBatchId: `EXPORT-${kind.toUpperCase()}-${now}`,
    direction: 'export', source: 'HarborGrid', mode: 'best-effort', conflictPolicy: 'local-wins',
    status: 'applied', receivedAt: now,
    items: records.map(record => ({
      key: `${entity}:${isTasks ? (record as { taskId: string }).taskId : (record as { apptId: string }).apptId}`,
      entity, externalId: isTasks ? (record as { taskId: string }).taskId : (record as { apptId: string }).apptId,
      action: 'export' as const, status: 'applied' as const, rawPayload: record, attempts: 1
    })),
    summary: { total: records.length, applied: records.length, rejected: 0, conflicts: 0, skipped: 0 }
  };
  exchangeBatches.unshift(batch);
  exchangeAudit('TOS_EXPORT', batch.id, `导出 ${isTasks ? '作业任务' : '闸口状态'} ${records.length} 条`);
  return batch;
}
export const exportTasksBatch = () => exportBatch('tasks');
export const exportGateBatch = () => exportBatch('gate');
