# HarborGrid · 港口集装箱堆场作业协同与闸口调度系统

React + Vite + TypeScript 前端，Node.js + Express + TypeScript 服务端。面向中型集装箱码头，把船期、箱务、堆场区块、闸口预约、设备作业和异常闭环放在同一套作业控制台中。

## 启动

```bash
npm install
npm run dev
```

前端默认 Vite 端口 5178，API 默认 `http://localhost:4107`。

## 业务能力

- 船舶航次、泊位窗口、计划/已完成翻箱和延误风险
- 箱号、尺寸、冷藏温度、危险品级别、海关状态、进出口/中转流向
- 堆场区块容量、堆存热度、冷藏插座、危险品隔离和设备可达性
- 闸口预约时隙、车道、OCR/称重/封志校验状态与同车冲突检测
- 岸桥、场桥、集卡、正面吊设备池和约束感知任务队列
- 自动堆位推荐、冷藏/危险品约束、海关查验阻塞、任务指派审计
- TOS 交换适配层：导入航次/箱务/堆位/作业事件，导出任务与闸口状态，批次留痕可重试
- React 控制台支持船期驾驶舱、闸口操作、堆场热区、设备任务与 TOS 交换操作

## TOS 交换适配层

`POST /api/exchange/import` 接收 TOS 批次报文（`voyages` / `containers` / `yardPositions` / `workEvents`），外部字段（`cntrNo`、`ctrStatus`、`flow`、`weightKg`、`eventTime` 等）经映射表转换为领域模型；`GET /api/exchange/export/tasks|gate` 导出本系统数据为 TOS 报文。每次交换落批次台账（`GET /api/exchange/batches[/:id]`），记录逐条映射结果、字段级差异、拒绝原因与原始报文，支持 `POST .../retry` 只重试失败项、`POST .../items/:key/resolve` 裁决冲突。

关键语义决策：

- **冲突优先级**：默认 `local-wins` —— 上次外部同步后发生过本地人工修改的实体，外部更新不覆盖，记为 `conflict` 由调度员逐条裁决（接受外部 / 保留本地）；批次可显式声明 `conflictPolicy: 'external-wins'`。
- **未知枚举隔离**：映射失败的记录不进入领域模型，原始报文留存在批次项中，映射表扩展后可重试，海关状态等关键字段绝不猜测默认值。
- **幂等**：`batchId` 为幂等键，重复批次返回首次结果；作业事件按 `eventId` 去重；与本地一致的外部更新记为 `skipped` 不产生写操作。
- **局部失败回滚**：默认 `best-effort` 单项隔离；`mode: 'atomic'` 时任一失败即整体回滚到批次前快照，批次状态 `rolled-back`，修正后可单项重试。
- **删除语义**：只认显式 `op: 'delete'`，绝不从"报文缺少某实体"推断删除（防止全量同步掩盖本地变化）；有进行中预约/任务的箱拒绝外部删除。

当前使用内存种子数据，领域服务通过 REST API 暴露，后续可接入 TOS、OCR、称重、RTG 遥测、预约配额和优化求解器。
