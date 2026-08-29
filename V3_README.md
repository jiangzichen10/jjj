# WorldQuant BRAIN v3 — Resumable Round Orchestrator

版本定位：V2.2 Audited Fixed 之上的可复用整轮编排层。V2.2 继续负责 Candidate / Simulation / Check / Repair 的事实与安全语义；v3 负责整轮预算、批次、自适应选择、signal-family 去重、断点恢复、Research Telemetry 与汇总报告。

## 1. 不变的安全边界

- `machine_lib_V2_1.py` 不修改；启动真实 Round 前校验既有 SHA-256。
- `alpha_results.db` 继续是 Simulation / Alpha Fact Truth。
- `ppl_runner.db` 继续是 Workflow Truth，同时保存 v3 additive Round/Telemetry durable facts。
- Cache-first、Resume-first；timeout/进程中断不等于 failure。
- 保留 V2.2 的 PostGate、SERVER SLOT GUARD、writer lock、sim_key/settings hard validation 与 Repair Budget。
- v3 只允许在用户显式 `--allow-simulation-post` 后自动 Simulation POST；不会自动 Submit、PowerPoolSelected、PATCH、PUT、DELETE。
- 已成功/已确认提交的 signal family 自动进入 protected set，不再消耗新 Simulation。

当前交付数据库仍保持 V2.2 原始事实未改写。按当前数据库编号，新建 v3 正式 Round 时下一个 Run ID 将是 `run_0005`。

## 2. 默认 Round 目标

`ppl_plan_v3.yaml` + `ppl_round_v3.yaml` 默认：

- Objective：`MAXIMIZE_DISTINCT_PPL_READY_SIGNAL_FAMILIES`
- 总 logical new Simulation budget：2000
- Initial Search：1600
- Repair Reserve：400
- Batch Size：40
- Exploration：30%
- GLB concurrency：4（仍受现有 SERVER SLOT GUARD 限制）
- Normal Near-Pass：最多 1 次有新 POST 的 repair attempt / family
- Strong Near-Pass：最多 2 次有新 POST 的 repair attempt / family
- 同一个 signal family 的 Initial Search 只消耗一个新 Simulation representative；后续微调统一进入受限 Repair Reserve。
- 同族最终只保留一个 live PRE-TAG 通过且综合指标最优的 Family Winner。

2000 是硬上限，不是必须跑满的 KPI。没有安全候选时允许提前结束。

## 3. 已保护 family

`rescue_evidence.json` 中用户已确认提交的 `rK2ZVL93` 会在新 Round 初始化时自动保护：

`techindi_model/predicted_first_quantile_one_day_return_2/IDENTITY/NORMAL/TS_MEAN`

因此 `ZYEVroY0`、`9qpNzjOr` 等同 family 变体不会在 v3 Initial Search / Repair 中继续消耗新 POST。

## 4. 启动完整 Production Round

在项目目录：

```cmd
cd /d "F:\一二三\v3"
python ppl_runner.py --start-round --allow-simulation-post
```

这条命令会：

1. 校验两个 DB integrity 与 `machine_lib_V2_1.py` hash；
2. 自动创建下一个独立 PRODUCTION_RESEARCH Run；
3. 在线 GET-only discovery；
4. 建立 v3 candidate pool，并在任何真实 POST 前冻结 Manifest 与完整 discovery universe；
5. Seed 已成功 family protection；
6. 按 40 条一批滚动执行 Search；
7. 每批 Local Gate + selective PRE-TAG live `/check` + Family Dedup；
8. Search 后进入受限 Near-Pass Repair；
9. 最多消耗 2000 个 logical new Simulation；
10. 每个 Batch 更新 Research Telemetry 和可读报告；
11. 输出 Family Winners / Manual Tag Queue / Near-Pass / Failure Matrix / Budget Audit。

最终 Submit 与 PowerPoolSelected 仍由用户人工完成。

## 5. 查看状态（只读）

```cmd
python ppl_runner.py --round-status --run-id run_0005
```

`--round-status` 不初始化/迁移数据库、不写 audit log、不获取 writer lock。

## 6. 中断后继续

```cmd
python ppl_runner.py --resume-round --run-id run_0005 --allow-simulation-post
```

v3 在每个真实批次持久化精确 `planned_post_sim_keys`。Resume 前会重新从 `alpha_results.db` 重建 logical budget：

- 已有 COMPLETE/RUNNING/SUBMITTED/UNCERTAIN fact：按 durable fact 恢复预算；
- Resume Existing：不重新 POST；
- 若进程中断后存在“已进入 POST intent 但没有任何 durable alpha fact”的歧义 sim_key：fail-closed，报 `ROUND_UNRESOLVED_POST_INTENT`，不会自动重复 POST。

Research Telemetry 同样以数据库为 durable source。即使进程在 CSV/JSON 报告刷新前被强制终止，已持久化的 candidate decision、POST intent、event、batch、simulation fact 仍可在恢复后重新生成报告。

## 7. 控制单次进程只跑若干 Batch

例如首次部署只跑 1 个 Batch 验证环境：

```cmd
python ppl_runner.py --start-round --allow-simulation-post --max-batches 1
```

之后：

```cmd
python ppl_runner.py --resume-round --run-id run_0005 --allow-simulation-post
```

`--max-batches` 只是单次 invocation 限制，不会重置 Round budget。

## 8. Offline Discovery 仅用于预演/测试

```cmd
python ppl_runner.py --start-round --offline-discovery
```

它复用已有 discovery snapshot，不进行 live discovery，也不会因为没有 `--allow-simulation-post` 而执行真实 Simulation。

注意：V2.2 旧 snapshot 的 field/dataset 覆盖可能远小于 v3 的 1600 Search 目标，因此完整 Production Round 应优先进行在线 discovery，而不是依赖旧 offline snapshot。

## 9. 可复用配置

v3 不把“2000”写死在 orchestrator 代码里。以后可以复制 `ppl_plan_v3.yaml`，修改预算/Region/Dataset scope，再通过：

```cmd
python ppl_runner.py --start-round --round-plan your_round_plan.yaml --round-policy ppl_round_v3.yaml --allow-simulation-post
```

启动新的独立 Round。

同一 Round Resume 时必须使用与创建时一致的 Round Policy / execution semantics；出现 policy drift 会硬阻断。

## 10. Research Telemetry：为什么要完整记录第一轮

v3 的记录目标不是只回答“最后哪些 Alpha 成功”，还要能够在下一轮优化时回答：

- 当时为什么选这个 Candidate，为什么跳过另一个；
- 哪些 Dataset / Field / Operator 真正有较高的成功率，而不是单纯被测试得更多；
- 每一个 logical Simulation 是 NEW_POST、CACHE 还是 RESUME；
- 某个 Near-Pass 为什么进入 Repair、Repair 后变好还是变坏；
- 哪个 Batch 开始某类信号边际收益下降；
- 当时使用的是哪一版 ranking / allocation / family / repair policy；
- 如果以后修改排序策略，能否基于旧数据离线 replay，而不用重新花 Simulation Budget。

因此 v3 增加五类 additive durable telemetry：

1. `ppl_round_events`：Round 黑匣子时间线，一件关键事件一行。
2. `ppl_round_candidate_decisions`：记录被发现、被选中、被跳过的 Candidate 及原因和当时评分。
3. `ppl_round_simulation_ledger`：每个 `(round_id, sim_key)` 一行，追踪 logical Simulation 全生命周期。
4. `ppl_round_snapshots`：每个 Batch 的预算、候选池、产出率和策略状态快照。
5. `ppl_round_manifests`：冻结代码、配置、规则、policy version、Theme/limit snapshot 等实验环境。

这些表只在首次启动 v3 Round 时 additive 创建，不修改 V2.2 core schema version，也不预写入本次交付的原始数据库快照。

## 11. Candidate Decision 记录

为了避免“只看成功样本”的幸存者偏差，v3 不只保存实际 Simulation 的 Candidate，还保存完整 discovery universe 以及每批决策。

典型 decision 包括：

- `DISCOVERED`
- `SELECTED`
- `SELECTED_REPAIR`
- `SKIP_PROTECTED_FAMILY`
- `SKIP_ALREADY_TESTED_FAMILY`
- `SKIP_REDUNDANT_FAMILY`
- `SKIP_LOW_SCORE` / batch limit 类跳过
- `SKIP_REPAIR_CAP`
- `SKIP_NO_SAFE_REPAIR_PLAN`
- `HOLD_UNCERTAIN`

同时保存 selection rank、selection score、quality / novelty / family / dataset / operator / repair-risk 分量，以及 `EXPLOIT` / `EXPLORE` / `CACHE` / `RESUME` 等选择模式。

这样以后可以正确计算“成功数 / 实际测试数”以及“某类候选本来有多少机会”，而不是只数最后的赢家。

## 12. Simulation Ledger

`ppl_round_simulation_ledger` 以 logical `sim_key` 去重，主要追踪：

- sequence / batch / phase；
- Candidate、Parent、Family、Expression；
- Dataset / Field / Operator；
- Region / Universe / Neutralization / Decay / Truncation；
- origin：`NEW_POST` / `CACHE` / `RESUME`；
- selection mode；
- POST / completion timestamps 与 duration；
- Alpha ID；
- Sharpe / Fitness / Turnover / Returns；
- HT Ratio / PP Corr / Prod Corr / Sub-universe / 2Y Sharpe；
- Local Gate / PRE-TAG / Near-Pass classification；
- Repair strategy / verdict；
- Family Winner / Variant / Rejected 与原因。

它是后续分析每一单位 Simulation Budget 真实产出的核心表。

## 13. Manifest 与策略版本

每个 Round 开始时冻结 Manifest，至少包含：

- v3 project version / telemetry version；
- V3 code-tree SHA-256；
- `machine_lib_V2_1.py` expected / actual hash；
- `ppl_plan_v3.yaml` / `ppl_round_v3.yaml` / `ppl_rules.yaml` 等文件 hash 与关键配置；
- Region / Universe / Simulation settings；
- Search / Repair budget、Batch Size、exploration fraction、并发；
- 当前 Theme / live threshold snapshot（可获得时）；
- family / ranking / winner / allocation / repair / telemetry policy version。

当前初始 policy IDs：

- `V3_RANK_001`
- `V3_WINNER_001`
- `V3_ALLOC_001`
- `V3_REPAIR_001`
- `V3_FAMILY_001`
- `V3_TELEMETRY_001`

以后即使算法调整为 `_002`，旧 Round 仍能知道自己当时由哪套策略产生，便于做真实 A/B 与离线 replay。

## 14. 每批 Snapshot 与 Failure Matrix

每个完成的 Batch 会保存 Snapshot，用于观察：

- Search / Repair / Total Budget 已用和剩余；
- Cache / Resume / New POST；
- Candidate / Family 覆盖；
- Dataset、Field、Operator、Dataset×Operator 的真实产出；
- Local Gate / PRE-TAG / Near-Pass / distinct family winner 转化率；
- 当前 protected family、Near-Pass、Repair 状态；
- 当前 exploitation / exploration 行为。

同时生成 `failure_matrix.csv`，按 durable diagnosis/check facts 汇总主要 blocker，为后续决定哪些 Dataset / Operator / Window / Repair 应降权提供基础。

## 15. 输出报告目录

为兼容第一版 v3，根 `reports/` 下仍保留原来的 flat summary/winner 文件。

完整研究包写入：

```text
reports/<round_id>/
├─ manifest.json
├─ summary.md
├─ summary.json
├─ timeline.jsonl
├─ simulation_ledger.csv
├─ candidate_decisions.csv
├─ batch_snapshots.jsonl
├─ ppl_family_winners.csv
├─ manual_tag_queue.csv
├─ near_pass_queue.csv
├─ repair_history.csv
├─ failure_matrix.csv
├─ budget_audit.csv
├─ candidates_final.csv
└─ batches/
   ├─ batch_001.json
   ├─ batch_002.json
   └─ ...
```

当前 round ID 格式为 `round_<run_id>`，因此 `run_0005` 的完整研究目录通常为：

`reports/round_run_0005/`

该研究包会在每个完成 Batch 后刷新，而不是等 2000 POST 全部结束才第一次生成。Round 结束时再生成最终版本。

其中后续优化最有价值的文件通常是：

- `simulation_ledger.csv`：完整 Simulation 分母与结果；
- `candidate_decisions.csv`：选择/跳过逻辑与排名；
- `batch_snapshots.jsonl`：策略随批次演化；
- `failure_matrix.csv`：主要失败结构；
- `manifest.json`：实验可复现条件。

如果要做最完整的后续复盘，建议同时保留这一 Round 结束后的 `ppl_runner.db` 和 `alpha_results.db`；CSV/JSON 是分析友好的导出，数据库仍是 durable truth。

## 16. v3 新增/修改文件

- `ppl_engine/round_store.py`：v3 additive durable round/batch/winner/telemetry tables。
- `ppl_engine/round_orchestrator.py`：整轮编排器与 telemetry hooks/report export。
- `ppl_engine/research_telemetry.py`：Research Telemetry、ledger、decision、manifest、snapshot、failure matrix。
- `ppl_plan_v3.yaml`：默认 2000 / 1600 / 400 Production Research 配置。
- `ppl_round_v3.yaml`：Round policy、telemetry 与 policy version 配置。
- `tests/test_v3_round_orchestrator.py`：v3 orchestration + telemetry 回归测试。
- `V3_README.md`：使用与研究记录说明。
- `V3_BUILD_AUDIT.md`：构建与安全审计。

`ppl_runner.py` 仅增加 v3 CLI 路由，原 V2.2 命令继续保留。

## 17. 部署

最终包不包含真实 `key.txt`。解压到本机后把你自己的 `key.txt` 放回项目根目录再运行。
