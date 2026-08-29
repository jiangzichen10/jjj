# Production Repair 闭环补齐报告（V2.2 Power Pool / PPL Alpha Runner）

项目目录：`F:\一二三\v2`　｜　正式生产 Run：`run_0002`　｜　完成时间：2026-08-16

本报告对应指令第三十九节的 24 项交付物。**未产生任何真实 Simulation POST / PATCH / Submit / PowerPoolSelected 写入。**

---

## 1. 问题 1 真实根因

正式 Production Repair 缺少可执行入口。具体证据：

- `ppl_runner.py --execute-phase10b-repair` 指向 `phase10b.py` 的 `prepare_repair_batch()`，该函数第一步调用 `validate_phase10b_config()`，**硬性要求 `run_profile == "LIVE_VALIDATION"`**（`phase10b.py:25`），而 `run_0002` 的 `run_profile = PRODUCTION_RESEARCH`。
- `prepare_repair_batch()` 只读取 `plan_status='READY'` 的计划（`phase10b.py:172`），而当前 23 个计划的真实状态是 `DEFERRED_INITIAL_SEARCH`。
- 项目中没有任何 CLI 能“把指定的 Production Repair Plan 安全提升为可执行状态”，也没有任何面向 `PRODUCTION_RESEARCH` 的 repair 执行路径。

## 2. 问题 2 真实根因

PRE-TAG `/check` 结果没有回流到 Repair Planning。具体证据：

- `diagnosis.py::_check_failures()` 只在 `eligibility_outcome == "FAIL"` 时才把 `HIGH_TURNOVER_RETURNS_RATIO` 映射为 `HT_RETURNS_RATIO_FAIL`（`diagnosis.py:88`）。而 ZYEVroY0 等 11 个候选的 HT Ratio 是 **WARNING**（`eligibility_outcome=WARNING`），因此永远不会生成该 repair。
- `diagnosis.py::evaluate_local_pre_gate()` 明确把 `HIGH_TURNOVER_RETURNS_RATIO / SUB_UNIVERSE / POWER_POOL_CORRELATION / THEME_MATCH` 列入 `unknown_live_facts`（`diagnosis.py:62`），本地 gate PASS 时 `primary_failure=NO_FAILURE` → `plan_repairs` 不产出计划。
- 于是“真正接近 Finalist 的 Alpha”反而没有 Repair Plan。检查数据已持久化在 `ppl_check_results`（`HT_HIGH_TURNOVER_RETURNS_RATIO` → `HIGH_TURNOVER_RETURNS_RATIO`，`raw_value_json` / `raw_limit_json` / `eligibility_outcome` 齐全），只是没有任何一层去消费它。

## 3. 修改文件列表

| 文件 | 类型 | 目的 |
|---|---|---|
| `ppl_engine/production_repair.py` | 新增 | 问题1：Production Repair 薄适配入口（preview / execute / list） |
| `ppl_engine/check_derived_repair.py` | 新增 | 问题2：Check-derived HT Ratio Repair Planning（仅 Planning） |
| `ppl_engine/store.py` | 修改 | 新增 `load_repair_plans` / `repair_budget_state` / `transition_repair_plan`（事务化+审计+幂等） |
| `ppl_engine/state_machine.py` | 修改 | 新增 `REPAIR_PLAN_TRANSITIONS` 合法转换表 |
| `ppl_engine/contracts.py` | 修改 | `REPAIR_PLAN_STATUSES` 补充 `DEFERRED_INITIAL_SEARCH` / `DEFERRED_PHASE_END` |
| `ppl_engine/live_execution.py` | 修改 | `run_local_analysis` 增加可选参数 `repair_reserve_remaining`（默认原值，向后兼容） |
| `ppl_runner.py` | 修改 | 新增 `--production-repair` / `--list-repair-plans` / `--plan-check-derived` / `--plan-id` / `--plan-source` |
| `tests/test_production_repair.py` | 新增 | 问题1 的 21 个离线测试 |
| `tests/test_check_derived_repair.py` | 新增 | 问题2 的 11 个离线测试 |

## 4. 是否修改 DB schema

**否。** `ppl_repair_plans` 已具备 `plan_status / target_failure / committed_posts / consumed_posts / projected_new_posts / blocked_reason / candidate_spec_json / operator_requirements_json` 等全部所需字段；`ppl_diagnoses` 已有 `source_phase`（新增记录用 `PRE_TAG_CHECK` 区分）；check 结果已持久化在 `ppl_check_results`。无需 ALTER / DROP / 新表。

## 5. 是否修改 machine_lib_V2_1.py / alpha_results.db

- `machine_lib_V2_1.py`：**未修改**，SHA-256 仍为 `0F8944F696EAC8481771AE1DF87EBD2F467CF69922939B46E783944E9A794762`。
- `alpha_results.db`：**schema 未修改**，仍仅含 `alpha_results` / `alpha_contexts` 两张表，未 ALTER / DROP / DELETE。

## 6–12. 真实 CLI 命令

```bash
# 6. Production Repair CLI（preview 默认，POST 需二次显式授权）
python ppl_runner.py --production-repair --run-id run_0002 --plan-id <plan_id> [--plan-id <plan_id> ...]

# 7. 单 Plan Preview（不 POST）
python ppl_runner.py --production-repair --run-id run_0002 --plan-id <plan_id>

# 8. 单 Plan Execute（显式授权后才 POST）
python ppl_runner.py --production-repair --run-id run_0002 --plan-id <plan_id> --allow-simulation-post

# 9. 多 Plan Preview
python ppl_runner.py --production-repair --run-id run_0002 --plan-id <id1> --plan-id <id2>

# 10. 多 Plan Execute
python ppl_runner.py --production-repair --run-id run_0002 --plan-id <id1> --plan-id <id2> --allow-simulation-post

# 11. 查看 Deferred（本地诊断）Repair Plans
python ppl_runner.py --list-repair-plans --run-id run_0002 --plan-source deferred

# 12. 查看 Check-derived Repair Plans
python ppl_runner.py --list-repair-plans --run-id run_0002 --plan-source check-derived
```

## 13. HT Ratio Proposal 生成方式

```bash
python ppl_runner.py --plan-check-derived --run-id run_0002
```

该命令读取已持久化的 `ppl_check_sessions` + `ppl_check_results`，对满足条件的候选生成 `HT_RATIO_SIGNAL_HORIZON` 提案并写入 `ppl_repair_plans`（`plan_status=PLANNED`，`target_failure=HT_RETURNS_RATIO_FAIL`）。**仅 Planning，不 Simulation，不消耗预算，幂等**（`UNIQUE(run_id, repair_signature)` 去重）。生成条件复用现有 `evaluate_local_pre_gate` + eligibility 语义：Local Gate PASS、Sharpe ≥ 1.0、Turnover ∈ [0.01,0.70] 且 ≥ 0.20、Sub-universe 非 FAIL、PP Corr 非 FAIL、HT Ratio 为真实 WARNING/FAIL 且 `raw_value < 0.75`、session RESOLVED、非 raw PENDING。`base_gate=PENDING` 不会被伪装成 PASS（保留在提案上下文里）。

## 14. Budget 行为

- 计划生成（含 Check-derived）：**0 消耗**。
- Cache Restore / Resume / Already-exists：**0 消耗**。
- 仅 `NEW_SIMULATION_REQUIRED / RETRY_PER_V21_POLICY` 且最终 `COMPLETE/RUNNING/SUBMITTED/UNCERTAIN` 才写 `committed_posts / consumed_posts`。
- Repair Reserve = 48（来自 `simulation_budget_allocation` 的 `repair_reserve_fraction 0.40 × 120`），`remaining = 48 - consumed`。
- 同一 sim_key 重复请求不重复扣预算；执行前硬校验 `len(new_post_plans) > remaining → PRODUCTION_REPAIR_BUDGET_EXCEEDED`。

## 15. Cache / Resume 行为

每个计划的 repair sim_key 在 Preview 与 Execute 前均做 TOCTOU 检查（复用 `classify_cache_read_only` + `_alpha_facts`）：

- `COMPLETE` → `CACHE_RESTORE`（不 POST，不扣预算）
- `RUNNING/SUBMITTED` → `RESUME_EXISTING`（不新 POST，不扣预算）
- `UNCERTAIN_SUBMISSION` → `HOLD_UNCERTAIN`（**停止扩展，绝不重 POST**）
- `CACHE_MISS` 且无同 run 候选 → `NEW_SIMULATION_REQUIRED`（唯一允许新 POST）
- `CACHE_MISS` 但 repair expression 恰与已有候选同 sim_key → `ALREADY_EXISTS`（不 POST，不建重复 child）

## 16. 状态机变化

- 新增 `REPAIR_PLAN_TRANSITIONS`（`state_machine.py`）。合法转换核心：`DEFERRED_INITIAL_SEARCH / DEFERRED_PHASE_END / PLANNED → READY → EXECUTED`。
- `DEFERRED_INITIAL_SEARCH → READY` 仅通过 `store.transition_repair_plan()`（事务 + `ppl_live_execution_audits` 审计 + 幂等），**禁止裸 SQL `UPDATE ... SET plan_status='READY'`**。
- 状态枚举未扩充新值（复用 `READY` / `PLANNED` 等已有状态），仅在 `REPAIR_PLAN_STATUSES` 补记两个已在 DB 中实际使用的 deferred 值。

## 17. 测试结果

- 全量：**401 passed**（369 旧 + 32 新）。
- 新增 32 个测试全部通过：`test_production_repair.py`（21）、`test_check_derived_repair.py`（11）。
- 说明：首轮全量曾出现 2 个环境性失败，均与本次改动无关——(a) `test_concurrency.py::test_11_stop_event...` 是时序敏感测试（断言 scheduler <0.5s，沙箱内实测 ~4.9s，隔离复跑可复现，属既有 flaky）；(b) 一处因我自己遗留的 `--basetemp` 目录导致的 setup 冲突。清理后全新跑 401 全绿。

## 18–23. 安全确认

| 项 | 结果 |
|---|---|
| machine_lib_V2_1.py 保持不变 | ✅ SHA-256 未变 |
| alpha_results.db schema 保持不变 | ✅ 仅 2 表，未改动 |
| 真实 Simulation POST | ✅ **NO** |
| PATCH | ✅ **NO** |
| Submit | ✅ **NO** |
| PowerPoolSelected 写入 | ✅ **NO** |
| 自动执行全部 23 个 / 全部 Check-derived / 递归 Repair | ✅ 全部禁止（必须显式 `--plan-id`） |

## 24. 推荐 Repair Wave 1 候选（只推荐，不执行）

**A 类（Local Gate PASS，但 HT Ratio 不足）** — 由 `--plan-check-derived` 生成的 11 个提案中，优先：

| 优先级 | Alpha | Sharpe | Turnover | HT Ratio | 距 0.75 |
|---|---|---|---|---|---|
| 1 | **ZYEVroY0** | 2.42 | 0.6249 | **0.7374** | 仅差 0.0126 |
| 2 | WjArW37Q | 2.66 | 0.6701 | 0.6380 | 0.112 |
| 3 | 0mpZpxKG | 2.51 | 0.6279 | 0.6354 | 0.115 |

**B 类（Sharpe 高但 Turnover > 0.70）** — 23 个 deferred 计划中，优先：

| 优先级 | Alpha | Sharpe | Turnover | 计划 |
|---|---|---|---|---|
| 1 | **ak1m7YR2** | 3.18 | 0.9584 | TURNOVER_HIGH_TS_MEAN_2 |
| 2 | gJ8NZZvg | 2.75 | 0.7239 | TURNOVER_HIGH_TS_MEAN_2 |
| 3 | leWw3Opx | 2.43 | 0.9028 | TURNOVER_HIGH_TS_MEAN_2 |
| 4 | O0GdqlQY | 2.31 | 0.8223 | TURNOVER_HIGH_TS_MEAN_2 |
| 5 | QPGR2J1Q | 2.28 | 0.7809 | TURNOVER_HIGH_TS_MEAN_2 |

> 以上数值均从 `ppl_runner.db` + `alpha_results.db` 实时读取，未硬编码。

---

## 附：两个需要人工留意的发现

1. **现有 HT Ratio repair 策略对 A 类候选大量撞已有 sim_key**：`repair_engine.py` 里 `HT_RETURNS_RATIO_FAIL` 的唯一策略是 `HT_RATIO_SIGNAL_HORIZON`（`ts_delta(字段, 2)`）。但 initial search 的 RETURN 路由已包含 `ts_delta windows [1,2]`，因此 ZYEVroY0 等的 repair expression `ts_delta(predicted_first_quantile_one_day_return_2, 2)` **在初始搜索中已存在并 COMPLETE**。实测 11 个 HT 提案里 6 个 `CACHE_RESTORE`、5 个 `ALREADY_EXISTS`，即当前策略对这批 near-miss 基本不产生“新”候选。真正“克制”的 HT Ratio 修复（如同一算子族内小步调整，而非把 ts_mean 换成 ts_delta）需要另立策略——本次按指令**未擅自新增 operator**，仅如实报告。
2. **过期锁文件**：`F:\一二三\v2\ppl_runner.lock`（pid 16900）当前仍存在，会阻塞所有带 `SingleRunnerLock` 的命令（含 `--production-repair`、`--plan-check-derived`）。正式执行前需先人工确认该进程是否存活并清理锁。
