# WorldQuant_BRAIN_v3 v3.0.4m — PPC Controlled Branch

## 1. 版本定位

- 项目版本：`v3.0.4m`
- 基线版本：`v3.0.4l` + 已覆盖的 PPC Controlled Branch 试验补丁
- 本版本只收敛 `PP_CORRELATION_FAIL` 自动 Repair 的分支、计数、结果评价与审计语义。
- `HIGH_TURNOVER` 的既有三阶段链式 Repair（Decay +2 → Decay +4 → Hump）保持原语义，不改策略。
- 不包含数据库、credentials、reports、`machine_lib_V2_1.py`。

## 2. 核心行为

### 2.1 PPC Branch Anchor / Best Node

`PP_CORRELATION_FAIL` 只在它是当前 primary failure 时进入 PPC Controlled Branch。

PPC Branch 只沿 PPC Repair edge 回溯 Anchor，不跨越此前的 HIGH_TURNOVER 等非 PPC Repair。这样不会为了降低 PPC 又回退到一个已经被其他 Repair 修复前的祖先。

每个 PPC Branch 维护：

- `anchor_candidate_id`
- `best_candidate_id`
- `attempts_used`
- `attempts_remaining`
- `evaluation_pending`
- `success / exhausted`

只有 durable outcome=`IMPROVED` 的 Child 才晋升 Best Node；`WORSE`、`NO_MEANINGFUL_CHANGE`、`REJECT_SIDE_EFFECT` 只关闭当前分支，Best Node 不变。

### 2.2 Attempt 语义

PPC attempt 不再按 `plan_status=EXECUTED` 直接计数。

只有同时满足：

1. `target_failure=PP_CORRELATION_FAIL`
2. Repair 产生了有效变化
3. 已有可评价的 PPC durable outcome

才消耗一次 PPC strategy attempt。

因此：

- `PLANNED`：不计
- `UNCERTAIN_SUBMISSION`：不计 strategy attempt
- Simulation COMPLETE 但 PPC PRE-TAG outcome 尚未得到：不计，并锁住 Branch
- Parent/Repair 同 sim_key 的 no-op：硬拒绝，不计
- 有效 cache restore 且 PPC outcome 可评价：可计一次

默认最大次数 `3`，由配置控制。

### 2.3 PPC Outcome

Durable outcome 共有：

- `TARGET_PASS`
- `IMPROVED`
- `NO_MEANINGFUL_CHANGE`
- `WORSE`
- `REJECT_SIDE_EFFECT`

默认配置：

```yaml
near_pass:
  ppc_controlled_branch:
    max_attempts: 3
    meaningful_improvement_min: 0.01
    meaningful_worsening_min: 0.01
    require_no_new_fixed_blockers: true
    require_not_strategy_rejected: true
    max_sharpe_drop_abs: null
    max_fitness_drop_abs: null
    same_family_windows: [2, 3, 4, 5]
```

`meaningful_improvement_min` / `meaningful_worsening_min` 都可配置，不写死在策略流程里。

### 2.4 Legacy window=None 修复

历史 settings-only Repair Child 可能保留：

- expression=`ts_mean(...,2)`
- metadata `window=NULL`

v3.0.4m 不再把 NULL 当作 0，而是从 canonical expression 恢复真实 window。无法可靠恢复时 fail closed。

Same-family Repair 使用配置窗口池 `[2,3,4,5]`，选择最近的未访问 window；不会默认跳到 22/66。

### 2.5 No-op Guard

Production Repair 在 cache/POST 处理前检查：

`repair_sim_key == parent_sim_key`

命中时抛出：

`REPAIR_NO_EFFECTIVE_CHANGE_SAME_SIM_KEY`

不会 POST、不会把 no-op 标成有效 Repair、不会消耗 PPC strategy attempt。

### 2.6 Budget 修正

Production Repair 的 Repair 预算使用：

`max(plan_level_consumed, round_durable_repair_consumed)`

避免 UNCERTAIN → 显式 retry 时，一个逻辑 Repair Plan 实际产生两个远端 POST budget unit，但 plan-level 只记录一个而导致 reserve 被高估。

### 2.7 Repair Report

`repair_history.csv` 的 Repair edge JOIN 使用 `repair_signature`，并显式输出 `repair_signature` 列，避免同 Parent 多个 SAME_FAMILY variant 时仅靠 `parent_candidate_id + repair_type` 发生错配。

## 3. run_0005 Shadow 验证

在生产数据库副本上完成的本地验证：

- 历史缺失 PPC outcome 可回填：18 条
- 18/18 outcome=`WORSE`
- 网络请求：0
- Simulation POST：0
- Check 请求：0
- 第二次 backfill preview：eligible=0，证明幂等
- PPC attempts：历史 PPC Repair 按 target failure 隔离，不混入 HIGH_TURNOVER Repair
- 18 个 MARKET 修差分支重新回到 Best Parent
- 下一策略为 `SAME_FAMILY_MICRO_TUNE`
- 关键窗口：`3→4`、`4→5`、`3→4`、`3→4`
- legacy `window=NULL + ts_mean(...,2)`：正确生成 `2→3`
- same-sim-key no-op：0
- run_0005 durable Repair budget：38，remaining reserve=362

真实 run_0005 数据库未被本地 Shadow backfill 修改。

## 4. 离线测试

最终代码分组完整运行：

`789 passed, 0 failed`

并通过：

`python -m compileall -q ppl_engine ppl_runner.py tests`

## 5. 覆盖后的执行顺序

### Step 1 — 先验证版本和离线测试

```powershell
python -m pytest -q -p no:cacheprovider --disable-warnings .\tests\test_ppc_controlled_branch_v1.py
```

再运行 Repair 重点回归：

```powershell
python -m pytest -q -p no:cacheprovider --disable-warnings `
.\tests\test_near_pass.py `
.\tests\test_rescue_evidence_trust.py `
.\tests\test_production_repair.py `
.\tests\test_v3_round_orchestrator.py `
.\tests\test_ppc_controlled_branch_v1.py
```

### Step 2 — 查看真实 Round 状态

```powershell
python ppl_runner.py --round-status --run-id run_0005
```

覆盖前已知安全暂停点应仍为 Batch 45 / REPAIR / PAUSED，Repair consumed=38，uncertain=0。

### Step 3 — 只读预览历史 PPC outcome backfill

```powershell
python ppl_runner.py --backfill-ppc-repair-outcomes --run-id run_0005
```

本次 run_0005 预期：

- `eligible_count=18`
- `pending_count=0`
- `verdict_counts.WORSE=18`
- `writes=0`
- network/posts/checks=0

如果本机输出与此明显不一致，先停止，不要继续。

### Step 4 — 显式确认本地 outcome backfill

```powershell
python ppl_runner.py --backfill-ppc-repair-outcomes --run-id run_0005 --confirm-ppc-outcome-backfill
```

这是本地数据库 durable outcome 写入，不会联网、不会 Simulation POST。

### Step 5 — 再做一次只读幂等检查

```powershell
python ppl_runner.py --backfill-ppc-repair-outcomes --run-id run_0005
```

预期 `eligible_count=0`、`writes=0`。

### Step 6 — 重建报告

```powershell
python ppl_runner.py --rebuild-round-reports --run-id run_0005
```

`reports\round_run_0005\repair_history.csv` 应包含 `repair_signature` 与已回填的 PPC outcome。

### Step 7 — 再查 Round Status

```powershell
python ppl_runner.py --round-status --run-id run_0005
```

确认仍是 `PAUSED / REPAIR / current_batch=45`，且预算没有因 backfill 改变。

### Step 8 — 只跑 1 个真实 Canary Batch

前面全部符合预期后才执行：

```powershell
python ppl_runner.py --resume-round --run-id run_0005 --allow-simulation-post --max-batches 1
```

本次只允许一个 Batch。重点检查：

- PPC Repair 是否从 Best Parent 出发
- 是否为 SAME_FAMILY window branch，而不是继续沿 MARKET 坏 Child
- PPC attempts 是否仍为 1→2，而不是受 HIGH_TURNOVER 历史污染
- 是否没有 same-sim-key no-op

## 6. 不包含的下一阶段功能

v3.0.4m 只把 branch state machine 做正确。本版本暂不引入：

- target-failure-specific historical evidence 动态排序
- MARKET 自动降权
- 新的 structural decorrelation operator

这些应在真实 Canary 验证当前状态机后再做，避免一次修改同时改变 branch architecture 与 ranking policy。
