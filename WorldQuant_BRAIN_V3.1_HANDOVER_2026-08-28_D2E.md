# WorldQuant BRAIN V3.1 项目交接文档 — D2E 路线修订版

日期：2026-08-28
当前推荐开发基线：`WorldQuant_BRAIN_v3.1_D2E_READY.zip`
当前状态：`V3.1-D2 OFFLINE COMPLETE + D2E INFRASTRUCTURE READY`
生产状态：`NOT PRODUCTION READY`
下一真实阶段：`V3.1-D2E Compatibility Evidence Run`
D2E 真实平台 Run：`run_0006`
D3 Adaptive Canary Run：`run_0007`
历史冻结 Run：`run_0005`

---

## 0. 新聊天第一条建议

继续 WorldQuant BRAIN V3.1 开发。请以 `WorldQuant_BRAIN_v3.1_D2E_READY.zip` 和本交接文档作为当前开发基线。A/B/C1-C6 已完成；D1 已完成但严格 SHADOW_ONLY；D2 Evidence Infrastructure 已离线完成；D2E 只负责真实平台兼容模式证据收集。`run_0006` 永久保持 `PHASE_COMPATIBILITY authoritative + SHADOW_ONLY + Adaptive control disabled`，不得在同一 run 中升级为 D3。D3 必须从新的 `run_0007` 开始。不要恢复 `run_0005`，不要提前进入 D3/D4。

---

## 1. 正式阶段状态

- V3.1-A Continuous Lifecycle — COMPLETE
- V3.1-B Unattended Runtime — COMPLETE
- V3.1-C1 Strategy Compatibility Bridge — COMPLETE
- V3.1-C2 Qualification Extraction — COMPLETE
- V3.1-C3 Qualification Safe Reload — COMPLETE
- V3.1-C4 Execution Identity Split — COMPLETE
- V3.1-C4 Search Strategy Extraction — COMPLETE
- V3.1-C4 Repair Strategy Extraction — COMPLETE
- V3.1-C5 Dedicated Search Policy — COMPLETE
- V3.1-C5 Dedicated Repair Policy — COMPLETE
- V3.1-C6 Q/S/R Atomic Policy Bundle — COMPLETE
- V3.1-C POLICY / STRATEGY LAYER — OFFLINE COMPLETE
- V3.1-D1 Adaptive Scheduler Shadow — COMPLETE / SHADOW_ONLY
- V3.1-D2 Scheduler Evidence Infrastructure — OFFLINE COMPLETE
- V3.1-D2 Calibration Tooling — COMPLETE / READ ONLY
- V3.1-D2E Compatibility Evidence Infrastructure — READY
- V3.1-D2E Real WorldQuant Evidence Run — NOT STARTED
- V3.1-D3 Adaptive Canary — NOT STARTED

Adaptive Scheduler 当前仍：`authoritative=false`。

---

## 2. 路线图逻辑冲突与正式修正

旧路线存在矛盾：D2 要用真实 matured Shadow outcome 校准 activation gate，但旧路线又把第一次真实 WorldQuant run 放在 D3/D4/E 之后。没有真实 compatibility evidence，就无法按证据原则确定 D3 threshold。

正式路线现在改为：

`D2 offline evidence infrastructure`
→ `D2E run_0006 Compatibility Evidence`
→ `Evidence Calibration Report`
→ `D2 Safety Gate Review`
→ `D3 run_0007 Adaptive Canary`
→ `D4 Adaptive Authoritative`
→ `E Long-run Hardening`
→ `Production Candidate`

D2E 不是 D3，也不是 Adaptive Canary。

---

## 3. run 生命周期正式定义

### run_0005
V3.0.x historical production。FROZEN。严禁作为 V3.1 当前运行继续恢复。

### run_0006
V3.1 第一次真实平台运行，但身份是 `COMPATIBILITY_EVIDENCE`。

整个生命周期永久：

- `scheduler_authority = PHASE_COMPATIBILITY`
- `scheduler_shadow = SHADOW_ONLY`
- `adaptive_control = DISABLED`
- `authority_transition_allowed = false`
- `automatic_evidence_stop = false`

run_0006 不能在 safe checkpoint 后切成 Adaptive。它是后续 run_0007 的 clean control group。

### run_0007
仅当 D2 Safety Gate 基于 run_0006 真实证据通过、且用户明确授权后，才作为 D3 Adaptive Canary 创建。

### run_0008（如顺序不变）
可作为 D4 / production candidate，而不是复用 run_0006。

---

## 4. D2 已完成的核心内容

D2 已实现：

- Scheduler Evaluation Ledger；
- actual_action / shadow_action / agreement / timestamp / batch；
- Scheduler policy version/hash 与 Evidence policy version/hash 独立；
- Search/Repair backlog、queue age、slot、fairness、consecutive state；
- matured 100/500 Search/Repair productivity；
- 后验 actual-action outcome attribution；
- COMPLETE / READY / Near-Pass / distinct family / Repair verdict / effective Simulation；
- 未执行 alternative 只标记 `COUNTERFACTUAL_PROXY`；
- deterministic replay；
- hard starvation guard 与 stress harness；
- Scheduler Safety Gate mechanism；
- scheduler failure -> future `PHASE_COMPATIBILITY` fallback；
- durable identity/duplicate-post/DB corruption/core invariant failure -> fail closed。

D2 没有写死 activation sample threshold；当前 threshold 仍为 `null`。

---

## 5. D2E 新增保护

专用 policy：`ppl_round_v31_d2e.yaml`。

新增 durable research-run identity：

- mode = `COMPATIBILITY_EVIDENCE`
- expected_run_id = `run_0006`
- scheduler_authority = `PHASE_COMPATIBILITY`
- scheduler_shadow = `SHADOW_ONLY`
- adaptive_control = `DISABLED`
- authority_transition_allowed = `false`
- automatic_evidence_stop = `false`

代码保护：

- run_0006 被保留给 D2E；
- default Continuous 即使不显式指定 run ID、自动解析出 next run = run_0006，也会被拒绝；
- D2E 必须显式 `--run-id run_0006`；
- D2E policy 不能创建 run_0007；
- resume 时先检查 durable research-run lock；
- 任何 authority lock drift 被拒绝；
- 如果已有 run_0006 不是 D2E durable identity，直接 fail closed，不允许静默改造。

注意：这是一项实验 run 身份锁，不是对所有未来 run 的通用 Adaptive policy。

---

## 6. Matured Evidence 预注册语义

真实 run_0006 开始前已经固定 maturation rule type：

- 以 durable state maturation 为主，不先写死分钟数；
- terminal failed/missing Simulation = mature zero-yield；
- Search COMPLETE 必须等 resolved classification；
- Repair COMPLETE 必须等 durable repair verdict；
- RUNNING / SUBMITTED / UNCERTAIN / unresolved COMPLETE 继续 right-censored；
- `minimum_observation_age_seconds = null`。

禁止看到 run_0006 结果后再为了改善统计表现任意选择“20/30/60 分钟算成熟”。如果真实 long-running censoring 确实需要 time fallback，应先从 run_0006 latency evidence 提出并单独评审。

---

## 7. Calibration Report

命令：

`python ppl_runner.py --scheduler-evidence-report --run-id run_0006`

报告只读，SQLite `mode=ro + query_only=ON`，不会建表、改 threshold、切 authority 或产生 POST。

当前报告包括：

- actual/shadow action distribution；
- agreement/disagreement matrix；
- 95% Wilson interval；
- replay consistency；
- matured/censored NEW_POST；
- Search READY/Near-Pass actual yield；
- Repair success actual yield；
- Scheduler score margin；
- both-backlog fairness coverage；
- zero-slot safety coverage；
- durable research-run identity；
- pre-registered maturation protocol；
- Search/Repair durable observation-latency proxy；
- 100/500 productivity score series与 absolute step-change summary；
- qualitative evidence coverage monitor。

报告不自动推荐：

- minimum observations；
- Search minimum samples；
- Repair minimum samples；
- maturity age；
- window stability threshold；
- activation threshold。

这些必须基于 run_0006 真实证据和明确风险/精度要求确定。

---

## 8. Evidence Sufficiency 不得重新变成 Global Budget

run_0006 不设“达到 500 条自动 COMPLETE”之类生命周期预算。

Evidence monitor 只能观察：

- Search matured samples；
- Repair matured samples；
- agreement/disagreement；
- censoring；
- window volatility；
- fairness/slot-pressure coverage；
- family diversity 等。

当前 monitor 不自动 stop、不自动 pause，也不自动宣布 READY_FOR_CALIBRATION。

当证据看起来足够，用户人工暂停 run_0006，生成 Calibration Report，然后进行 D2 Safety Gate Review。

---

## 9. Runtime obligation 边界保持不变

以下属于 durable runtime obligation，不与 Search/Repair expected-value 竞争：

- Remote Poll；
- Check；
- Auth Recovery；
- endpoint retry；
- Report retry；
- Recovery。

Adaptive Research Scheduler 只讨论安全空闲 Simulation capacity 应分给 Search 还是 Repair。

---

## 10. 必须保持的安全不变量

1. duplicate Simulation POST = 0
2. UNCERTAIN_SUBMISSION 禁止自动 re-POST
3. durable simulation_url 不得丢失
4. sim_key 不得因 Qualification/Search/Repair policy 修改而变化
5. Core DB / durable truth 错误 fail closed
6. recoverable network/429/401/5xx 不应全局 halt
7. Search/Repair/Qualification Strategy 不允许直接 HTTP POST
8. Strategy 不允许直接控制 durable workflow transition
9. DB active policy 是 durable truth
10. YAML 只是 candidate policy
11. Q/S/R policy 只能 safe checkpoint 原子切换
12. run_0005 保持冻结
13. run_0006 永久 Compatibility Evidence authority lock
14. run_0007 才允许作为 D3 canary
15. Global Research Budget = UNLIMITED
16. Local Safety Bounds = BOUNDED

---

## 11. Qualification / 平台规则注意事项

- `2-year Sharpe >= 1.58` 仍只是历史聊天示例，绝不是生产 Qualification Rule；
- PPL 真实规则继续以当前源码、配置与平台 evidence 为准；
- PPL 支持 region 不写死；
- Data Coverage 默认 >=0.90 且可配置；
- 不做自动 Dataset Historical Coverage Preflight；
- Active Theme 是可刷新平台 evidence；
- Manual Finalization 仍需 fresh platform Check。

---

## 12. 当前离线验证

D2E 代码完成后的收集：`963` tests。

Sanitized runnable：`948 passed / 0 runnable failures`。

- Compatibility：247 passed；1 production-DB-bound unavailable；
- Phase 2.1–10B：320 passed；2 production-DB-bound unavailable；
- Production/Remote/Repair：149 passed；
- V3.1 A/B/C/D1/D2/Calibration/D2E：149 passed；
- V3 Round Orchestrator：83 passed；
- 12 Foundation tests unavailable because clean package lacks root alpha_results.db。

总 unavailable：15，原因与之前一致，不是新增失败。

Focused D1+D2+Calibration+D2E：39 passed。
New D2E focused：7 passed。

`compileall`: PASS。

`ROUND_SCHEMA_VERSION = 4`。

`machine_lib_V2_1.py` SHA256：
`58634f1eb01880edc88b7d9904edf3716335c35c17d57aaa0215985d82fa34e4`

`round_orchestrator.py` `.shadow_action` execution refs：0。

Live WorldQuant requests during this implementation：0。

---

## 13. 关键开发错误与防范新增记录

历史错误继续保留原交接记录。D2/D2E 新增：

1. 不能把计划/推演写成已完成开发结果；必须先有源码与测试证据。
2. Scheduler policy identity 与 Evidence policy identity 必须分离。
3. 独立 additive D2 table 不应无理由 bump core ROUND_SCHEMA_VERSION。
4. soft fairness penalty 不能当 no-starvation 证明；必须有 bounded hard guard。
5. Repair Shadow fingerprint 必须在 slot trimming 后记录 final compatibility intent。
6. D2 threshold 不能在无真实 evidence 时拍数字。
7. 实验 run identity 不能只校验 CLI requested_run_id；还必须校验自动解析后的 resolved run ID，否则 default next-run allocation 可能绕过 run_0006 reservation。

---

## 14. 下一步真实运行

下一步不是 D3，而是 D2E `run_0006`。

启动真实 POST 前仍应先确认本地工作目录、DB backup/空白新 run 状态、machine hash、policy file 与登录环境无误。

D2E 使用的 orchestration policy 必须是：
`ppl_round_v31_d2e.yaml`

run ID 必须显式：
`run_0006`

run_0006 运行中只能收集 evidence；禁止 Adaptive authority transition。

D2E 数据足够后：人工暂停 -> 生成 evidence calibration report -> 提议 threshold -> D2 Safety Gate Review -> 再决定是否创建 `run_0007` 进入 D3。

---

## 15. 不要提前做完整 V4

继续保持 V4-compatible，但当前不要引入完整 Campaign / Event Sourcing / Facts-Projection 重构 / full TargetProfile / PPL-ATMO-Regular 全统一抽象。
