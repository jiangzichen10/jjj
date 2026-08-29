# V3.0.4l 稳定性修复说明

版本来源：v3.0.4k
目标：只修复已由代码、日志和 Batch 23 证据确认的稳定性问题，并补齐同一状态链上的回归保护；不调整 Ranking、Dataset 策略、PPL 判定阈值、Repair 策略本身。

## 1. 修复前代码回顾结论

本次先对 v3.0.4k 的真实代码路径做了回顾，而不是直接按现象打补丁。

确认问题：

1. 长时间运行后，GET Simulation 返回 401/403 时，machine_lib_V2_1 会调用 login() 重新认证；但 V3 的 requests.Session.request 网络 Guard 只允许 POST /simulations，导致合法 POST /authentication 被误判为 PHASE10A_UNEXPECTED_POST。
2. V2.1 SERVER SLOT GUARD 会把未 dispatch 的尾部任务明确返回为 status=NEW + Deferred: ...。V3 在调用 V2.1 前已把这些 Candidate 改成 SIMULATION_PENDING，但返回后没有释放，因此会形成 SIMULATION_PENDING + simulation_status=NONE 的卡死状态。
3. 同一问题也会影响未来 REPAIR：Repair child 在 V2.1 明确 deferred 后同样可能停留在 PENDING，Repair plan 也可能失去可重试状态。
4. Resume-first 如果本地 polling 返回后仍有 RUNNING/SUBMITTED，旧代码没有单独阻止本轮继续创建新 Batch。虽然 V2.1 内部有 Server Slot Guard，但 Round 层应继续坚持 resume-first，不应依赖下一层再兜底。
5. 预先检查 REPAIR 分支时发现一个尚未在 run_0005 触发的确定性问题：REPAIR batch 保存的是 selected_plan_ids，而 research telemetry 默认从 selected_candidate_ids 取 batch scope。未来进入 REPAIR 时可能触发 ROUND_TELEMETRY_BATCH_SCOPE_EMPTY。
6. Batch 的 status=COMPLETED 在当前代码里更接近“本次 batch orchestration pass 已结束”，现有文档没有足够证据把它定义为“所有远端 Simulation 已终态”。因此本版不贸然新增/修改 Batch 状态机；改为把原始选择、有效执行、deferred、nonterminal 分开报告，避免为修显示问题破坏 max-batches/recovery 语义。

未纳入本版：

- READY_FOR_MANUAL_FINALIZATION 19 -> 15：目前只有 Batch 20 周期刷新这一高概率解释，证据还不足，不按 Bug 修。
- REMOTE_NOT_FOUND=4：当前未增长，继续监控。
- post_consumed 与 post_confirmed 历史差值：已能由 reconciliation 历史 + Batch 14 durable POST 解释，不是本版 Bug。

## 2. v3.0.4l 改动

### 2.1 认证刷新白名单收窄修复

V3 Guard 现在允许：

- POST <BRAIN_API_URL>/simulations
- POST <同一 BRAIN API host>/authentication 及其 authentication 子路径

仍然禁止：

- PATCH / PUT / DELETE
- 任意其他 POST
- 跨 host 的伪 authentication POST

Authentication POST 不计入 Simulation post_attempted / post_confirmed / budget。

### 2.2 Search SERVER SLOT deferred 自动释放

只在 V2.1 返回明确证据时自动释放：

- status == NEW
- error 以 `Deferred: an existing server-side simulation is still RUNNING` 开头
- sim_key 属于本次 eligible POST intent

释放动作：

- SIMULATION_PENDING -> PLANNED
- simulation_status 保持 NONE
- alpha_id 必须为空
- execution_action -> NEW_SIMULATION_REQUIRED
- 不消费 POST budget
- 从旧 Batch planned_post_sim_keys 中移除
- 从旧 Batch effective selected_candidate_ids 中移除

关键安全原则：没有明确 V2.1 deferred 标记时，仅仅“没有 durable fact”仍然视为歧义，保持 fail-closed，不自动重试。

### 2.3 Repair SERVER SLOT deferred 对称修复

Repair child 遇到同一明确 deferred 标记时：

- child -> PLANNED/NONE
- repair plan -> READY
- blocked_reason -> SERVER_SLOT_DEFERRED
- committed_posts/consumed_posts -> 0
- 从当前 Repair Batch intent 中移除未 dispatch 的 sim_key/plan_id

### 2.4 Resume-first Round 层硬门

如果 Resume-first 调用结束后仍有 RUNNING/SUBMITTED：

- Round -> PAUSED
- stop_reason = RESUME_FIRST_STILL_NONTERMINAL
- 不分配新 Batch
- 不创建新 POST

下一次 resume 仍先处理这些已存在远端 Simulation。

### 2.5 REPAIR telemetry batch scope 修复

REPAIR 分支现在显式把实际 Repair child candidate_ids 传给 research telemetry，不再依赖 REPAIR Batch 中本来为空的 selected_candidate_ids_json。

### 2.6 Batch 报告语义增强，不改状态机

新增 effective_execution_scope：

- original_selected_decision_rows
- effective_selected_candidates
- effective_selected_repair_plans
- effective_planned_post_intents
- effective_planned_resume_intents
- released_or_not_effective_candidates
- logical_posts_consumed
- deferred_undispatched_candidates
- nonterminal_candidates_at_batch_return

selection_summary_scope 固定说明为 ORIGINAL_SELECTION_DECISIONS。

这样 Batch 14 的“原始 26 / 实际 4”和 Batch 23 的“原始 12 / 实际 9 / deferred 3 / nonterminal 4”不会再混在一起理解。

## 3. 当前 run_0005 / Batch 23 恢复方式

升级代码后，不要直接 resume。先执行一次本地恢复：

```powershell
python ppl_runner.py --recover-interrupted-batch --run-id run_0005 --batch-no 23 --confirm-undispatched-tail
```

这个命令：

- 不联网
- 不 POST Simulation
- 不调用 /check
- 应保留 9 个 durable dispatched
- 应释放 3 个确认未 POST 的 deferred Candidate
- 将 Batch 23 重新置为 RECOVERED，等待正常 finalization

当前已知现场的预期关键值：

```text
action = RECOVER_SERVER_SLOT_DEFERRED_TAIL
source_batch_status = COMPLETED
durable_dispatched = 9
released_undispatched = 3
unresolved_intents = []
network_requests = 0
simulation_posts = 0
check_requests = 0
```

如果实际输出不是 9 / 3，先停止，不要继续 resume。

恢复输出符合预期后再执行：

```powershell
python ppl_runner.py --resume-round --run-id run_0005 --allow-simulation-post --max-batches 10
```

Batch 23 会先 finalization：已 COMPLETE 的走 cache，4 个已有 Simulation 走 resume/poll，不应重复 POST。若这 4 个在本地 polling 结束后仍未终态，Round 会安全 PAUSE，不再新开 Batch。

## 4. 回归验证

代码编译：通过。

测试套件总收集数：636。

按文件组完整跑完，636/636 全部通过，包括：

- V3 Round orchestrator
- Production Repair
- Production logging / network guard
- Phase 10A / 10B
- Phase 2.1 - Phase 9.1
- audit / concurrency / execution hash
- HT / near-pass / classifier
- rescue evidence / same-family micro tune

新增覆盖：

- 合法同 host /authentication 可以通过 Guard
- 非法 arbitrary/cross-host POST 仍被拒绝
- Search deferred tail 只认明确 V2.1 marker
- Search deferred 不扣预算且旧 Batch intent 收缩
- 已 COMPLETED 的旧 Server Slot Batch 可用显式命令安全 reopen
- Resume-first 暴露 still nonterminal 状态
- Repair deferred child 回到 READY/PLANNED 且 0 budget
- Repair Batch deferred intent 收缩
- Repair telemetry 显式 candidate scope

## 5. 冻结项与完整性

`machine_lib_V2_1.py` 未修改。

期望 SHA-256：

```text
0f8944f696eac8481771ae1df87ebd2f467cf69922939b46e783944e9a794762
```

本 Hotfix ZIP 不包含：

- ppl_runner.db
- alpha_results.db
- credentials.txt
- reports/
- machine_lib_V2_1.py

目的是避免覆盖用户当前 live run_0005 数据和冻结的 machine_lib。
