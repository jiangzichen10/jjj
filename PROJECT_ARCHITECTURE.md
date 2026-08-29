# WorldQuant BRAIN Alpha 自动遍历系统架构

## 1. 系统主链路

```mermaid
flowchart TD
    NB["Alpha遍历_V2.ipynb<br/>配置参数并编排 Stage 1–3"]
    LIB["machine_lib.py<br/>认证、候选生成、模拟、评分与提交检查"]
    AUTH["登录认证<br/>项目目录 txt → 环境变量备用"]
    API["WorldQuant BRAIN API<br/>authentication / data-fields / simulations / alphas"]
    SIM["Simulation Engine<br/>提交模拟 → 轮询状态 → 获取指标"]
    CACHE[("SQLite<br/>alpha_results.db")]
    FILTER["Alpha 筛选与晋级<br/>score_results + promote_candidates"]
    CHECK["提交检查<br/>check_submission"]
    RESULT["最终结果<br/>gold_bag / CSV / SQLite"]

    NB -->|"importlib.reload + 函数调用"| LIB
    LIB --> AUTH
    AUTH -->|"POST /authentication"| API
    LIB -->|"GET /data-fields"| API
    LIB --> SIM
    SIM -->|"POST /simulations"| API
    API -->|"进度 URL / Alpha ID"| SIM
    SIM -->|"GET /alphas/{id}"| API
    SIM <-->|"按 expression + settings 去重和保存"| CACHE
    SIM -->|"Sharpe / Fitness / Turnover / Margin / Positions"| FILTER
    FILTER -->|"Stage 1 晋级"| NB
    FILTER -->|"Stage 2 晋级与 Stage 3 修复候选"| NB
    NB --> CHECK
    CHECK -->|"GET /alphas/{id}/check"| API
    CHECK --> RESULT
    FILTER --> RESULT
```

主流程可以概括为：

```text
Notebook
   ↓
machine_lib
   ↓
WorldQuant BRAIN API
   ↓
Simulation
   ↓
Alpha 筛选与晋级
   ↓
提交检查
   ↓
结果保存
```

## 2. Notebook 编排层

文件：`Alpha遍历_V2.ipynb`

Notebook 不实现底层 HTTP 和评分算法，主要负责设置参数、组织执行顺序和展示结果。

```mermaid
flowchart TD
    LOAD["加载 machine_lib<br/>importlib.reload 并打印实际路径"]
    CONFIG["搜索配置<br/>REGION / UNIVERSE / DATASET_ID<br/>NEUTRALIZATION / DECAY / TRUNCATION"]
    LOGIN["s = login()"]
    FIELDS["获取并预处理字段<br/>get_datafields → prepare_fields"]
    S1["Stage 1<br/>first_order_candidates<br/>低成本一阶探索"]
    S1SIM["simulate_candidates"]
    S1SELECT["score_results<br/>promote_candidates"]
    S2["Stage 2<br/>second_order_candidates<br/>Group 结构增强"]
    S2SIM["simulate_candidates"]
    S2SELECT["score_results<br/>promote_candidates"]
    S3["Stage 3<br/>targeted_repair_candidates<br/>Decay / Hump / Trade When"]
    S3SIM["simulate_candidates"]
    FINAL["合并 Stage 2 与 Stage 3 结果<br/>排序、去重"]
    SUBMIT["check_submission"]
    EXPORT["导出 CSV<br/>结果同时保存在 SQLite"]

    LOAD --> CONFIG --> LOGIN --> FIELDS
    FIELDS --> S1 --> S1SIM --> S1SELECT
    S1SELECT -->|"晋级 Alpha"| S2
    S2 --> S2SIM --> S2SELECT
    S2SELECT -->|"需要降低换手等定向修复"| S3
    S2SELECT -->|"已满足要求"| FINAL
    S3 --> S3SIM --> FINAL
    FINAL --> SUBMIT --> EXPORT
```

## 3. `machine_lib.py` 功能分层

| 层 | 主要函数 | 职责 |
|---|---|---|
| 登录认证 | `login`、`_find_credentials_file`、`_read_credentials_file` | 查找并解析账号密码，创建已认证的 `requests.Session` |
| HTTP 基础层 | `_request_with_retry`、`_get_json` | 超时、限流、重试和 JSON 获取 |
| 数据字段层 | `get_datasets`、`get_datafields`、`prepare_fields` | 获取数据集字段并构造预处理表达式 |
| Alpha 候选层 | `first_order_candidates`、`second_order_candidates`、`targeted_repair_candidates` | 生成 Stage 1–3 候选表达式和元数据 |
| 模拟设置层 | `build_settings`、`simulation_key` | 生成 BRAIN Settings，并计算缓存键 |
| 模拟执行层 | `simulate_candidates`、`_post_single_simulation`、`_wait_simulation`、`fetch_alpha_result` | 提交模拟、轮询进度、立即获取 Alpha 指标 |
| 缓存层 | `init_cache`、`cache_get`、`cache_put` | 用 SQLite 避免重复模拟并保存结果 |
| 筛选层 | `score_results`、`promote_candidates` | 多指标评分、阈值过滤、字段多样性晋级 |
| 提交检查层 | `get_check_submission_detailed`、`check_submission`、`view_alphas` | 检查提交条件并展示通过的 Alpha |

## 4. API 调用关系

```mermaid
sequenceDiagram
    participant N as Notebook
    participant L as machine_lib.py
    participant C as SQLite Cache
    participant A as WorldQuant BRAIN API

    N->>L: login()
    L->>L: 查找并解析凭据 txt
    L->>A: POST /authentication
    A-->>L: 已认证 Session

    N->>L: get_datafields(...)
    L->>A: GET /data-fields
    A-->>L: 字段列表
    L-->>N: DataFrame

    N->>L: simulate_candidates(candidates, settings)
    loop 每个唯一 expression + settings
        L->>C: cache_get(simulation_key)
        alt 缓存命中且状态 COMPLETE
            C-->>L: 已有模拟指标
        else 需要模拟
            L->>A: POST /simulations
            A-->>L: Location 进度地址
            loop 直到完成、失败或超时
                L->>A: GET Location
                A-->>L: 模拟状态
            end
            L->>A: GET /alphas/{alpha_id}
            A-->>L: Sharpe / Fitness / Turnover 等指标
            L->>C: cache_put(结果)
        end
    end
    L-->>N: results DataFrame

    N->>L: score_results + promote_candidates
    L-->>N: 晋级候选

    N->>L: check_submission(alpha_ids)
    L->>A: GET /alphas/{alpha_id}/check
    A-->>L: PASS / FAIL 与失败项目
    L-->>N: gold_bag
```

## 5. Simulation 内部流程

```mermaid
flowchart TD
    INPUT["候选 Candidate<br/>expr + field + stage + decay"]
    SETTINGS["build_settings<br/>Region / Universe / Delay<br/>Neutralization / Truncation"]
    KEY["simulation_key<br/>SHA-256 expression + settings"]
    LOOKUP{"SQLite 中是否已有<br/>COMPLETE 结果？"}
    HIT["直接复用缓存结果"]
    POST["POST /simulations"]
    POLL["轮询 Location"]
    STATE{"模拟状态"}
    FETCH["GET /alphas/{alpha_id}"]
    SAVE["cache_put 保存指标"]
    ERROR["保存 ERROR 与错误信息"]
    DF["返回 pandas DataFrame"]

    INPUT --> SETTINGS --> KEY --> LOOKUP
    LOOKUP -->|"是"| HIT --> DF
    LOOKUP -->|"否"| POST --> POLL --> STATE
    STATE -->|"PENDING / RUNNING / QUEUED"| POLL
    STATE -->|"COMPLETE / WARNING"| FETCH --> SAVE --> DF
    STATE -->|"ERROR / FAILED / 超时"| ERROR --> DF
```

缓存唯一性由以下组合决定：

```text
simulation_key = SHA-256(expression + 完整 settings)
```

因此表达式相同但 Region、Universe、Decay、Neutralization 或其他 Settings 不同时，仍会视为不同模拟。

## 6. Alpha 漏斗筛选

```mermaid
flowchart TD
    FIELD["预处理字段<br/>winsorize + ts_backfill"]
    STAGE1["Stage 1 候选<br/>Raw / Rank / Zscore<br/>核心时序算子 + 专属窗口"]
    SCORE1["多指标评分"]
    PASS1{"满足 Stage 1<br/>晋级阈值？"}
    DROP1["停止扩展"]
    STAGE2["Stage 2 候选<br/>Group Neutralize / Group Rank<br/>5 个核心 Group"]
    SCORE2["再次评分"]
    PASS2{"满足 Stage 2<br/>晋级阈值？"}
    DROP2["停止扩展"]
    REPAIR{"是否需要<br/>定向修复？"}
    KEEP["直接保留优秀 Alpha"]
    STAGE3["Stage 3 修复<br/>Decay / Hump / Trade When"]
    MERGE["合并、重新评分、按 alpha_id 去重"]
    FINAL["最终候选"]

    FIELD --> STAGE1 --> SCORE1 --> PASS1
    PASS1 -->|"否"| DROP1
    PASS1 -->|"是"| STAGE2 --> SCORE2 --> PASS2
    PASS2 -->|"否"| DROP2
    PASS2 -->|"是"| REPAIR
    REPAIR -->|"质量好且换手健康"| KEEP --> MERGE
    REPAIR -->|"换手偏高等问题"| STAGE3 --> MERGE
    MERGE --> FINAL
```

`score_results` 使用的指标权重：

| 指标 | 权重 |
|---|---:|
| 绝对 Sharpe | 35% |
| 绝对 Fitness | 25% |
| 绝对 Margin | 15% |
| Turnover Quality | 15% |
| 持仓覆盖数量 | 10% |

`promote_candidates` 在评分之外还会执行：

- 最低 Sharpe、Fitness 和持仓数量过滤。
- Turnover 上限过滤。
- 同一字段保留数量限制，维持字段多样性。
- 负 Sharpe 候选通过反转表达式统一方向。

## 7. 提交检查与结果产物

```mermaid
flowchart LR
    FINAL["最终 Alpha ID"] --> CHECK["GET /alphas/{id}/check"]
    FINAL --> CSV["final_results.csv"]
    CHECK --> RESULT{"全部检查通过？"}
    RESULT -->|"是"| GOLD["加入 gold_bag"]
    RESULT -->|"否"| FAIL["打印失败检查名称"]
    GOLD --> VIEW["view_alphas<br/>获取并展示最终指标"]
    VIEW --> DISPLAY["Notebook 展示"]
```

主要产物：

| 产物 | 内容 |
|---|---|
| `alpha_results.db` | 所有模拟缓存、状态、指标、候选元数据和错误信息 |
| `stage1_results.csv` | Stage 1 模拟及评分结果 |
| `stage2_results.csv` | Stage 2 模拟及评分结果 |
| `stage3_results.csv` | Stage 3 定向修复结果 |
| `final_results.csv` | 合并、评分和去重后的最终候选 |
| `gold_bag` | 通过 BRAIN submission check 的 Alpha ID 及自相关指标 |

## 8. 边界说明

- Notebook 是流程编排入口，`machine_lib.py` 是核心实现。
- 账号密码仅来自项目目录 txt，或在没有 txt 时来自环境变量；不会写入 Python 源码。
- SQLite 是模拟缓存，不替代 BRAIN API 的真实 Alpha 数据。
- Stage 1–3 是逐级缩小搜索空间的漏斗，不是对全部算子做无边界笛卡尔积。
- Submission Check 是最终质量门槛，不参与前面候选表达式的生成。
