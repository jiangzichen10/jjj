# SOURCE_PACKAGE_MANIFEST

1. 源项目路径: `F:\一二三\WorldQuant_BRAIN_v3`
2. 输出目录: `F:\一二三\WorldQuant_BRAIN_v3.0.4o_source`
3. 输出 ZIP 路径: `F:\一二三\WorldQuant_BRAIN_v3.0.4o_source.zip`
4. 当前版本号: `v3.0.4o`
5. 保留文件总数: 169
6. 保留目录总数: 4
7. 排除的主要目录: `.git, .pytest_cache, .workbuddy, __pycache__, backup, logs, reports`
8. 排除的敏感文件: `alpha_results.db, credentials.txt, ppl_runner.db, ppl_state.json, ppl_summary.json`
9. 是否发现数据库文件(源): 是(4 个, 未复制)
10. 是否发现 credentials.txt(源): 是(1 个, 未复制)
11. 是否发现 .git(源): 否
12. 最终检查结果: PASS

### 排除规则

- 目录: .git, .pytest_cache, .workbuddy, __pycache__, backup, logs, reports
- 文件后缀: .db, .db-shm, .db-wal, .pyc, .pyo
- 精确文件: alpha_results.db, credentials.txt, ppl_runner.db, ppl_state.json, ppl_summary.json

### 必须存在项检查
- ppl_engine: OK
- tests: OK
- ppl_runner.py: OK
- machine_lib_V2_1.py: OK
- VERSION.txt: OK

### 禁止项检查
- 禁止项均不存在: OK

### 保留目录列表
- ppl_engine
- tests
- tests\fixtures
- tests\fixtures\live_sanitized
