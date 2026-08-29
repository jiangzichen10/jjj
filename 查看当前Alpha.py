import sqlite3
import pandas as pd

DB_PATH = r"F:\一二三\v2\alpha_results.db"

# 独立进程，只读数据库
conn = sqlite3.connect(
    f"file:{DB_PATH}?mode=ro",
    uri=True,
    timeout=30
)

# =========================================================
# 最近 90 分钟任务状态
# =========================================================
status_df = pd.read_sql_query(
    """
    SELECT
        status,
        COUNT(*) AS count
    FROM alpha_results
    WHERE datetime(updated_at) >= datetime('now', '-180 minutes')
    GROUP BY status
    ORDER BY count DESC
    """,
    conn
)

# =========================================================
# 最近 90 分钟 COMPLETE Alpha
# =========================================================
df = pd.read_sql_query(
    """
    SELECT
        alpha_id,
        expr,
        sharpe,
        fitness,
        turnover,
        margin,
        long_count,
        short_count,
        updated_at
    FROM alpha_results
    WHERE status = 'COMPLETE'
      AND datetime(updated_at) >= datetime('now', '-90 minutes')
    ORDER BY updated_at DESC
    """,
    conn
)

conn.close()

for col in [
    "sharpe",
    "fitness",
    "turnover",
    "margin",
    "long_count",
    "short_count"
]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

df["positions"] = (
    df["long_count"].fillna(0) +
    df["short_count"].fillna(0)
)

df["abs_sharpe"] = df["sharpe"].abs()
df["abs_fitness"] = df["fitness"].abs()

df["strict_pass"] = (
    (df["abs_sharpe"] >= 0.80) &
    (df["abs_fitness"] >= 0.45) &
    (df["positions"] >= 100) &
    (df["turnover"] <= 0.70)
)

df["direction"] = df["sharpe"].apply(
    lambda x: "FLIP" if pd.notna(x) and x < 0 else "NORMAL"
)

print("\n===== 最近 90 分钟任务状态 =====")
print(status_df.to_string(index=False))

print("\n最近 COMPLETE:", len(df))
print("当前严格达标:", int(df["strict_pass"].sum()))

print("\n===== 当前 Top 30 =====")

top = (
    df[df["positions"] >= 100]
    .sort_values(
        ["strict_pass", "abs_sharpe", "abs_fitness"],
        ascending=[False, False, False]
    )
    [
        [
            "alpha_id",
            "sharpe",
            "fitness",
            "turnover",
            "positions",
            "direction",
            "strict_pass",
            "expr",
        ]
    ]
    .head(30)
)

pd.set_option("display.max_colwidth", 100)
pd.set_option("display.width", 220)

print(top.to_string(index=False))