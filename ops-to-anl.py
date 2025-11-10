#!/usr/bin/env python3
"""
Minimal policy-driven ETL skeleton (MySQL) — YOU implement the TODOs.

pip install mysql-connector-python python-dotenv
Env:
  MYSQL_HOST=127.0.0.1
  MYSQL_PORT=3306
  MYSQL_USER=root
  MYSQL_PASSWORD=rootpass
  USER_HASH_SALT=STATIC_SALT:CHANGE_ME
"""

import os, json, datetime, hashlib
import mysql.connector

# ---- config / policy ----

POLICY_PATH = "policy_engine.json"
DB = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
}
SALT = os.getenv("USER_HASH_SALT", "STATIC_SALT:CHANGE_ME")

with open(POLICY_PATH, "r", encoding="utf-8") as f:
    POLICY = json.load(f)

# ---- helpers you can use or replace ----

def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def date_key(dt) -> int | None:
    if not dt: return None
    return int(dt.strftime("%Y%m%d"))

def age_bin(dob) -> str | None:
    if not dob: return None
    years = (datetime.date.today().year - dob.year) - (
        (datetime.date.today().month, datetime.date.today().day) < (dob.month, dob.day)
    )
    if years < 18: return "<18"
    if years <= 24: return "18-24"
    if years <= 34: return "25-34"
    if years <= 44: return "35-44"
    if years <= 64: return "45-64"
    return "65+"

# ---- DB I/O ----

def get_conn():
    return mysql.connector.connect(**DB, autocommit=False)

def fetch_all(conn, table_fqn: str):
    """SELECT * from a single source table (for dims)."""
    cur = conn.cursor(dictionary=True)
    cur.execute(f"SELECT * FROM {table_fqn}")
    for row in cur:
        yield row
    cur.close()

def fetch_join(conn, base: str, joins: list[dict]) -> list[dict]:
    """
    Build a simple join for fact pipelines.
    Example joins: [{"with":"ops.order_items","on":["order_id"]}]
    """
    # TODO: implement from your policy; minimal default for our model:
    sql = """
      SELECT o.*, oi.product_id, oi.quantity, oi.unit_price_cents
      FROM ops.orders o
      JOIN ops.order_items oi ON oi.order_id = o.order_id
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    return rows

def upsert(conn, table_fqn: str, rows: list[dict], unique_keys: list[str]):
    if not rows: return
    cols = list(rows[0].keys())
    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    nonkeys = [c for c in cols if c not in set(unique_keys)]
    update = ", ".join([f"{c}=VALUES({c})" for c in nonkeys]) or f"{cols[0]}={cols[0]}"
    sql = f"INSERT INTO {table_fqn} ({col_list}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update}"
    data = [tuple(r[c] for c in cols) for r in rows]
    cur = conn.cursor()
    cur.executemany(sql, data)
    conn.commit()
    cur.close()

# ---- policy application (your original idea, but structured) ----

def apply_rule(value, rule: dict, src_col: str):
    action = rule.get("action")

    # remove → emit nothing
    if action == "remove":
        return None, None

    # keep → same name/value
    if action == "keep":
        return src_col, value

    # alias → rename only
    if action == "alias":
        return rule["emit_as"], value

    # hash
    if action == "hash":
        target = rule["emit_as"]
        using = rule.get("using")
        if using == "sha256_salted":
            return target, sha256_hex(f"{SALT}:{'' if value is None else str(value)}")
        elif using == "sha256":
            return target, sha256_hex("" if value is None else str(value))
        else:
            raise ValueError(f"Unknown hash method: {using}")

    # transform
    if action == "transform":
        using = rule.get("using")
        target = rule["emit_as"]
        if using == "age_bin":
            return target, age_bin(value)
        if using == "prefix3":
            return target, (value or "")[:3]
        if using == "date_key":
            return target, date_key(value)
        raise ValueError(f"Unknown transform: {using}")

    raise ValueError(f"Unknown action: {action}")

def transform_row(row: dict, pipeline: dict) -> dict:
    out = {}
    for src_name, rule in pipeline.get("columns", {}).items():
        key = src_name.split(".")[-1]   # support qualified names
        emit, new_val = apply_rule(row.get(key), rule, key)
        if emit is not None:
            out[emit] = new_val

    # computed fields (e.g., revenue_cents = quantity * unit_price_cents)
    for emit, spec in pipeline.get("computed", {}).items():
        expr = spec["expression"]
        # VERY SIMPLE parser (safe subset): replace identifiers by values from 'out' or 'row'
        # TODO: replace with your own safe eval
        tmp = expr
        for k, v in {**row, **out}.items():
            tmp = tmp.replace(k, str(0 if v is None else v))
        out[emit] = eval(tmp)  # TODO: replace eval with your safe evaluator
    return out

# ---- pipeline runners (minimal) ----

def run_dim(conn, pipeline: dict):
    source = pipeline.get("source") or pipeline.get("table") or pipeline.get("from")
    emit_to = pipeline["emit_to"]
    uniq = pipeline.get("uniqueness", [])
    batch = []
    for row in fetch_all(conn, source):
        batch.append(transform_row(row, pipeline))
    upsert(conn, emit_to, batch, uniq)

def lookup_key(conn, table_fqn: str, match: dict, key_col: str):
    where = " AND ".join([f"{k}=%s" for k in match.keys()])
    vals = list(match.values())
    cur = conn.cursor()
    cur.execute(f"SELECT {key_col} FROM {table_fqn} WHERE {where} LIMIT 1", vals)
    res = cur.fetchone()
    cur.close()
    return res[0] if res else None

def run_fact(conn, pipeline: dict):
    emit_to = pipeline["emit_to"]
    uniq = pipeline.get("fact_pk", [])
    rows = fetch_join(conn, pipeline["source"], pipeline.get("joins", []))
    out = []
    for r in rows:
        t = transform_row(r, pipeline)
        # resolve surrogate keys
        for dest_key, map_spec in (pipeline.get("dim_mappings") or {}).items():
            table = map_spec["lookup"]
            on = map_spec["on"]
            match = {dst: t[src] for dst, src in on.items()}
            t[dest_key] = lookup_key(conn, table, match, dest_key)
        out.append(t)
    upsert(conn, emit_to, out, uniq)

def main():
    conn = get_conn()
    try:
        # run dims first, then facts
        for p in POLICY["pipelines"]:
            if ".dim_" in p.get("emit_to", ""):
                run_dim(conn, p)
        for p in POLICY["pipelines"]:
            if ".fact_" in p.get("emit_to", ""):
                run_fact(conn, p)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
