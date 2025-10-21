# app/db.py
import mysql.connector
from .config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def execute_sql(sql):
    """Execute read-only SQL query and return results"""
    if any(word in sql.lower() for word in ["insert","update","delete","drop","alter","truncate","create"]):
        raise ValueError("Only read-only SELECT queries are allowed")
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"columns": columns, "rows": rows}

def get_schema():
    """Return schema of all tables"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SHOW TABLES;")
    tables = [t[0] for t in cur.fetchall()]
    schema = {}
    for t in tables:
        cur.execute(f"DESCRIBE {t};")
        schema[t] = [{"Field": row[0], "Type": row[1]} for row in cur.fetchall()]
    cur.close()
    conn.close()
    return schema
