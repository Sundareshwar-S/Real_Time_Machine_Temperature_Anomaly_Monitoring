"""MySQL access for the pipeline (layer 5).

Phase 0 only needs a connection and the schema setup:
    python -m src.db --init
Batch writes for the processor are added in Phase 2.
"""
import argparse

import pymysql

from src import config


def connect():
    """Open a new connection to the machine_monitor database."""
    return pymysql.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DATABASE,
    )


def init_schema():
    """Create every table in db/schema.sql. Safe to run more than once."""
    sql = config.SCHEMA_SQL.read_text()
    # Drop comment lines, then run one statement at a time (PyMySQL runs a
    # single statement per execute call unless multi-statements are enabled).
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    statements = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]

    with connect() as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
            cur.execute("SHOW TABLES")
            tables = [row[0] for row in cur.fetchall()]
        conn.commit()
    return tables


def main():
    parser = argparse.ArgumentParser(description="MySQL helpers for the pipeline.")
    parser.add_argument("--init", action="store_true", help="create the tables from db/schema.sql")
    args = parser.parse_args()

    if args.init:
        tables = init_schema()
        print(f"Schema applied to {config.MYSQL_DATABASE}. Tables: {', '.join(tables)}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
