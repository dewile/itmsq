"""
Lists queues and services in SBMQ.
Run with: hatch run python -m itmsq.setup_sbmq
"""

import pyodbc

CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=127.0.0.1;"
    "DATABASE=SBMQ;"
    "UID=ais;"
    "PWD=ais;"
)


def main() -> None:
    conn = pyodbc.connect(CONNECTION_STRING, autocommit=True)
    cursor = conn.cursor()

    print("── Queues ───────────────────────────────────────")
    cursor.execute(
        "SELECT SCHEMA_NAME(schema_id) AS [schema], name FROM sys.service_queues ORDER BY name"
    )
    for row in cursor.fetchall():
        print(f"  [{row.schema}].[{row.name}]")

    print("\n── Services ─────────────────────────────────────")
    cursor.execute(
        """
        SELECT s.name AS service, SCHEMA_NAME(q.schema_id) AS [schema], q.name AS queue
        FROM sys.services s
        JOIN sys.service_queues q ON s.service_queue_id = q.object_id
        ORDER BY s.name
        """
    )
    for row in cursor.fetchall():
        print(f"  {row.service}  →  [{row.schema}].[{row.queue}]")

    conn.close()


if __name__ == "__main__":
    main()
