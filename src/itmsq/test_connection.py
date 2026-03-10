"""
Connection test — run with: hatch run python -m itmsq.test_connection
"""

import pyodbc

CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=127.0.0.1;"
    "DATABASE=SBMQ;"
    "UID=your_user;"
    "PWD=your_password;"
)


def main() -> None:
    print(f"Connecting to {CONNECTION_STRING!r} ...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING, timeout=10)
        cursor = conn.cursor()
        cursor.execute("SELECT @@SERVERNAME AS server, @@VERSION AS version")
        row = cursor.fetchone()
        print(f"  OK — server : {row.server}")
        print(f"       version: {row.version.splitlines()[0]}")

        cursor.execute(
            "SELECT name FROM sys.databases WHERE name = 'SBMQ'"
        )
        db = cursor.fetchone()
        print(f"  DB 'SBMQ'  : {'found' if db else 'NOT FOUND'}")

        conn.close()
        print("Connection closed cleanly.")
    except pyodbc.Error as e:
        print(f"  FAILED: {e}")


if __name__ == "__main__":
    main()
