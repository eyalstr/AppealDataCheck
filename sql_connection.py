# sql_connection.py

import os
import pyodbc
from dotenv import load_dotenv
from logging_utils import log_and_print

load_dotenv()

def get_sql_connection():
    try:
        server = os.getenv("SQL_SERVER")
        db = os.getenv("SQL_DATABASE")
        user = os.getenv("SQL_USER")
        password = os.getenv("SQL_PASSWORD")

        if not all([server, db, user, password]):
            raise ValueError("Missing one or more SQL connection environment variables.")

        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={db};"
            f"UID={user};"
            f"PWD={password};"
            f"Trusted_Connection=yes;"
        )

        return pyodbc.connect(conn_str)

    except Exception as e:
        log_and_print(f"❌ SQL connection error: {e}", "error")
        raise
