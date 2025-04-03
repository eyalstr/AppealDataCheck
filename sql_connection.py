import pyodbc
import os
from logging_utils import log_and_print
from dotenv import load_dotenv

load_dotenv()  # Ensure environment variables are loaded

def get_sql_connection():
    try:
        server = os.getenv("SQL_SERVER")
        database = os.getenv("SQL_DATABASE")
        username = os.getenv("SQL_USERNAME")
        password = os.getenv("SQL_PASSWORD")

        if not all([server, database, username, password]):
            raise ValueError("Missing one or more SQL connection environment variables.")

        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )

        connection = pyodbc.connect(conn_str)
        return connection

    except Exception as e:
        log_and_print(f"❌ SQL connection error: {e}", "error")
        raise
