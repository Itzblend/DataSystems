import os
import psycopg2
import click
from contextlib import contextmanager

from typing import List

DB_HOST = os.getenv("PSQL_HOST")
DB_PORT = os.getenv("PSQL_PORT")
DB_NAME = os.getenv("PSQL_DB")
DB_USER = os.getenv("PSQL_USER")
DB_PASSWORD = os.getenv("PSQL_PASS")


@contextmanager
def pg_cursor() -> psycopg2.extensions.cursor:
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cursor = conn.cursor()
        yield cursor
    except psycopg2.Error as e:
        print("Error: Could not connect to database.")
        print(e)
    finally:
        conn.commit()
        conn.close()


def copy_ndjson_to_table(file_path, schema_name, table_name, etl_script_path):
    insertion_file = open(file_path, "r")
    with pg_cursor() as cursor:
        cursor.execute(
            f"""
                       CREATE TEMP TABLE {table_name}_staging (data JSONB) ON COMMIT DROP
                       """
        )
        cursor.copy_expert(
            f"""
                           COPY {table_name}_staging
                           FROM STDIN WITH CSV quote e'\x01' delimiter e'\x02'
                           """,
            insertion_file,
        )
        cursor.execute(open(etl_script_path, "r").read())
    insertion_file.close()


def _init_db():
    init_files = []
    for root, dirs, files in os.walk("src/trains/sql/ddl"):
        for file in files:
            init_files.append(os.path.join(root, file))
    print(init_files)
    with pg_cursor() as cursor:
        for file in init_files:
            with open(file, "r") as f:
                cursor.execute(f.read())


def collect_files(file_path: str, file_extension: str) -> List[str]:
    data_files = []
    for root, dirs, files in os.walk(file_path):
        for file in files:
            if file.endswith(file_extension):
                data_files.append(os.path.join(root, file))
    return data_files
