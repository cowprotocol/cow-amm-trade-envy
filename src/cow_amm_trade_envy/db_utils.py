import psycopg2
import pandas as pd
from psycopg2.extras import execute_values


def get_pkeys(table_name: str, conn) -> list:
    """
    Retrieves the primary key columns for a given table in PostgreSQL.
    """
    pk_query = f"""
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_name = '{table_name}';
    """
    with conn.cursor() as cursor:
        cursor.execute(pk_query)
        primary_keys = cursor.fetchall()
    return [pk[0] for pk in primary_keys]


def upsert_data(table_name: str, df: pd.DataFrame, conn):
    """
    Upserts data into a PostgreSQL table.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';"
        )
        columns = [row[0] for row in cursor.fetchall()]

    df = df[columns]
    primary_keys = get_pkeys(table_name, conn)

    column_list = ", ".join(columns)
    value_placeholders = ", ".join(["%s"] * len(columns))

    update_clause = ", ".join(
        [
            f"{column} = EXCLUDED.{column}"
            for column in columns
            if column not in primary_keys
        ]
    )
    conflict_clause = (
        f"ON CONFLICT ({', '.join(primary_keys)}) DO UPDATE SET {update_clause}"
    )

    upsert_query = f"""
    INSERT INTO {table_name} ({column_list})
    VALUES {value_placeholders}
    {conflict_clause}
    """

    with conn.cursor() as cursor:
        execute_values(
            cursor,
            f"""
            INSERT INTO {table_name} ({column_list})
            VALUES %s
            {conflict_clause}
            """,
            df.values.tolist(),
        )
    conn.commit()
