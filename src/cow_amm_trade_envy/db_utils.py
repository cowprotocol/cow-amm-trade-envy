import duckdb
import pandas as pd


def get_pkeys(table_name: str, conn: duckdb.DuckDBPyConnection) -> list:
    pk_query = f"""
    SELECT tc.constraint_name, kcu.column_name 
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_name = '{table_name}';
    """

    primary_keys = conn.execute(pk_query).fetchall()
    primary_keys = [pk[1] for pk in primary_keys]
    return primary_keys


def upsert_data(table_name: str, df: pd.DataFrame, conn: duckdb.DuckDBPyConnection):
    columns = conn.table(table_name).columns
    df = df[columns]
    primary_keys = get_pkeys(table_name, conn)
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
    INSERT INTO {table_name}
    SELECT * FROM df
    {conflict_clause}
    """

    df = df[columns]  # duckdb reads from in-memory DB, dont delete this
    conn.execute(upsert_query)
