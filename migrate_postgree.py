import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import sys
import tomli
from faker_transform import apply_faker

TABLES = [
    # "clientes",
    "mercadorias",
    # "vendedores"
]

try:
    with open("streamlit/secrets.toml", "rb") as f:
        secrets = tomli.load(f)

    NEON_CONFIG = secrets["postgres"]
    LOCAL_DB_CONFIG = secrets["postgres_local"]

except Exception as e:
    print("Erro ao ler secrets.toml:", e)
    sys.exit(1)


def connect_local():

    return psycopg2.connect(**LOCAL_DB_CONFIG)


def connect_neon():

    cfg = NEON_CONFIG.copy()
    cfg["sslmode"] = "require"
    return psycopg2.connect(**cfg)


def migrate_table(local_conn, neon_conn, table):

    print("\n===================================")
    print("Migrando:", table)
    print("===================================")

    query = f"SELECT * FROM {table}"

    df = pd.read_sql(query, local_conn)

    print("linhas lidas:", len(df))

    if df.empty:
        return

    print("Aplicando faker...")

    df = apply_faker(table, df)

    cur = neon_conn.cursor()

    print("Recriando tabela no Neon...")

    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

    columns_sql = []

    for col in df.columns:

        if df[col].dtype == "int64":
            t = "BIGINT"

        elif df[col].dtype == "float64":
            t = "NUMERIC"

        elif "datetime" in str(df[col].dtype):
            t = "TIMESTAMP"

        else:
            t = "TEXT"

        columns_sql.append(f'"{col}" {t}')

    create_sql = f"""
    CREATE TABLE {table} (
        {",".join(columns_sql)}
    )
    """

    cur.execute(create_sql)

    neon_conn.commit()

    print("Inserindo dados...")

    df = df.where(pd.notnull(df), None)

    columns = ",".join([f'"{c}"' for c in df.columns])

    placeholders = ",".join(["%s"] * len(df.columns))

    insert_sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

    data = [tuple(x) for x in df.values]

    execute_batch(cur, insert_sql, data, page_size=1000)

    neon_conn.commit()

    cur.close()

    print("Tabela migrada!")


def main():

    print("\nConectando banco local...")

    local_conn = connect_local()

    print("Conectando Neon...")

    neon_conn = connect_neon()

    for t in TABLES:

        migrate_table(local_conn, neon_conn, t)

    local_conn.close()
    neon_conn.close()

    print("\nMigração concluída")


if __name__ == "__main__":

    main()