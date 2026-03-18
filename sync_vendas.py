"""
================================================================
  SYNC INCREMENTAL: vendas (PostgreSQL Local → Neon)
================================================================
  Uso manual  : python sync_vendas.py
  Agendado    : Task Scheduler → python C:\\caminho\\sync_vendas.py

  Lógica:
    1. Busca a maior data_emissao já no Neon
    2. Lê do banco local apenas registros POSTERIORES a essa data
    3. Aplica anonimização (mesmas regras do migrar_vendas.py)
    4. Insere no Neon apenas os registros novos

  Requer: base_migracao.py na mesma pasta
          pip install psycopg2-binary pandas tomli faker
================================================================
"""

import sys
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime
from pathlib import Path

# Importa anonimização e helpers do módulo base
from base_migracao import (
    carregar_secrets,
    montar_config_local,
    montar_dsn_neon,
    processar_vendas,
    _coerce,
    FATOR_FINANCEIRO,
)

# ================================================================
#  CONFIGURAÇÕES
# ================================================================

# Colunas a sincronizar (deve bater com o migrar_vendas.py)
COLUNAS_MANTER = [
    "tipo",
    "vendedor",
    "data_emissao",
    "cliente",
    "data_pedido",
    "mercadoria",
    "quant",
    "valor_liq",
    "valor_5910",
]

# Coluna de controle incremental
COLUNA_DATA = "data_emissao"

# Data mínima de segurança (nunca sincroniza antes disso)
DATA_MINIMA = "2023-01-01"

# Schema no Neon
SCHEMA = "public"
TABELA = "vendas"

# Tamanho do lote de inserção
BATCH_SIZE = 1000


# ================================================================
#  FUNÇÕES
# ================================================================

def conectar_local(config: dict):
    try:
        conn = psycopg2.connect(**config)
        print("   ✅ Conectado: PostgreSQL Local")
        return conn
    except Exception as e:
        print(f"   ❌ Erro ao conectar ao banco local: {e}")
        sys.exit(1)


def conectar_neon(dsn: str):
    try:
        conn = psycopg2.connect(dsn)
        print("   ✅ Conectado: Neon Cloud")
        return conn
    except Exception as e:
        print(f"   ❌ Erro ao conectar ao Neon: {e}")
        sys.exit(1)


def buscar_ultima_data_neon(conn) -> str:
    """Retorna a maior data_emissao já existente no Neon."""
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT MAX("{COLUNA_DATA}") FROM "{TABELA}"')
            resultado = cur.fetchone()[0]
            return str(resultado.date()) if resultado else None
    except Exception as e:
        print(f"   ⚠️  Tabela pode não existir ainda no Neon: {e}")
        return None


def ler_novos_registros(conn, desde: str) -> pd.DataFrame:
    """Lê do banco local apenas registros posteriores à data informada."""
    query = f"""
        SELECT {', '.join(COLUNAS_MANTER)}
        FROM {SCHEMA}."{TABELA}"
        WHERE "{COLUNA_DATA}" > '{desde}'
        ORDER BY "{COLUNA_DATA}"
    """
    return pd.read_sql(query, conn)


def inserir_neon(conn, df: pd.DataFrame):
    """Insere os registros anonimizados no Neon em lotes."""
    cols   = list(df.columns)
    cols_q = ", ".join('"' + c + '"' for c in cols)
    ph     = ", ".join(["%s"] * len(cols))
    sql    = 'INSERT INTO "' + TABELA + '" (' + cols_q + ') VALUES (' + ph + ')'

    rows = [
        tuple(_coerce(v) for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    total   = len(rows)
    batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

    with conn.cursor() as cur:
        for i in range(0, total, BATCH_SIZE):
            lote = rows[i:i + BATCH_SIZE]
            psycopg2.extras.execute_batch(cur, sql, lote)
            batch_num = (i // BATCH_SIZE) + 1
            if batch_num % 10 == 0 or batch_num == batches:
                print(f"   Lote {batch_num}/{batches} inserido...")

    conn.commit()


# ================================================================
#  PIPELINE PRINCIPAL
# ================================================================

def main():
    inicio = datetime.now()
    sep = "=" * 60

    print(f"\n{sep}")
    print("  SYNC INCREMENTAL: vendas → Neon")
    print(sep)
    print(f"  Início : {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Fator financeiro: {FATOR_FINANCEIRO:.0%}\n")

    # 1. Credenciais
    print("🔌 Conectando...")
    secrets   = carregar_secrets()
    local_cfg = montar_config_local(secrets)
    neon_dsn  = montar_dsn_neon(secrets)

    conn_local = conectar_local(local_cfg)
    conn_neon  = conectar_neon(neon_dsn)

    # 2. Descobre a partir de quando sincronizar
    print(f"\n🔍 Verificando última data no Neon...")
    ultima_data = buscar_ultima_data_neon(conn_neon)

    if ultima_data:
        print(f"   📅 Última data no Neon : {ultima_data}")
        desde = ultima_data
    else:
        print(f"   ⚠️  Nenhum registro encontrado. Usando data mínima: {DATA_MINIMA}")
        desde = DATA_MINIMA

    # 3. Lê novos registros do local
    print(f"\n📥 Buscando registros posteriores a {desde}...")
    df = ler_novos_registros(conn_local, desde)
    conn_local.close()

    if df.empty:
        print("   ✅ Nenhum registro novo. Banco Neon já está atualizado.")
        conn_neon.close()
        duracao = (datetime.now() - inicio).seconds
        print(f"\n{sep}")
        print(f"  ✅ Sync concluído sem novidades — {duracao}s")
        print(f"{sep}\n")
        return

    print(f"   → {len(df):,} novos registros encontrados")
    print(f"   → Período: {df[COLUNA_DATA].min()} a {df[COLUNA_DATA].max()}")

    # 4. Anonimização (mesmas regras do migrar_vendas.py)
    print(f"\n🔒 Anonimizando...")
    df = processar_vendas(df)
    print(f"   ✅ Datas e valores anonimizados")

    # 5. Insere no Neon
    print(f"\n📤 Inserindo {len(df):,} registros no Neon...")
    inserir_neon(conn_neon, df)
    conn_neon.close()

    # 6. Resumo
    duracao = (datetime.now() - inicio).seconds
    print(f"\n{sep}")
    print(f"  ✅ SYNC CONCLUÍDO!")
    print(f"  Registros inseridos : {len(df):,}")
    print(f"  Período sincronizado: {df[COLUNA_DATA].min()} a {df[COLUNA_DATA].max()}")
    print(f"  Duração             : {duracao}s")
    print(f"  Fator financeiro    : {FATOR_FINANCEIRO:.0%}")
    print(f"{sep}\n")


if __name__ == "__main__":
    main()