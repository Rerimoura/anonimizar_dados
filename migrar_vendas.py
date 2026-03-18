"""
================================================================
  MIGRAÇÃO: tabela VENDAS
================================================================
  Uso: python migrar_vendas.py
  Requer: base_migracao.py na mesma pasta
================================================================
"""

import pandas as pd
from base_migracao import executar_migracao, processar_vendas

# Sem anonimizações de coluna individual — tudo é tratado
# pelo pos_processamento (processar_vendas)
MAPA_EXATO = {}

COLUNAS_MANTER = [
    "tipo",          # mantido original
    "vendedor",      # mantido original — FK para vendedores
    "data_emissao",  # será aleatorizado dentro do período do arquivo
    "cliente",       # mantido original — FK para clientes
    "data_pedido",   # data_emissao anonimizada - 1 a 5 dias
    "mercadoria",    # mantido original — FK para mercadorias
    "quant",         # mantido original
    "valor_liq",     # será escalado pelo fator financeiro
    "valor_5910",    # será escalado pelo fator financeiro
]

# ── Filtro de período ─────────────────────────────────────────
DATA_INICIO = "2023-01-01"   # ← altere aqui se precisar mudar o corte

def filtrar_e_processar(df: pd.DataFrame) -> pd.DataFrame:
    # 1. Filtra registros a partir de DATA_INICIO
    total_antes = len(df)
    df["data_emissao"] = pd.to_datetime(df["data_emissao"], errors="coerce")
    df = df[df["data_emissao"] >= DATA_INICIO].copy()
    print(f"   📅 Filtro de período: {total_antes:,} → {len(df):,} registros (a partir de {DATA_INICIO})")

    # 2. Aplica anonimização de datas e valores
    df = processar_vendas(df)
    return df

if __name__ == "__main__":
    executar_migracao("vendas", MAPA_EXATO, COLUNAS_MANTER, pos_processamento=filtrar_e_processar)