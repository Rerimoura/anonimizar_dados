"""
================================================================
  MIGRAÇÃO: tabela MERCADORIAS
================================================================
  Uso: python migrar_mercadorias.py
  Requer: base_migracao.py na mesma pasta
================================================================
"""

from base_migracao import executar_migracao

MAPA_EXATO = {
    "descricao":      "descricao_produto",
    "nome_fornecedor":"nome_fornecedor",
    "grupo":          "grupo",
    "sub_grupo":      "grupo",
    "nome_divisao":   "divisao_az",
}

COLUNAS_MANTER = [
    "mercadoria",       # chave primária — mantido original
    "descricao",        # será anonimizado
    "fornecedor",       # código numérico — mantido original
    "nome_fornecedor",  # será anonimizado (mantém relação por fornecedor)
    "grupo",            # será anonimizado
    "sub_grupo",        # será anonimizado
    "divisao",          # código numérico — mantido original
    "nome_divisao",     # será anonimizado (Divisão A-Z)
    "qtde_und",         # mantido original
    "qtde",             # mantido original
    "peso_unitario",    # mantido original
]

if __name__ == "__main__":
    executar_migracao("mercadorias", MAPA_EXATO, COLUNAS_MANTER)