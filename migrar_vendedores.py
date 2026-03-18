"""
================================================================
  MIGRAÇÃO: tabela VENDEDORES
================================================================
  Uso: python migrar_vendedores.py
  Requer: base_migracao.py na mesma pasta
================================================================
"""

from base_migracao import executar_migracao

MAPA_EXATO = {
    "nome":             "pessoa",   # varchar(60)  → nome fictício
    "nomesup":          "pessoa",   # varchar(60)  → nome fictício
    "data_admissao":    "data",     # date         → data aleatória
    "data_desligamento":"data",     # date         → data aleatória
    # supervisor: código numérico — preservado como está
}

COLUNAS_MANTER = [
    "vendedor",          # chave primária — mantido original
    "nome",              # será anonimizado
    "supervisor",        # código numérico — mantido original
    "nomesup",           # será anonimizado
    "data_admissao",     # será anonimizado
    "data_desligamento", # será anonimizado
]

if __name__ == "__main__":
    executar_migracao("vendedores", MAPA_EXATO, COLUNAS_MANTER)