"""
================================================================
  MIGRAÇÃO: tabela CLIENTES
================================================================
  Uso: python migrar_clientes.py
  Requer: base_migracao.py na mesma pasta
================================================================
"""

from base_migracao import executar_migracao

# ── Colunas sensíveis e como anonimizá-las ───────────────────
MAPA_EXATO = {
    "cnpj":           "cnpj",
    "raz_social":     "empresa",
    "fantasia":       "empresa",
    "endereco":       "endereco",
    "bairro":         "bairro",
    "cidade":         "cidade",      # nome da cidade → cidade fictícia
    "uf":             "uf",          # ex: "MG" → UF aleatória do Brasil
    "atividade":      "atividade",   # ex: "Supermercado" → atividade fictícia
    "email":          "email",
    "email_xml":      "email",
    "telefone":       "telefone",
    "tele_comp":      "telefone",
    "contato":        "pessoa",
    "cep":            "cep",
    "limite_aberto":  "financeiro",
    "limite_trading": "financeiro",
}

# ── Escolha as colunas que serão criadas no Neon ─────────────
# Comente as que NÃO quer subir, descomente as que quer subir.
# Para subir todas sem filtro, use: COLUNAS_MANTER = None
COLUNAS_MANTER = [
    "cliente",          # chave primária — manter sempre
    "cnpj",             # será anonimizado
    "raz_social",       # será anonimizado
    # "fantasia",         # será anonimizado
    # "endereco",       # descomente se quiser incluir
    # "bairro",         # descomente se quiser incluir
    "cidade",           # será anonimizado
    "uf",               # será anonimizado
    "atividade",        # será anonimizado
    # "email",          # descomente se quiser incluir
    # "email_xml",      # descomente se quiser incluir
    "limite_aberto",    # será anonimizado
    # "limite_trading", # descomente se quiser incluir
    "situacao",
    "classificacao",
    # "telefone",       # descomente se quiser incluir
    # "tele_comp",      # descomente se quiser incluir
    "antecipado",
    "rede",
    # "codatividade",
    # "codcidade",
    "data_cadastro",        # será anonimizado
    # "longitude",      # descomente se quiser incluir
    # "latitude",       # descomente se quiser incluir
    # "contato",        # descomente se quiser incluir
    # "cep",            # descomente se quiser incluir
    # "inscricao",      # descomente se quiser incluir
]

if __name__ == "__main__":
    executar_migracao("clientes", MAPA_EXATO, COLUNAS_MANTER)