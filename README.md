# 🔒 Pipeline de Anonimização Cloud

> Migração de banco de dados PostgreSQL local para Neon Cloud com anonimização automática de dados sensíveis, integridade referencial preservada e sincronização incremental agendada.

<div align="center">

![Python](https://img.shields.io/badge/Python-3.13-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Neon](https://img.shields.io/badge/Neon-Cloud-00E599?style=for-the-badge&logo=neon&logoColor=black)
![pandas](https://img.shields.io/badge/pandas-2.x-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Faker](https://img.shields.io/badge/Faker-pt__BR-9B59B6?style=for-the-badge)

</div>

---

## 📋 Sobre o projeto

Dados comerciais reais — clientes, vendas, fornecedores — não podem ser expostos publicamente em portfólios sem risco de violação de privacidade e LGPD. Este pipeline resolve esse problema de forma robusta:

- **Lê** as tabelas do banco PostgreSQL local
- **Detecta automaticamente** colunas sensíveis por nome
- **Anonimiza** cada dado de forma determinística (mesmo valor original → sempre mesmo valor fictício)
- **Migra** para Neon Cloud mantendo integridade referencial completa
- **Sincroniza** incrementalmente via Task Scheduler, enviando apenas registros novos

---

## 🏗️ Arquitetura

```
┌─────────────────────┐       ┌──────────────────────┐       ┌─────────────────────┐
│   PostgreSQL Local  │──────▶│   Pipeline Python     │──────▶│     Neon Cloud      │
│   (dados reais)     │       │   (anonimização)      │       │  (dados fictícios)  │
└─────────────────────┘       └──────────────────────┘       └─────────────────────┘
                                         │
                          ┌──────────────┴──────────────┐
                          │  Task Scheduler (diário)    │
                          │  sync_vendas.py → log.txt   │
                          └─────────────────────────────┘
```

---

## 📁 Estrutura do repositório

```
pipeline-anonimizacao-cloud/
│
├── base_migracao.py          # Módulo central — anonimização, conexão, inserção
│
├── migrar_clientes.py        # Migração da tabela clientes
├── migrar_vendedores.py      # Migração da tabela vendedores
├── migrar_mercadorias.py     # Migração da tabela mercadorias
├── migrar_vendas.py          # Migração da tabela vendas (a partir de 2023)
│
├── sync_vendas.py            # Sincronização incremental diária
├── executar_sync.bat         # Agendador Windows Task Scheduler
│
└── .streamlit/
    └── secrets.toml          # Credenciais (NÃO versionar — ver .gitignore)
```

---

## ⚙️ Configuração

### 1. Instale as dependências

```bash
pip install psycopg2-binary pandas faker toml
```

### 2. Configure as credenciais

Crie o arquivo `.streamlit/secrets.toml` com a seguinte estrutura:

```toml
[postgres_local]
host     = "seu-host-local"
database = "seu-banco"
user     = "seu-usuario"
password = "sua-senha"
port     = 5432

[postgres]
host     = "ep-xxxx.us-east-1.aws.neon.tech"
database = "neondb"
user     = "neondb_owner"
password = "sua-senha-neon"
port     = 5432
```

> ⚠️ **Nunca versione o `secrets.toml`**. Adicione-o ao `.gitignore`.

### 3. Execute a migração inicial

```bash
# Migre cada tabela individualmente — na ordem recomendada:
python migrar_clientes.py
python migrar_vendedores.py
python migrar_mercadorias.py
python migrar_vendas.py
```

### 4. Configure o sync incremental

Edite os caminhos em `executar_sync.bat` e registre no Windows Task Scheduler:

```
Ação: Iniciar programa
Programa: C:\caminho\executar_sync.bat
Frequência: Diária
Horário: 06:00 (ou o horário de sua preferência)
Marcar: "Executar independente de o usuário estar logado"
```

---

## 🔒 Estratégia de anonimização

A anonimização é **determinística** via hash MD5: o mesmo valor original sempre gera o mesmo valor fictício em todas as execuções, preservando os relacionamentos entre tabelas.

| Tipo de dado | Coluna(s) | Exemplo original | Exemplo anonimizado |
|---|---|---|---|
| CNPJ | `cnpj` | `12.345.678/0001-90` | `58.291.047.3291-74`* |
| Razão social | `raz_social`, `fantasia` | `SUPERMERCADO BIZ LTDA` | `Oliveira & Santos Comércio Ltda` |
| Cidade | `cidade` | `Uberlândia` | `Cidade Gama` |
| UF | `uf` | `MG` | `CE` |
| E-mail | `email`, `e_mail` | `joao@empresa.com` | `carlos.silva@example.org` |
| Data | `data_cadastro` | `2019-03-15` | `2024-05-18` |
| Financeiro | `valor_liq`, `desconto` | `R$ 1.500,00` | `R$ 892,50` (~60%) |
| Produto | `descricao` | `NESCAU POTE 200G` | `Achocolatado Premium 200g` |
| Fornecedor | `nome_fornecedor` | `MONDELEZ BRASIL LTDA` | `Fornecedor da Mata & Cia Ltda` |
| Divisão | `nome_divisao` | `MONDELEZ` | `Divisão A` |
| Nota fiscal | `nota` | `10234567` | `9847291` |
| Vendedor | `nome`, `nomesup` | `João Silva` | `Carlos Pereira` |

*CNPJ gerado intencionalmente inválido.

**Colunas preservadas intactas** (chaves de relacionamento): `cliente`, `vendedor`, `mercadoria`, `fornecedor`, `filial`, `divisao`, `setor`, `rede` e demais códigos numéricos internos.

---

## 🔧 Como funciona por dentro

### Detecção automática de colunas

O sistema usa três níveis de prioridade:

```python
# 1. Chaves de relacionamento — NUNCA anonimizar
COLUNAS_PRESERVAR = {"vendedor", "cliente", "mercadoria", "fornecedor", ...}

# 2. Match exato no nome da coluna
MAPA_EXATO = {
    "cnpj":          "cnpj",
    "raz_social":    "empresa",
    "valor_liq":     "financeiro",
    "data_cadastro": "data",
    ...
}

# 3. Match parcial como fallback
MAPA_PARCIAL = {"email": "email", "valor": "financeiro", ...}
```

### Hash determinístico

```python
def _hash_seed(valor) -> int:
    h = hashlib.md5(str(valor).encode()).hexdigest()
    return int(h[:8], 16)

# Mesmo CNPJ original → sempre mesmo CNPJ fictício
# Garante que JOINs entre tabelas funcionem no Power BI
```

### Sync incremental

```python
# Busca última data no Neon → traz apenas o que é mais recente
ultima_data = SELECT MAX(data_emissao) FROM vendas  # Neon
novos = SELECT * FROM vendas WHERE data_emissao > ultima_data  # Local
novos_anon = processar_vendas(novos)
INSERT INTO vendas ...  # Apenas os registros novos
```

### Tratamento de tipos PostgreSQL → pandas

A função `_coerce()` normaliza automaticamente as incompatibilidades:

| Tipo recebido | Exemplo | Convertido para |
|---|---|---|
| `float` inteiro | `7397565.0` | `7397565` (int) |
| `string` money BR | `"R$ 2.500,00"` | `2500.0` (float) |
| `Decimal` | `Decimal('1500.00')` | `1500.0` (float) |
| `NaN` / `None` | — | `None` (NULL) |

---

## 📊 Resultados

| Métrica | Valor |
|---|---|
| Clientes migrados | 26.473 |
| Registros de vendas | 1.250.000+ |
| Tempo de migração inicial | ~8 minutos |
| Tempo de sync incremental | < 30 segundos |
| Tipos de anonimização | 12+ |
| Integridade referencial | 100% |

---

## 🛠️ Personalização por tabela

Cada script de migração tem configuração declarativa independente. Para adicionar ou remover colunas, edite apenas o script da tabela correspondente:

```python
# migrar_clientes.py — exemplo
MAPA_EXATO = {
    "cnpj":          "cnpj",
    "raz_social":    "empresa",
    "cidade":        "cidade",
    # adicione novas colunas aqui
}

COLUNAS_MANTER = [
    "cliente",       # sobe (original)
    "cnpj",          # sobe (anonimizado)
    "raz_social",    # sobe (anonimizado)
    # "email",       # comentado = NÃO sobe
]
```

---

## 📄 Licença

MIT License — sinta-se livre para usar, adaptar e distribuir.

---

<div align="center">

Desenvolvido por **Rerisson Moura** — Coordenador de Dados | BI & Big Data | 8+ anos

[![LinkedIn](https://img.shields.io/badge/LinkedIn-rerimoura-0077B5?style=flat-square&logo=linkedin)](https://linkedin.com/in/rerimoura)
[![Portfólio](https://img.shields.io/badge/Portfólio-rerisson.dev-2563EB?style=flat-square&logo=globe)](https://rerisson.dev)

</div>
