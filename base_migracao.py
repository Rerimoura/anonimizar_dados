"""
================================================================
  BASE_MIGRACAO.PY — Módulo compartilhado
================================================================
  Contém todas as funções de conexão, anonimização e inserção.
  Importado pelos 4 scripts individuais de migração.
  Não execute este arquivo diretamente.
================================================================
"""

import hashlib
import random
import psycopg2
import psycopg2.extras
import pandas as pd
from faker import Faker
from pathlib import Path
from datetime import datetime

try:
    import toml
except ImportError:
    raise ImportError("Execute: pip install toml")

fake = Faker("pt_BR")

SCHEMA = "public"
FATOR_FINANCEIRO = round(random.uniform(0.55, 0.80), 4)

# ── Caches de consistência entre tabelas ─────────────────────
_cache_cnpj    = {}
_cache_empresa = {}
_cache_pessoa  = {}
_cache_cidade  = {}
_cache_email   = {}

CIDADES_FICTICIAS = [
    "Cidade Alfa", "Cidade Beta", "Cidade Gama", "Cidade Delta",
    "Cidade Épsilon", "Cidade Zeta", "Cidade Eta", "Cidade Teta",
    "Cidade Iota", "Cidade Kappa", "Cidade Lambda", "Cidade Mu",
    "Cidade Nu", "Cidade Xi", "Cidade Ômicron", "Cidade Pi",
]

# ── Colunas que NUNCA são anonimizadas (chaves e flags) ──────
COLUNAS_PRESERVAR = {
    "vendedor", "supervisor", "cliente", "mercadoria",
    "fornecedor", "filial", "carga", "setor", "rede",
    "divisao", "codatividade", "codcidade",
    "situacao", "classificacao", "tipo", "tm", "antecipado",
    "logradouro",
}

# ── Mapa parcial (fallback por padrão no nome da coluna) ─────
MAPA_PARCIAL = {
    "email":    "email",
    "endereco": "endereco",
    "bairro":   "bairro",
    "valor":    "financeiro",
    "desconto": "financeiro",
    "limite":   "financeiro",
}


# ================================================================
#  CREDENCIAIS
# ================================================================

def carregar_secrets(caminho: str = None) -> dict:
    candidatos = []
    if caminho:
        candidatos.append(Path(caminho))
    candidatos += [
        Path("streamlit") / "secrets.toml",
        Path.home() / "streamlit" / "secrets.toml",
    ]
    for path in candidatos:
        if path.exists():
            print(f"   📄 secrets.toml: {path.resolve()}")
            return toml.load(path)
    caminhos_str = "\n      ".join(str(p.resolve()) for p in candidatos)
    raise FileNotFoundError(
        f"secrets.toml não encontrado. Verificados:\n      {caminhos_str}"
    )


def montar_config_local(secrets: dict) -> dict:
    cfg = secrets["postgres_local"]
    return {
        "host":     cfg["host"],
        "port":     int(cfg.get("port", 5432)),
        "dbname":   cfg["database"],
        "user":     cfg["user"],
        "password": cfg["password"],
    }


def montar_dsn_neon(secrets: dict) -> str:
    cfg = secrets["postgres"]
    return (
        f"postgresql://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg.get('port', 5432)}"
        f"/{cfg['database']}?sslmode=require"
    )


# ================================================================
#  ANONIMIZAÇÃO
# ================================================================

def _hash_seed(valor) -> int:
    h = hashlib.md5(str(valor).encode()).hexdigest()
    return int(h[:8], 16)


def gerar_cnpj_ficticio(original, apenas_digitos: bool = False) -> str:
    """
    apenas_digitos=True  -> 14 dígitos sem formatacao (para colunas numeric)
    apenas_digitos=False -> formatado XX.XXX.XXX/XXXX-XX (para varchar)
    """
    chave = f"{original}_d" if apenas_digitos else str(original)
    if chave not in _cache_cnpj:
        seed = _hash_seed(original)
        d = str(seed % 10**14).zfill(14)
        _cache_cnpj[chave] = d if apenas_digitos else f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"
    return _cache_cnpj[chave]


def gerar_empresa_ficticia(original) -> str:
    if original not in _cache_empresa:
        seed = _hash_seed(original)
        fake.seed_instance(seed)
        sufixos = ["Comércio Ltda", "Supermercados Ltda", "Atacado e Varejo Ltda",
                   "Distribuidora Ltda", "Mercado Ltda", "Alimentos Ltda"]
        _cache_empresa[original] = (
            f"{fake.last_name()} & {fake.last_name()} "
            f"{fake.random_element(sufixos)}"
        )
    return _cache_empresa[original]


def gerar_pessoa_ficticia(original) -> str:
    if original not in _cache_pessoa:
        seed = _hash_seed(original)
        fake.seed_instance(seed)
        _cache_pessoa[original] = fake.name()
    return _cache_pessoa[original]


def gerar_cidade_ficticia(original) -> str:
    if original not in _cache_cidade:
        seed = _hash_seed(original)
        _cache_cidade[original] = CIDADES_FICTICIAS[seed % len(CIDADES_FICTICIAS)]
    return _cache_cidade[original]


def gerar_email_ficticio(original) -> str:
    if original not in _cache_email:
        seed = _hash_seed(original)
        fake.seed_instance(seed)
        _cache_email[original] = fake.email()
    return _cache_email[original]


UFS_BRASIL = [
    "AC","AL","AM","AP","BA","CE","DF","ES","GO","MA",
    "MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN",
    "RO","RR","RS","SC","SE","SP","TO",
]

ATIVIDADES_FICTICIAS = [
    "Supermercado", "Atacado", "Mercearia", "Hipermercado",
    "Conveniência", "Distribuidora", "Açougue", "Padaria",
    "Empório", "Mini Mercado",
]

# Divisões fictícias de A a Z
DIVISOES_FICTICIAS = [
    "Divisão A", "Divisão B", "Divisão C", "Divisão D",
    "Divisão E", "Divisão F", "Divisão G", "Divisão H",
    "Divisão I", "Divisão J", "Divisão K", "Divisão L",
    "Divisão M", "Divisão N", "Divisão O", "Divisão P",
    "Divisão Q", "Divisão R", "Divisão S", "Divisão T",
    "Divisão U", "Divisão V", "Divisão W", "Divisão X",
    "Divisão Y", "Divisão Z",
]

# Categorias de produto para descrição coerente
_CATEGORIAS_PROD = [
    "Biscoito", "Chocolate", "Suco", "Detergente", "Shampoo",
    "Sabonete", "Café", "Macarrão", "Arroz", "Feijão",
    "Óleo", "Vinagre", "Molho", "Achocolatado", "Leite",
    "Manteiga", "Margarina", "Iogurte", "Refrigerante", "Água",
]
_ADJETIVOS_PROD = [
    "Original", "Premium", "Clássico", "Tradicional", "Especial",
    "Light", "Zero", "Integral", "Natural", "Suave",
]
_EMBALAGENS_PROD = [
    "200g", "500g", "1kg", "250ml", "500ml", "1L",
    "100g", "300g", "750ml", "2L", "50g", "180g",
]

_cache_fornecedor_nome = {}
_cache_grupo           = {}
_cache_subgrupo        = {}
_cache_descricao_prod  = {}


def gerar_uf_ficticia(original) -> str:
    """UF aleatória mas determinística (mesmo valor original → mesma UF fictícia)."""
    from base_migracao import _hash_seed
    seed = _hash_seed(original)
    return UFS_BRASIL[seed % len(UFS_BRASIL)]


def gerar_atividade_ficticia(original) -> str:
    """Atividade aleatória mas determinística."""
    from base_migracao import _hash_seed
    seed = _hash_seed(original)
    return ATIVIDADES_FICTICIAS[seed % len(ATIVIDADES_FICTICIAS)]


def gerar_nome_fornecedor_ficticio(original) -> str:
    """Mesmo fornecedor original → mesmo nome fictício (mantém relação)."""
    if original not in _cache_fornecedor_nome:
        seed = _hash_seed(original)
        fake.seed_instance(seed)
        _cache_fornecedor_nome[original] = f"Fornecedor {fake.last_name()} & Cia Ltda"
    return _cache_fornecedor_nome[original]


def gerar_grupo_ficticio(original) -> str:
    """Grupo fictício determinístico."""
    if original not in _cache_grupo:
        grupos = [
            "Alimentos", "Bebidas", "Higiene Pessoal", "Limpeza",
            "Cuidados com o Lar", "Beleza", "Infantil", "Pet",
            "Utilidades", "Saúde",
        ]
        seed = _hash_seed(original)
        _cache_grupo[original] = grupos[seed % len(grupos)]
    return _cache_grupo[original]


def gerar_subgrupo_ficticio(original) -> str:
    """Sub-grupo fictício determinístico."""
    if original not in _cache_subgrupo:
        subgrupos = [
            "Biscoitos", "Chocolates", "Laticínios", "Frios",
            "Mercearia", "Higiene", "Limpeza Geral", "Cosméticos",
            "Bebidas Quentes", "Bebidas Frias", "Snacks", "Cereais",
        ]
        seed = _hash_seed(original)
        _cache_subgrupo[original] = subgrupos[seed % len(subgrupos)]
    return _cache_subgrupo[original]


def gerar_descricao_produto(original) -> str:
    """Descrição de produto coerente e determinística."""
    if original not in _cache_descricao_prod:
        seed = _hash_seed(original)
        cat  = _CATEGORIAS_PROD[seed % len(_CATEGORIAS_PROD)]
        adj  = _ADJETIVOS_PROD[(seed // 7) % len(_ADJETIVOS_PROD)]
        emb  = _EMBALAGENS_PROD[(seed // 13) % len(_EMBALAGENS_PROD)]
        _cache_descricao_prod[original] = f"{cat} {adj} {emb}"
    return _cache_descricao_prod[original]


def detectar_tipo_coluna(nome_coluna: str, mapa_exato: dict) -> str | None:
    """
    Prioridade: COLUNAS_PRESERVAR > mapa_exato (da tabela) > MAPA_PARCIAL
    """
    col = nome_coluna.lower()
    if col in COLUNAS_PRESERVAR:
        return None
    if col in mapa_exato:
        return mapa_exato[col]
    for padrao, tipo in MAPA_PARCIAL.items():
        if padrao in col:
            return tipo
    return None


def anonimizar_valor(valor, tipo: str):
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return valor
    v = str(valor).strip()
    if not v:
        return valor

    if tipo == "cnpj":
        # Banco armazena cnpj como numeric(14) - salva apenas os 14 digitos
        return int(gerar_cnpj_ficticio(v, apenas_digitos=True))
    elif tipo == "empresa":
        return gerar_empresa_ficticia(v)
    elif tipo == "pessoa":
        return gerar_pessoa_ficticia(v)
    elif tipo == "cidade":
        return gerar_cidade_ficticia(v)

    elif tipo == "uf":
        seed = _hash_seed(v)
        return UFS_BRASIL[seed % len(UFS_BRASIL)]

    elif tipo == "atividade":
        seed = _hash_seed(v)
        return ATIVIDADES_FICTICIAS[seed % len(ATIVIDADES_FICTICIAS)]
    elif tipo == "email":
        return gerar_email_ficticio(v)
    elif tipo == "endereco":
        seed = _hash_seed(v)
        fake.seed_instance(seed)
        return f"Rua {fake.last_name()}, {seed % 1500 + 1}"
    elif tipo == "bairro":
        bairros = ["Centro", "Jardim Norte", "Vila Sul", "Parque Leste",
                   "Residencial Oeste", "Alto da Serra", "Novo Horizonte"]
        return bairros[_hash_seed(v) % len(bairros)]
    elif tipo == "telefone":
        seed = _hash_seed(v)
        return str(60000000000 + (seed % 9999999999)).zfill(11)
    elif tipo == "cep":
        seed = _hash_seed(v)
        return str(10000000 + (seed % 89999999)).zfill(8)
    elif tipo == "numero_end":
        return _hash_seed(v) % 9000 + 1
    elif tipo == "complemento":
        ops = ["Apto 1", "Sala 2", "Loja 3", "Casa", ""]
        return ops[_hash_seed(v) % len(ops)]
    elif tipo == "financeiro":
        try:
            num = float(str(valor).replace(",", "."))
            ruido = random.uniform(0.92, 1.08)
            return round(num * FATOR_FINANCEIRO * ruido, 2)
        except (ValueError, TypeError):
            return valor
    elif tipo == "data":
        import datetime, random as _rnd
        seed = _hash_seed(v)
        _rnd.seed(seed)
        inicio = datetime.date(2018, 1, 1)
        fim    = datetime.date(2024, 12, 31)
        delta  = (fim - inicio).days
        return inicio + datetime.timedelta(days=_rnd.randint(0, delta))

    elif tipo == "nota_fiscal":
        return str(9_000_000 + (_hash_seed(v) % 999_999))
    elif tipo == "pedido":
        return str(5_000_000 + (_hash_seed(v) % 4_999_999))
    elif tipo == "nome_fornecedor":
        return gerar_nome_fornecedor_ficticio(v)
    elif tipo == "grupo":
        return gerar_grupo_ficticio(v)
    elif tipo == "subgrupo":
        return gerar_subgrupo_ficticio(v)
    elif tipo == "descricao_produto":
        return gerar_descricao_produto(v)
    elif tipo == "divisao_az":
        seed = _hash_seed(v)
        return DIVISOES_FICTICIAS[seed % len(DIVISOES_FICTICIAS)]
    return valor


# ================================================================
#  BANCO DE DADOS
# ================================================================

def conectar(config: dict, label: str):
    try:
        conn = (
            psycopg2.connect(config["dsn"])
            if "dsn" in config
            else psycopg2.connect(**config)
        )
        print(f"   ✅ Conectado: {label}")
        return conn
    except Exception as e:
        print(f"   ❌ Erro ao conectar em {label}: {e}")
        raise


def obter_ddl(conn, schema: str, tabela: str, colunas_manter: list = None) -> str:
    """Gera o CREATE TABLE apenas com as colunas desejadas."""
    filtro_col = ""
    params = [tabela, schema, tabela]
    if colunas_manter:
        placeholders = ", ".join(["%s"] * len(colunas_manter))
        filtro_col = f"AND column_name IN ({placeholders})"
        params += colunas_manter

    query = f"""
        SELECT
            'CREATE TABLE IF NOT EXISTS ' || quote_ident(%s) || ' (' ||
            string_agg(
                quote_ident(column_name) || ' ' || data_type ||
                CASE WHEN character_maximum_length IS NOT NULL
                     THEN '(' || character_maximum_length || ')'
                     ELSE '' END,
                ', ' ORDER BY ordinal_position
            ) || ');'
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        {filtro_col}
    """
    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        return row[0] if row else None


def ler_tabela(conn, schema: str, tabela: str) -> pd.DataFrame:
    return pd.read_sql(f'SELECT * FROM {schema}."{tabela}"', conn)


def recriar_no_neon(conn_neon, tabela: str, ddl: str):
    with conn_neon.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{tabela}" CASCADE')
        cur.execute(ddl)
    conn_neon.commit()


def _coerce(v):
    """
    Normaliza valores para inserção no PostgreSQL:
      - None / NaN           -> None
      - float inteiro        -> int  (ex: 7397565.0 -> 7397565)
      - string money BR      -> float (ex: "R$ 2.500,00" -> 2500.0)
      - Decimal              -> float
    """
    if v is None:
        return None

    # Decimal (vem de colunas numeric/money via psycopg2)
    try:
        from decimal import Decimal
        if isinstance(v, Decimal):
            return float(v)
    except Exception:
        pass

    if isinstance(v, float):
        if pd.isna(v):
            return None
        if v == int(v):
            return int(v)
        return v

    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # Formato money brasileiro: "R$ 2.500,00" ou "2.500,00" ou "-1.234,56"
        cleaned = s.replace('R$', '').replace(' ', '').replace('.', '').replace(',', '.')
        try:
            num = float(cleaned)
            return int(num) if num == int(num) else num
        except ValueError:
            pass

    return v


def inserir_no_neon(conn_neon, tabela: str, df: pd.DataFrame):
    if df.empty:
        return
    cols   = list(df.columns)
    cols_q = ", ".join('"' + c + '"' for c in cols)
    ph     = ", ".join(["%s"] * len(cols))
    sql    = 'INSERT INTO "' + tabela + '" (' + cols_q + ') VALUES (' + ph + ')'
    rows   = [
        tuple(_coerce(v) for v in row)
        for row in df.itertuples(index=False, name=None)
    ]
    with conn_neon.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
    conn_neon.commit()


# ================================================================
#  PÓS-PROCESSAMENTO ESPECÍFICO POR TABELA
# ================================================================

def processar_vendas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lógicas especiais para a tabela vendas:
      - data_emissao : data aleatória dentro do período real do arquivo
      - data_pedido  : data_emissao anonimizada - random(1..5) dias
      - valor_liq    : aplica fator de escala
      - valor_5910   : aplica fator de escala
    """
    import random
    from datetime import timedelta

    # ── data_emissao ────────────────────────────────────────────
    # Calcula o range real do arquivo
    datas = pd.to_datetime(df["data_emissao"], errors="coerce").dropna()
    data_min = datas.min().date()
    data_max = datas.max().date()
    delta_total = (data_max - data_min).days

    if delta_total <= 0:
        delta_total = 1

    def aleatorizar_data_emissao(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return v
        seed = _hash_seed(str(v))
        return data_min + timedelta(days=(seed % delta_total))

    df["data_emissao"] = df["data_emissao"].apply(aleatorizar_data_emissao)

    # ── data_pedido = data_emissao - random(1..5) dias ──────────
    def calcular_data_pedido(row):
        try:
            emissao = row["data_emissao"]
            if emissao is None or (isinstance(emissao, float) and pd.isna(emissao)):
                return emissao
            seed  = _hash_seed(str(row.name))   # usa o índice como seed
            dias  = 1 + (seed % 5)              # 1 a 5 dias
            return emissao - timedelta(days=dias)
        except Exception:
            return row["data_pedido"]

    df["data_pedido"] = df.apply(calcular_data_pedido, axis=1)

    # ── valores financeiros ──────────────────────────────────────
    for col in ["valor_liq", "valor_5910"]:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: anonimizar_valor(v, "financeiro") if v is not None else v
            )

    return df


# ================================================================
#  PIPELINE GENÉRICO (chamado por cada script individual)
# ================================================================

def executar_migracao(nome_tabela: str, mapa_exato: dict, colunas_manter: list = None, pos_processamento=None):
    """
    colunas_manter    : lista de colunas a subir para o Neon. None = todas.
    pos_processamento : função opcional chamada após anonimização padrão.
                        Recebe o DataFrame e deve retorná-lo modificado.
    """
    inicio = datetime.now()
    largura = 55
    print("\n" + "=" * largura)
    print(f"  MIGRAÇÃO: {nome_tabela.upper()}")
    print("=" * largura)
    print(f"  Fator financeiro: {FATOR_FINANCEIRO:.0%}\n")

    # Credenciais
    print("🔌 Conectando...")
    secrets   = carregar_secrets()
    local_cfg = montar_config_local(secrets)
    neon_dsn  = montar_dsn_neon(secrets)

    conn_local = conectar(local_cfg,          "PostgreSQL Local")
    conn_neon  = conectar({"dsn": neon_dsn},  "Neon Cloud")

    # Leitura
    print(f"\n📥 Lendo tabela '{nome_tabela}'...")
    df = ler_tabela(conn_local, SCHEMA, nome_tabela)
    print(f"   → {len(df):,} registros lidos, {len(df.columns)} colunas no total")

    if df.empty:
        print("   ⚠️  Tabela vazia. Nada a migrar.")
        conn_local.close()
        conn_neon.close()
        return

    # Filtro de colunas
    if colunas_manter:
        # Valida se todas as colunas existem
        inexistentes = [c for c in colunas_manter if c not in df.columns]
        if inexistentes:
            raise ValueError(
                f"Colunas não encontradas em '{nome_tabela}': {inexistentes}\n"
                f"Colunas disponíveis: {list(df.columns)}"
            )
        df = df[colunas_manter]
        print(f"   → {len(df.columns)} colunas selecionadas: {colunas_manter}")
    else:
        print(f"   → Todas as {len(df.columns)} colunas serão migradas")

    # Anonimização
    print("\n🔒 Anonimizando colunas sensíveis...")
    anon_log = []
    for col in df.columns:
        tipo = detectar_tipo_coluna(col, mapa_exato)
        if tipo:
            df[col] = df[col].apply(lambda v, t=tipo: anonimizar_valor(v, t))
            anon_log.append(f"{col} ({tipo})")

    if anon_log:
        for entry in anon_log:
            print(f"   🔒 {entry}")
    else:
        print("   ℹ️  Nenhuma coluna sensível detectada")

    # Pós-processamento específico da tabela (se fornecido)
    if pos_processamento:
        print("\n⚙️  Aplicando pós-processamento específico...")
        df = pos_processamento(df)

    # Migração para Neon
    print(f"\n☁️  Recriando tabela no Neon...")
    ddl = obter_ddl(conn_local, SCHEMA, nome_tabela, colunas_manter)
    recriar_no_neon(conn_neon, nome_tabela, ddl)

    print(f"📤 Inserindo {len(df):,} registros...")
    inserir_no_neon(conn_neon, nome_tabela, df)

    conn_local.close()
    conn_neon.close()

    duracao = (datetime.now() - inicio).seconds
    print(f"\n{'=' * largura}")
    print(f"  ✅ '{nome_tabela}' migrada com sucesso!")
    print(f"  Registros : {len(df):,}")
    print(f"  Colunas   : {len(df.columns)} migradas, {len(anon_log)} anonimizadas")
    print(f"  Duração   : {duracao}s")
    print(f"{'=' * largura}\n")