# -*- coding: utf-8 -*-
# ==============================================================================
# 1. IMPORTS E CONFIGURAÇÃO INICIAL
# ==============================================================================
# Bibliotecas padrão do Python
import os # Para interagir com o sistema operacional (caminhos de arquivos, criar diretórios)
import re # Para expressões regulares (não utilizado neste código, mas comum em projetos web)
import math # Para operações matemáticas (não utilizado, mas pode ser útil)
import zipfile # Para criar e manipular arquivos .zip
import uuid # Para gerar identificadores únicos (usado para criar pastas temporárias)
import shutil # Para operações de alto nível em arquivos e diretórios (como remover uma pasta)
from pathlib import Path # Para manipulação de caminhos de arquivo de forma orientada a objetos
from functools import wraps # Ferramenta para criar 'decorators'
import webview
from flask import send_from_directory
from io import StringIO # Mantido no topo conforme solicitado
# Bibliotecas de terceiros (instaladas via pip)
import pandas as pd # A principal biblioteca para manipulação de dados (planilhas)
from flask import (Flask, render_template, redirect, url_for, session, request,
                   flash, send_file, jsonify) # Componentes essenciais do Flask
from werkzeug.utils import secure_filename # Para garantir que nomes de arquivos enviados sejam seguros
import warnings
import sys, ctypes, pathlib
import csv # Adicionado para usar csv.Sniffer


def _windows_downloads_known_folder():
    # FOLDERID_Downloads = {374DE290-123F-4565-9164-39C4925E467B}
    from uuid import UUID
    guid = UUID("{374DE290-123F-4565-9164-39C4925E467B}")
    pPath = ctypes.c_wchar_p()
    SHGetKnownFolderPath = ctypes.windll.shell32.SHGetKnownFolderPath
    SHGetKnownFolderPath.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_void_p, ctypes.POINTER(ctypes.c_wchar_p)]
    SHGetKnownFolderPath.restype = ctypes.c_uint32
    hr = SHGetKnownFolderPath(ctypes.byref(ctypes.c_byte.from_buffer_copy(guid.bytes_le)), 0, None, ctypes.byref(pPath))
    if hr != 0:
        raise OSError(hr)
    return pPath.value


def get_downloads_dir() -> str:
    if sys.platform.startswith("win"):
        try:
            p = _windows_downloads_known_folder()
            if p:
                return p
        except Exception:
            pass
    return str(Path.home() / "Downloads")

# Salvar sempre em Downloads\Planilhex do usuário atual
# se já existir no seu app, mantenha o seu:
BASE_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)



# ---------------- Sessão Flask ----------------
from flask_session import Session

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', os.urandom(32))
app.config["SESSION_TYPE"] = "filesystem"
app.config["SESSION_PERMANENT"] = False
Session(app)

@app.route('/favicon.ico')
def favicon():
    return send_from_directory('static', 'favicon.ico')
@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    module="openpyxl.styles.stylesheet"
)

# ---- LOGGING VERBOSO ----
import logging, sys
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
app.logger.setLevel(logging.INFO)
logging.getLogger('werkzeug').setLevel(logging.INFO)
# para garantir que prints apareçam
print = lambda *a, **k: (__import__('builtins').print(*a, flush=True, **k))


# ==============================================================================
# 2. FUNÇÕES AUXILIARES
# ==============================================================================
def _fix_headers(df):
    # tenta achar linha de header nas 10 primeiras linhas
    for i in range(min(10, len(df))):
        row = df.iloc[i]
        nonempty = row.dropna().astype(str).str.strip()
        # aceita header com >=1 célula não vazia e não totalmente numérica
        if (nonempty.size >= 1) and not all(nonempty.str.fullmatch(r"\d+")):
            new_cols = nonempty.reindex_like(row).fillna("").tolist()
            new_cols = [c.strip() if c else f"col_{j}" for j, c in enumerate(new_cols)]
            df = df.iloc[i+1:].reset_index(drop=True)
            df.columns = new_cols
            break

    cols = pd.Series(list(df.columns), dtype="object").astype(str)
    cols = cols.str.strip().str.replace(r"\s+", " ", regex=True)
    cols = cols.where(cols != "", other=pd.Series([f"col_{i}" for i in range(len(cols))]))
    df.columns = list(cols)
    return df





# ==============================================================================
# 3. AUTENTICAÇÃO E SESSÃO
# ==============================================================================
def require_login(f):
    """
    Este é um "decorator". Qualquer rota que usar '@require_login'
    só poderá ser acessada por um usuário que já fez login.
    """
    @wraps(f) # Preserva o nome e outras informações da função original
    def decorated_function(*args, **kwargs):
        # Verifica se a chave 'logged_in' não está na sessão (ou é False)
        if not session.get('logged_in'):
            # Envia uma mensagem de aviso para o usuário
            flash("Você precisa estar logado para acessar esta página.", "warning")
            # Redireciona para a página de login
            return redirect(url_for('login'))
        # Se o usuário estiver logado, permite que a função original seja executada
        return f(*args, **kwargs)
    return decorated_function
@app.route('/', methods=['GET', 'POST'])
def login():
    """
    Rota da página de login. Responde a requisições GET (para mostrar a página)
    e POST (para processar o formulário de login).
    """
    # Se o usuário já está logado, redireciona para a home.
    if session.get('logged_in'):
        return redirect(url_for('home'))
    # Se o método for POST, o usuário está tentando fazer login.
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

       
        # ATENÇÃO: Este método de autenticação é INSEGURO e serve apenas para desenvolvimento.
        # Em um ambiente de produção, use um banco de dados e senhas criptografadas (hash).
        if username == 'Planilhex' and password == 'Planilhex':
            # Se as credenciais estiverem corretas, armazena na sessão que o usuário está logado.
            session['logged_in'] = True
            session['username'] = username
            # Retorna uma resposta JSON de sucesso para a requisição 'fetch' do JavaScript.
            # O JavaScript no frontend será responsável por fazer o redirecionamento.
            return jsonify({"success": True, "redirect_url": url_for('home')}), 200
        else:
            # Se as credenciais estiverem erradas, retorna um JSON de erro com status 401 (Não Autorizado).
            return jsonify({"success": False, "message": "Usuário ou senha incorretos"}), 401
           
    # Se o método for GET, simplesmente mostra a página de login.
    return render_template('index.html')
@app.route('/logout')
@require_login # Garante que só um usuário logado pode tentar deslogar.
def logout():
    """Encerra a sessão do usuário, limpando todos os dados da sessão."""
    session.clear()
    flash('Você foi desconectado com sucesso.', 'info')
    return redirect(url_for('login'))
# ==============================================================================
# 4. ROTAS DAS PÁGINAS E FERRAMENTAS
# ==============================================================================
@app.route('/home')
@require_login # Protege a página principal
def home():
    """Serve a página principal do menu de ferramentas."""
    return render_template('home.html')
# --- ROTAS PARA SERVIR AS PÁGINAS DAS FERRAMENTAS ---
# Estas rotas simplesmente carregam e exibem os arquivos HTML de cada ferramenta.
@app.route('/unir.html')
@require_login
def unir_page():
    """Serve a página da ferramenta 'Unir Planilhas'."""
    return render_template('unir.html')
@app.route('/dividir.html')
@require_login
def dividir_page():
    """Serve a página da ferramenta 'Dividir Planilhas'."""
    return render_template('dividir.html')
@app.route('/cruzar.html')
@require_login
def cruzar_page():
    """Serve a página da ferramenta 'Cruzar Dados'."""
    return render_template('cruzar.html')


# NOVA FERRAMENTA: Separar planilha cruzada por valor de coluna
@app.route('/separar_por_coluna.html')
@require_login
def separar_page():
    """Serve a página da ferramenta 'Separar por Coluna'."""
    return render_template('separar_por_coluna.html')
####################################################################################

def _norm_text(s: str) -> str:
    s = "" if s is None else str(s)
    s = re.sub(r"\s+", " ", s).strip().upper()   # trim + caixa alta
    return s

def _digits_only(s: str) -> str:
    s = "" if s is None else str(s)
    return re.sub(r"\D+", "", s)                 # só dígitos (CPF/CNPJ/telefone)
########################################################################################

# se já existir no seu app, mantenha o seu:
BASE_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

# decorator de login já existe no seu projeto
# from yourproject.auth import require_login

# -------------------- Normalização binária --------------------
BIN_MAP = {
    "SIM":"SIM","S":"SIM","TRUE":"SIM","1":"SIM","YES":"SIM","Y":"SIM","CONSTA":"SIM","TEM":"SIM",
    "NÃO":"NÃO","NAO":"NÃO","N":"NÃO","FALSE":"NÃO","0":"NÃO","NO":"NÃO",
    "NÃO CONSTA":"NÃO","NAO CONSTA":"NÃO","NÃO TEM":"NÃO","NAO TEM":"NÃO"
}
def _norm_bin_val(x):
    if x is None: return None
    s = str(x).strip().upper()
    return BIN_MAP.get(s, s)

def _ensure_xlsx(name: str, default="resultado.xlsx"):
    name = (name or "").strip()
    if not name:
        return default
    low = name.lower()
    if not (low.endswith(".xlsx") or low.endswith(".xls") or low.endswith(".csv")):
        return name + ".xlsx"
    return name












import re
from io import BytesIO, StringIO

# --- SUBSTITUA sua ler_planilha inteira por esta versão ---
import re
from io import BytesIO, StringIO

def ler_planilha(arquivo_recebido):
    """
    Leitura robusta e previsível (.csv/.tsv/.txt/.xlsx/.xlsm/.xls).
    Estratégia:
      - Sempre lê SEM header e promove depois com _fix_headers.
      - CSV: usa Sniffer; fallback heurístico; não “normaliza” linhas.
      - Nunca retorna (0,0) por erro silencioso: se não houver dados, lança erro.
    """
    import pandas as pd, chardet, csv as _csv
    from pathlib import Path

    def _fix_headers(df: pd.DataFrame) -> pd.DataFrame:
        # promove a primeira linha “não totalmente numérica” entre as 10 primeiras
        promoted = False
        for i in range(min(10, len(df))):
            row = df.iloc[i].astype(str).str.strip()
            if (row != "").any() and not all(row.str.fullmatch(r"\d+")):
                cols = [c if c else f"col_{j}" for j, c in enumerate(row)]
                df = df.iloc[i+1:].reset_index(drop=True)
                df.columns = [str(c).strip() for c in cols]
                promoted = True
                break
        if not promoted:
            df.columns = [f"col_{i}" for i in range(df.shape[1])]
        # normalização final
        cols = pd.Series(df.columns, dtype="object").astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
        cols = [c if c else f"col_{i}" for i, c in enumerate(cols)]
        df.columns = cols
        return df

    fname = arquivo_recebido.filename or ""
    ext = Path(fname).suffix.lower()

    # lê bytes
    arquivo_recebido.seek(0)
    raw = arquivo_recebido.read()
    arquivo_recebido.seek(0)

    if ext in {'.xlsx', '.xlsm', '.xls'}:
        df_raw = pd.read_excel(BytesIO(raw), header=None, dtype=str)
        if df_raw.empty or df_raw.shape[1] == 0:
            raise ValueError("Arquivo Excel sem dados.")
        df = _fix_headers(df_raw)

    elif ext in {'.csv', '.tsv', '.txt'}:
        enc = (chardet.detect(raw).get('encoding') or 'utf-8')
        txt = raw.decode(enc, errors='replace')
        sample = txt[:20000]

        # detecta separador
        try:
            dialect = _csv.Sniffer().sniff(sample, delimiters=';,\t|')
            sep = dialect.delimiter
        except Exception:
            # heurística com desconto para vírgula decimal
            import re as _re
            counts = {d: sample.count(d) for d in [';', ',', '\t', '|']}
            counts[','] -= len(_re.findall(r'\d,\d{1,3}(?:\D|$)', sample))
            sep = max(counts, key=counts.get) or ';'

        def _read_csv(t, s):
            return pd.read_csv(
                StringIO(t),
                sep=s,
                header=None,
                dtype=str,
                engine='python',
                on_bad_lines='skip',
                keep_default_na=False,
                quoting=_csv.QUOTE_MINIMAL
            )

        df_raw = _read_csv(txt, sep)
        # fallback: tentar outros se veio 1 coluna
        if df_raw.shape[1] == 1:
            for cand in [';', '\t', '|', ',']:
                if cand == sep: 
                    continue
                if cand in sample:
                    try:
                        df_try = _read_csv(txt, cand)
                        if df_try.shape[1] > 1:
                            df_raw = df_try; sep = cand; break
                    except Exception:
                        pass

        if df_raw.empty or df_raw.shape[1] == 0:
            raise ValueError("CSV/TXT sem dados legíveis.")

        df = _fix_headers(df_raw)

    else:
        raise ValueError(f"Formato '{ext}' não suportado. Use .csv, .tsv, .txt, .xlsx, .xlsm ou .xls.")

    # limpeza final segura
    if len(df) > 0:
        df = df.dropna(axis=1, how='all')
    df = df.fillna("")

    # renomeia única coluna genérica
    if df.shape[1] == 1 and re.fullmatch(r"col_\d+", str(df.columns[0])):
        df.columns = ['VALOR']

    if df.shape == (0, 0):
        raise ValueError("Nenhuma célula válida após leitura.")

    return df





# -------------------- Leitura de amostra (rápida para UI) --------------------


# -------------------- ROTAS --------------------
@app.route('/process/get_colunas', methods=['POST'])
@require_login
def get_colunas():
    try:
        if 'arquivo' not in request.files or not request.files['arquivo'].filename:
            return jsonify({"success": False, "error": "Nenhum arquivo enviado."}), 400
        arquivo = request.files['arquivo']
        df = ler_planilha(arquivo)

        return jsonify({"success": True, "colunas": list(df.columns)})
    except Exception as e:
        print(f"[get_colunas] erro: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/process/get_valores', methods=['POST'])
@require_login
def get_valores():
    try:
        if 'arquivo' not in request.files or not request.files['arquivo'].filename:
            return jsonify({"success": False, "error": "Nenhum arquivo enviado."}), 400

        coluna = request.form.get('coluna')
        if not coluna:
            return jsonify({"success": False, "error": "Coluna não informada."}), 400

        arq = request.files['arquivo']
        filename = arq.filename or ""
        extensao = Path(filename).suffix.lower()

        def norm_two(series: pd.Series):
            s = series.astype(str).str.strip().replace({"": None}).dropna()
            u = s.map(_norm_bin_val)
            vc = u.value_counts()
            if len(vc) >= 2:
                return vc.index[:2].tolist()
            return u.drop_duplicates().tolist()[:2]

        if extensao == '.csv':
            import chardet, csv as _csv
            arq.seek(0)
            raw = arq.read()
            enc = (chardet.detect(raw).get('encoding') or 'utf-8')
            txt = raw.decode(enc, errors='replace')

            sample = txt[:20000]
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=';,\t|,')
                sep = dialect.delimiter
            except Exception:
                sep = ';' if sample.count(';') >= sample.count(',') else ','

            # Cabeçalho a partir de uma amostra
            from io import StringIO
            df_head = pd.read_csv(StringIO(txt), sep=sep, header=None, dtype=str,
                                  on_bad_lines='skip', quoting=_csv.QUOTE_MINIMAL, nrows=300, engine='python')
            df_head = _fix_headers(df_head)
            if coluna not in df_head.columns:
                return jsonify({"success": False, "error": f"Coluna '{coluna}' não encontrada."}), 400

            # Itera em chunks usando StringIO (evita passar FileStorage ao pandas)
            found: list[str] = []
            f_iter = StringIO(txt)
            for chunk in pd.read_csv(f_iter, sep=sep, dtype=str, engine='python',
                                     on_bad_lines='skip', quoting=_csv.QUOTE_MINIMAL,
                                     chunksize=20000, header=None):
                # alinha colunas ao cabeçalho deduzido
                chunk.columns = list(df_head.columns)[:len(chunk.columns)]
                if coluna not in chunk.columns:
                    continue
                vals = norm_two(chunk[coluna])
                for v in vals:
                    if v not in found:
                        found.append(v)
                if len(found) >= 2:
                    return jsonify({"success": True, "valores": found[:2]})

            return jsonify({"success": True, "valores": found[:2] if found else []})

        elif extensao in {'.xlsx', '.xlsm', '.xls'}:
            steps = [500, 2000, 10000, 50000]
            for n in steps:
                arq.seek(0)
                df = pd.read_excel(arq, header=None, dtype=str, nrows=n)
                df = _fix_headers(df)
                if coluna not in df.columns:
                    continue
                vals = norm_two(df[coluna])
                if len(vals) >= 2:
                    return jsonify({"success": True, "valores": vals[:2]})

            arq.seek(0)
            df_all = pd.read_excel(arq, header=None, dtype=str)
            df_all = _fix_headers(df_all)
            if coluna not in df_all.columns:
                return jsonify({"success": False, "error": f"Coluna '{coluna}' não encontrada."}), 400
            vals = norm_two(df_all[coluna])
            return jsonify({"success": True, "valores": vals[:2]})
        else:
            return jsonify({"success": False, "error": f"Formato '{extensao}' não suportado."}), 400

    except Exception as e:
        print(f"[get_valores] erro: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/process/separar', methods=['POST'])
@require_login
def process_separar():
    try:
        if 'arquivo' not in request.files or not request.files['arquivo'].filename:
            return jsonify({"success": False, "error": "Nenhum arquivo enviado."}), 400

        arquivo = request.files['arquivo']

        coluna = (request.form.get('coluna') or '').strip()
        valor_consta = (request.form.get('valor_consta') or '').strip()
        valor_nao_consta = (request.form.get('valor_nao_consta') or '').strip()

        nome_consta = _ensure_xlsx((request.form.get('nome_consta') or 'consta.xlsx').strip() or 'consta.xlsx')
        nome_nao_consta = _ensure_xlsx((request.form.get('nome_nao_consta') or 'nao_consta.xlsx').strip() or 'nao_consta.xlsx')


        if not coluna or not valor_consta or not valor_nao_consta:
            return jsonify({"success": False, "error": "Todos os campos são obrigatórios."}), 400
        if valor_consta == valor_nao_consta:
            return jsonify({"success": False, "error": "Os dois valores não podem ser iguais."}), 400

        df = ler_planilha(arquivo)
        if coluna not in df.columns:
            return jsonify({"success": False, "error": f"Coluna '{coluna}' não encontrada."}), 400

        col_norm = df[coluna].map(_norm_bin_val)
        v1 = _norm_bin_val(valor_consta)
        v2 = _norm_bin_val(valor_nao_consta)

        df_consta = df[col_norm == v1]
        df_nao_consta = df[col_norm == v2]

        # debug útil
        print(f"[separar] coluna={coluna} v1={v1} -> {len(df_consta)} | v2={v2} -> {len(df_nao_consta)}")

        dir_temp = os.path.join(BASE_OUTPUT_DIR, str(uuid.uuid4()))
        os.makedirs(dir_temp, exist_ok=True)
        caminho_consta = os.path.join(dir_temp, secure_filename(nome_consta))
        caminho_nao_consta = os.path.join(dir_temp, secure_filename(nome_nao_consta))

        # exporta respeitando a extensão pedida
        if nome_consta.lower().endswith(".csv"):
            df_consta.to_csv(caminho_consta, index=False, sep=';', encoding='utf-8-sig')
        else:
            df_consta.to_excel(caminho_consta, index=False)

        if nome_nao_consta.lower().endswith(".csv"):
            df_nao_consta.to_csv(caminho_nao_consta, index=False, sep=';', encoding='utf-8-sig')
        else:
            df_nao_consta.to_excel(caminho_nao_consta, index=False)

        # zipa e devolve
        caminho_zip = os.path.join(BASE_OUTPUT_DIR, f"separado_{uuid.uuid4().hex}.zip")
        with zipfile.ZipFile(caminho_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(caminho_consta, arcname=secure_filename(nome_consta))
            zipf.write(caminho_nao_consta, arcname=secure_filename(nome_nao_consta))

        shutil.rmtree(dir_temp, ignore_errors=True)

        return send_file(
            caminho_zip,
            as_attachment=True,
            download_name=os.path.basename(caminho_zip),
            mimetype="application/octet-stream",
            etag=False, conditional=False, max_age=0
        )
    except Exception as e:
        print(f"[process_separar] erro: {e}")
        return jsonify({"success": False, "error": f"Ocorreu um erro no servidor: {e}"}), 500
    
# -------------------- PÁGINA DUPLICADOS --------------------
@app.route('/duplicados.html')
@require_login
def duplicados_page():
    return render_template('duplicados.html')


# -------------------- ESTATÍSTICAS (opcional p/ conferência) --------------------
@app.route('/process/duplicados_stats', methods=['POST'])
@require_login
def duplicados_stats():
    if 'arquivo' not in request.files or not request.files['arquivo'].filename:
        return jsonify({"success": False, "error": "Nenhum arquivo enviado."}), 400

    coluna = (request.form.get('coluna') or '').strip()
    df = ler_planilha(request.files['arquivo'])
    if coluna not in df.columns:
        return jsonify({"success": False, "error": f"Coluna '{coluna}' não encontrada."}), 400

    s_lit = df[coluna].astype(str)
    s_dig = s_lit.map(_digits_only)

    total = len(df)
    vazio_lit = (s_lit.str.strip() == "").sum()
    vazio_dig = (s_dig == "").sum()
    len11     = (s_dig.str.len() == 11).sum()

    dup_lit_todos     = int(s_lit.duplicated(keep=False).sum())
    dup_lit_sem_vazio = int(((s_lit.str.strip() != "") & s_lit.duplicated(keep=False)).sum())
    dup_dig_todos     = int(s_dig.duplicated(keep=False).sum())
    dup_dig_sem_vazio = int(((s_dig != "") & s_dig.duplicated(keep=False)).sum())
    dup_dig_len11     = int((((s_dig.str.len()==11)) & s_dig.duplicated(keep=False)).sum())

    top = s_dig[s_dig != ""].value_counts().head(10)
    top_list = [{"chave": k, "qtd": int(v)} for k, v in top.items()]

    return jsonify({
        "success": True,
        "total_linhas": int(total),
        "vazios_literal": int(vazio_lit),
        "vazios_digitos": int(vazio_dig),
        "qtd_len11": int(len11),
        "dup_literal_todos": dup_lit_todos,
        "dup_literal_sem_vazio": dup_lit_sem_vazio,
        "dup_digitos_todos": dup_dig_todos,
        "dup_digitos_sem_vazio": dup_dig_sem_vazio,
        "dup_digitos_len11": dup_dig_len11,
        "top10_chaves_por_qtd": top_list
    })


# -------------------- PROCESSO PRINCIPAL (automático) --------------------
@app.route('/process/duplicados', methods=['POST'])
@require_login
def process_duplicados():
    try:
        if 'arquivo' not in request.files or not request.files['arquivo'].filename:
            return jsonify({"success": False, "error": "Nenhum arquivo enviado."}), 400

        coluna = (request.form.get('coluna') or '').strip()
        nome_saida = (request.form.get('nome_saida') or 'duplicados.xlsx').strip() or 'duplicados.xlsx'

        df = ler_planilha(request.files['arquivo'])
        if coluna not in df.columns:
            return jsonify({"success": False, "error": f"Coluna '{coluna}' não encontrada."}), 400

        base = df[coluna].astype(str)
        dig  = base.map(_digits_only)

        # Heurística automática:
        # - Se houver muitos valores com 11 dígitos (CPF), foca neles;
        # - Caso contrário, usa somente os dígitos (ignora vazios).
        m_len11     = dig.str.len() == 11
        qtd_len11   = int(m_len11.sum())
        nao_vazios  = int((dig != "").sum())
        prop_len11  = (qtd_len11 / max(nao_vazios, 1))

        if (qtd_len11 >= 100) or (prop_len11 >= 0.20):
            chave = dig.where(m_len11, "")
        else:
            chave = dig

        mask_dup = (chave != "") & chave.duplicated(keep=False)

        df_dup = df[mask_dup].copy()
        if df_dup.empty:
            return jsonify({"success": False, "error": "Nenhum duplicado encontrado."}), 400

        counts = chave[mask_dup].value_counts()
        df_dup["__PLANILHEX_CHAVE"]   = chave[mask_dup]
        df_dup["__PLANILHEX_QTD_DUP"] = chave[mask_dup].map(counts)
        df_dup = df_dup.sort_values(["__PLANILHEX_CHAVE","__PLANILHEX_QTD_DUP"], ascending=[True,False])

        out_path = os.path.join(BASE_OUTPUT_DIR, secure_filename(_ensure_xlsx(nome_saida, "duplicados.xlsx")))
        if out_path.lower().endswith(".csv"):
            df_dup.to_csv(out_path, index=False, sep=';', encoding='utf-8-sig')
        else:
            df_dup.to_excel(out_path, index=False)

        return send_file(out_path, as_attachment=True, download_name=os.path.basename(out_path),
                         mimetype="application/octet-stream", etag=False, conditional=False, max_age=0)
    except Exception as e:
        app.logger.error(f"[process_duplicados] erro: {e}")
        return jsonify({"success": False, "error": f"Ocorreu um erro no servidor: {e}"}), 500










def _detect_sep(text: str) -> str:
    import collections
    cands = [';', ',', '\t', '|']
    lines = [ln for ln in text.splitlines()[:500] if ln.strip()]
    if not lines:
        return ';'
    scores = collections.Counter()
    for ln in lines:
        for d in cands:
            scores[d] += ln.count(d)
    # preferir ';' em empate com ',' para evitar conflito com decimal
    best = max(cands, key=lambda d: (scores[d], d == ';'))
    return best or ';'

def _read_csv_no_header(text: str) -> pd.DataFrame:
    from io import StringIO
    sep = _detect_sep(text)
    df = pd.read_csv(
        StringIO(text),
        sep=sep,
        engine='python',
        header=None,
        dtype=str,
        on_bad_lines='skip'
    )
    # se ainda veio 1 coluna mas contém delimitadores, dividir manualmente
    if df.shape[1] == 1:
        col = df.iloc[:, 0].astype(str)
        if any(x in col.str[:20000].to_string() for x in [';', ',', '\t', '|']):
            parts = col.str.split(sep, expand=True)
            df = parts.astype(str)
    # nomes padrão
    df.columns = [f"col_{i}" for i in range(df.shape[1])]
    return df.fillna("")

# --- ROTAS DE PROCESSAMENTO (AÇÕES DOS FORMULÁRIOS) ---
# --- ROTAS DE PROCESSAMENTO (AÇÕES DOS FORMULÁRIOS) ---
# --- MANTENHA só esta versão da rota /process/unir ---
from pathlib import Path

@app.route('/process/unir', methods=['POST'])
@require_login
def process_unir():
    try:
        files = [f for f in request.files.getlist('files[]') if f and f.filename]
        if not files:
            return jsonify({"success": False, "error": "Selecione os arquivos para unir."}), 400

        modo = (request.form.get('modo_uniao') or 'linhas').strip().lower()  # 'linhas' | 'colunas'
        app.logger.info("UNIR - Recebidos %d arquivos: %s", len(files), [f.filename for f in files])

        dfs = []
        for f in files:
            df_i = ler_planilha(f)
            # SOMENTE no modo colunas damos nomes distintos
            if modo == 'colunas' and df_i.shape[1] == 1 and str(df_i.columns[0]).lower().startswith(("col_", "valor")):
                df_i.columns = [Path(f.filename).stem]
            dfs.append(df_i)
            app.logger.info("UNIR - %s: %d linhas, %d colunas | cols=%s",
                            f.filename, len(df_i), df_i.shape[1], list(map(str, df_i.columns)))

        if not dfs:
            return jsonify({"success": False, "error": "Nenhum dado útil nos arquivos enviados."}), 400

        import pandas as pd

        if modo == 'colunas':
            # Lado a lado: alinhar pelo índice
            dfs_alinhados = [d.reset_index(drop=True) for d in dfs]
            df_final = pd.concat(dfs_alinhados, axis=1)
        else:
            # Por linhas: garantir mesmo cabeçalho quando TODOS têm 1 coluna
            if all(d.shape[1] == 1 for d in dfs):
                for d in dfs:
                    d.columns = ['VALOR']  # nome comum para empilhar
                df_final = pd.concat(dfs, ignore_index=True, sort=False)
            else:
                # superconjunto de colunas mantendo ordem da 1ª ocorrência
                all_cols, seen = [], set()
                for d in dfs:
                    for c in d.columns:
                        if c not in seen:
                            seen.add(c); all_cols.append(c)
                dfs_norm = [d.reindex(columns=all_cols, fill_value="") for d in dfs]
                df_final = pd.concat(dfs_norm, ignore_index=True, sort=False)

        app.logger.info("UNIR - TOTAL: %d linhas, %d colunas", len(df_final), df_final.shape[1])

        nome_saida = (request.form.get('nome_saida_uniao') or 'uniao_resultado.xlsx').strip() or 'uniao_resultado.xlsx'
        ext = Path(nome_saida).suffix.lower()
        if ext not in {'.xlsx', '.xls', '.csv'}:
            nome_saida += '.xlsx'; ext = '.xlsx'

        caminho = os.path.join(BASE_OUTPUT_DIR, secure_filename(nome_saida))
        if ext == '.csv':
            df_final.to_csv(caminho, index=False, sep=';', encoding='utf-8-sig')
        else:
            df_final.to_excel(caminho, index=False)

        return send_file(caminho, as_attachment=True,
                         download_name=os.path.basename(caminho),
                         mimetype="application/octet-stream",
                         etag=False, conditional=False, max_age=0)
    except Exception as e:
        app.logger.error(f"Erro em /process/unir: {e}")
        return jsonify({"success": False, "error": f"Ocorreu um erro no servidor: {e}"}), 500






















@app.route('/process/dividir', methods=['POST'])
@require_login
def process_dividir():
    """
    Regra:
      - Se codigo_manual: linha 1 = codigo_manual em A1; linha 2 = cabeçalho detectado; dados a partir da 1ª linha após o cabeçalho original.
      - Se não houver codigo_manual: linha 1 = cabeçalho detectado; dados a seguir.
    Nunca gravar header do pandas.
    """
    try:
        if 'arquivo' not in request.files or not request.files['arquivo'].filename:
            return jsonify({"success": False, "error": "Nenhum arquivo selecionado."}), 400
        if not request.form.get('linhas_por_arquivo'):
            return jsonify({"success": False, "error": "O número de linhas por arquivo é obrigatório."}), 400

        arquivo = request.files['arquivo']
        linhas_por_arquivo = int(request.form['linhas_por_arquivo'])
        nome_base = (request.form.get('nome_base') or 'dividido').strip() or 'dividido'
        formato_saida = (request.form.get('formato_saida') or 'xlsx').strip() or 'xlsx'
        codigo_manual = (request.form.get('codigo_manual') or '').strip()

        import pandas as pd, numpy as np, os, uuid, zipfile, shutil, re
        from pathlib import Path
        from werkzeug.utils import secure_filename

        df = ler_planilha(arquivo)  # mantém tudo como veio

        if df.empty:
            return jsonify({"success": False, "error": "Arquivo vazio."}), 400

        # --- detectar a linha do cabeçalho entre as 5 primeiras linhas ---
        def is_texty(x):
            s = str(x).strip()
            if s == "" or s.lower() in {"nan","none","null","#n/a","n/a"}: return False
            # considera "NB", "CPF_INTERESSADO", etc. como texto
            return bool(re.search(r"[A-Za-zÀ-ÿ_]", s))

        header_idx = None
        scan_rows = min(len(df), 5)
        best_score, best_idx = -1, 0
        for i in range(scan_rows):
            row = df.iloc[i]
            score = sum(is_texty(v) for v in row)  # conta quantas células parecem texto
            if score > best_score:
                best_score, best_idx = score, i
        header_idx = best_idx

        # nomes das colunas
        colunas = [str(v).strip() for v in df.iloc[header_idx].tolist()]
        ncols = len(colunas)

        # dados após o cabeçalho original
        dados = df.iloc[header_idx+1:].copy()
        dados.columns = colunas

        # limpeza de NaN textual
        NAN_STRINGS = {"nan","NaN","NAN","none","None","NULL","null","nil","Nulo","nulo","N/A","n/a","#N/A"}
        def clean_cell(v):
            if v is None or (isinstance(v, float) and np.isnan(v)): return ""
            s = str(v).strip()
            return "" if s in NAN_STRINGS else v

        dados = dados.applymap(clean_cell)

        # prefixo para cada parte
        if codigo_manual:
            code_row   = pd.DataFrame([[""] * ncols], columns=colunas)
            code_row.iat[0, 0] = codigo_manual          # somente A1 recebe o código
            header_row = pd.DataFrame([colunas], columns=colunas)
            prefixo = pd.concat([code_row, header_row], ignore_index=True)
        else:
            prefixo = pd.DataFrame([colunas], columns=colunas)

        total = len(dados)
        dir_temp = os.path.join(BASE_OUTPUT_DIR, str(uuid.uuid4()))
        os.makedirs(dir_temp, exist_ok=True)

        for i, start in enumerate(range(0, total, linhas_por_arquivo), start=1):
            parte = dados.iloc[start:start+linhas_por_arquivo].copy()
            bloco = pd.concat([prefixo, parte], ignore_index=True).reindex(columns=colunas)

            nome_bloco = f"{nome_base}_{i}.{formato_saida}"
            caminho_bloco = os.path.join(dir_temp, secure_filename(nome_bloco))

            if formato_saida.lower() == 'csv':
                bloco.to_csv(caminho_bloco, index=False, header=False, sep=';', encoding='utf-8-sig')
            else:
                with pd.ExcelWriter(caminho_bloco, engine='openpyxl') as w:
                    bloco.to_excel(w, index=False, header=False)

        caminho_zip = os.path.join(BASE_OUTPUT_DIR, f"{secure_filename(nome_base)}_dividido.zip")
        with zipfile.ZipFile(caminho_zip, 'w', zipfile.ZIP_DEFLATED) as z:
            for p in Path(dir_temp).glob('*'):
                z.write(p, arcname=p.name)

        shutil.rmtree(dir_temp)
        return _send_and_cleanup(caminho_zip, os.path.basename(caminho_zip))

    except Exception as e:
        app.logger.error(f"Erro em /process/dividir: {e}")
        return jsonify({"success": False, "error": f"Ocorreu um erro no servidor: {e}"}), 500
















# ==== helpers de nomes/cabeçalho ====
import unicodedata

def _norm_name(s):
    s = "" if s is None else str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.upper()
    s = re.sub(r"[^A-Z0-9]+", " ", s)  # remove pontuação/espaços extras
    return s.strip()

def _match_col(df, asked):
    mapa = {_norm_name(c): str(c) for c in df.columns}
    return mapa.get(_norm_name(asked))

def _try_promote_header(df: pd.DataFrame, target_name: str) -> pd.DataFrame:
    tgt = _norm_name(target_name)
    head = df.head(10).fillna("")
    for ridx in head.index:
        row_norm = [_norm_name(x) for x in head.loc[ridx].tolist()]
        if tgt in row_norm:
            new_cols = [str(x).strip() if str(x).strip() else f"col_{i}"
                        for i, x in enumerate(df.loc[ridx].tolist())]
            df2 = df.loc[ridx+1:].reset_index(drop=True).copy()
            df2.columns = pd.Series(new_cols, dtype="object").astype(str).str.strip().str.replace(r"\s+"," ",regex=True)
            return df2
    return df






@app.route('/process/cruzar', methods=['POST'])
@require_login
def process_cruzar():
    try:
        # arquivos
        if 'primaria' not in request.files or not request.files['primaria'].filename:
            return jsonify({"success": False, "error": "A planilha primária não foi enviada."}), 400
        if 'secundaria' not in request.files or not request.files['secundaria'].filename:
            return jsonify({"success": False, "error": "A planilha secundária não foi enviada."}), 400
        file_primaria   = request.files['primaria']
        file_secundaria = request.files['secundaria']

        # formulário
        col_prim_form = (request.form.get('coluna_primaria') or '').strip()
        col_sec_form  = (request.form.get('coluna_secundaria') or '').strip()
        nome_saida    = (request.form.get('nome_saida') or 'saida.xlsx').strip() or 'saida.xlsx'
        new_column_name = request.form.get('new_column_name', 'Verificacao_Planilhex')
        found_value     = request.form.get('found_value', 'ENCONTRADO')
        not_found_value = request.form.get('not_found_value', 'NAO ENCONTRADO')
        if not col_prim_form or not col_sec_form:
            return jsonify({"success": False, "error": "É necessário selecionar a coluna base para ambas as planilhas."}), 400

        # leitura
        df_prim = ler_planilha(file_primaria)
        df_sec  = ler_planilha(file_secundaria)

        # logs de diagnóstico
        app.logger.info("CRUZAR: col_prim_form=%r col_sec_form=%r", col_prim_form, col_sec_form)
        app.logger.info("PRIM cols=%s", list(map(str, df_prim.columns)))
        app.logger.info("SEC  cols=%s", list(map(str, df_sec.columns)))

        # casar coluna; se não achar, tenta promover linha a cabeçalho
        col_prim = _match_col(df_prim, col_prim_form)
        if not col_prim:
            df_prim = _try_promote_header(df_prim, col_prim_form)
            col_prim = _match_col(df_prim, col_prim_form)

        col_sec = _match_col(df_sec, col_sec_form)
        if not col_sec:
            df_sec = _try_promote_header(df_sec, col_sec_form)
            col_sec = _match_col(df_sec, col_sec_form)

        # valida nomes finais
        if not col_prim:
            return jsonify({"success": False,
                            "error": f"Coluna '{col_prim_form}' não encontrada na planilha primária. "
                                     f"Colunas: {', '.join(map(str, df_prim.columns))}"}), 400
        if not col_sec:
            return jsonify({"success": False,
                            "error": f"Coluna '{col_sec_form}' não encontrada na planilha secundária. "
                                     f"Colunas: {', '.join(map(str, df_sec.columns))}"}), 400

        # matching por texto e dígitos
        prim_text   = set(df_prim[col_prim].map(_norm_text));   prim_text.discard("")
        prim_digits = set(df_prim[col_prim].map(_digits_only)); prim_digits.discard("")
        sec_text    = df_sec[col_sec].map(_norm_text)
        sec_digits  = df_sec[col_sec].map(_digits_only)

        mask = sec_text.isin(prim_text) | sec_digits.isin(prim_digits)
        df_sec[new_column_name] = mask.map({True: found_value, False: not_found_value})

        app.logger.info("[cruzar] prim='%s' sec='%s' prim_text=%d prim_digits=%d matches=%d total=%d",
                        col_prim, col_sec, len(prim_text), len(prim_digits), int(mask.sum()), len(mask))

        # saída
        caminho_saida = os.path.join(BASE_OUTPUT_DIR, secure_filename(nome_saida))
        if nome_saida.lower().endswith('.csv'):
            df_sec.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8-sig')
        else:
            df_sec.to_excel(caminho_saida, index=False)

        return send_file(caminho_saida, as_attachment=True,
                         download_name=os.path.basename(caminho_saida),
                         mimetype="application/octet-stream",
                         etag=False, conditional=False, max_age=0)
    except Exception as e:
        app.logger.exception("Erro em /process/cruzar")  # inclui traceback
        return jsonify({"success": False, "error": f"Ocorreu um erro no servidor: {e}"}), 500

def separar_por_coluna(df, coluna, valor_consta, valor_nao_consta, nome_consta, nome_nao_consta):
    """
    Função que separa um DataFrame em dois com base em valores de uma coluna específica.
    Retorna os caminhos dos arquivos Excel gerados.
    """
    try:
        if coluna not in df.columns:
            raise ValueError(f"Coluna '{coluna}' não encontrada no DataFrame.")
        # Filtra os dados com base nos valores fornecidos
        df_consta = df[df[coluna].astype(str) == valor_consta]
        df_nao_consta = df[df[coluna].astype(str) == valor_nao_consta]
        # Cria diretório temporário para salvar os arquivos
        dir_temp = os.path.join(BASE_OUTPUT_DIR, str(uuid.uuid4()))
        os.makedirs(dir_temp)
        # Define os caminhos dos arquivos
        caminho_consta = os.path.join(dir_temp, secure_filename(nome_consta))
        caminho_nao_consta = os.path.join(dir_temp, secure_filename(nome_nao_consta))
        # Salva os DataFrames como arquivos Excel
        df_consta.to_excel(caminho_consta, index=False)
        df_nao_consta.to_excel(caminho_nao_consta, index=False)
        return caminho_consta, caminho_nao_consta, dir_temp
    except Exception as e:
        app.logger.error(f"Erro na função separar_por_coluna: {e}")
        raise ValueError(f"Erro ao separar por coluna: {e}")
    


# ===================== NOTIFICAÇÕES: CONVERSOR UNIVERSAL (atualizado) =====================
# Entradas: .txt (fixo 80), .csv, .xls/.xlsx, .json
# Saídas: csv_semicolon | csv_comma | tsv | xlsx | json | parquet

def _rng(a,b): return slice(a-1, b)

HEADER_MAP = {"CS_TIPO_REGISTRO": _rng(1,1),"NU_LINHA": _rng(2,9),"ID_CAMPANHA": _rng(10,15),
              "CS_TIPO_CONTEUDO": _rng(16,23),"DT_GERACAO_ARQUIVO": _rng(24,31),
              "HR_GERACAO_ARQUIVO": _rng(32,35),"FILLER": _rng(36,80)}
DETALHE_MAP = {"CS_TIPO_REGISTRO": _rng(1,1),"NU_LINHA": _rng(2,9),"NU_NB": _rng(10,19),
              "CS_ESPECIE": _rng(20,22),"ID_BANCO": _rng(23,25),"ID_ORGAO_PAGADOR": _rng(26,31),
              "CS_MEIO_PAGAMENTO": _rng(32,33),"ID_OL_MANUTENCAO": _rng(34,41),"GEX": _rng(42,46),
              "CS_RESPOSTA": _rng(47,48),"DT_VISUALIZACAO_NOTIFICACAO": _rng(49,56),
              "HR_VISUALIZACAO_NOTIFICACAO": _rng(57,62),"CS_CANAL_NOTIFICACAO": _rng(63,64),
              "FILLER": _rng(65,80)}
TRAILER_MAP = {"CS_TIPO_REGISTRO": _rng(1,1),"NU_LINHA": _rng(2,9),"QT_REGISTROS": _rng(10,17),
               "FILLER": _rng(18,80)}

def _fw_extract(line:str, fmap:dict) -> dict:
    return {k: line[v].rstrip() for k,v in fmap.items()}

ALL_COLS = ["CS_TIPO_REGISTRO","NU_LINHA","ID_CAMPANHA","CS_TIPO_CONTEUDO","DT_GERACAO_ARQUIVO",
            "HR_GERACAO_ARQUIVO","NU_NB","CS_ESPECIE","ID_BANCO","ID_ORGAO_PAGADOR","CS_MEIO_PAGAMENTO",
            "ID_OL_MANUTENCAO","GEX","CS_RESPOSTA","DT_VISUALIZACAO_NOTIFICACAO","HR_VISUALIZACAO_NOTIFICACAO",
            "CS_CANAL_NOTIFICACAO","QT_REGISTROS"]

def _align_cols(df):
    import pandas as pd
    if df.empty: return pd.DataFrame(columns=ALL_COLS)
    for c in ALL_COLS:
        if c not in df.columns: df[c] = ""
    return df[ALL_COLS]

def _parse_notificacoes_txt_unificado(file_storage):
    import pandas as pd
    file_storage.seek(0)
    raw = file_storage.read().decode("utf-8", errors="replace").splitlines()
    lines = [ (ln[:80] + " "*80)[:80] for ln in raw if ln.strip() != "" ]
    hdr_rows, det_rows, trl_rows = [], [], []
    for ln in lines:
        t = ln[0:1]
        if t == "1": hdr_rows.append(_fw_extract(ln, HEADER_MAP))
        elif t == "2": det_rows.append(_fw_extract(ln, DETALHE_MAP))
        elif t == "3": trl_rows.append(_fw_extract(ln, TRAILER_MAP))
    df_h, df_d, df_t = map(pd.DataFrame, (hdr_rows, det_rows, trl_rows))
    if not df_d.empty and "NU_NB" in df_d.columns: df_d = df_d.sort_values("NU_NB")
    df_all = pd.concat([_align_cols(df_h), _align_cols(df_d), _align_cols(df_t)], ignore_index=True)
    return df_all, "TXT_FIXED_80"

def _read_any_to_df(file_storage):
    from pathlib import Path
    ext = Path(file_storage.filename or "").suffix.lower()

    if ext == ".txt":
        df, dtype = _parse_notificacoes_txt_unificado(file_storage)
        return df, dtype, "notificacoes"

    if ext in {".xlsx", ".xls"}:
        file_storage.seek(0)
        df_raw = pd.read_excel(file_storage, header=None, dtype=str)
        df = _fix_headers(df_raw).dropna(axis=1, how='all').fillna("")
        return df, ext.upper().lstrip("."), Path(file_storage.filename).stem or "planilha"

    if ext == ".csv":
        import chardet, csv as _csv
        file_storage.seek(0)
        raw = file_storage.read()
        enc = (chardet.detect(raw).get('encoding') or 'utf-8')
        txt = raw.decode(enc, errors='replace')
        sample = txt[:20000]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=';,\t|,')
            sep = dialect.delimiter
        except Exception:
            sep = ';' if sample.count(';') >= sample.count(',') else ','
        from io import StringIO
        df_raw = pd.read_csv(StringIO(txt), sep=sep, header=None, dtype=str, on_bad_lines='skip', engine='python')
        df = _fix_headers(df_raw).dropna(axis=1, how='all').fillna("")
        return df, "CSV", Path(file_storage.filename).stem or "dados"

    if ext == ".json":
        file_storage.seek(0)
        try:
            df = pd.read_json(file_storage)
        except ValueError:
            file_storage.seek(0)
            df = pd.read_json(file_storage, lines=True)
        df = df.astype(str).fillna("")
        return df, "JSON", Path(file_storage.filename).stem or "dados"

    # fallback tenta TXT fixo
    try:
        df, dtype = _parse_notificacoes_txt_unificado(file_storage)
        return df, dtype, "notificacoes"
    except Exception:
        raise ValueError("Formato de entrada não suportado. Use .txt, .csv, .xls, .xlsx ou .json.")

def _save_df(df, out_format:str, base_dir:str, base_name:str):
    out_format = (out_format or "csv_semicolon").lower()
    ext_map = {
        "csv_semicolon": ("csv", {"sep":";","encoding":"utf-8-sig"}),
        "csv_comma":     ("csv", {"sep":",","encoding":"utf-8-sig"}),
        "tsv":           ("tsv", {"sep":"\t","encoding":"utf-8-sig"}),
        "xlsx":          ("xlsx", {}),
        "json":          ("json", {}),
        "parquet":       ("parquet", {}),
    }
    if out_format not in ext_map:
        raise ValueError("Formato de saída inválido.")
    ext, opts = ext_map[out_format]
    # monta nome sem duplicar extensão
    base = secure_filename((base_name or "convertido").rsplit(".",1)[0])
    fname = f"{base}.{('csv' if ext in {'csv','tsv'} else ext)}"
    fpath = os.path.join(base_dir, fname)

    if ext in {"csv","tsv"}:
        df.to_csv(fpath, index=False, sep=opts["sep"], encoding=opts["encoding"])
    elif ext == "xlsx":
        df.to_excel(fpath, index=False)
    elif ext == "json":
        df.to_json(fpath, orient="records", force_ascii=False)
    else:  # parquet
        try:
            df.to_parquet(fpath, index=False)
        except Exception as e:
            raise ValueError(f"PARQUET indisponível: instale 'pyarrow' ou 'fastparquet'. Detalhe: {e}")
    return fpath, fname

@app.route('/notificacoes.html')
@require_login
def notificacoes_page():
    return render_template('notificacoes.html')

@app.route('/process/notificacoes_importar_txt', methods=['POST'])
@require_login
def notificacoes_importar_txt():
    try:
        if 'arquivo' not in request.files or not request.files['arquivo'].filename:
            return jsonify({"success": False, "error": "Envie um arquivo."}), 400

        arq = request.files['arquivo']
        saida_fmt = (request.form.get('saida_formato') or 'csv_semicolon').strip().lower()
        base_name = (request.form.get('nome_saida') or '').strip()

        df, detected_type, suggested = _read_any_to_df(arq)
        base_final = base_name or suggested or "convertido"

        dir_temp = os.path.join(BASE_OUTPUT_DIR, f"conv_{uuid.uuid4().hex}")
        os.makedirs(dir_temp, exist_ok=True)
        out_path, out_name = _save_df(df, saida_fmt, dir_temp, base_final)

        resp = send_file(
            out_path,
            as_attachment=True,
            download_name=out_name,
            mimetype="application/octet-stream",
            etag=False, conditional=False, max_age=0
        )
        resp.headers['X-Input-Type'] = detected_type
        resp.headers['X-Output-Format'] = saida_fmt.upper()
        return resp
    except Exception as e:
        app.logger.error(f"[notificacoes_importar_txt] {e}")
        return jsonify({"success": False, "error": f"Erro: {e}"}), 500








#==========================================================================







# ==== DUPLICADOS: resolver por data =================================================

@app.route('/duplicados_resolver.html')
@require_login
def duplicados_resolver_page():
    return render_template('duplicados_resolver.html')


def _parse_datetime_flex(s: pd.Series) -> pd.Series:
    """Converte datas em série pandas robustamente.
    Suporta strings variadas, números Excel (serial), timestamps e vazio."""
    x = s.copy()
    if pd.api.types.is_datetime64_any_dtype(x):
        return x

    num_mask = pd.to_numeric(x, errors="coerce")
    dt_excel = pd.to_datetime(num_mask, origin="1899-12-30", unit="D", errors="coerce")

    dt_str1 = pd.to_datetime(x, dayfirst=True, errors="coerce")  # Brasil
    dt_merged = dt_str1.fillna(dt_excel)
    return dt_merged


def _choose_idx_to_drop(group: pd.DataFrame, data_col: str, drop_newest: bool) -> list[int]:
    """Recebe um grupo duplicado e retorna os índices a remover segundo a regra."""
    dt = _parse_datetime_flex(group[data_col])

    if drop_newest:
        key_val = dt.min()     # manter o mais antigo
        keep_idx = group.loc[dt == key_val].index.min()
        drop_idx = [i for i in group.index if i != keep_idx]
    else:
        key_val = dt.max()     # manter o mais recente
        keep_idx = group.loc[dt == key_val].index.min()
        drop_idx = [i for i in group.index if i != keep_idx]

    if pd.isna(dt).all():      # todas NaT: manter a primeira
        keep_idx = group.index.min()
        drop_idx = [i for i in group.index if i != keep_idx]
    return drop_idx


@app.route('/process/duplicados_resolver', methods=['POST'])
@require_login
def duplicados_resolver():
    """
    Entrada:
      - arquivo: FileStorage
      - coluna_chave: str  (onde detecta duplicados)
      - coluna_data:  str  (usada para decidir recente/antigo)
      - criterio: 'excluir_recente' | 'excluir_antigo'
      - usar_digitos: '1' | '0'  (opcional; se '1' usa só dígitos na coluna-chave)
      - nome_saida: nome do arquivo final (xlsx/csv)
    Saída:
      - arquivo com registros REMANESCENTES e headers com relatório.
    """
    try:
        if 'arquivo' not in request.files or not request.files['arquivo'].filename:
            return jsonify({"success": False, "error": "Nenhum arquivo enviado."}), 400

        arquivo = request.files['arquivo']
        df = ler_planilha(arquivo)

        coluna_chave = (request.form.get('coluna_chave') or '').strip()
        coluna_data  = (request.form.get('coluna_data')  or '').strip()
        criterio     = (request.form.get('criterio')     or '').strip()
        usar_digitos = (request.form.get('usar_digitos') or '0').strip() == '1'
        nome_saida   = _ensure_xlsx((request.form.get('nome_saida') or 'resolvido.xlsx').strip() or 'resolvido.xlsx')

        if not coluna_chave or not coluna_data or criterio not in {'excluir_recente', 'excluir_antigo'}:
            return jsonify({"success": False, "error": "Parâmetros inválidos. Informe coluna_chave, coluna_data e critério."}), 400
        if coluna_chave not in df.columns:
            return jsonify({"success": False, "error": f"Coluna-chave '{coluna_chave}' não encontrada."}), 400
        if coluna_data not in df.columns:
            return jsonify({"success": False, "error": f"Coluna de data '{coluna_data}' não encontrada."}), 400

        total_before = int(len(df))

        chave_series = df[coluna_chave].astype(str)
        if usar_digitos:
            chave_series = chave_series.map(_digits_only)

        chave_clean = chave_series.str.strip()
        dup_mask = (chave_clean != "") & chave_clean.duplicated(keep=False)

        if not dup_mask.any():
            return jsonify({"success": False, "error": "Nenhum conjunto duplicado encontrado segundo a coluna-chave."}), 400

        linhas_dup = int(dup_mask.sum())
        grupos_dup = int(chave_clean[dup_mask].nunique())

        df_dup = df[dup_mask].copy()
        df_nd  = df[~dup_mask].copy()

        drop_newest = (criterio == 'excluir_recente')
        df_dup['_KEY_'] = chave_clean[dup_mask].values

        to_drop = []
        for _, g in df_dup.groupby('_KEY_', sort=False):
            to_drop.extend(_choose_idx_to_drop(g, coluna_data, drop_newest=drop_newest))

        df_final = pd.concat([df_nd, df_dup.drop(index=to_drop)], ignore_index=False).sort_index()
        df_final = df_final.drop(columns=['_KEY_'], errors='ignore')

        total_after = int(len(df_final))
        removidas   = int(total_before - total_after)

        out_path = os.path.join(BASE_OUTPUT_DIR, secure_filename(nome_saida))
        if out_path.lower().endswith(".csv"):
            df_final.to_csv(out_path, index=False, sep=';', encoding='utf-8-sig')
        else:
            df_final.to_excel(out_path, index=False)

        resp = send_file(
            out_path,
            as_attachment=True,
            download_name=os.path.basename(out_path),
            mimetype="application/octet-stream",
            etag=False, conditional=False, max_age=0
        )
        # Relatório via headers
        resp.headers['X-Total-Linhas']      = str(total_before)
        resp.headers['X-Linhas-Duplicadas'] = str(linhas_dup)
        resp.headers['X-Grupos-Duplicados'] = str(grupos_dup)
        resp.headers['X-Total-Apos']        = str(total_after)
        resp.headers['X-Removidas']         = str(removidas)
        return resp

    except Exception as e:
        app.logger.error(f"[duplicados_resolver] erro: {e}")
        return jsonify({"success": False, "error": f"Ocorreu um erro no servidor: {e}"}), 500







#########################################################################


# ==== DUPLICADOS: separar em antigas × recentes (apenas duplicadas) ================
@app.route('/duplicadas_split.html')
@require_login
def duplicadas_split_page():
    return render_template('duplicadas_split.html')

# Alias para quem digitar "duplicados" (sem o 'a')
#@app.route('/duplicados_split.html')
#@require_login
#def duplicados_split_alias():
#    return render_template('duplicadas_split.html')




@app.route('/process/duplicadas_split', methods=['POST'])
@require_login
def process_duplicadas_split():
    """
    Entrada (multipart/form-data):
      - arquivo: planilha
      - coluna_chave: str (detecta duplicados)
      - usar_digitos: '1'|'0'  (se '1', usa só dígitos da chave)
      - coluna_data:  str (decide antigas × recentes entre as duplicadas)
      - incluir_total: '1'|'0' (opcional; inclui um terceiro arquivo com TODAS as duplicadas)
      - nome_antigas:  str (opcional; default 'duplicadas_antigas.xlsx')
      - nome_recentes: str (opcional; default 'duplicadas_recentes.xlsx')
      - nome_total:    str (opcional; só se incluir_total=1; default 'duplicadas_todas.xlsx')

    Saída:
      - Um .zip com 2 arquivos (antigas, recentes) ou 3 (se incluir_total=1).
      - Headers com métricas (X-Total-Duplicadas, X-Grupos, X-Antigas, X-Recentes).
    """
    try:
        if 'arquivo' not in request.files or not request.files['arquivo'].filename:
            return jsonify({"success": False, "error": "Nenhum arquivo enviado."}), 400

        arquivo = request.files['arquivo']
        df = ler_planilha(arquivo)

        coluna_chave = (request.form.get('coluna_chave') or '').strip()
        coluna_data  = (request.form.get('coluna_data')  or '').strip()
        usar_digitos = (request.form.get('usar_digitos') or '0').strip() == '1'
        incluir_total = (request.form.get('incluir_total') or '0').strip() == '1'

        nome_antigas  = _ensure_xlsx((request.form.get('nome_antigas')  or 'duplicadas_antigas.xlsx').strip() or 'duplicadas_antigas.xlsx')
        nome_recentes = _ensure_xlsx((request.form.get('nome_recentes') or 'duplicadas_recentes.xlsx').strip() or 'duplicadas_recentes.xlsx')
        nome_total    = _ensure_xlsx((request.form.get('nome_total')    or 'duplicadas_todas.xlsx').strip() or 'duplicadas_todas.xlsx')

        if not coluna_chave or not coluna_data:
            return jsonify({"success": False, "error": "Informe coluna_chave e coluna_data."}), 400
        if coluna_chave not in df.columns:
            return jsonify({"success": False, "error": f"Coluna-chave '{coluna_chave}' não encontrada."}), 400
        if coluna_data not in df.columns:
            return jsonify({"success": False, "error": f"Coluna de data '{coluna_data}' não encontrada."}), 400

        # prepara chave para duplicidade
        chave_series = df[coluna_chave].astype(str)
        if usar_digitos:
            chave_series = chave_series.map(_digits_only)
        chave_clean = chave_series.str.strip()

        dup_mask = (chave_clean != "") & chave_clean.duplicated(keep=False)
        if not dup_mask.any():
            return jsonify({"success": False, "error": "Nenhuma duplicata encontrada para a coluna selecionada."}), 400

        # apenas duplicadas
        df_dup = df[dup_mask].copy()
        df_dup['__KEY__'] = chave_clean[dup_mask].values

        # parse da data
        df_dup['__DT__'] = _parse_datetime_flex(df_dup[coluna_data])

        # separar antigas (mínimo por chave) e recentes (máximo por chave)
        # empates: mantém TODAS as que empatarem no min/max
        grp = df_dup.groupby('__KEY__', sort=False)['__DT__']
        min_dt = grp.transform('min')
        max_dt = grp.transform('max')

        antigas_mask  = (df_dup['__DT__'].eq(min_dt)) | (df_dup['__DT__'].isna() & min_dt.isna())
        recentes_mask = (df_dup['__DT__'].eq(max_dt)) | (df_dup['__DT__'].isna() & max_dt.isna())


        df_antigas  = df_dup[antigas_mask].drop(columns=['__KEY__','__DT__'])
        df_recentes = df_dup[recentes_mask].drop(columns=['__KEY__','__DT__'])

        # métricas
        total_dup = int(len(df_dup))
        grupos = int(df_dup['__KEY__'].nunique())
        qtd_antigas = int(len(df_antigas))
        qtd_recentes = int(len(df_recentes))

        # salvar em pasta temp
        dir_temp = os.path.join(BASE_OUTPUT_DIR, f"dup_split_{uuid.uuid4().hex}")
        os.makedirs(dir_temp, exist_ok=True)

        def _save_any(df_out, path_name):
            path = os.path.join(dir_temp, secure_filename(path_name))
            if path.lower().endswith('.csv'):
                df_out.to_csv(path, index=False, sep=';', encoding='utf-8-sig')
            else:
                df_out.to_excel(path, index=False)
            return path

        p_ant = _save_any(df_antigas, nome_antigas)
        p_rec = _save_any(df_recentes, nome_recentes)

        files_to_zip = [(p_ant, secure_filename(nome_antigas)),
                        (p_rec, secure_filename(nome_recentes))]

        if incluir_total:
            p_tot = _save_any(df_dup.drop(columns=['__KEY__','__DT__']), nome_total)
            files_to_zip.append((p_tot, secure_filename(nome_total)))

        # zip
        zip_path = os.path.join(BASE_OUTPUT_DIR, f"duplicadas_split_{uuid.uuid4().hex}.zip")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for full, arc in files_to_zip:
                zf.write(full, arcname=arc)

        # limpar pasta temp
        shutil.rmtree(dir_temp, ignore_errors=True)

        resp = send_file(
            zip_path,
            as_attachment=True,
            download_name=os.path.basename(zip_path),
            mimetype="application/octet-stream",
            etag=False, conditional=False, max_age=0
        )
        # headers de relatório
        resp.headers['X-Total-Duplicadas'] = str(total_dup)
        resp.headers['X-Grupos']            = str(grupos)
        resp.headers['X-Antigas']           = str(qtd_antigas)
        resp.headers['X-Recentes']          = str(qtd_recentes)
        resp.headers['X-Incluiu-Total']     = '1' if incluir_total else '0'
        return resp

    except Exception as e:
        app.logger.error(f"[process_duplicadas_split] erro: {e}")
        return jsonify({"success": False, "error": f"Ocorreu um erro no servidor: {e}"}), 500






from flask import after_this_request

def _send_and_cleanup(path, download_name=None):
    @after_this_request
    def _cleanup(response):
        try: os.remove(path)
        except Exception: pass
        return response
    return send_file(
        path,
        as_attachment=True,
        download_name=download_name or os.path.basename(path),
        mimetype="application/octet-stream",
        etag=False, conditional=False, max_age=0
    )



# ======================================================================
# 5. PONTO DE ENTRADA DA APLICAÇÃO
# ======================================================================
class WindowAPI:
    def ping(self):
        return "ok"

def _run_flask():
    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False)

if __name__ == '__main__':
    import threading, time, socket, os, sys, ctypes

    def fail(msg, code=1):
        try:
            ctypes.windll.user32.MessageBoxW(0, msg, "Planilhex", 0x10)  # MB_ICONERROR
        except Exception:
            print(msg, file=sys.stderr)
        sys.exit(code)

    # Sobe o backend local (somente loopback)
    threading.Thread(target=_run_flask, daemon=True).start()

    # Aguarda o servidor ficar de pé
    for _ in range(100):
        try:
            with socket.create_connection(("127.0.0.1", 5000), timeout=0.2):
                break
        except OSError:
            time.sleep(0.1)
    else:
        fail("Falha ao iniciar o servidor local (porta 5000). Feche outras instâncias e tente de novo.")

    # Força UI desktop (WebView2). Sem fallback para navegador.
    os.environ['PYWEBVIEW_GUI'] = 'edgechromium'
    os.environ['PYWEBVIEW_EXPERIMENTAL_GUI'] = '0'

    try:
        window = webview.create_window(
            "Planilhex",
            "http://127.0.0.1:5000",
            width=1200, height=800,
            resizable=True, frameless=False,
            js_api=WindowAPI(),
        )
        webview.start(gui='edgechromium', debug=False)
    except Exception:
        fail(
            "Não foi possível iniciar a interface desktop.\n"
            "Instale o Microsoft Edge WebView2 Runtime e tente novamente.",
            code=2
        )

