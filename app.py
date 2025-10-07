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
app.config['SECRET_KEY'] = 'a-chave-secreta-do-planilhex-2025'
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
# ==============================================================================
# 2. FUNÇÕES AUXILIARES
# ==============================================================================
def _fix_headers(df):
    # acha a primeira linha com pelo menos 2 células não vazias e NÃO só números
    for i in range(min(10, len(df))):
        row = df.iloc[i]
        nonempty = row.dropna().astype(str).str.strip()
        if (nonempty.size >= 2) and not all(nonempty.str.fullmatch(r"\d+")):
            new_cols = nonempty.reindex_like(row).fillna("").tolist()
            new_cols = [c.strip() if c else f"col_{j}" for j, c in enumerate(new_cols)]
            df = df.iloc[i+1:].reset_index(drop=True)
            df.columns = new_cols
            break

    # limpa espaços e nomes vazios
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


# -------------------- Leitura completa (lenta, mas robusta) --------------------
def ler_planilha(arquivo_recebido):
    try:
        extensao = Path(arquivo_recebido.filename).suffix.lower()
        arquivo_recebido.seek(0)

        if extensao in {'.xlsx', '.xlsm', '.xls'}:
            df_raw = pd.read_excel(arquivo_recebido, header=None, dtype=str)
            df = _fix_headers(df_raw)

        elif extensao == '.csv':
            import chardet, csv as _csv
            raw = arquivo_recebido.read()
            arquivo_recebido.seek(0)
            enc = chardet.detect(raw).get('encoding') or 'utf-8'
            txt = raw.decode(enc, errors='replace')
            sample = txt[:20000]
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=';,\t|,')
                sep = dialect.delimiter
            except Exception:
                sep = ';' if sample.count(';') >= sample.count(',') else ','
            from io import StringIO
            df_raw = pd.read_csv(StringIO(txt), sep=sep, engine='python',
                                 header=None, dtype=str, on_bad_lines='skip',
                                 quoting=_csv.QUOTE_MINIMAL)
            df = _fix_headers(df_raw)
        else:
            raise ValueError(f"Formato '{extensao}' não suportado. Use .csv, .xlsx, .xlsm ou .xls.")

        df = df.dropna(axis=1, how='all').fillna("")
        return df

    except Exception as e:
        app.logger.error(f"Erro ao ler o arquivo {arquivo_recebido.filename}: {e}")
        raise ValueError(f"Não foi possível ler o arquivo '{arquivo_recebido.filename}'.")

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
# ==============================================================================
























# --- ROTAS DE PROCESSAMENTO (AÇÕES DOS FORMULÁRIOS) ---
@app.route('/process/unir', methods=['POST'])
@require_login
def process_unir():
    try:
        files = [f for f in request.files.getlist('files[]') if f and f.filename]
        if not files:
            return jsonify({"success": False, "error": "Por favor, selecione os arquivos para unir."}), 400

        app.logger.info("UNIR - Recebidos %d arquivos: %s", len(files), [f.filename for f in files])

        dfs = []
        for f in files:
            try:
                df_i = ler_planilha(f)
            except Exception as e:
                app.logger.error("UNIR - Falha lendo %s: %s", f.filename, e)
                return jsonify({"success": False, "error": f"Erro ao ler '{f.filename}': {e}"}), 400
            app.logger.info("UNIR - %s: %d linhas, %d colunas", f.filename, len(df_i), df_i.shape[1])
            dfs.append(df_i)

        if not dfs:
            return jsonify({"success": False, "error": "Nenhum dado lido dos arquivos enviados."}), 400

        # União de colunas com ordem estável (primeira ocorrência prevalece)
        all_cols, seen = [], set()
        for df in dfs:
            for c in df.columns:
                if c not in seen:
                    seen.add(c); all_cols.append(c)

        dfs_norm = [df.reindex(columns=all_cols, fill_value="") for df in dfs]
        df_final = pd.concat(dfs_norm, ignore_index=True)
        app.logger.info("UNIR - TOTAL após concat: %d linhas, %d colunas", len(df_final), df_final.shape[1])

        # Nome e extensão de saída
        nome_saida = (request.form.get('nome_saida_uniao') or 'uniao_resultado.xlsx').strip()
        if not nome_saida:
            nome_saida = 'uniao_resultado.xlsx'
        ext = Path(nome_saida).suffix.lower()
        allowed_ext = {'.xlsx', '.xls', '.csv'}
        if ext not in allowed_ext:
            nome_saida += '.xlsx'
            ext = '.xlsx'

        caminho_saida = os.path.join(BASE_OUTPUT_DIR, secure_filename(nome_saida))
        if ext == '.csv':
            df_final.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8-sig')
        else:
            df_final.to_excel(caminho_saida, index=False)

        return send_file(
            caminho_saida,
            as_attachment=True,
            download_name=os.path.basename(caminho_saida),
            mimetype="application/octet-stream",
            etag=False, conditional=False, max_age=0
        )
    except Exception as e:
        app.logger.error(f"Erro em /process/unir: {e}")
        return jsonify({"success": False, "error": f"Ocorreu um erro no servidor: {e}"}), 500


@app.route('/process/dividir', methods=['POST'])
@require_login
def process_dividir():
    """Processa a divisão de uma planilha em múltiplos arquivos menores."""
    try:
        # --- Validações Iniciais ---
        if 'arquivo' not in request.files or not request.files['arquivo'].filename:
            return jsonify({"success": False, "error": "Nenhum arquivo selecionado."}), 400
        if not request.form.get('linhas_por_arquivo'):
            return jsonify({"success": False, "error": "O número de linhas por arquivo é obrigatório."}), 400
        # --- Obtém os dados do formulário ---
        arquivo = request.files['arquivo']
        linhas_por_arquivo = int(request.form['linhas_por_arquivo'])
        nome_base = (request.form.get('nome_base') or 'dividido').strip() or 'dividido'
        formato_saida = (request.form.get('formato_saida') or 'xlsx').strip() or 'xlsx'

        # --- Lógica de Processamento ---
        df = ler_planilha(arquivo)
        total_linhas = len(df)
       
        dir_temp = os.path.join(BASE_OUTPUT_DIR, str(uuid.uuid4()))
        os.makedirs(dir_temp)
        for i, start_row in enumerate(range(0, total_linhas, linhas_por_arquivo)):
            bloco = df.iloc[start_row : start_row + linhas_por_arquivo]
            nome_bloco = f"{nome_base}_{i+1}.{formato_saida}"
            caminho_bloco = os.path.join(dir_temp, secure_filename(nome_bloco))
           
            # Salva o bloco no formato escolhido
            if formato_saida == 'csv':
                bloco.to_csv(caminho_bloco, index=False, sep=';', encoding='utf-8-sig')
            else: # Abrange .xlsx e .xls
                bloco.to_excel(caminho_bloco, index=False)
       
        caminho_zip = os.path.join(BASE_OUTPUT_DIR, f"{secure_filename(nome_base)}_dividido.zip")
        with zipfile.ZipFile(caminho_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in Path(dir_temp).glob('*'):
                zipf.write(file_path, arcname=file_path.name)
       
        shutil.rmtree(dir_temp)
       
        # Envia o arquivo .zip para o usuário.
        return send_file(
            caminho_zip,
            as_attachment=True,
            download_name=os.path.basename(caminho_zip), # garante nome correto
            mimetype="application/octet-stream",
            etag=False,
            conditional=False,
            max_age=0
        )
    except Exception as e:
        # ALTERAÇÃO PRINCIPAL: Em caso de erro, retorna uma resposta JSON
        # em vez de redirecionar a página.
        app.logger.error(f"Erro em /process/dividir: {e}")
        return jsonify({"success": False, "error": f"Ocorreu um erro no servidor: {e}"}), 500
    

@app.route('/process/cruzar', methods=['POST'])
@require_login
def process_cruzar():
    try:
        if 'primaria' not in request.files or not request.files['primaria'].filename:
            return jsonify({"success": False, "error": "A planilha primária não foi enviada."}), 400
        if 'secundaria' not in request.files or not request.files['secundaria'].filename:
            return jsonify({"success": False, "error": "A planilha secundária não foi enviada."}), 400

        file_primaria   = request.files['primaria']
        file_secundaria = request.files['secundaria']
        coluna_primaria   = request.form.get('coluna_primaria')
        coluna_secundaria = request.form.get('coluna_secundaria')
        nome_saida = (request.form.get('nome_saida') or 'saida.xlsx').strip() or 'saida.xlsx'

        new_column_name = request.form.get('new_column_name', 'Verificacao_Planilhex')
        found_value     = request.form.get('found_value', 'ENCONTRADO')
        not_found_value = request.form.get('not_found_value', 'NAO ENCONTRADO')

        if not coluna_primaria or not coluna_secundaria:
            return jsonify({"success": False, "error": "É necessário selecionar a coluna base para ambas as planilhas."}), 400

        df_primaria   = ler_planilha(file_primaria)
        df_secundaria = ler_planilha(file_secundaria)

        if coluna_primaria not in df_primaria.columns:
            return jsonify({"success": False, "error": f"Coluna '{coluna_primaria}' não encontrada na planilha primária."}), 400
        if coluna_secundaria not in df_secundaria.columns:
            return jsonify({"success": False, "error": f"Coluna '{coluna_secundaria}' não encontrada na planilha secundária."}), 400

        # conjuntos normalizados da primária
        prim_text   = set(df_primaria[coluna_primaria].map(_norm_text));   prim_text.discard("")
        prim_digits = set(df_primaria[coluna_primaria].map(_digits_only)); prim_digits.discard("")

        # séries normalizadas da secundária
        sec_text   = df_secundaria[coluna_secundaria].map(_norm_text)
        sec_digits = df_secundaria[coluna_secundaria].map(_digits_only)

        # match por texto normalizado OU por dígitos
        mask = sec_text.isin(prim_text) | sec_digits.isin(prim_digits)
        df_secundaria[new_column_name] = mask.map({True: found_value, False: not_found_value})

        app.logger.info("[cruzar] prim_text=%d prim_digits=%d matches=%d total=%d",
                        len(prim_text), len(prim_digits), int(mask.sum()), len(mask))

        caminho_saida = os.path.join(BASE_OUTPUT_DIR, secure_filename(nome_saida))
        if nome_saida.lower().endswith('.csv'):
            df_secundaria.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8-sig')
        else:
            df_secundaria.to_excel(caminho_saida, index=False)

        return send_file(
            caminho_saida,
            as_attachment=True,
            download_name=os.path.basename(caminho_saida),
            mimetype="application/octet-stream",
            etag=False, conditional=False, max_age=0
        )

    except Exception as e:
        app.logger.error(f"Erro em /process/cruzar: {e}")
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

