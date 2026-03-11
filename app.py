from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import pyodbc
import json
import os
from datetime import datetime, timedelta
import traceback

app = Flask(__name__)
app.secret_key = 'chave_seguranca_logistica'

CONFIG_FILE = 'config.json'

def carregar_config():
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    except:
        return {}

def conectar_banco():
    config = carregar_config()
    if 'database' not in config: return None
    db = config['database']
    try:
        conn_str = (f"DRIVER={{SQL Server}};SERVER={db['server']};"
                    f"DATABASE={db['database']};UID={db['username']};PWD={db['password']}")
        return pyodbc.connect(conn_str)
    except:
        return None

@app.route('/')
def index():
    return redirect(url_for('buscar_produto'))

@app.route('/conexao', methods=['GET', 'POST'])
def conexao():
    config = carregar_config()
    if request.method == 'POST':
        config['database'] = {k: request.form.get(k) for k in ['server', 'database', 'username', 'password']}
        with open(CONFIG_FILE, 'w') as f: json.dump(config, f, indent=4)
        return redirect(url_for('buscar_produto'))
    return render_template('conexao.html', config=config.get('database', {}))

@app.route('/buscar', methods=['GET', 'POST'])
def buscar_produto():
    conn = conectar_banco()
    if not conn: return redirect(url_for('conexao'))
    resultados = []
    termo = request.form.get('termo_busca', '').strip() if request.method == 'POST' else ""
    if termo:
        cursor = conn.cursor()
        query = "SELECT DISTINCT Codigo, Descricao, Cod_EAN FROM PRODU WHERE CAST(Codigo AS VARCHAR) LIKE ? OR Cod_EAN LIKE ? OR Descricao LIKE ? ORDER BY Descricao"
        cursor.execute(query, f'%{termo}%', f'%{termo}%', f'%{termo}%')
        resultados = [dict(zip([c[0] for c in cursor.description], r)) for r in cursor.fetchall()]
    conn.close()
    return render_template('buscar_produto.html', resultados=resultados, termo_busca=termo)

@app.route('/produto/<int:codigo>', methods=['GET', 'POST'])
def detalhes_produto(codigo):
    conn = conectar_banco()
    if not conn: return redirect(url_for('conexao'))
    try:
        cursor = conn.cursor()
        
        # 1. BUSCA PREÇO E DADOS BÁSICOS (POLÍTICA 432)
        query_prod = """
        SELECT pc.Cod_Produt, pr.Descricao, fa.Fantasia, pr.Unidade_Venda, pr.Cod_EAN,
        Prc_Venda_V = CASE WHEN ROUND(pc.Prc_Promoc * 100, 2) > 0 THEN
            CASE WHEN ROUND(pc.Per_Descon * 100, 2) > 0 THEN ROUND(pc.Prc_Promoc * (1 - pc.Per_Descon / 100), 2)
                 WHEN ROUND(pl.Per_AcrAutPrc * 100, 2) > 0 THEN ROUND(pc.Prc_Promoc * (1 + pl.Per_AcrAutPrc / 100), 2)
                 WHEN ROUND(pl.Per_DscAutPrc * 100, 2) > 0 THEN ROUND(pc.Prc_Promoc * (1 - pl.Per_DscAutPrc / 100), 2)
                 ELSE pc.Prc_Promoc END
        ELSE
            CASE WHEN ROUND(px.Prc_Venda * 100, 2) > 0 AND ROUND(pc.Per_Descon * 100, 2) > 0 THEN ROUND(px.Prc_Venda * (1 - pc.Per_Descon / 100), 2)
                 WHEN ROUND(pl.Per_AcrAutPrc * 100, 2) > 0 THEN ROUND(px.Prc_Venda * (1 + pl.Per_AcrAutPrc / 100), 2)
                 WHEN ROUND(pl.Per_DscAutPrc * 100, 2) > 0 THEN ROUND(px.Prc_Venda * (1 - pl.Per_DscAutPrc / 100), 2)
                 ELSE ISNULL(px.Prc_Venda, 0) END END
        FROM PCXPR pc INNER JOIN PRODU pr ON pc.Cod_Produt = pr.Codigo INNER JOIN PRXAP pa ON pc.Cod_Produt = pa.Cod_Produt AND pa.Flg_Padrao = 1
        INNER JOIN FABRI fa ON pr.Cod_Fabricante = fa.Codigo LEFT JOIN PRXES px ON pc.Cod_Produt = px.Cod_Produt AND px.Cod_Estabe = 0
        INNER JOIN POCOM pl ON pc.Id_PolCom = pl.Id_PolCom WHERE pc.Cod_Produt = ? AND pc.Id_PolCom = 432
        """
        cursor.execute(query_prod, codigo)
        res = cursor.fetchone()
        if not res: return "Produto sem precificação na política 432", 404
        produto = {'codigo': res[0], 'descricao': res[1], 'fabricante': res[2], 'unidade': res[3], 'cod_ean': res[4], 'preco': res[5]}

        # 2. BUSCA ESTOQUE COM LOCAL FÍSICO (CONFORME ANEXO)
        query_est = """
        SELECT 
            dp.Cod_Lote, dp.Dat_Vencim, dp.Qtd_Fisico, dp.Cod_Dep AS Deposito, 
            Loc_Fis = dbo.FN_FormataEndereco(dp.Num_Rua, dp.Num_Col, dp.Num_Niv, dp.Num_Apt),
            'Fisico' as Origem 
        FROM PRLTL dp 
        WHERE dp.Cod_Estabe = 0 AND dp.Cod_Produt = ? AND dp.Qtd_Fisico > 0
        UNION ALL
        SELECT 
            fr.Cod_Lote, fr.Dat_Vencim, fr.Qtd_Fisico, fr.Cod_Dep AS Deposito, 
            Loc_Fis = d.Cod_LocFis,
            'Lote' as Origem 
        FROM PRLOT fr 
        INNER JOIN DPXPR d ON (fr.Cod_Estabe = d.Cod_Estabe AND fr.Cod_Dep = d.Cod_Dep AND fr.Cod_Produt = d.Cod_Produt)
        WHERE fr.Cod_Estabe = 0 AND fr.Cod_Produt = ? AND fr.Qtd_Fisico > 0
        """
        cursor.execute(query_est, codigo, codigo)
        estoque = [dict(zip([c[0] for c in cursor.description], r)) for r in cursor.fetchall()]

        # 3. BUSCA ÚLTIMAS ENTRADAS
        d_fim = datetime.now()
        d_ini = d_fim - timedelta(days=90)
        if request.method == 'POST':
            try: d_ini = datetime.strptime(request.form.get('data_inicio'), '%Y-%m-%d')
            except: pass
            try: d_fim = datetime.strptime(request.form.get('data_fim'), '%Y-%m-%d')
            except: pass

        query_nf = """
        SELECT it.Dat_Movimento, cb.Numero, cb.Tip_NF, it.Cod_Lote, (it.Qtd_Pedido + it.Qtd_Bonificacao) as Total,
        Emitente = (SELECT Razao_Social FROM FORNE WHERE Codigo = cb.Cod_EmiFornec)
        FROM NFECB cb INNER JOIN NFEIT it ON cb.Protocolo = it.Protocolo
        WHERE cb.Status = 'F' AND it.Cod_Produto = ? AND it.Dat_Movimento BETWEEN ? AND ? AND cb.Tip_NF = 'C'
        ORDER BY it.Dat_Movimento DESC
        """
        cursor.execute(query_nf, codigo, d_ini.strftime('%Y%m%d'), d_fim.strftime('%Y%m%d 23:59'))
        entradas = [dict(zip([c[0] for c in cursor.description], r)) for r in cursor.fetchall()]

        conn.close()
        return render_template('resultado_produto.html', produto=produto, estoque=estoque, entradas=entradas,
                               data_inicio=d_ini.strftime('%Y-%m-%d'), data_fim=d_fim.strftime('%Y-%m-%d'),
                               data_atual=datetime.now())
    except Exception as e:
        return f"Erro: {str(e)}", 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)