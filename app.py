import os
import pyodbc
import pandas as pd
import numpy as np
from flask import Flask, render_template, jsonify
from datetime import datetime, date

app = Flask(__name__)

# =====================================================
# CONFIGURAÇÕES E CONEXÃO
# =====================================================
def conectar_sql_server():
    """Estabelece conexão com o SQL Server"""
    try:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={os.getenv('DB_SERVER', 'localhost')};"
            f"DATABASE={os.getenv('DB_NAME', 'DMD')};"
            f"UID={os.getenv('DB_USER', 'sa')};"
            f"PWD={os.getenv('DB_PASSWORD', 'arte171721')}"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        print(f"Erro fatal na conexão SQL: {e}")
        return None

def json_safe(value):
    """Converte tipos numpy/pandas/data para tipos nativos Python seguros para JSON"""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (np.integer, int)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.strftime('%d/%m/%Y')
    return str(value).strip()

# =====================================================
# MOTOR DE ANÁLISE DE CRÉDITO
# =====================================================
def calcular_risco_cliente(row):
    """
    Calcula score e gera análise baseada nas colunas da tabela clien.
    Score Base: 1000 pontos.
    """
    score = 1000
    sugestoes = []
    tendencias = []
    
    # Extração e normalização de dados
    limite = float(row.get('Limite_Credito') or 0)
    debito = float(row.get('Total_Debito') or 0)
    atraso_atual = int(row.get('Atraso_Atual') or 0)
    maior_atraso = int(row.get('Maior_Atraso') or 0)
    bloqueado = int(row.get('Bloqueado') or 0)
    data_cadastro = row.get('Data_Cadastro')
    renda_presumida = float(row.get('Vlr_LimCreAnt') or 0) # Exemplo de uso de campo histórico

    # 1. Fator Bloqueio
    if bloqueado == 1:
        score = 0
        motivo = row.get('Motivo_Bloqueio') or "Não especificado"
        sugestoes.append(f"CRÍTICO: Cliente bloqueado no sistema. Motivo: {motivo}")
        return 0, "risco-muito-alto", "Bloqueado", sugestoes, ["Cliente inativo para crédito"]

    # 2. Fator Atraso Atual (Peso Alto)
    if atraso_atual > 0:
        penalidade = atraso_atual * 10
        score -= penalidade
        tendencias.append(f"Inadimplência ativa: {atraso_atual} dias de atraso.")
        sugestoes.append("Suspender novas vendas até regularização.")
        if atraso_atual > 30:
            sugestoes.append("Encaminhar para departamento de cobrança jurídica.")

    # 3. Fator Histórico de Atraso (Peso Médio)
    if maior_atraso > 0:
        # Penaliza histórico, mas menos que atraso atual
        score -= (maior_atraso * 2) 
        if maior_atraso > 10:
            tendencias.append(f"Histórico de pagamentos instável (Maior atraso: {maior_atraso} dias).")

    # 4. Fator Utilização de Limite
    utilizacao = (debito / limite * 100) if limite > 0 else 0
    if utilizacao > 95:
        score -= 150
        tendencias.append("Limite de crédito tomado quase totalmente.")
        sugestoes.append("Não autorizar aumento de limite no momento.")
    elif utilizacao > 80:
        score -= 50

    # 5. Fator Tempo de Casa (Fidelidade)
    if data_cadastro:
        if isinstance(data_cadastro, str):
            # Tenta converter se vier string
            try: data_cadastro = datetime.strptime(data_cadastro, '%Y-%m-%d')
            except: pass
            
        if isinstance(data_cadastro, (datetime, date)):
            dias_cliente = (datetime.now() - pd.to_datetime(data_cadastro)).days
            if dias_cliente < 90:
                score = min(score, 600) # Teto para clientes novos
                tendencias.append("Cliente novo (menos de 3 meses). Histórico insuficiente.")
            elif dias_cliente > 730: # 2 anos
                score += 50 # Bônus fidelidade

    # Normalização final do Score (0 a 1000)
    score = max(0, min(1000, int(score)))

    # Definição da Classificação
    if score >= 800:
        cor = "risco-baixo"
        classificacao = "Baixo Risco"
        if utilizacao < 50:
            sugestoes.append("Cliente elegível para aumento de limite.")
    elif score >= 500:
        cor = "risco-moderado"
        classificacao = "Risco Médio"
        sugestoes.append("Vendas a prazo permitidas com cautela.")
    else:
        cor = "risco-alto"
        classificacao = "Alto Risco"
        sugestoes.append("Sugerido venda somente à vista ou cartão.")

    return score, cor, classificacao, sugestoes, tendencias

# =====================================================
# ROTAS DA APLICAÇÃO
# =====================================================
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/clientes')
def clientes():
    conn = conectar_sql_server()
    if not conn:
        return jsonify([])

    try:
        # Selecionamos apenas colunas necessárias para a grade para performance
        cols = [
            "Codigo", "Razao_Social", "Fantasia", "Cgc_Cpf", 
            "Limite_Credito", "Total_Debito", "Atraso_Atual", 
            "Maior_Atraso", "Bloqueado", "Data_Cadastro"
        ]
        query = f"SELECT {', '.join(cols)} FROM clien"
        df = pd.read_sql(query, conn)
        
        data = []
        for _, row in df.iterrows():
            # Cálculo simplificado para a tabela (cache ou on-the-fly)
            score_val, cor, classif, _, _ = calcular_risco_cliente(row)
            
            item = {
                "Codigo": json_safe(row['Codigo']),
                "Razao_Social": json_safe(row['Razao_Social']),
                "Limite_Credito": json_safe(row['Limite_Credito']),
                "Total_Debito": json_safe(row['Total_Debito']),
                "Atraso_Atual": json_safe(row['Atraso_Atual']),
                "score": score_val,
                "classificacao": classif,
                "cor": cor.replace('risco-', '') # Remove prefixo para o template class
            }
            data.append(item)

        return jsonify(data)
    except Exception as e:
        print(f"Erro em /clientes: {e}")
        return jsonify([])
    finally:
        conn.close()

@app.route('/dashboard')
def dashboard():
    conn = conectar_sql_server()
    if not conn:
        return jsonify({"error": "Falha conexao"})

    try:
        # Queries agregadas no SQL são mais rápidas que processar Pandas
        query = """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN Bloqueado = 1 THEN 1 ELSE 0 END) as bloqueados,
                AVG(CASE WHEN Limite_Credito > 0 THEN (Total_Debito/Limite_Credito)*100 ELSE 0 END) as uso_medio
            FROM clien
        """
        df_stats = pd.read_sql(query, conn)
        
        # Para calcular a média de score, precisamos iterar (ou criar uma func no SQL)
        # Faremos uma amostragem ou cálculo simplificado aqui
        df_all = pd.read_sql("SELECT Limite_Credito, Total_Debito, Atraso_Atual, Maior_Atraso, Bloqueado, Data_Cadastro FROM clien", conn)
        scores = []
        classificacoes_count = {"Baixo Risco": 0, "Risco Médio": 0, "Alto Risco": 0, "Bloqueado": 0}
        
        top_clientes = []

        for _, row in df_all.iterrows():
            s, _, c, _, _ = calcular_risco_cliente(row)
            scores.append(s)
            
            # Contagem para o gráfico
            if c == "Baixo Risco": classificacoes_count["Baixo Risco"] += 1
            elif c == "Risco Médio": classificacoes_count["Risco Médio"] += 1
            elif c == "Alto Risco": classificacoes_count["Alto Risco"] += 1
            else: classificacoes_count["Bloqueado"] += 1

        # Identificar Top Clientes (simulação simples baseada em quem tem mais crédito e bom score)
        # Em produção, faríamos sorting adequado
        media_score = int(sum(scores) / len(scores)) if scores else 0
        
        return jsonify({
            "total_clientes": int(df_stats.iloc[0]['total']),
            "clientes_bloqueados": int(df_stats.iloc[0]['bloqueados']),
            "media_score": media_score,
            "media_utilizacao": round(float(df_stats.iloc[0]['uso_medio']), 1),
            "classificacoes": classificacoes_count,
            "top_clientes": [] # Pode implementar lógica de top 5 aqui se desejar
        })
    except Exception as e:
        print(f"Erro dashboard: {e}")
        return jsonify({"error": str(e)})
    finally:
        conn.close()

@app.route('/analise/<int:codigo>')
def analise_detalhada(codigo):
    conn = conectar_sql_server()
    if not conn: return jsonify({"error": "Sem conexão"})

    try:
        # Pega TUDO do cliente para análise profunda
        query = f"SELECT * FROM clien WHERE Codigo = {codigo}"
        df = pd.read_sql(query, conn)

        if df.empty:
            return jsonify({"error": "Cliente não encontrado"})

        row = df.iloc[0]
        score, cor, classificacao, sugestoes, tendencias = calcular_risco_cliente(row)
        
        # Dados Financeiros Calculados
        limite = float(row['Limite_Credito'] or 0)
        debito = float(row['Total_Debito'] or 0)
        utilizacao = round((debito / limite * 100), 2) if limite > 0 else 0
        
        limite_sugerido = "Manter Atual"
        if score > 850: limite_sugerido = f"R$ {limite * 1.20:,.2f}"
        if score < 400: limite_sugerido = "Reduzir ou Bloquear"

        response = {
            "cliente": {k: json_safe(v) for k, v in row.items()},
            "score": score,
            "cor_risco": cor,
            "classificacao_risco": classificacao,
            "limite_sugerido": limite_sugerido,
            "indicadores": {
                "utilizacao_limite": utilizacao,
                "dias_atraso_atual": int(row['Atraso_Atual'] or 0),
                "maior_atraso_historico": int(row['Maior_Atraso'] or 0),
                "media_atraso": int(row['Atraso_MedAtu'] or 0)
            },
            "tendencias_pagamento": tendencias,
            "sugestoes": sugestoes,
            "data_analise": datetime.now().strftime("%d/%m/%Y %H:%M")
        }
        
        return jsonify(response)

    except Exception as e:
        print(f"Erro na análise detalhada: {e}")
        return jsonify({"error": str(e)})
    finally:
        conn.close()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)