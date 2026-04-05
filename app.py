from flask import Flask, render_template
import psycopg2
import os

app = Flask(__name__)

def conectar():
    conn = psycopg2.connect(
        os.environ.get("DATABASE_URL"),
        options="-c client_encoding=UTF8"
    )
    return conn

# Página inicial
@app.route("/")
def index():

    conn = conectar()
    cursor = conn.cursor()

    query = """
    SELECT 
        AVG(valor_combustivel),
        MAX(valor_combustivel),
        MIN(valor_combustivel),
        COUNT(*)
    FROM coleta_preco
    """

    cursor.execute(query)
    dados = cursor.fetchone()

    cursor.close()
    conn.close()

    if dados:
        media = round(dados[0], 2) if dados[0] else 0
        maior = round(dados[1], 2) if dados[1] else 0
        menor = round(dados[2], 2) if dados[2] else 0
        total = dados[3] if dados[3] else 0
    else:
        media = 0
        maior = 0
        menor = 0
        total = 0

    return render_template(
        "index.html",
        media=media,
        maior=maior,
        menor=menor,
        total=total
)


# 1 - Menor e maior preço
@app.route("/menor_maior")
def menor_maior():

    conn = conectar()
    cursor = conn.cursor()

    query = """
    SELECT 
        p.nome_posto,
        c.tipo_combustivel,

        MAX(cp.valor_combustivel) AS maior_preco,
        MIN(cp.valor_combustivel) AS menor_preco,

        MAX(cp.data_coleta) FILTER (
            WHERE cp.valor_combustivel = (
                SELECT MAX(cp2.valor_combustivel)
                FROM coleta_preco cp2
                WHERE cp2.id_posto = cp.id_posto 
                AND cp2.id_combustivel = cp.id_combustivel
            )
        ) AS data_maior,

        MAX(cp.data_coleta) FILTER (
            WHERE cp.valor_combustivel = (
                SELECT MIN(cp2.valor_combustivel)
                FROM coleta_preco cp2
                WHERE cp2.id_posto = cp.id_posto 
                AND cp2.id_combustivel = cp.id_combustivel
            )
        ) AS data_menor

    FROM coleta_preco cp
    JOIN posto p ON cp.id_posto = p.id_posto
    JOIN combustivel c ON cp.id_combustivel = c.id_combustivel

    GROUP BY 
        p.nome_posto,
        c.tipo_combustivel

    ORDER BY 
        p.nome_posto,
        c.tipo_combustivel;
    """

    cursor.execute(query)
    dados = cursor.fetchall()

    cursor.close()
    conn.close()

    return render_template("menor_maior.html", dados=dados)

# 2 - Média por posto
@app.route("/media_posto")
def media_posto():

    conn = conectar()
    cursor = conn.cursor()

    query = """
    SELECT 
        p.nome_posto,
        c.tipo_combustivel,
        AVG(cp.valor_combustivel),
        COUNT(cp.id_coleta)
    FROM coleta_preco cp
    JOIN posto p ON cp.id_posto = p.id_posto
    JOIN combustivel c ON cp.id_combustivel = c.id_combustivel
    GROUP BY p.nome_posto, c.tipo_combustivel
    """

    cursor.execute(query)
    dados = cursor.fetchall()

    cursor.close()
    conn.close()

    return render_template("media_posto.html", dados=dados)


# 3 - Preço mais recente
@app.route("/preco_recente")
def preco_recente():

    conn = conectar()
    cursor = conn.cursor()

    query = """
    SELECT 
        p.nome_posto,
        c.tipo_combustivel,
        cp.valor_combustivel,
        cp.data_coleta
    FROM coleta_preco cp
    JOIN posto p ON cp.id_posto = p.id_posto
    JOIN combustivel c ON cp.id_combustivel = c.id_combustivel
    ORDER BY cp.data_coleta DESC
    """

    cursor.execute(query)
    dados = cursor.fetchall()

    cursor.close()
    conn.close()

    return render_template("preco_recente.html", dados=dados)


# 4 - Evolução de preços
@app.route("/evolucao")
def evolucao():

    conn = conectar()
    cursor = conn.cursor()

    query = """
    SELECT 
        c.tipo_combustivel,
        cp.valor_combustivel,
        cp.data_coleta
    FROM coleta_preco cp
    JOIN combustivel c ON cp.id_combustivel = c.id_combustivel
    ORDER BY cp.data_coleta
    """

    cursor.execute(query)
    dados = cursor.fetchall()

    cursor.close()
    conn.close()

    # Converter datas para string
    dados_convertidos = []

    for linha in dados:
        dados_convertidos.append([
            linha[0],
            float(linha[1]),
            linha[2].strftime('%d/%m/%Y')
        ])

    return render_template("evolucao.html", dados=dados_convertidos)


if __name__ == "__main__":
    app.run(debug=True)