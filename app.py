from flask import Flask, render_template
import psycopg2

app = Flask(__name__)

def conectar():
    conn = psycopg2.connect(
        host="localhost",
        database="postos_gasolina",
        user="postgres",
        password="G*07f00o",
        port="5432",
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

    return render_template(
        "index.html",
        media=round(dados[0],2),
        maior=round(dados[1],2),
        menor=round(dados[2],2),
        total=dados[3]
    )


# 1 - Menor e maior preço
@app.route("/menor_maior")
def menor_maior():

    conn = conectar()
    cursor = conn.cursor()

    query = """
    SELECT 
        c.tipo_combustivel,

        MIN(cp.valor_combustivel) AS menor_preco,
        (SELECT p.nome_posto
         FROM coleta_preco cp2
         JOIN posto p ON cp2.id_posto = p.id_posto
         WHERE cp2.id_combustivel = c.id_combustivel
         ORDER BY cp2.valor_combustivel ASC
         LIMIT 1) AS posto_menor,

        MAX(cp.valor_combustivel) AS maior_preco,
        (SELECT p.nome_posto
         FROM coleta_preco cp3
         JOIN posto p ON cp3.id_posto = p.id_posto
         WHERE cp3.id_combustivel = c.id_combustivel
         ORDER BY cp3.valor_combustivel DESC
         LIMIT 1) AS posto_maior

    FROM coleta_preco cp
    JOIN combustivel c ON cp.id_combustivel = c.id_combustivel
    GROUP BY c.tipo_combustivel, c.id_combustivel
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