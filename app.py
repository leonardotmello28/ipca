from flask import Flask, jsonify
from flask_restx import Api, Resource
import sidrapy
import pandas as pd

app = Flask(__name__)
api = Api(app, version="1.0", title="IPCA API", description="API para cálculo e consulta de IPCA")

# Função para obter e processar os dados
def process_ipca_data():
    # Obtendo dados da API do SIDRA
    data = sidrapy.get_table(
        table_code='1737',
        territorial_level='1',
        ibge_territorial_code='all',
        period='last%20472',
        header='y',
        variable='63, 69, 2263, 2264, 2265'
    )

    # Processamento do DataFrame
    ipca = (
        pd.DataFrame(data)
        .loc[1:, ['V', 'D2C', 'D3N']]
        .rename(columns={'V': 'value', 'D2C': 'date', 'D3N': 'variable'})
        .assign(
            variable=lambda x: x['variable'].replace({
                'IPCA - Variação mensal': 'Var. mensal (%)',
                'IPCA - Variação acumulada no ano': 'Var. acumulada no ano (%)'
            }),
            date=lambda x: pd.to_datetime(x['date'], format="%Y%m"),
            value=lambda x: x['value'].astype(float)
        )
        .pipe(lambda x: x.loc[x.date > '2016-01-01'])
    )

    # Adicionando colunas
    ipca['month_year'] = ipca['date'].dt.strftime('%m/%Y')
    ipca['year'] = ipca['date'].dt.strftime('%Y')
    return ipca

# Função para calcular o índice multiplicativo
def calculate_multiplicative_index(ipca_data):
    # Referência a variável específica
    cit_ref = ipca_data.loc[1866, 'variable']
    val_ipca = ipca_data[ipca_data['variable'] == cit_ref]

    # Seleção de colunas específicas
    val_ipca = val_ipca[['variable', 'value', 'date']]

    # Calcula o fator
    val_ipca['Fator'] = (1 + val_ipca['value'] / 100)

    # Inicializa a coluna 'Indice Multiplicativo' com zeros
    val_ipca['Indice Multiplicativo'] = 0.0

    # Calcula o índice multiplicativo
    n = len(val_ipca)
    val_ipca.iloc[n - 1, val_ipca.columns.get_loc('Indice Multiplicativo')] = val_ipca.iloc[n - 1, val_ipca.columns.get_loc('Fator')]

    for x in range(n - 2, -1, -1):
        val_ipca.iloc[x, val_ipca.columns.get_loc('Indice Multiplicativo')] = (
            val_ipca.iloc[x + 1, val_ipca.columns.get_loc('Indice Multiplicativo')] *
            val_ipca.iloc[x, val_ipca.columns.get_loc('Fator')]
        )

    return val_ipca[['value', 'date', 'Fator', 'Indice Multiplicativo']]

# Namespace para a API
ns = api.namespace("ipca", description="Operações relacionadas ao IPCA")

# Recurso para obter os dados processados
@ns.route("/data")
class IPCAData(Resource):
    def get(self):
        """Retorna os dados do IPCA processados"""
        ipca_data = process_ipca_data()
        return jsonify(ipca_data.to_dict(orient="records"))

# Recurso para calcular o índice multiplicativo
@ns.route("/indice-multiplicativo")
class IPCAIndice(Resource):
    def get(self):
        """Retorna os dados com o índice multiplicativo calculado"""
        ipca_data = process_ipca_data()
        val_ipca = calculate_multiplicative_index(ipca_data)
        return jsonify(val_ipca.to_dict(orient="records"))

if __name__ == "__main__":
    app.run(debug=True)
