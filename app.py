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
        data
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
        .pipe(lambda x: x.loc[x['variable'] == 'Var. mensal (%)'])  # Filtra apenas "Var. mensal (%)"
        .pipe(lambda x: x.loc[x.date > '2016-01-01'])
    )

    # Formata a data para dd/mm/yyyy
    ipca['date'] = ipca['date'].dt.strftime('%d/%m/%Y')
    ipca['Mes-ano'] = pd.to_datetime(ipca['date'], format='%d/%m/%Y').dt.to_period('M').astype(str)
    return ipca


# Define o namespace
ns = api.namespace("ipca", description="Operações relacionadas ao IPCA")


# Recurso para retornar apenas "Var. mensal (%)" e a data formatada
@ns.route("/mensal")
class IPCAVarMensal(Resource):
    def get(self):
        """Retorna a variável 'Var. mensal (%)' com data no formato dd/mm/yyyy"""
        ipca_data = process_ipca_data()

        # Seleciona apenas as colunas necessárias
        result = ipca_data[['variable', 'value', 'date', 'Mes-ano']].to_dict(orient="records")
        return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True)
