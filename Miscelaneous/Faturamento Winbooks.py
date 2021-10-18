# -*- coding: utf-8 -*-
"""
Le dados faturamento Winbooks.

Created on Thu Jan 29 18:38:00 2021

@author: Celso Leite
"""
# %% Imports
from typing import Sequence
import pandas as pd

PATH = 'C:\\Users\\cpcle\\OneDrive\\Área de Trabalho\\' +\
    'Veneta\\Dados Winbooks\\'


def faturamento_mensal(mes):
    """
    Gera arquivo de faturamento mensal Winbooks.
    """
    # Constantes
    ARQ_1 = PATH + '08 - relatorio (detalhe clientes) - ' + mes + '.xls'
    ARQ_2 = PATH + '06 - relatorio detalhado - lista - ' + mes + '.xlsx'

    clientes = pd.read_excel(ARQ_1, sheet_name=0,
                             usecols={0, 1, 3, 6, 7, 10},
                             skiprows=3,
                             names=['Nome', 'Qtd', 'Total',
                                    'Cidade', 'Estado', 'CNPJ'],
                             dtype={'Nome': 'str', 'Qtd': 'int64',
                                    'Total': 'float64', 'Cidade': 'str',
                                    'Estado': 'str', 'CNPJ': 'str'})

    # Lê arquivo de livros
    livros = pd.read_excel(ARQ_2, sheet_name=0,
                           usecols={0, 1, 2, 5, 6},
                           names=['Nome', 'Titulo', 'ISBN', 'Qtd',
                                  'Total'],
                           skiprows=5,
                           dtype={'Nome': 'str', 'Titulo': 'str',
                                  'ISBN': 'str', 'Qtd': 'str',
                                  'Total': 'float64'},
                           na_values=[None, ''],
                           )

    livros.dropna(axis=0, subset=['ISBN'], inplace=True)
    livros['Qtd'] = pd.to_numeric(livros['Qtd'])
    return pd.merge(livros, clientes[['Nome', 'Cidade', 'Estado', 'CNPJ']],
                    how='left', on='Nome')


# %% Roda
vendas = pd.DataFrame()

for a in ['2020-{:02d}'.format(mes) for mes in range(1, 13)]:
    df = faturamento_mensal(a)
    df['Mes'] = pd.to_datetime(a + '-01', format='%Y-%m-%d')
    vendas = vendas.append(df, ignore_index=True)

# %% Resumo
res = vendas[['Mes', 'Total', 'Qtd']].groupby(
    pd.Grouper(key='Mes', freq='M')).sum()
res.to_excel(PATH + 'Resumo.xlsx')

# %% Access

notas = pd.read_pickle(
    r'C:\Users\cpcle\OneDrive\Documentos\Celso\Python\veneta-dash\data\Notas.pkl')

res = notas[['Emissao', 'Vendas', 'Receita Líquida']].groupby(
    pd.Grouper(key='Emissao', freq='M')).sum()

res.to_excel(PATH + 'Resumo Acc.xlsx')

# %%
res = notas[notas['Titulo'] == 'Frete'][['Emissao', 'Vendas', 'Receita Líquida']].groupby(
    pd.Grouper(key='Emissao', freq='M')).sum()
res.to_excel(PATH + 'Resumo Acc2.xlsx')

# %%
