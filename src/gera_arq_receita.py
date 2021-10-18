# -*- coding: utf-8 -*-
"""
Calcula faturamento mensal, dividindo Lançamentos x Catálogo

Created on Tue Feb 28 20:39:00 2021

@author: Celso Leite
"""
import pandas as pd

PATH = 'C:/Users/cpcle/OneDrive/Documentos/Celso/Python/veneta-dash/data/'
PATH_DT = 'C:/Users/cpcle/OneDrive/Área de Trabalho/'
ISBN = PATH + 'Cadastro de Produtos.xlsx'
CADASTRO = PATH + 'Tabela_Veneta_2020.xlsx'
NOTAS = PATH + 'Notas.pkl'
FATURAMENTO = PATH_DT + 'Resumo Faturamento.xlsx'
FAT_PICKLE = PATH + 'receita.pkl'

# Lista dos títulos do PNLD
PNLD = ['CAROLINA - 0991L18606130IL',
        'ANGOLA JANGA - 1132L18606130IL',
        'CUMBE - 1146L18606130IL',
        'Angola Janga - 1132L - e-pub',
        'Carolina - 0991L - e-pub',
        'Cumbe - 1146L - e-pub']


def exclui_excecoes(a):

    return \
        [x for x in a if x not in
            ['Frete',
             'Camiseta Veneta',
             'Livros - REF. 97885631373 - Le Caravage T01 Bresilien',
             'Ilustracao original - LN',
             'Jogo de futebol de botao - LN',
             'Risografia - LN',
             'VIVA A REVOLUCAO! - e-pub']
         ]


def faturamento_mensal():
    """
    Calcula o faturamento mensal com e sem PNLD.
    """
    # Abre arquivo de Notas e agrupa por Mês
    notas = pd.read_pickle(NOTAS)
    resumo_com = notas.groupby(
        pd.Grouper(key='Emissao', freq='MS')).\
        aggregate({'Receita Líquida': 'sum'})

    resumo_sem = notas[~notas['Titulo'].isin(PNLD)].groupby(
        pd.Grouper(key='Emissao', freq='MS')).\
        aggregate({'Receita Líquida': 'sum'})

    pd.concat([resumo_com, resumo_sem], axis=1).to_excel(FATURAMENTO)


def faturamento_lanc_vs_cat():
    """
    Calcula Faturamento Mensal, agrupado por Lançamentos e Catálogo.
    """
    # Abre arquivos de Cadastro de Produtos (ISBN e Data de Lançamento)
    cad = pd.read_excel(CADASTRO, sheet_name=0,
                        usecols='A,J', dtype={'ISBN': str}, parse_dates=['Lançamento']).dropna()
    isbn = pd.read_excel(ISBN, sheet_name=0, usecols='A,D', dtype={
                         'ISBN': str, 'Título Excel': str, 'Título WB': str, 'Centro de Custo': str}).dropna().set_index(keys=['Título Excel'])

    # Abre arquivo de Notas e agrupa por Mês e Título
    resumo = pd.read_pickle(NOTAS).groupby(
        [pd.Grouper(key='Emissao', freq='MS'),
         'Titulo']).aggregate({'Receita Líquida': 'sum'})
    resumo.reset_index(inplace=True)

    # Separa Frete
    frete = resumo.loc[resumo['Titulo'] == 'Frete',
                       ['Emissao', 'Receita Líquida']]
    frete.rename({'Receita Líquida': 'Frete'}, axis=1, inplace=True)

    resumo = resumo.loc[(resumo['Receita Líquida'] != 0) &
                        (resumo['Titulo'] != 'Frete')]

    # Exclui Títulos do PNLD
    resumo = resumo[~resumo['Titulo'].isin(PNLD)]

    # Coloca ISBN
    resumo = resumo.merge(isbn, how='left', left_on='Titulo',
                          right_on='Título Excel')

    try:
        assert resumo['ISBN'].isna().sum() == 0

    except AssertionError:
        a = resumo.loc[resumo['ISBN'].isna(), 'Titulo'].unique()
        a = exclui_excecoes(a)
        if len(a) > 0:
            print('Falta cadastro do ISBN do título:\n{}'.format(a))
            exit()

    # Coloca Data de Lançamento
    resumo = resumo.merge(cad, how='left', on='ISBN')

    try:
        assert resumo['Lançamento'].isna().sum() == 0

    except AssertionError:
        a = resumo.loc[resumo['Lançamento'].isna(), 'Titulo'].unique()
        a = exclui_excecoes(a)
        if len(a) > 0:
            print('Falta cadastro do Lançamento do título:\n{}'.format(a))

    resumo['lanc'] = (
        (resumo['Emissao'].dt.year - resumo['Lançamento'].dt.year)*12 +
        (resumo['Emissao'].dt.month - resumo['Lançamento'].dt.month)
    )

    resumo['6meses'] = resumo['lanc'].map(lambda x: 'Lançamento' if (x < 6)
                                          else 'Catálogo')
    resumo['12meses'] = resumo['lanc'].map(lambda x: 'Lançamento' if (x < 12)
                                           else 'Catálogo')

    a = resumo.groupby([pd.Grouper(key='Emissao', freq='MS'),
                        '6meses']).aggregate({'Receita Líquida': 'sum'})\
        .reset_index().pivot(index='Emissao', columns='6meses', values='Receita Líquida')

    b = resumo.groupby([pd.Grouper(key='Emissao', freq='MS'),
                        '12meses']).aggregate({'Receita Líquida': 'sum'})\
        .reset_index().pivot(index='Emissao', columns='12meses', values='Receita Líquida')

    c = a.merge(b, on='Emissao', how='outer', suffixes=['_6M', '_12M'])

    c = c.merge(frete, on='Emissao', how='outer')
    c.to_excel(FATURAMENTO)
    c.set_index(['Emissao']).to_pickle(FAT_PICKLE)

    # a['Mês'] = a.index.month
    # a['Ano'] = a.index.year
    # a.pivot(index='Mês', columns='Ano').to_excel(FATURAMENTO)

    # a = resumo[resumo['6meses'] == 'Catálogo']
    # a = a.groupby([pd.Grouper(key='Emissao', freq='MS'), 'Titulo']
    #               ).aggregate({'Receita Líquida': 'sum'}).reset_index().to_excel(FATURAMENTO)


def verifica_lancamentos():
    cad = pd.read_excel(CADASTRO, sheet_name=0, usecols='A,B,J').dropna()
    cad.groupby([pd.Grouper(key='Lançamento', freq='MS')]
                ).aggregate({'ISBN': 'count'}).to_excel(FATURAMENTO)


def gera_receita():
    faturamento_mensal()
    faturamento_lanc_vs_cat()


if __name__ == '__main__':
    gera_receita()
