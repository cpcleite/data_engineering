# -*- coding: utf-8 -*-
"""
Verifica arquivo do Winbooks x Notas (access).

O arquivo Notas é formado da leitura dos XML`s das Notas Fiscais.
Os arquivos do Winbooks são os relatórios Nota a Nota - Livro a Livro (dados),
do módulo de faturamento, com a opção Vendas.

Created on Tue Feb 02 17:46:00 2021

@author: Celso Leite
"""
import pandas as pd

# Lê Arquivo Winbooks
PATH_W = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Faturamento\\'
PATH_N = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Python\\veneta-dash\\data\\'
PATH_P = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Despesas e Custos\\'
WINBOOKS = PATH_W + 'nota a nota - livro a livro - desde 2019 somente vendas.xlsx'
NOTAS = PATH_N + 'Notas.pkl'
COMP = PATH_W + 'comp.xlsx'
COMP2 = PATH_W + 'comp2.xlsx'
COMP3 = PATH_W + 'comp3.xlsx'
COMP4 = PATH_W + 'comp4.xlsx'
COMP5 = PATH_W + 'comp5.xlsx'
PROD = PATH_P + 'Cadastro de Produtos.xlsx'
ANO = [2019, 2020, 2021]

# Lê dados do winbooks "Nota a Nota - Livro a Livro - (Dados)"
winb = pd.read_excel(WINBOOKS, sheet_name=0, header=0,
                     dtype={'nof_nfinum': int,	'emissa': object, 'pedido':	str,
                            'nof_numero': str, 'pvdcli': str, 'razao': str, 'empresa': str, 'cfo_descri': str, 'ufe_sigla': str, 'cidade': str, 'fun_nome': str,
                            'fun_nome2': str, 'nof_frete': float, 'isbn': str, 'pro_descri': str, 'assunto': str, 'cla_codigo': str,	'pro_unidade': str, 'qtd': int, 'unit': float, 'total': float,	'codpro': str})


# Análise do arquivo de faturamento
df = winb.groupby('cfo_descri').agg({'total': 'sum', 'nof_frete': 'sum'})
print(df)

filtro = winb['cfo_descri'] == 'DEVOLUCAO DE VENDA'
winb.loc[filtro, 'nof_frete'] = - winb.loc[filtro, 'nof_frete']
winb.loc[filtro, 'total'] = - winb.loc[filtro, 'total']

df = winb.groupby('cfo_descri').agg({'total': 'sum', 'nof_frete': 'sum'})
print(df)

df = winb.groupby(pd.Grouper(key='emissa', freq='MS')).agg(
    {'total': 'sum', 'nof_frete': 'sum'})

print(df)

# Muda valores das notas de entrada para negativo
# winb['total'] = np.where((winb['codpro'] < '5000'), -
#                          winb['total'], winb['total'])

# Totaliza itens por mês
a = winb[['emissa', 'total']].groupby(
    pd.Grouper(key='emissa', freq='MS')).sum()

# Calcula frete de cada nota fiscal
b = winb[['emissa', 'nof_nfinum', 'nof_frete']]\
    .groupby(['emissa', 'nof_nfinum'], as_index=False).max()

# Totaliza frete por mês
b = b[['emissa', 'nof_frete']].groupby(
    pd.Grouper(key='emissa', freq='MS')).sum()

# Mescla dataframes
a = pd.concat([a, b], join='outer', axis=1)
a['Total'] = a['total'] + a['nof_frete']
a.columns = ['itens_wb', 'frete_wb', 'total_wb']

# Prepara Resumos do Access

# Lê dados so Access "Notas"
notas = pd.read_pickle(NOTAS)
notas['NF'] = pd.to_numeric(notas['NF'])

# Resume receita total por mês
b = notas[notas['Emissao'].dt.year.isin(ANO)][['Emissao', 'Receita Líquida']]\
    .groupby(pd.Grouper(key='Emissao', freq='MS')).sum()

b.columns = ['total_acc']
a = pd.concat([a, b], join='outer', axis=1)

# Resume frete por mês
b = notas[(notas['Emissao'].dt.year.isin(ANO))
          & (notas['Titulo'] == 'Frete')][['Emissao', 'Receita Líquida']]\
    .groupby(pd.Grouper(key='Emissao', freq='MS')).sum()
b.columns = ['frete_acc']

# Resume itens por mês
c = notas[(notas['Emissao'].dt.year.isin(ANO))
          & (notas['Titulo'] != 'Frete')][['Emissao', 'Receita Líquida']]\
    .groupby(pd.Grouper(key='Emissao', freq='MS')).sum()
c.columns = ['itens_acc']

# Mescla dataframes
a = pd.concat([a, b, c], axis=1, join='outer')

# Tabela comparação
#  itens_wb - winbooks
#  frete_wb - winbooks
#  total_wb - winbooks
#  total_acc - Access
#  itens_acc - Access
#  frete_acc - Access
a.to_excel(COMP)

# Compara frete, nota a nota

# Calcula frete de cada nota fiscal
a = winb[['emissa', 'nof_nfinum', 'nof_frete']]\
    .groupby(['emissa', 'nof_nfinum'])\
    .max()

a.index.set_names(['Emissao', 'NF'], inplace=True)
a.columns = ['frete_wb']

# Calcula frete de cada nota fiscal
b = notas[(notas['Emissao'].dt.year.isin(ANO))
          & (notas['Titulo'] == 'Frete')][['Emissao', 'NF', 'Receita Líquida']]\
    .groupby(['Emissao', 'NF']).sum()
b.columns = ['frete_acc']

# Mescla dataframes
a = pd.concat([a, b], axis=1, join='outer')
a.groupby(pd.Grouper(level='Emissao', freq='MS')).sum().to_excel(COMP5)
a.fillna(0, inplace=True)

# Filtra notas com diferença no frete
a = a[abs(a['frete_wb'] - a['frete_acc']) >= 0.005]

# Comparativo Frete
a.to_excel(COMP2)

# Resume Por Data e Nota
a = notas[(notas['Emissao'].dt.year.isin(ANO))
          & (notas['Titulo'] != 'Frete')
          & (notas['Receita Líquida'] != 0.0)][['Emissao', 'NF', 'Receita Líquida']]\
    .groupby(['Emissao', 'NF']).sum()
a.columns = ['itens_acc']

b = winb[['emissa', 'nof_nfinum', 'total']]\
    .groupby(['emissa', 'nof_nfinum']).sum()
b.index.set_names(['Emissao', 'NF'], inplace=True)
b.columns = ['itens_wb']

# Mescla dataframes
a = pd.concat([b, a], axis=1)
a.fillna(0, inplace=True)

# Notas com valor divergente
a = a[abs(a['itens_wb'] - a['itens_acc']) >= 0.005]

# Arquivo das notas com totais divergentes
a.to_excel(COMP3)

# Resume por Emissao, Nota e Titulo
a = notas[(notas['Emissao'].dt.year.isin(ANO))
          & (notas['Titulo'] != 'Frete')
          & (notas['Receita Líquida'] != 0.0)][['Emissao', 'NF', 'Titulo', 'Receita Líquida']]\
    .groupby(['Emissao', 'NF', 'Titulo'], as_index=False).sum()

# Lê arquivo dos códigos ISBN dos Títulos
prod = pd.read_excel(PROD, sheet_name='Cadastro', usecols='A,D',
                     header=0, names=['ISBN', 'Titulo'], dtype={'ISBN': str,  'Produto': str})
prod = prod.loc[~prod['ISBN'].isnull()]
prod = prod.loc[~prod['Titulo'].isnull()]

# Separa somente Títulos com descrição única
b = prod.groupby('Titulo', as_index=False).count().copy()
b = b[b['ISBN'] == 1]
b = prod[prod['Titulo'].isin(b['Titulo'])].set_index('Titulo')
# b.columns = ['ISBN']

# Coloca ISBN no arquivo do Access
a = a.join(b, on='Titulo', how='left')

# Resume Winbooks por Nota e Titulo
b = winb[['emissa', 'nof_nfinum', 'isbn', 'pro_descri', 'total']]\
    .groupby(['emissa', 'nof_nfinum', 'isbn', 'pro_descri'], as_index=False)\
    .sum()
b.columns = ['Emissao', 'NF', 'ISBN', 'Titulo', 'Valor Winb']

# Itens com valor divergente
# a['ISBN'] = pd.to_numeric(a['ISBN'])

c = a.merge(b, how='outer', on=['Emissao', 'NF', 'ISBN'],
            suffixes=['', '_wb'], sort=True)
c.fillna(0, inplace=True)
d = c[(abs(c['Receita Líquida'] - c['Valor Winb']) >= 0.005)]
d.to_excel(COMP4)
