# -*- coding: utf-8 -*-
"""
Verifica Movimentos Winbooks contra Access (Notas).

Created on Wed May 19 14:13:00 2021

@author: Celso Leite
"""
import pandas as pd
import glob
import urllib
from sqlalchemy import create_engine

PATH = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Estoques\\'
PATH_NF = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Devoluções\\'
PATH_CAD = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Despesas e Custos\\'
PATH_PROD = PATH
PATH_DASH = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Python\\veneta-dash\\data\\'
PATH_SQL = "C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\"

ARQ_MOV = PATH + 'movimentos.pkl'
ARQ_TIPO = PATH + 'Tipos de Movimento.xlsx'
ARQ_NF = PATH_NF + 'NFCAB.pkl'
ARQ_PROD = PATH_PROD + 'Custos de Produção - Celso.xlsx'
ARQ_CAD = PATH_CAD + 'Cadastro de Produtos.xlsx'
ARQ_NOTAS = PATH_DASH + 'Notas.pkl'
ARQ_SQL = PATH_SQL + 'NFe.accdb'
ARQ_APPEND = PATH + 'Producao.csv'


def main(df_notas=pd.DataFrame()):
    print('\nIniciando Verificação dos Estoques...')
    print('Lendo arquivo de Movimentos de Estoque (movimentos.pkl)...')

    # *************************************************************************
    # ***                  Le Movimentos do Winbooks                        ***
    # *************************************************************************

    # Lê arquivo de Movimentos de Estoque (Nota a Nota - Livro a Livro (Dados))
    df = pd.read_pickle(ARQ_MOV)

    print('Lendo cadastro de Tipos de Movimentos (Tipos de Movimento.xlsx)...')

    # Lê cadastro de Tipos de Movimento ('cfo_descri')
    tp = pd.read_excel(ARQ_TIPO, sheet_name=0, header=0,
                       dtype={'cfo_descri': str, 'Vendas': int,
                              'Estoque': int, 'Terceiros': int})\
        .set_index('cfo_descri')

    # Mescla tabelas e seleciona colunas
    df = df.merge(tp, how='left', on='cfo_descri', validate='many_to_one')
    df = df.loc[:, ['nof_numero', 'emissa', 'razao', 'empresa',
                    'cfo_descri', 'isbn', 'nof_nfinum',
                    'pro_descri', 'qtd', 'codpro',
                    'Vendas', 'Estoque', 'Terceiros']]

    del tp

    # Verifica se todos os tipos de movimentos estão cadastrados
    try:
        assert ~df[['Vendas', 'Estoque', 'Terceiros']].isnull().any().any()
    except AssertionError:
        print('Faltou cadastro do Tipo do Movimento.\n')
        print(df.loc[df['Vendas'].isnull(), 'cfo_descri'].drop_duplicates())

    # Calcula Movimentos de Estoque
    df['Vendas'] = df['qtd']*df['Vendas']
    df['Estoque'] = df['qtd']*df['Estoque']
    df['Terceiros'] = df['qtd']*df['Terceiros']

    # *************************************************************************
    # ***                 Completa os dados com dbf da NF                   ***
    # *************************************************************************
    print('Lendo cabeçalhos das NF (NFCAB.pkl)...')

    # Lê os cabeçalhos das notas (NFCAB.dbf)
    nf = pd.read_pickle(
        ARQ_NF)[['NOF_NUMERO', 'EMP_RAZSOC', 'CLI_CGCCPF', 'NFE_SITUAC', 'NFE_CHAVE', 'NOF_TIPO', 'NOF_SERIE', 'NOF_NFITER', 'CFO_DESCRI']]

    nf = nf[~nf['CFO_DESCRI'].str.strip().isin(
            ['CARTA DE CORREÇÃO', 'CANCELADA'])]

    nf.rename(mapper={'NOF_NUMERO': 'nof_numero',
                      'EMP_RAZSOC': 'empresa'}, axis=1, inplace=True)

    df = df.merge(nf, how='left',
                  on=['nof_numero', 'empresa'],
                  validate='many_to_one')
    del nf

    # Zera Movimentos Neutros de Estoque (NOF_TIPO == 3)
    df.loc[df['NOF_TIPO'] == 3, ['Estoque', 'Terceiros']] = (0, 0)

    # Exclui movimentos bugados
    df = df.loc[~((df['empresa'] == 'EDITORA VENETA') &
                  (df['nof_numero'] == 16686)), ]

    # Garante que só há uma nota fiscal para cada nof_numero (movimento)
    a = df[['nof_numero', 'empresa', 'nof_nfinum']
           ].drop_duplicates(keep='first')
    a = a.loc[a.duplicated(subset=['nof_nfinum', 'empresa'], keep=False), ]
    a = a[a['nof_nfinum'].str.len() == 6]
    a['nof_nfinum'] = a['nof_nfinum'].astype('int')
    a = a.loc[a['nof_nfinum'] != 0, ].sort_values(
        ['empresa', 'nof_nfinum'])
    try:
        assert a.shape[0] == 0

    except AssertionError:
        a.merge(df, on=['nof_numero', 'empresa']
                ).to_excel(PATH+'duplicidades.xlsx')

    del a

    # Exclui NF inválidas
    df = df.loc[df['NFE_SITUAC'] != 16, ]

    # Verifica se todas as notas estão com CPF/CNPJ
    try:
        assert ~df['CLI_CGCCPF'].isnull().any()
    except AssertionError:
        print('Nota Fiscal não está no DBF do Winbooks.')
        print(df.loc[df['CLI_CGCCPF'].isnull(), [
              'nof_nfinum', 'empresa']].drop_duplicates())

    print('Salvando arquivo de Movimentos (Movimentos.csv)...')
    df.to_csv(PATH + 'Movimentos.csv')

    # *************************************************************************
    # ***                   Gera movimentos sem NF                          ***
    # *************************************************************************
    # Movimentos com Finalização Manual (Tipo 27), que não possuem NFe
    # df.loc[(df['NFE_SITUAC'].isin([27])) &
    #        (df['NOF_TIPO'] != 3) &
    #        (~df['cfo_descri'].isin(['DEVOLUCAO DE VENDA',
    #                                 'RET. DE CONSIGNACAO',
    #                                 'RET. DEPOSITO FECH. A.G.',
    #                                 'OUTRA ENTRADA NAO ESP.',
    #                                 'OUTRA SAIDA NAO ESP.',
    #                                 'INVENTARIO ENTRADA',
    #                                 'INVENTARIO SAIDA',
    #                                 ])) &
    #        (df['NFE_CHAVE'].str.strip() == ''), ].to_excel(PATH + 'teste.xlsx')

    a = df.loc[(df['NFE_SITUAC'] == 27) &
               (df['NFE_CHAVE'].str.strip() == '') &
               (~df['cfo_descri'].isin(
                   ['DEVOLUCAO DE VENDA',
                    'RET. DE CONSIGNACAO',
                    'RET. DEPOSITO FECH. A.G.',
                    'OUTRA ENTRADA NAO ESP.',
                    'OUTRA SAIDA NAO ESP.',
                    'INVENTARIO ENTRADA',
                    'INVENTARIO SAIDA',
                    ])) &
               (df['NOF_TIPO'] != 3), ['emissa', 'empresa', 'NOF_SERIE', 'nof_nfinum', 'isbn', 'CLI_CGCCPF', 'Estoque', 'Terceiros', 'Vendas', 'codpro', 'CFO_DESCRI']]

    print('Lendo cadastros do Access (NFe.accdb)...')

    # Conexão para o Access
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + ARQ_SQL + ';'
        r'ExtendedAnsiSQL=1;'
    )
    connection_uri = \
        f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}"

    # Create Engine
    engine = create_engine(connection_uri)

    # Lê cadastro de clientes e produtos
    with engine.begin() as conn:
        clientes = pd.read_sql(
            'SELECT CNPJ, Razao, Grupo FROM CLIENTES', conn)
        titulos = pd.read_sql(
            'SELECT Produto AS isbn, Titulo FROM PRODUTOS', conn)

    del engine, connection_uri, conn_str

    print('Gerando planilha de movimentos manuais...')

    a = a.merge(titulos, how='left', on='isbn')
    a['CNPJ'] = a['CLI_CGCCPF'].str.strip()
    a = a.merge(clientes, how='left', on='CNPJ')

    # Cria item da NF (sequencial)
    a['Item'] = 1
    a['Item'] = a.groupby(['emissa', 'empresa', 'NOF_SERIE', 'nof_nfinum'])[
        'Item'].cumsum()

    a['Filial'] = a['empresa'].map(lambda x: 1 if x == 'EDITORA VENETA' else 2)
    a['CFOP'] = a['codpro'].str.replace('.', '', regex=False)
    a['Receita Líquida'] = 0
    a['Receita Bruta'] = 0

    a.drop(['CNPJ', 'empresa', 'codpro'], axis=1, inplace=True)
    a = a[['emissa', 'Filial', 'NOF_SERIE', 'nof_nfinum', 'Item', 'isbn',
           'Razao', 'Grupo', 'CLI_CGCCPF',
           'Receita Bruta', 'Titulo', 'Receita Líquida', 'Terceiros', 'Estoque', 'Vendas', 'CFOP', 'CFO_DESCRI']]
    a.rename(axis=1, inplace=True,
             mapper={'emissa': 'Emissao', 'NOF_SERIE': 'Serie',
                     'nof_nfinum': 'NF', 'isbn': 'Produto', 'Razao': 'Cliente', 'CLI_CGCCPF': 'CNPJ', 'Estoque': 'Estoque Veneta', 'Terceiros': 'Estoque Terceiros'})

    # Salva arquivo com movimentos sem NFe, no formato do Notas
    print('Salvando movimentos manuais (movimentos sem NF.csv)...')
    a.to_csv(PATH + 'movimentos sem NF.csv')

    print('Lendo arquivo de Notas (Notas.pkl)...')

    # Abre arquivo de Notas do Access
    if df_notas.isnull().all().all():
        # Inclui campo CFO_DESCRI
        nf = pd.read_pickle(ARQ_NF)

        nf = nf.loc[:, ['EMP_RAZSOC', 'NOF_EMISSA',
                        'CLI_CGCCPF', 'NOF_NFINUM', 'CFO_DESCRI']]

        nf = nf[~nf['CFO_DESCRI'].str.strip().isin(
            ['CARTA DE CORREÇÃO', 'CANCELADA'])]

        nf['Filial'] = nf['EMP_RAZSOC'].map(
            {'EDITORA VENETA': 1, 'EDITORA VENETA - FILIAL': 2})

        nf['Emissao'] = pd.to_datetime(nf['NOF_EMISSA'])

        nf['NF'] = pd.to_numeric(nf['NOF_NFINUM'])

        nf['CNPJ'] = nf['CLI_CGCCPF'].str.strip()

        nf.drop(labels=['EMP_RAZSOC', 'NOF_EMISSA',
                'NOF_NFINUM', 'CLI_CGCCPF'], axis=1, inplace=True)

        # Garante que as descrições são únicas
        nf.drop_duplicates(inplace=True)

        filtro = nf.duplicated(
            subset=['Emissao', 'Filial', 'NF', 'CNPJ'], keep=False)

        # Pega somente a primeira descr das notas 0
        if filtro.sum() > 0:
            aux = nf[filtro].value_counts('NF')
            if (aux.shape[0] == 1) & (aux.index[0] == 0):
                nf.drop_duplicates(
                    subset=['Emissao', 'Filial', 'NF', 'CNPJ'], inplace=True)

        notas = pd.read_pickle(ARQ_NOTAS)[
            ['Emissao', 'Filial', 'NF', 'CNPJ', 'Vendas', 'Estoque Veneta', 'Estoque Terceiros', 'Produto', 'CFOP']]

        notas['NF'] = pd.to_numeric(notas['NF'])

        notas = notas.merge(nf, how='left', on=[
            'Emissao', 'Filial', 'NF', 'CNPJ'], validate='many_to_one')

        del nf, filtro

    else:
        notas = df_notas.loc[:,
                             ['Emissao', 'Filial', 'NF', 'CNPJ', 'Vendas', 'Estoque Veneta', 'Estoque Terceiros', 'Produto', 'CFO_DESCRI', 'CFOP']]

    # *************************************************************************
    # ***              Zera movimentos das Notas com Tipo 3                 ***
    # *************************************************************************
    # Notas com movimentação de estoque ignorada (NOF_TIPO = 3)
    mov0 = df.loc[(df['NOF_TIPO'] == 3) & (df['nof_nfinum'].astype(int) != 0), [
        'emissa', 'empresa', 'nof_nfinum', 'CLI_CGCCPF', 'isbn', 'cfo_descri']]

    mov0.columns = ['Emissao', 'Filial', 'nof_nfinum',
                    'CLI_CGCCPF', 'Produto', 'cfo_descri']
    mov0['Filial'] = mov0['Filial'].map(
        lambda x: 1 if x == 'EDITORA VENETA' else 2)
    mov0['CNPJ'] = mov0['CLI_CGCCPF'].str.strip()
    mov0['NFaux'] = mov0['nof_nfinum'].astype(int)
    mov0 = mov0.drop_duplicates(
        subset=['Emissao', 'Filial', 'NFaux', 'CNPJ', 'Produto'])

    # Atualiza movimentos sem efeito de estoque
    notas['NFaux'] = notas['NF'].astype(int)
    notas = notas.merge(mov0, how='left', on=[
                        'Emissao', 'Filial', 'NFaux', 'CNPJ', 'Produto'])
    notas.loc[~notas['cfo_descri'].isnull(), ['Estoque Veneta',
                                              'Estoque Terceiros']] = (0, 0)
    notas.drop(axis=1, columns=[
               'cfo_descri', 'CLI_CGCCPF', 'nof_nfinum', 'NFaux'], inplace=True)

    print('Incluindo movimentos manuais...')
    # Inclui movimentos manuais (sem NFe)
    notas = pd.concat([notas, a[notas.columns]], axis=0, ignore_index=True)
    del a

    # *************************************************************************
    # ***                Outras Saídas (5949, 6949, 7949)                   ***
    # *************************************************************************
    filtro = (notas['CFO_DESCRI'].isin(['OUTRA ENTRADA NAO ESP.',
                                        'OUTRA SAIDA NAO ESP.'])) & \
        (notas['CFOP'].isin(['1949', '2949', '5949', '6949', '7949']))

    notas.loc[filtro, ['Estoque Veneta', 'Estoque Terceiros']] = (0, 0)

    a = notas.loc[filtro, ['Emissao', 'Filial', 'NF',
                           'CNPJ', 'Produto', 'CFO_DESCRI']]
    a['NFaux'] = a['NF'].astype(int)
    a['Filial'] = a['Filial'].astype(int)
    a['CLI_CGCCPF'] = a['CNPJ']
    a.rename(axis=1, inplace=True, mapper={
             'CFO_DESCRI': 'cfo_descri', 'NF': 'nfo_nfinum'})

    mov0 = pd.concat(
        [mov0, a], ignore_index=True)

    mov0.to_csv(PATH + 'movimentos sem efeito de estoque.csv')
    del mov0, a

    # *************************************************************************
    # ***                Movimentos de Ajuste de Estoques                   ***
    # *************************************************************************

    a = df.loc[(df['cfo_descri'].isin(['RET. DEPOSITO FECH. A.G.',
                                       'OUTRA ENTRADA NAO ESP.',
                                       'OUTRA SAIDA NAO ESP.',
                                       'INVENTARIO ENTRADA',
                                       'INVENTARIO SAIDA',
                                       'ENTR. P/ TRANSF. ESTOQUE',
                                       'REM. P/ TRANSF. ESTOQUE',
                                       'RET. DE CONSERTO/REPARO',
                                       'REM. P/ CONSERTO/REPARO',
                                       ])) &
               (df['NOF_TIPO'] != 3),
               ['emissa', 'empresa', 'NOF_SERIE', 'nof_nfinum', 'isbn', 'CLI_CGCCPF', 'Estoque', 'Terceiros', 'Vendas', 'codpro', 'CFO_DESCRI']]

    a = a.merge(titulos, how='left', on='isbn')
    a['CNPJ'] = a['CLI_CGCCPF'].str.strip()
    a = a.merge(clientes, how='left', on='CNPJ')

    del clientes, titulos

    a['Item'] = 1
    a['Item'] = a.groupby(['emissa', 'empresa', 'NOF_SERIE', 'nof_nfinum'])[
        'Item'].cumsum()

    a['Filial'] = a['empresa'].map(lambda x: 1 if x == 'EDITORA VENETA' else 2)
    a['CFOP'] = a['codpro'].str.replace('.', '', regex=False)
    a['Receita Líquida'] = 0
    a['Receita Bruta'] = 0

    a.drop(['CNPJ', 'empresa', 'codpro'], axis=1, inplace=True)
    a = a[['emissa', 'Filial', 'NOF_SERIE', 'nof_nfinum', 'Item', 'isbn',
           'Razao', 'Grupo', 'CLI_CGCCPF',
           'Receita Bruta', 'Titulo', 'Receita Líquida', 'Terceiros', 'Estoque', 'Vendas', 'CFOP', 'CFO_DESCRI']]
    a.rename(axis=1, inplace=True,
             mapper={'emissa': 'Emissao', 'NOF_SERIE': 'Serie',
                     'nof_nfinum': 'NF', 'isbn': 'Produto', 'Razao': 'Cliente', 'CLI_CGCCPF': 'CNPJ', 'Estoque': 'Estoque Veneta', 'Terceiros': 'Estoque Terceiros'})

    print('Salvando movimentos de ajustes (movimentos de ajustes.csv)...')
    a.to_csv(PATH + 'movimentos de ajustes.csv')

    print('Incluindo movimentos de ajustes...')
    # Inclui movimentos manuais
    notas = pd.concat([notas, a[notas.columns]], axis=0, ignore_index=True)
    del a

    notas['empresa'] = notas['Filial'].map(
        lambda x: 'EDITORA VENETA' if x != 2 else 'EDITORA VENETA - FILIAL')

    notas.drop(['Filial'], axis=1, inplace=True)

    # *************************************************************************
    # ***        Inclui Movimentos de Entrada de Gráfica (Produção)         ***
    # *************************************************************************
    # Lê arquivo com produção
    prod = pd.read_excel(ARQ_PROD,  sheet_name="Tiragens", usecols="A,D,E",
                         header=0, parse_dates=['Emissao'], na_values=[None], names=['Produto', 'Estoque Veneta', 'Emissao'], dtype={'Estoque Veneta': int, 'Produto': object})

    # Busca Título no cadastro
    cadastro = pd.read_excel(ARQ_CAD, usecols="A,D",
                             header=0, sheet_name='Cadastro', names=['Produto', 'Titulo'], dtype={'Cadastro': object, 'Produto': object})

    cadastro.dropna(axis=0, subset=['Produto'], inplace=True)

    prod = prod.merge(cadastro, how='left', on='Produto',
                      validate='many_to_one')

    del cadastro

    # Salva arquivo com produção
    prod.to_csv(ARQ_APPEND)

    # coloca no formato do arquivo notas
    prod['Estoque Terceiros'] = 0
    prod['Vendas'] = 0
    prod['NF'] = 0
    prod['CNPJ'] = 0
    prod['empresa'] = 'EDITORA VENETA'

    notas = pd.concat([notas, prod[['Emissao', 'NF', 'CNPJ', 'Vendas',
                                    'Estoque Veneta', 'Estoque Terceiros', 'Produto', 'empresa']]], axis=0)
    del prod

    # *************************************************************************
    # ***                 Monta comparativos de Movimentação Mensal         ***
    # *************************************************************************
    notas.rename(columns={'NF': 'nof_nfinum',
                          'Produto': 'isbn', 'CNPJ': 'CLI_CGCCPF',
                          'Emissao': 'emissa', 'Estoque Veneta': 'Estoque', 'Estoque Terceiros': 'Terceiros'}, inplace=True)

    access = notas.groupby(['isbn', pd.Grouper(key='emissa', freq='MS')]).agg(
        {'Vendas': 'sum', 'Estoque': 'sum', 'Terceiros': 'sum'})

    del notas

    winb = df.groupby(['isbn', pd.Grouper(key='emissa', freq='MS')]).agg(
        {'Vendas': 'sum', 'Estoque': 'sum', 'Terceiros': 'sum'})

    # Guarda relação dos Títulos
    produtos = df[['isbn', 'pro_descri']].drop_duplicates()

    del df

    # Monta comparativo
    comp = access.merge(winb, how='outer', on=[
                        'isbn', 'emissa'], suffixes=['_ac', '_wb'], validate='one_to_one')
    comp.fillna(0, inplace=True)

    # Calcula diferenças entre Access e Winbooks
    comp['Vendas'] = comp['Vendas_ac'] - comp['Vendas_wb']
    comp['Estoque'] = comp['Estoque_ac'] - comp['Estoque_wb']
    comp['Terceiros'] = comp['Terceiros_ac'] - comp['Terceiros_wb']
    comp.drop(['Vendas_ac', 'Estoque_ac', 'Terceiros_ac', 'Vendas_wb',
               'Estoque_wb', 'Terceiros_wb'], axis=1, inplace=True)

    comp.reset_index(inplace=True)

    filtro = (comp['Vendas'] != 0) | (
        comp['Estoque'] != 0) | (comp['Terceiros'] != 0)

    # Inclui Títulos no comparativo
    comp = comp[filtro].merge(produtos, how='left',
                              on='isbn', validate='many_to_many')

    print('Salvando planilha de conferência (Movimentos de Estoque.csv)...')

    comp[(comp['Estoque'] != 0) & (comp['emissa'] < '2021-09')].groupby([
        'isbn', 'pro_descri'])['emissa'].max().to_csv(PATH+'Movimentos Estoque - conf.csv')

    comp[(comp['Terceiros'] != 0) & (comp['emissa'] < '2021-09')].groupby([
        'isbn', 'pro_descri'])['emissa'].max().to_csv(PATH+'Movimentos Terceiros - conf.csv')

    comp[(comp['Vendas'] != 0) & (comp['emissa'] < '2021-09')].groupby([
        'isbn', 'pro_descri'])['emissa'].max().to_csv(PATH+'Movimentos Vendas - conf.csv')

    if __name__ == '__main__':
        print('\nPronto.')


if __name__ == '__main__':
    main()
