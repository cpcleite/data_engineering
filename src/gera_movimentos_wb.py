# -*- coding: utf-8 -*-
"""
Created on Mon Jun 07 16:48:00 2021.

Gera o relatório de movimentos de estoque e consignação.

Lê o arquivo:
        "movimentos.pkl",
    criado a partir dos arquivos:
        "nota a nota - livro a livro *.xlsx"

    pela rotina Consolida_Movimentos

@author: Celso Leite
"""
import glob
import pandas as pd

PATH = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Estoques\\'
PATH_NF = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Devoluções\\'
PATH_NOTAS = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Python\\veneta-dash\\data\\'

ARQ_MOV = PATH + 'movimentos.pkl'
ARQ_CLI = PATH + 'clientes.pkl'
ARQ_NF = PATH_NF + 'NFCAB.pkl'
ARQ_NAT = PATH + 'Naturezas.xlsx'
PADRAO = PATH + 'anoportipoestoque-??.xls'
PADRAO_MOV = PATH + 'nota a nota - livro a livro *.xlsx'
ARQ_HIST = PATH + 'saldo_historico_dia.csv'
ARQ_SALDO = PATH + 'movimentos_wb.csv'
ARQ_PROD = PATH + 'Custos de Produção - Celso.xlsx'
ARQ_NOTAS = PATH_NOTAS + 'Notas.pkl'


def consolida_movimentos():
    """
    Consolida os relatórios "nota a nota - livro a livro *.xlsx".

    Gera arquivo "movimentos.pkl"
    """
    tipos = {'nof_nfinum': str,	'emissa': object, 'pedido':	str,
             'nof_numero': int, 'pvdcli': str, 'razao': str, 'empresa': str, 'cfo_descri': str, 'ufe_sigla': str, 'cidade': str,
             'fun_nome': str, 'fun_nome2': str, 'nof_frete': str, 'isbn': str, 'pro_descri': str, 'assunto': str, 'cla_codigo': str,	'pro_unidade': str, 'qtd': int, 'unit': float, 'total': float,	'codpro': str}

    arquivos = glob.glob(PADRAO_MOV)

    print('\nConsolidando movimentos de estoque...\n',
          '\nProcessando arquivos:')

    df = pd.DataFrame()
    for arq in arquivos:
        print(arq.split('\\')[-1])
        df = df.append(pd.read_excel(arq, sheet_name=0,
                                     header=0, dtype=tipos), ignore_index=True)

    print(df.shape)

    df['emissa'] = pd.to_datetime(df['emissa'])

    df.to_pickle(ARQ_MOV)


def cria_movimentos_estoque():
    """
    Cria movimentação de estoques a partir do arquivo de movimentos (gerado na rotina consolida_movimentos), cadastro de notas (dbf) e cadastro de clientes (dbf)

    Gera arquivo "movimentos_wb.csv"
    """

    # Lê arquivo dos movimentos (nota a nota - livro a livro pkl)
    df = pd.read_pickle(ARQ_MOV)
    df = df[['nof_nfinum', 'emissa', 'nof_numero', 'razao', 'empresa',
             'cfo_descri', 'isbn', 'pro_descri', 'cla_codigo', 'qtd']]

    # Lê os cabeçalhos das notas (NFCAB.pkl)
    nf = pd.read_pickle(ARQ_NF)[['NOF_NUMERO', 'EMP_RAZSOC', 'CLI_CGCCPF',
                                 'NOF_TIPO', 'NOF_SERIE', 'NOF_NFITER', 'NOF_DATFIM', 'NOF_TIPCLI']]

    nf['NOF_DATFIM'] = pd.to_datetime(nf['NOF_DATFIM']).fillna(
        value=pd.to_datetime('2000-01-01'))

    nf.rename(mapper={'NOF_NUMERO': 'nof_numero',
                      'EMP_RAZSOC': 'empresa'}, axis=1, inplace=True)

    # Inclui dados da Nota Fiscal e CNPJ
    df = df.merge(nf, how='left',
                  on=['nof_numero', 'empresa'],
                  validate='many_to_one')
    del nf

    # Lê arquivo de clientes
    cl = pd.read_pickle(ARQ_CLI)[['CLI_CGCCPF', 'CLI_CORTE']].dropna(
        axis=0, subset=['CLI_CGCCPF']).drop_duplicates(subset=['CLI_CGCCPF']).fillna(pd.Timestamp.max)

    # Inclui Data de Corte do Cliente (início das movimentações de estoque)
    df = df.merge(cl, how='left',
                  on=['CLI_CGCCPF'],
                  validate='many_to_one')
    try:
        assert not df.loc[df['NOF_TIPCLI'] == 1, 'CLI_CORTE'].hasnans

    except AssertionError:
        print('\nFalta Cadastro de Data de Corte do Cliente')
        print(df.loc[df['CLI_CORTE'].isnull() & (df['NOF_TIPCLI'] == 1), [
              'razao', 'CLI_CGCCPF']].drop_duplicates())

    del cl

    df['CLI_CORTE'].fillna('2000-01-01', inplace=True)

    df['Tipo'] = df['NOF_TIPO'].map(
        {1.0: 'Entrada', 2.0: 'Saída', 3.0: 'Neutro'})

    mov = df.groupby(['isbn', 'pro_descri', 'emissa', 'razao',
                      'CLI_CGCCPF', 'CLI_CORTE', 'qtd', 'Tipo', 'NOF_NFITER', 'cfo_descri', 'NOF_DATFIM'], as_index=False)['qtd'].sum()

    # Lê arquivo das Naturezas
    nat = pd.read_excel(ARQ_NAT, usecols='A,J', dtype=str, sheet_name=0,
                        header=0, names=['cfo_descri', 'Tipo Estoque']).fillna('')

    # Inclui o Tipo da Movimentação, com base na descrição
    mov = mov.merge(nat, how='left', on='cfo_descri', validate='many_to_one')

    try:
        assert mov['Tipo Estoque'].isna().sum() == 0
    except AssertionError:
        print('\nFalta Cadastro de Naturezas...\n')
        print(mov.loc[mov['Tipo Estoque'].isna(),
              'cfo_descri'].drop_duplicates())

    cons_tipo = {'Consignação - Entrada': -1,
                 'Consignação - Saída': 1,
                 'Consignação - Venda': -1,
                 'Eventos - Entrada': 0,
                 'Eventos - Saída': 0,
                 'Eventos - Venda': 0,
                 '': 0}

    even_tipo = {'Consignação - Entrada': 0,
                 'Consignação - Saída': 0,
                 'Consignação - Venda': 0,
                 'Eventos - Entrada': -1,
                 'Eventos - Saída': 1,
                 'Eventos - Venda': -1,
                 '': 0}

    fisi_tipo = {'Entrada': 1,
                 'Saída': -1,
                 'Neutro': 0}

    mov['Físico'] = mov['qtd'] * mov['Tipo'].map(fisi_tipo)
    mov['Consignação'] = mov['qtd'] * \
        mov['Tipo Estoque'].map(cons_tipo) * \
        ((mov['emissa'] >= mov['CLI_CORTE']) |
         (mov['NOF_DATFIM'] >= mov['CLI_CORTE']))
    mov['Eventos'] = mov['qtd'] * mov['Tipo Estoque'].map(even_tipo)

    mov.to_csv(ARQ_SALDO)


def compara_saldos(historico, movimentos):
    """
    Compara os saldos do relatório
        HISTÓRICO DE PRODUTOS TOTALIZADO POR ANO - POR TIPO DE ESTOQUE
    com o relatório de movimentações gerado pela rotina
        cria_movimentos_estoque()
    """
    # Lê o arquivo do histórico dos produtos (saldo_historico_dia)
    his = pd.read_csv(historico, header=0, sep=',', index_col=0, dtype={
                      'Produto': str, 'Saldo_V': int, 'Saldo_T': int, 'Saldo_E': int}, parse_dates=['Emissao'])

    # Lê arquivo dos movimentos (movimentos_wb) - nota a nota livro a livro
    mov = pd.read_csv(movimentos, header=0, sep=',', index_col=0,
                      dtype={'isbn': str, 'pro_descri': str, 'razao': str,
                             'CLI_CGCCPF': str, 'Tipo': str, 'NOF_NFITER': str,
                             'cfo_descri': str, 'qtd': int,
                             'Tipo Estoque': str, 'Físico': int,
                             'Consignação': int, 'Eventos': int},
                      parse_dates=['emissa'])[['isbn', 'pro_descri', 'emissa', 'Físico', 'Consignação', 'Eventos', 'cfo_descri']]

    # Corrige ISBN do DESERAMA
    mov.loc[mov['pro_descri'] == 'DESERAMA', 'isbn'] = '9786586691160'
    mov.loc[mov['pro_descri'] == 'DESERAMA - EBOOK', 'isbn'] = '9786586691535'

    # Guarda primeiro inventário
    inventário = mov.loc[mov['cfo_descri'].isin(
        ['INVENTARIO ENTRADA', 'INVENTARIO SAIDA']),
        ['isbn', 'pro_descri', 'emissa']].groupby(['isbn', 'pro_descri'], as_index=False)['emissa'].min()

    # Pega movimento acumulado do dia de cada produto
    a = mov.groupby(['isbn', 'pro_descri'], as_index=False)

    mov['Físico'] = a['Físico'].cumsum().astype('int')
    mov['Consignação'] = a['Consignação'].cumsum().astype('int')
    mov['Eventos'] = a['Eventos'].cumsum().astype('int')

    mov['ordem'] = mov.index

    a = mov.groupby(['isbn', 'emissa'])['ordem'].max()
    mov = mov.loc[a, ['emissa', 'pro_descri',
                      'isbn', 'Físico', 'Consignação', 'Eventos']]

    # Ajusta descrições com erro
    his.loc[his['Produto'].str.contains(
        'ENTRE O ENCARDIDO,'), 'Produto'] = 'ENTRE O ENCARDIDO, O BRANCO E O BRANQUÍSSIMO'

    his.loc[his['Produto'].str.contains(
        'DESINFORMAÇÃO: CRISE'), 'Produto'] = 'DESINFORMAÇÃO: CRISE POLÍTICA E SAÍDAS DEMOCRÁTICA'

    his.loc[his['Produto'].str.contains(
        'DELIVERY FIGHT'), 'Produto'] = 'DELIVERY FIGHT! – A LUTA CONTRA OS PATROES SEM ROS'

    his.loc[his['Produto'].str.contains(
        'NOVAS GERACOES -'), 'Produto'] = 'A ARTE DE VIVER PARA AS NOVAS GERACOES - EBOOK'

    # Garante que todos os produtos estão os dois arquivos
    a = set(his['Produto'].drop_duplicates().to_list()) - \
        set(mov['pro_descri'].drop_duplicates().to_list())

    try:
        assert len(a) == 0
    except AssertionError:
        print('\nProdutos em somente um dos arquivos\n')
        for x in a:
            print(x)
        del x

    # Compara relatório do histórico com os movimentos
    cmp = his.merge(mov, how='left', left_on=[
                    'Emissao', 'Produto'], right_on=['emissa', 'pro_descri'], validate='one_to_one').fillna(method='ffill')

    filtro = (cmp['Saldo_T'] != cmp['Consignação']) & (
        cmp['Emissao'] < '2021-06')
    a = cmp.loc[filtro, ['Emissao', 'Produto', 'Saldo_T', 'Consignação']]
    a.to_excel(PATH + 'Erros da comparação histórico com movimentos.xlsx')

    del cmp, a, filtro

    # Abre arquivo de Custos
    pro = pd.read_excel(ARQ_PROD, sheet_name="Tiragens", usecols="A:E",
                        header=0, parse_dates=['Data'], na_values=[None], dtype={'ISBN': str, 'Título': str, 'Custo': float, 'Tiragem': int})

    # Custos de produção anterior ao primeiro inventário antes de 2021
    pro1 = pro.merge(inventário[inventário['emissa'] < '2021-01'], how='left',
                     left_on='ISBN', right_on='isbn')

    filtro = pro1['Data'] < pro1['emissa']

    # Salva produções posteriores
    pro2 = pro[~filtro]

    # Totaliza produções anteriores ao primeiro inventário
    pro = pro1[filtro].groupby(
        ['isbn', 'emissa'], as_index=False)[['Tiragem', 'Custo']].sum()

    # Coloca Descrição do Produto
    his = his.merge(mov[['isbn', 'pro_descri']].drop_duplicates().rename(
        {'pro_descri': 'Produto'}, axis=1), how='inner', on='Produto', validate='many_to_one')

    # Coloca data do primeiro inventário
    tmp = his.merge(pro[['isbn', 'emissa', 'Tiragem', 'Custo']], how='left',
                    on='isbn', validate='many_to_one').fillna({'emissa': pd.to_datetime('2100-01-01')})

    # Filtra Saldos do dia do primeiro inventário
    filter = tmp[tmp['Emissao'] <= tmp['emissa']].groupby(
        ['isbn'], as_index=False)['Emissao'].max()

    saldo = tmp.merge(filter, how='inner', on=['isbn', 'Emissao'])

    tmp = pd.concat([saldo, tmp[tmp['Emissao'] > tmp['emissa']]], axis=0)
    tmp.to_excel(PATH + 'tmp.xlsx')

    # Vendas Anteriores aos primeiro inventário
    notas = pd.read_pickle(ARQ_NOTAS)

    notas = notas.merge(inventário.loc[inventário['emissa'] < '2021-01',
                                       ['isbn', 'emissa']], how='left', left_on='Produto', right_on='isbn')

    notas = notas[notas['Emissao'] <= notas['emissa']].groupby('Produto')[
        'Vendas'].sum()

    saldo = saldo.merge(notas, how='left', left_on='isbn',
                        right_on='Produto', validate='one_to_one')

    saldo = saldo.merge(pro2[['ISBN', 'Data', 'Tiragem', 'Custo']].rename(
        {'Data': 'emissa', 'ISBN': 'isbn'}, axis=1), how='outer', on=['isbn', 'emissa'], suffixes=['', '_2'])

    saldo.to_excel(PATH + 'custo.xlsx')

    # mov.to_pickle(PATH + 'm.pkl')
    # his.to_pickle(PATH + 'h.pkl')
    # cmp.to_pickle(PATH + 'c.pkl')


def verifica_integridade_estoque():
    """
        Verifica integridade dos movimentos de estoque
    """

    # Abre arquivo de Custos
    pro = pd.read_excel(ARQ_PROD, sheet_name="Tiragens", usecols="A:E",
                        header=0, parse_dates=['Data'], na_values=[None], dtype={'ISBN': str, 'Título': str, 'Custo': float, 'Tiragem': int})

    # Abre arquivo de Movimentos Winbooks
    mov = pd.read_csv(ARQ_SALDO, header=0, sep=',', index_col=0,
                      dtype={'isbn': str, 'pro_descri': str, 'razao': str,
                             'CLI_CGCCPF': str, 'Tipo': str, 'NOF_NFITER': str,
                             'cfo_descri': str, 'qtd': int,
                             'Tipo Estoque': str, 'Físico': int,
                             'Consignação': int, 'Eventos': int},
                      parse_dates=['emissa'])[['isbn', 'pro_descri', 'emissa', 'Físico', 'Consignação', 'Eventos', 'cfo_descri', 'qtd']]

    # Corrige ISBN do DESERAMA
    mov.loc[mov['pro_descri'] == 'DESERAMA', 'isbn'] = '9786586691160'

    ver = mov.groupby(['isbn', 'pro_descri'], as_index=False)[
        'emissa'].min().set_index('isbn')

    ver = ver.merge(pro.rename({'ISBN': 'isbn'}, axis=1).groupby(['isbn'])
                    ['Data'].min(), how='left', on='isbn')

    # Garante que não faltou cadastro de produção de livros
    filtro = ~ver['pro_descri'].str.contains('E-BOOK|EBOOK')
    try:
        assert not ver.loc[filtro, 'Data'].hasnans
    except AssertionError:
        print('\nFalta Produção dos Títulos...\n')
        print(ver.loc[filtro & (ver['Data'].isna()), ])

    del ver, filtro

    # Verifica se a planilha de produção tem chave única
    try:
        assert (pro.groupby(['ISBN', 'Data']).size() <= 1).all()

    except AssertionError:
        print('\nProdução não tem chave única. Duas entradas na mesma data...\n')
        a = pro.groupby(['ISBN', 'Data']).size()
        print(a[a > 1])
        exit()

    # Verifica se as entradas de gráfica estão consistentes
    ini = pro.groupby(['ISBN'])['Data'].min()
    ini = pro.merge(ini, how='inner', on=[
        'ISBN', 'Data'], validate='one_to_one')

    ini['Custo Médio'] = ini['Custo'] / ini['Tiragem']

    # Verifica se as datas de entrada de gráfica batem com a planilha de produção
    dat = mov.loc[mov['cfo_descri'] == 'ENTRADA DE GRAFICA',
                  ['emissa', 'isbn', 'qtd']]\
        .groupby(['isbn', 'emissa'])['qtd'].sum()

    dat = pd.concat([dat, pro.groupby(['ISBN', 'Data'])
                     ['Tiragem'].sum()], axis=1)

    try:
        assert not dat['Tiragem'].hasnans
    except AssertionError:
        print('\nFalta cadastro da produção na planilha Custos de Produção...\n')
        print(dat.loc[dat['Tiragem'].isna(), ['qtd']].reset_index().merge(
            mov[['isbn', 'pro_descri']].drop_duplicates(), how='left', left_on='level_0', right_on='isbn').rename({'level_1': 'Data'}, axis=1)[['isbn', 'Data', 'pro_descri', 'qtd']])

    filtro = dat.index.get_level_values(1) >= '2020-01-01'
    try:
        assert not dat.loc[filtro, 'qtd'].hasnans

    except AssertionError:
        print('\nNão encontrou entrada de gráfica no arquivo de movimentos...\n')
        print(dat.loc[filtro & dat['qtd'].isna(), ['Tiragem']].reset_index().merge(mov[['isbn', 'pro_descri']].drop_duplicates(), how='left',
              left_on='level_0', right_on='isbn').rename({'level_1': 'Data'}, axis=1)[['isbn', 'Data', 'pro_descri', 'Tiragem']].sort_values(['Data', 'isbn']))

    try:
        assert (dat.dropna()['Tiragem'] != dat.dropna()['qtd']).sum() == 0

    except AssertionError:
        print('\nDiferença na Tiragem cadastrada...\n')
        print(dat[dat['Tiragem'] != dat['qtd']].dropna().reset_index().merge(
            mov[['isbn', 'pro_descri']].drop_duplicates(), how='left', left_on='level_0', right_on='isbn').rename({'level_1': 'Data'}, axis=1)[['isbn', 'Data', 'pro_descri', 'Tiragem', 'qtd']])


def custos_produção(historico, movimentos):

    # Abre arquivo de Movimentos
    mov = pd.read_pickle(movimentos)

    # Pega último inventário antes de 2021
    custo = mov.loc[mov['cfo_descri'].isin(
        ['INVENTARIO ENTRADA', 'INVENTARIO SAIDA']),
        ['isbn', 'pro_descri', 'emissa']].groupby(['isbn', 'pro_descri'], as_index=False)['emissa'].min()

    # Abre arquivo de Custos
    pro = pd.read_excel(ARQ_PROD, sheet_name="Tiragens", usecols="A:E",
                        header=0, parse_dates=['Data'], na_values=[None], dtype={'ISBN': str, 'Título': str, 'Custo': float, 'Tiragem': int})

    # Custos de produção anterior ao primeiro inventário antes de 2021
    pro = pro.merge(custo[custo['emissa'] < '2021-01'],
                    how='left', left_on='ISBN', right_on='isbn')

    pro = pro[pro['Data'] < pro['emissa']].groupby(
        ['isbn', 'pro_descri', 'emissa'], as_index=False)[['Tiragem', 'Custo']].sum()

    # Saldos de cada produto na data do último inventário
    his = pd.read_csv(historico, header=0, sep=',', index_col=0, dtype={
                      'Produto': str, 'Saldo_V': int, 'Saldo_T': int, 'Saldo_E': int}, parse_dates=['Emissao'])

    his.merge(pro[['emissa', 'isbn']], how='left', left_on=['isbn'])
    # pro.to_excel(PATH + 'custo.xlsx')


def gera_mov_wb():
    consolida_movimentos()
    cria_movimentos_estoque()
    compara_saldos(ARQ_HIST, ARQ_SALDO)
    verifica_integridade_estoque()
    # custos_produção(ARQ_HIST, ARQ_MOV)


if __name__ == '__main__':
    gera_mov_wb()
