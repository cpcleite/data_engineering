""" Consolida os arquivos de Histórico de Estoques
    Prepara o arquivo da margem para o DashBoard

"""
import glob
import pandas as pd

PATH = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Estoques\\'
PATH_R = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Despesas e Custos\\'
PATH_DASH = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Python\\veneta-dash\\data\\'

ARQ_HIST = PATH + 'saldo_historico_dia.csv'
ARQ_PROD = PATH + 'Custos de Produção - Celso.xlsx'
ARQ_CAD = PATH_R + 'Cadastro de Produtos.xlsx'
ARQ_NOTAS = PATH_DASH + 'Notas.pkl'

IMP = 0.0228   # Taxa dos Impostos


def le_historicos_wb(arquivo):
    """
    Lê os arquivos gerados pelo relatório do Winbooks:
        HISTÓRICO DE PRODUTOS TOTALIZADO POR ANO - POR TIPO DE ESTOQUE
    """
    # Lê arquivo
    df = pd.read_excel(arquivo, header=None, usecols='A:H,L:Q', dtype=str,
                       names=['Emissao', 'NF', 'Razão', 'Operação', 'Entrada_V', 'Saída_V', 'Neutro_V', 'Saldo_V', 'Entrada_T', 'Saída_T', 'Saldo_T', 'Entrada_E', 'Saída_E', 'Saldo_E']).dropna(subset=['Emissao'])

    # Cria coluna de Produto
    filtro = df['Emissao'].str.contains('^Produto:')

    df['Produto'] = df.loc[filtro, 'Emissao'].str.strip('Produto:').str.strip()
    df['Produto'].fillna(method='ffill', axis=0, inplace=True)

    # Arruma a coluna das datas
    df['Emissao'] = pd.to_datetime(
        df['Emissao'], dayfirst=True, errors='coerce')

    # Elimina linhas inválidas
    df.dropna(subset=['Emissao'], inplace=True)

    # saldo do dia
    df_detalhada = df[['Emissao', 'Produto',
                       'Saldo_V', 'Saldo_T', 'Saldo_E']].copy()

    df_detalhada = df_detalhada.groupby(
        ['Produto', pd.Grouper(key='Emissao', freq='D')]).last().reset_index()

    df_detalhada = df_detalhada.astype(
        {'Saldo_V': int, 'Saldo_T': int, 'Saldo_E': int})

    df_detalhada['Arquivo'] = arquivo.split('\\')[-1]

    df = df.groupby(['Produto', pd.Grouper(
        key='Emissao', freq='M')]).last().reset_index()

    # df['Emissao'] = df['Emissao'] + pd.tseries.offsets.MonthEnd()

    df = df.astype({'Saldo_V': int, 'Saldo_T': int, 'Saldo_E': int})

    df['Arquivo'] = arquivo.split('\\')[-1]

    return df, df_detalhada


def consolida_historicos_wb(padrao=PATH + 'anoportipoestoque-??.xls'):
    """
    Transforma e consolida os arquivos de historico de estoque do Winbooks

    Input: "anoportipoestoque-??.xls"

    Output: "saldo_historico_mes.csv"
            "saldo_historico_dia.csv"
    """
    arquivos = glob.glob(padrao)

    print('\n')

    # Processa arquivos
    df = pd.DataFrame()
    df_d = pd.DataFrame()
    for arq in arquivos:
        print(arq)
        a, b = le_historicos_wb(arq)
        df = df.append(a, ignore_index=True)
        df_d = df_d.append(b, ignore_index=True)

    # Mantém movimentos do último arquivo para cada Título (diário)
    filtro = df_d.groupby('Produto', as_index=False)['Arquivo'].max()
    df_d = df_d.merge(filtro, how='inner', on=['Produto', 'Arquivo'])

    # Mantém movimentos do último arquivo para cada Título (mensal)
    filtro = df.groupby('Produto', as_index=False)['Arquivo'].max()
    df = df.merge(filtro, how='inner', on=['Produto', 'Arquivo'])

    # Mantem movimentos até junho 2021
    df = df.loc[df['Emissao'] < '2021-07-01']
    df_d = df_d.loc[df_d['Emissao'] < '2021-07-01']

    # Importa movimentos a partir de agosto
    arquivos = glob.glob(padrao.replace('-??.', '-????-??.'))

    for arq in arquivos:
        print(arq)
        a, b = le_historicos_wb(arq)
        df = df.append(a, ignore_index=True)
        df_d = df_d.append(b, ignore_index=True)

    # Salva arquivo diário
    df_d.to_csv(PATH + 'saldo_historico_dia.csv')

    # Salva arquivo mensal
    df.to_csv(PATH + 'saldo_historico_mes.csv')


def gera_estoques_diarios():
    """Esta rotina gera o arquivo com as movimentações e saldos diários para cada produto.

            - Lê arquivo de histórico de saldos
            - Acrescenta Dados de Produção - Tiragem e Custo
            - Acrescenta ISBN - Cadastro de Produtos
            - Calcula o Preço Médio dos Produtos
            - Completa todos os dias do calendário
    """
    import numpy as np
    import pandas as pd

    print('\nLendo arquivo histórico de saldos...')
    mov = pd.read_csv(ARQ_HIST, dtype={'Produto': str, 'Saldo_V': int,
                                       'Saldo_T': int, 'Saldo_E': int, 'Arquivo': str}, parse_dates=['Emissao'],
                      index_col=0)

    # Ajusta descrições truncadas no relatório do histórico
    mov.loc[mov['Produto'].str.contains(
        'ENTRE O ENCARDIDO,'), 'Produto'] = 'ENTRE O ENCARDIDO, O BRANCO E O BRANQUÍSSIMO'

    mov.loc[mov['Produto'].str.contains(
        'DESINFORMAÇÃO: CRISE'), 'Produto'] = 'DESINFORMAÇÃO: CRISE POLÍTICA E SAÍDAS DEMOCRÁTICA'

    mov.loc[mov['Produto'].str.contains(
        'DELIVERY FIGHT'), 'Produto'] = 'DELIVERY FIGHT! – A LUTA CONTRA OS PATROES SEM ROS'

    mov.loc[mov['Produto'].str.contains(
        'A ARTE DE VIVER PARA AS NOVAS GERACOES'), 'Produto'] = 'A ARTE DE VIVER PARA AS NOVAS GERACOES'

    mov.loc[mov['Produto'].str.contains(
        'COMO VOCE PODE RIR'), 'Produto'] = 'COMO VOCE PODE RIR'

    # Abre cadastro de produtos
    print('\nLendo arquivo do Cadastro dos Produtos...')
    cad = pd.read_excel(ARQ_CAD, sheet_name='Cadastro',
                        usecols='A,B', header=0, dtype=str,
                        names=['ISBN', 'Produto']).dropna(axis=0, how='any')

    mov = mov.merge(cad, how='left', on=['Produto'], validate='many_to_one')
    del cad

    # Garante que não falta cadastro de produtos (ISBN)
    try:
        assert not (mov['ISBN'].hasnans)
    except AssertionError:
        print('\nFalta Cadastro de Produtos\n')
        print(mov.loc[mov['ISBN'].isna(), 'Produto'].drop_duplicates())

    mov.rename({'Emissao': 'Data'}, axis=1, inplace=True)

    mov = mov.groupby(['ISBN', 'Data']).aggregate({'Saldo_V': 'last',
                                                   'Saldo_T': 'last',
                                                   'Saldo_E': 'last'})

    # Abre arquivo de Custos
    print('\nLendo arquivo de Custos de Produção...')
    pro = pd.read_excel(ARQ_PROD, sheet_name="Tiragens", usecols="A:E",
                        header=0, parse_dates=['Data'], na_values=[None], dtype={'ISBN': str, 'Título': str, 'Custo': float, 'Tiragem': int})

    # Ajusta data mínima de produção
    aux = mov.index.get_level_values(1).min()
    pro.loc[pro['Data'] < aux, 'Data'] = pd.to_datetime(aux)
    del aux

    pro = pro.groupby(['ISBN', 'Data']).aggregate(
        {'Tiragem': sum, 'Custo': sum})

    mov = pd.concat([mov, pro], axis=1)
    del pro

    # Completa Saldos
    cols = ['Saldo_V', 'Saldo_T', 'Saldo_E']
    mov[cols] = mov.groupby(['ISBN'])[cols].ffill().fillna(0)
    del cols

    # Custo Médio Inicial (Primeira produção)
    print('\nCalculando Custos Médios...')
    aux = mov.reset_index().dropna(subset=['Tiragem'])
    aux = aux.loc[aux.groupby('ISBN').head(1)
                  .index, ['ISBN', 'Tiragem', 'Custo']].set_index('ISBN')
    cmini = aux['Custo'] / aux['Tiragem']
    del aux

    mov['Custo Médio'] = np.nan
    isbn = ''
    i = 0

    # Calcula Custo Médio dos Produtos
    for ind, lin in mov.iterrows():
        if (ind[0] != isbn):
            cant = cmini.get(ind[0], default=0)
            isbn = ind[0]
            sant = 0

        if not pd.isna(lin['Tiragem']):
            cant = (cant * sant + lin['Custo']) / (lin['Tiragem'] + sant)

        mov.loc[ind, 'Custo Médio'] = cant

        i += 1
        if (i % 10000) == 0:
            print('Processados {:,d} registros'.format(i).replace(',', '.'))

    print('Processados {:,d} registros'.format(i).replace(',', '.'))

    # Preenche todos os dias
    print('\nIncluindo todos os dias do calendário...')
    rng = pd.date_range(start=mov.index.get_level_values(1).min() +
                        pd.offsets.MonthBegin(-1),
                        end=mov.index.get_level_values(1).max() +
                        pd.offsets.MonthBegin(1),
                        closed='left')

    aux = pd.DataFrame()
    for x in mov.index.get_level_values(0).drop_duplicates():
        df = pd.DataFrame(index=rng)
        df['ISBN'] = x
        aux = pd.concat([aux, df])
        del df

    del rng, x
    aux = aux.reset_index().set_index(['ISBN', 'index'])
    aux.index.names = ['ISBN', 'Data']

    mov = pd.concat([aux, mov], axis=1)
    del aux

    # Completa Saldos e Custo Médio
    cols = ['Saldo_V', 'Saldo_T', 'Saldo_E', 'Custo Médio']
    mov[cols] = mov.groupby(['ISBN'])[cols].ffill().fillna(0)
    del cols

    # Pega Vendas dos Produtos
    print('\nLendo arquivo de Vendas...')
    vnd = pd.read_pickle(ARQ_NOTAS)[['Emissao', 'Produto', 'Vendas']]\
        .rename({'Emissao': 'Data', 'Produto': 'ISBN'}, axis=1)

    vnd = vnd.loc[(vnd['ISBN'].between('9', 'A')) &
                  (vnd['Vendas'] != 0)]

    # Corrige ISBN do DESERAMA
    # vnd.loc[vnd['Titulo'] == 'DESERAMA', 'ISBN'] = '9786586691160'

    vnd = vnd.groupby(['ISBN', 'Data'])['Vendas'].sum()

    mov = mov.merge(vnd, how='outer', on=['ISBN', 'Data'])
    mov['Vendas'] = mov['Vendas'].fillna(0)
    mov['CMV'] = mov['Vendas'] * mov['Custo Médio']
    mov['Valor Estoques'] = (mov['Saldo_V'] + mov['Saldo_T'] +
                             mov['Saldo_E']) * mov['Custo Médio']

    print('\nGerando os arquivos...')
    mov.to_csv(PATH + 'estoques_diarios.csv')
    mov.to_pickle(PATH + 'estoques_diarios.pkl')
    mov.reset_index()\
        .groupby([pd.Grouper(key='Data', freq='M'), 'ISBN'])\
        .aggregate({'CMV': sum, 'Valor Estoques': ['last', 'mean'],
                    'Custo Médio': 'last'})\
        .to_pickle(PATH + 'estoques_mensais.pkl')

    print('\nPronto!')


def prepara_arquivo_margem():

    print('\nLendo arquivo de histórico dos saldos diários...')

    # Lê arquivo dos saldos diários (winbooks)
    est = pd.read_pickle(PATH + 'estoques_diarios.pkl').reset_index()

    # Separa somente Publicações
    est = est[est['ISBN'].between('9', 'A', inclusive='neither')]

    print('\nCalculando novas colunas...')

    # Calcula novas colunas
    est['Saldo Final'] = est[['Saldo_V', 'Saldo_T', 'Saldo_E']].sum(axis=1)
    est['Valor Estoque Total'] = est['Saldo Final'] * est['Custo Médio']
    est['Valor Estoque Veneta'] = est['Saldo_V'] * est['Custo Médio']

    est['Saldo Inicial'] = est.groupby('ISBN')['Saldo Final'].shift(1)

    est['Perdas'] = est['Saldo Final'] - est['Tiragem'].fillna(0) \
        - est['Saldo Inicial'] + est['Vendas']
    est['Valor Perdas'] = est['Perdas'] * est['Custo Médio']
    est['Valor Médio'] = est['Saldo Final'] * est['Custo Médio']
    est['Valor Estoque Terceiros'] = est[[
        'Saldo_T', 'Saldo_E']].sum(axis=1) * est['Custo Médio']

    est.to_csv(PATH + 'estoques_diarios_perdas.csv')

    print('\nResumindo por mês...')

    # Resume por Mês
    aux = est.groupby(['ISBN', pd.Grouper(key='Data', freq='MS')])\
        .aggregate({'Saldo Final': 'last',
                    'CMV': 'sum',
                    'Valor Estoque Total': 'last',
                    'Valor Estoque Terceiros': 'last',
                    'Valor Estoque Veneta': 'last',
                    'Valor Médio': 'mean', 'Tiragem': 'sum',
                    'Custo': 'sum',
                    'Vendas': 'sum', 'Perdas': 'sum',
                    'Valor Perdas': 'sum', 'Saldo_V': 'last'})

    aux[['Valor Inicial Estoque', 'Saldo Inicial']] = aux.groupby('ISBN')[
        ['Valor Estoque Total', 'Saldo Final']].shift(1)

    # Tabela Final
    sd_m = aux.reset_index()[
        ['ISBN', 'Data', 'Saldo Inicial', 'Tiragem', 'Custo', 'Vendas',
         'Perdas', 'Saldo Final', 'CMV', 'Valor Médio',
         'Valor Perdas', 'Valor Estoque Veneta',
         'Valor Estoque Terceiros', 'Saldo_V', 'Valor Estoque Total', 'Valor Inicial Estoque']]

    del est, aux

    print('\nLendo arquivo de notas...')

    # Lê arquivo de Notas do Access
    notas = pd.read_pickle(PATH_DASH + 'Notas.pkl')

    notas = notas[notas['Produto'].between('9', 'A', inclusive='neither')]

    aux = notas.groupby(['Produto', 'Titulo',
                         pd.Grouper(key='Emissao', freq='MS')])[
        ['Receita Bruta', 'Receita Líquida', 'Vendas']].sum()

    aux['Desconto'] = 1 - aux['Receita Líquida'] / aux['Receita Bruta']
    aux['Impostos'] = IMP * aux['Receita Líquida']

    print('\nLendo Cadastro de Royalties...')

    # Lê Cadastro de Royalties
    roy = pd.read_excel(PATH_R + 'Cadastro de Produtos.xlsx',
                        sheet_name='Royalties', usecols='A,C', header=0,
                        names=['Produto', 'txRoy'], dtype={'Produto': 'str', 'txRoy': float}).dropna(axis=0, subset=['Produto']).fillna(0.08)

    aux = aux.reset_index().merge(roy, how='left', on='Produto')
    del roy

    # Garante que todos os Livros têm Royalties cadastrados
    try:
        assert not aux.loc[aux['Produto'] != '97885631373', 'txRoy'].hasnans
    except AssertionError:
        print('\n###  Falta cadastro de Royalties... ###\n')
        print(aux.loc[aux['txRoy'].isna(),
              ['Produto', 'Titulo']].drop_duplicates())
        aux['txRoy'] = aux['txRoy'].fillna(0.08)

    aux['Royalties'] = aux['txRoy'] * aux['Receita Bruta']

    aux = sd_m.drop('Vendas', axis=1).merge(aux, how='left',
                                            right_on=['Produto', 'Emissao'],
                                            left_on=['ISBN', 'Data'])

    cad = pd.read_excel(PATH_R + 'Cadastro de Produtos.xlsx',
                        sheet_name='Cadastro', usecols='A,D', header=0,
                        names=['ISBN', 'Titulo'], dtype='str').dropna()

    aux['Titulo'] = aux[['ISBN']].merge(cad, how='left', on=['ISBN'])['Titulo']
    del cad

    try:
        assert not aux['Titulo'].hasnans
    except AssertionError:
        print('\nFalta Cadastro dos Títulos:')
        print(aux.loc[aux['Titulo'].isna(), 'ISBN'].drop_duplicates())
        aux.dropna(subset=['Titulo'], inplace=True)

    cols = ['Receita Bruta', 'Receita Líquida', 'Vendas',
            'Desconto', 'Impostos', 'txRoy', 'Royalties']
    aux[cols] = aux[cols].fillna(0)
    del cols

    aux = aux.drop(['Emissao', 'Produto'], axis=1).rename(
        {'Data': 'Emissao', 'Custo': 'Produção'}, axis=1)

    aux['Margem'] = aux['Receita Líquida'] - aux['Royalties'] -\
        aux['Impostos'] - aux['CMV']
    aux['Margem %'] = aux['Margem'] / aux['Receita Líquida']
    aux = aux[['Receita Bruta', 'Desconto', 'Receita Líquida', 'Vendas',
               'Royalties', 'Impostos', 'CMV', 'Margem', 'Margem %',
               'Perdas', 'Valor Perdas', 'Valor Estoque Veneta',
               'Valor Estoque Total', 'Valor Inicial Estoque',
               'Valor Estoque Terceiros', 'Emissao', 'Titulo', 'ISBN',
               'Saldo Inicial', 'Tiragem', 'Produção', 'Saldo Final', 'Saldo_V']]

    print('\nGerando arquivo da margem...')

    aux = aux[aux['Emissao'] < '2021-10']

    aux.set_index(['Titulo', 'Emissao']).to_pickle(PATH_DASH + 'margem.pkl')

    print('\nPronto.')


def gera_margem():
    print('\nConsolidando arquivos de Estoque do Winbooks...')
    consolida_historicos_wb()
    print('\nGerando arquivo dos estoques diários...')
    gera_estoques_diarios()
    print('\nGerando arquivo de margem...')
    prepara_arquivo_margem()


if __name__ == '__main__':
    gera_margem()
