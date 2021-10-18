""" Consolida os arquivos de Histórico de Estoques
    Prepara o arquivo da consignação para o DashBoard

"""
import glob
import pandas as pd

PATH = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Estoques\\'
PATH_R = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Despesas e Custos\\'
PATH_DASH = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Python\\veneta-dash\\data\\'
PATH_N = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Devoluções\\'


def le_historicos_wb(arquivo):
    """
    Lê os arquivos gerados pelo relatório do Winbooks:
        HISTÓRICO DE PRODUTOS TOTALIZADO POR ANO - POR TIPO DE ESTOQUE
    """
    # Lê arquivo
    df = pd.read_excel(arquivo, header=None, usecols='A,C,L,M,O,P', dtype=str,
                       names=['Emissao', 'Razão', 'Entrada_T', 'Saída_T',
                              'Entrada_E', 'Saída_E']).dropna(subset=['Emissao'])

    # Cria coluna de Produto
    filtro = df['Emissao'].str.contains('^Produto:')

    df['Produto'] = df.loc[filtro, 'Emissao'].str.strip('Produto:').str.strip()
    df['Produto'].fillna(method='ffill', axis=0, inplace=True)

    # Arruma a coluna das datas
    df['Emissao'] = pd.to_datetime(
        df['Emissao'], dayfirst=True, errors='coerce')

    # Elimina linhas inválidas
    df.dropna(subset=['Emissao'], inplace=True)

    df = df.astype({'Entrada_T': int, 'Saída_T': int, 'Entrada_E': int,
                    'Saída_E': int})

    # somente linhas com movimentação
    df = df[df[['Entrada_T', 'Saída_T', 'Entrada_E', 'Saída_E']].ne(
        0).any(axis=1)]

    df = df.groupby(['Produto', 'Razão', pd.Grouper(
        key='Emissao', freq='M')]).sum().reset_index()

    df['Arquivo'] = arquivo.split('\\')[-1]

    return df


def consolida_historicos_wb(padrao=PATH + 'anoportipoestoque-??.xls'):
    """
    Transforma e consolida os arquivos de historico de estoque do Winbooks

    Input: "anoportipoestoque-??.xls"

    Output: "mov_consignacao_mes.csv"
    """
    arquivos = glob.glob(padrao)

    print('\n')

    # Processa arquivos
    df = pd.DataFrame()
    for arq in arquivos:
        print(arq)
        a = le_historicos_wb(arq)
        df = df.append(a, ignore_index=True)

    # Mantém movimentos do último arquivo para cada Título (mensal)
    filtro = df.groupby('Produto', as_index=False)['Arquivo'].max()
    df = df.merge(filtro, how='inner', on=['Produto', 'Arquivo'])

    # Mantem movimentos até junho 2021
    df = df.loc[df['Emissao'] < '2021-07-01']

    # Importa movimentos a partir de agosto
    arquivos = glob.glob(padrao.replace('-??.', '-????-??.'))

    for arq in arquivos:
        print(arq)
        a = le_historicos_wb(arq)
        df = df.append(a, ignore_index=True)

    # Salva arquivo mensal
    df.to_csv(PATH + 'mov_consignacao_mes.csv')


def gera_arquivo_consig():
    """Esta rotina gera o arquivo com as movimentações e saldos diários para cada produto.

            - Lê arquivo de histórico de saldos
            - Acrescenta Dados de Produção - Tiragem e Custo
            - Acrescenta ISBN - Cadastro de Produtos
            - Calcula o Preço Médio dos Produtos
            - Completa todos os dias do calendário
    """
    import numpy as np
    import pandas as pd

    PATH_NOTAS = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Python\\veneta-dash\\data\\'
    PATH_CONS = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Python\\veneta-coml\\data\\'

    ARQ_HIST = PATH + 'mov_consignacao_mes.csv'
    ARQ_CAD = PATH_R + 'Cadastro de Produtos.xlsx'
    ARQ_NOTAS = PATH_NOTAS + 'Notas.pkl'

    print('\nLendo arquivo histórico de saldos...')
    mov = pd.read_csv(ARQ_HIST, dtype={'Produto': str, 'Razão': str,
                                       'Entrada_T': int, 'Saída_T': int,
                                       'Entrada_E': int, 'Saída_E': int, 'Arquivo': str}, parse_dates=['Emissao'],
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
                        usecols='A,B,D', header=0, dtype=str,
                        names=['ISBN', 'Produto', 'Titulo']).dropna(axis=0, how='any')

    mov = mov.merge(cad, how='left', on=['Produto'], validate='many_to_one')
    del cad

    # Garante que não falta cadastro de produtos (ISBN)
    try:
        assert not (mov['ISBN'].hasnans)
    except AssertionError:
        print('\nFalta Cadastro de Produtos\n')
        print(mov.loc[mov['ISBN'].isna(), 'Produto'].drop_duplicates())
        mov.loc[mov['Titulo'].isna(
        ), 'Titulo'] = mov.loc[mov['Titulo'].isna(), 'Produto']

    # Coloca CNPJ

    print('\nLendo arquivo de notas fiscais...')
    cad = pd.read_pickle(PATH_N + 'NFCAB.pkl')[['CLI_NOME', 'CLI_CGCCPF']]\
        .drop_duplicates(subset=['CLI_NOME']).dropna()

    cad['CLI_NOME'] = cad['CLI_NOME'].str.strip()

    mov['CNPJ'] = mov.merge(cad, how='left',
                            left_on=['Razão'], right_on=['CLI_NOME'])['CLI_CGCCPF'].str.strip()

    del cad

    # Coloca CNPJ de Empresa não Encontrada no Cadastro
    mov.loc[mov['Razão'].str.contains('LEITURA SHOPPING CIDADE LTDA'),
            'CNPJ'] = '03245897000101'

    # Garante que não há nenhuma Empresa sem CNPJ
    try:
        assert not (mov['CNPJ'].hasnans)
    except AssertionError:
        print('\nNão encontrou CNPJ...\n')
        print(mov.loc[mov['CNPJ'].isna(), 'Razão'].drop_duplicates())

    # Pega Grupo do arquivo Notas.pkl
    print('\nLendo arquivo de movimentos Access...')
    cad = pd.read_pickle(ARQ_NOTAS)[['CNPJ', 'Grupo']].drop_duplicates(
        subset=['CNPJ']).dropna()
    mov['Grupo'] = mov.merge(cad, how='left', on=['CNPJ'])[['Grupo']]
    del cad

    mov.loc[mov['Grupo'].isna(), 'Grupo'] = mov.loc[mov['Grupo'].isna(), 'Razão']

    mov.rename({'Emissao': 'Data'}, axis=1, inplace=True)

    mov = mov.groupby(['Produto', 'ISBN', 'Titulo', 'Grupo', 'Data'],
                      as_index=False).sum()

    mov[['Saída_T', 'Saída_E']] = mov[['Saída_T', 'Saída_E']].multiply(-1)

    mov['Saldo'] = mov[['Entrada_T', 'Saída_T',
                        'Entrada_E', 'Saída_E']].sum(axis=1)

    mov.sort_values(['Produto', 'Grupo', 'Data'],
                    ignore_index=True, inplace=True)

    mov['Saldo'] = mov.groupby(['Produto', 'Grupo'])['Saldo'].cumsum()

    # Pega custo médio do estoque
    print('\nLendo arquivo de estoques mensais...')
    cad = pd.read_pickle(PATH + 'estoques_mensais.pkl')['Custo Médio']
    mov['Custo Final'] = mov.merge(
        cad, how='left', on=['Data', 'ISBN'])['last']
    del cad

    # Garante que todos os custos foram encontrados
    try:
        assert not mov['Custo Final'].hasnans
    except AssertionError:
        print('\nNão encontrou o Custo Médio...\n')
        print(mov.loc[mov['Custo Final'].isna(), [
              'Data', 'ISBN', 'Produto']].drop_duplicates())

    mov['Valor Final'] = mov['Saldo'] * mov['Custo Final']
    mov['Data'] = mov['Data'] + pd.offsets.MonthBegin(-1)

    print('\nGerando os arquivos...')

    mov[['Grupo', 'Data', 'Titulo', 'Saldo', 'Valor Final']
        ].to_pickle(PATH_CONS + 'consig.pkl')

    print('\nPronto!')


def gera_consignacoes():
    consolida_historicos_wb()
    gera_arquivo_consig()


if __name__ == '__main__':
    gera_consignacoes()
