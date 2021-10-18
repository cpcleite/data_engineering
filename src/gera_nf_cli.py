from dbf import Table, export
import pandas as pd
import glob
from datetime import datetime, date

ARQ_ANT = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Devoluções\\NFCAB.pkl'
PATH = 'C:/Users/cpcle/Downloads/'
DBF = 'NFCAB.dbf'
PANDAS = 'NFCAB'
DATA_INI = datetime(2000, 1, 1)


def procura_pedido():
    tabela = Table(PATH + DBF)

    df = pd.DataFrame()

    with tabela:

        colunas = [1, 4, 5, 21]
        nomes = [tabela.field_names[col] for col in colunas]
        recs = 0

        for linha in tabela:
            try:
                data_emissao = linha[23]
            except:
                data_emissao = date(2000, 1, 1)
                print(linha._recnum)

            if (type(data_emissao) == date) & \
                    (data_emissao >= date(2020, 1, 1)):

                waux = [linha[col] for col in colunas]
                df = df.append(pd.Series(waux), ignore_index=True)
                recs += 1

            if recs % 1000 == 999:
                df.columns = nomes
                df.to_excel(PATH + PANDAS + str(recs + 1) + '.xlsx')
                df = pd.DataFrame()

            if linha._recnum % 1000 == 999:
                print(str(linha._recnum + 1), str(recs))

        if len(df) > 0:
            df.columns = [tabela.field_names[col] for col in colunas]
            df.to_excel(PATH + PANDAS + str(recs + 1) + '.xlsx')


def exporta_dbf():

    tabela = Table(PATH + DBF)
    tabela.open()
    export(tabela, PATH + 'NFCAB.csv', format='csv',
           header=True, encoding='UTF-8')
    tabela.close()


def grava_xlsx():
    nf = pd.DataFrame()

    test = Table(PATH + DBF)
    test.open()

    for record in test:
        aux = []
        for field in record:
            aux.append(field)

        nf = nf.append(pd.Series(aux), ignore_index=True)
        if record._recnum % 1000 == 0:
            nf.columns = test.field_names
            nf.to_excel(PATH + PANDAS + str(record._recnum) + ".xlsx")
            nf = pd.DataFrame()
            print(record._recnum)

    test.close()

    nf.columns = test.field_names
    nf.to_excel(PATH + PANDAS + str(record._recnum) + ".xlsx")


def grava_pickle(arq_dbf, arq_saida, dt_ini=DATA_INI):
    """    
    Grava os arquivos pickle das Notas Fiscais.

    Lê o arquivo DBF nfcab.dbf e nfcab.fpt e grava no formato pickle em arquivos com 1000 notas.

    """
    clientes = arq_dbf.split('/')[-1] == 'clientes.dbf'
    nf = pd.DataFrame()

    print('\nIniciando Leitura...')

    gravados = 0
    test = Table(arq_dbf)
    test.open()

    for record in test:
        if clientes or (record['NOF_EMISSA'] >= dt_ini.date()):
            aux = []
            for field in record:
                aux.append(field)

            nf = nf.append(pd.Series(aux), ignore_index=True)

            if ((gravados % 1000) == 0) and (gravados != 0):
                nf.columns = test.field_names
                nf.to_pickle(arq_saida + str(gravados) + ".pkl")
                nf = pd.DataFrame()
                print('Gravados: {:,d}'.format(gravados))

            gravados += 1

        if record._recnum % 1000 == 999:
            print('Lidos: {:,d}, gravados: {:,d}'.format(
                record._recnum + 1, gravados))

    test.close()

    if nf.shape[0] > 0:
        nf.columns = test.field_names
        nf.to_pickle(arq_saida + str(gravados) + ".pkl")


def consolida_pkl(arq_saida, dt_ini=DATA_INI):
    """
    Consolida os arquivos pkl com 1000 notas em um único arquivo.
    """
    padrao = arq_saida + '*.pkl'
    arquivos = glob.glob(padrao)

    if arq_saida.split('/')[-1] == 'NFCAB':
        df = pd.read_pickle(ARQ_ANT)
        df = df.loc[pd.to_datetime(df['NOF_EMISSA']) < dt_ini]
    else:
        df = pd.DataFrame()

    for arq in arquivos:
        print(arq)
        df = df.append(pd.read_pickle(arq), ignore_index=True)

    print(df.shape)

    # Ajusta formatos de campos chave
    if df.columns.str.contains('NOF_NUMERO').any():
        df['NOF_NUMERO'] = df['NOF_NUMERO'].astype('int64')
        df['EMP_RAZSOC'] = df['EMP_RAZSOC'].map(str.strip)

    elif df.columns.str.contains('CLI_CORTE').any():
        df['CLI_CORTE'] = pd.to_datetime(df['CLI_CORTE'], format='%Y-%m-%d')

    elif df.columns.str.contains('FOR_CORTE').any():
        df['FOR_CORTE'] = pd.to_datetime(df['FOR_CORTE'], format='%Y-%m-%d')

    # Salva arquivo consolidado
    df.to_pickle(arq_saida + '.pkl')
    # df.to_excel(PATH + 'arq.xlsx')
    # print(df.info())


if __name__ == '__main__':
    grava_pickle(PATH + 'NFCAB.dbf', PATH + 'NFCAB')
    consolida_pkl(PATH + 'NFCAB', datetime(2000, 7, 1))
    grava_pickle(PATH + 'clientes.dbf', PATH + 'clientes')
    consolida_pkl(PATH + 'clientes')
