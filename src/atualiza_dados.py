from Miscelaneous.Importa_do_Access import main as importa
from gera_nf_cli import grava_pickle, consolida_pkl
from gera_arq_margem import gera_margem
from gera_arq_consig import gera_consignacoes
from gera_arq_receita import gera_receita
from gera_arq_opex import gera_opex
from gera_movimentos_wb import gera_mov_wb

from shutil import copyfile
from os import remove, listdir
from datetime import datetime

P_CML = 'C:/Users/cpcle/OneDrive/Documentos/Celso/Python/veneta-coml/data/'
P_DSH = 'C:/Users/cpcle/OneDrive/Documentos/Celso/Python/veneta-dash/data/'
P_DNL = 'C:/Users/cpcle/Downloads/'
P_DEV = 'C:/Users/cpcle/OneDrive/Documentos/Celso/Veneta/Dados Winbooks/Devoluções/'
P_EST = 'C:/Users/cpcle/OneDrive/Documentos/Celso/Veneta/Dados Winbooks/Estoques/'

# Data de Início da atualização do NFCAB.pkl
INICIO = datetime(2021, 7, 1)


def main(Importa=False):
    """Gera todos os arquivos do dashboard.
    """
    if Importa:
        print('\nImportando dados do Access...')
        importa()

        print('\nCopiando dados do Access...')
        copyfile(P_DSH + 'Notas.pkl', P_CML + 'Notas.pkl')

        print('\nGerando arquivo de Cabeçalho das Notas Fiscais...')
        grava_pickle(P_DNL + 'NFCAB.dbf', P_DNL + 'NFCAB', INICIO)
        consolida_pkl(P_DNL + 'NFCAB', INICIO)

        print('\nCopiando arquivo de Cabeçalho das Notas Fiscais...')
        copyfile(P_DNL + 'NFCAB.pkl', P_DEV + 'NFCAB.pkl')
        aux = [file for file in listdir(P_DNL)
               if ('NFCAB' in file) and ('.pkl' in file)]
        for file in aux:
            remove(P_DNL + file)

        print('\nGerando arquivo de clientes...')
        grava_pickle(P_DNL + 'clientes.dbf', P_DNL + 'clientes', INICIO)
        consolida_pkl(P_DNL + 'clientes', INICIO)

        print('\nCopiando arquivo de clientes...')
        copyfile(P_DNL + 'clientes.pkl', P_EST + 'clientes.pkl')
        aux = [file for file in listdir(P_DNL)
               if ('clientes' in file) and ('.pkl' in file)]
        for file in aux:
            remove(P_DNL + file)

    print('\nGerando arquivo de margens...')
    gera_margem()
    copyfile(P_DSH + 'margem.pkl', P_CML + 'margem.pkl')

    print('\nGerando arquivo de consignações...')
    gera_consignacoes()

    print('\nGerando arquivo de receita...')
    gera_receita()

    print('\nGerando arquivo de gastos operacionais...')
    gera_opex()

    print('\nGerando arquivo de movimentos wb')
    gera_mov_wb()


if __name__ == '__main__':
    main(Importa=False)
