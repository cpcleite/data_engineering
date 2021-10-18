import pandas as pd
import Miscelaneous.Importa_do_Access as imp
import verifica_estoque_wb as ver

PATH = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Estoques\\'
PATH_NF = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Devoluções\\'
PATH_DASH = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Python\\veneta-dash\\data\\'

ARQ_XLSX = PATH + 'Notas.xlsx'
ARQ_NOTAS = PATH_DASH + 'Notas.pkl'
ARQ_NF = PATH_NF + 'NFCAB.pkl'

# Importa Notas do Access
print('\nImportando do Access')
notas = imp.main(False)

# Prepara campos chave Cabeçalho das NF's
notas['NFaux'] = notas['NF'].astype(int)
notas['CNPJaux'] = notas['CNPJ'].str.strip()
notas['ISBN'] = notas['Produto'].str.strip()

# Lê arquivo dos cabeçalhos das NF's (Winbooks)
nf = pd.read_pickle(ARQ_NF).loc[:, [
    'NOF_NFINUM', 'CLI_CGCCPF', 'EMP_RAZSOC', 'NOF_EMISSA', 'CFO_DESCRI']]

nf['Emissao'] = pd.to_datetime(nf['NOF_EMISSA'])
nf['NFaux'] = nf['NOF_NFINUM'].astype(int)
nf['CNPJaux'] = nf['CLI_CGCCPF'].str.strip()
nf['Filial'] = nf['EMP_RAZSOC'].map(
    lambda x: 1 if x == 'EDITORA VENETA' else 2)
nf['CFO_DESCRI'] = nf['CFO_DESCRI'].str.strip()

# Elimina colunas denecessárias
nf.drop(axis=1, inplace=True, columns=[
        'NOF_NFINUM', 'CLI_CGCCPF', 'EMP_RAZSOC', 'NOF_EMISSA'])

# Somente notas diferente de 0
nf = nf.loc[(nf['NFaux'] != 0) &
            (nf['CFO_DESCRI'] != 'CARTA DE CORREÇÃO')].drop_duplicates()

# Inclui Descrição da NOF
notas = notas.merge(nf, how='left', validate='many_to_one',  on=[
                    'Emissao', 'Filial', 'NFaux', 'CNPJaux'])

notas.drop(axis=1, inplace=True, columns=['NFaux', 'CNPJaux', 'ISBN'])

print('Salvando Notas.xlsx...')
notas.to_excel(ARQ_XLSX)

print('Salvando Notas.pkl...')
notas.to_pickle(ARQ_NOTAS)

print('Verificando...')
ver.main(notas)

print('\nPronto!\n')
