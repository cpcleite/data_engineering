import pandas as pd

PATH = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Veneta\\Dados Winbooks\\Estoques\\'

PATH_N = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Python\\veneta-dash\\data\\'

df = pd.read_pickle(PATH + 'clientes.pkl')
nt = pd.read_pickle(PATH_N + 'notas.pkl')

# print(nt.info())

df['CNPJ'] = df['CLI_CGCCPF'].str.strip()

nt = nt.loc[nt['Emissao'] >= '2020-11-01', 'CNPJ']

nt = nt.loc[nt.str.len() == 11].drop_duplicates()

nt = df.merge(nt, how='inner', on='CNPJ')['CLI_EMAIL']
nt = nt[nt.str.strip().str.len() != 0].drop_duplicates()
# print(nt.str.len().value_counts())

print(nt.head())
print(nt.shape)
print(nt.value_counts(ascending=False).head())

nt.to_csv('email.csv', index=False)
