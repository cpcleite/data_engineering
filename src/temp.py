import pandas as pd

PATH = 'C:/Users/cpcle/OneDrive/Documentos/Celso/Veneta/Dados Winbooks/Estoques/'
PATH_P = 'C:/Users/cpcle/OneDrive/Documentos/Celso/Veneta/Dados Winbooks/Despesas e Custos/'

MES = PATH + 'anoportipoestoque-2021-08.xls'
DT_CORTE = '2021-07-31'
PROD = PATH_P + 'Cadastro de Produtos.xlsx'
DIARIO = PATH + 'estoques_diarios.pkl'

# Lê arquivo mensal
df = pd.read_excel(MES, sheet_name=0, usecols='A,E:F,H,L:Q',
                   names=['Data', 'ME', 'MS', 'Veneta',
                          'TE', 'TS', 'Terc', 'EE', 'ES', 'Eve'],
                   skiprows=5, dtype=str)

# Prepara colunas de Título e de Datas
df = df.dropna(subset=['Data'])
filtro = df['Data'].str.contains('Produto:')
df.loc[filtro, 'Titulo'] = df.loc[filtro, 'Data']
df['Titulo'] = df['Titulo'].str.strip('Produto:').str.strip()
df['Titulo'].ffill(inplace=True)
df['Data'] = pd.to_datetime(df['Data'], errors='coerce', format='%d/%m/%Y')
df.dropna(subset=['Data'], inplace=True)

# Muda tipos para inteiro
df = df.astype(dtype={'ME': int, 'MS': int, 'Veneta': int,
                      'TE': int, 'TS': int, 'Terc': int,
                      'EE': int, 'ES': int, 'Eve': int})

# Elimina produtos que não são livros impressos
df = df[~df['Titulo'].str.contains(
    'EBOOK|AUDIOBOOK|AR12MSS|CURVA PVC|AR12MSS|38X38X300|ODUTO PVC 3/4|CAMISETA|ILUSTRACAO ORIGINAL|E-BOOK|E-PUB|GERACOES -', case=False)]

# Ajusta descrições truncadas no relatório do histórico
df.loc[df['Titulo'].str.contains(
    'ENTRE O ENCARDIDO,'), 'Titulo'] = 'ENTRE O ENCARDIDO, O BRANCO E O BRANQUÍSSIMO'

df.loc[df['Titulo'].str.contains(
    'DESINFORMAÇÃO: CRISE'), 'Titulo'] = 'DESINFORMAÇÃO: CRISE POLÍTICA E SAÍDAS DEMOCRÁTICA'

df.loc[df['Titulo'].str.contains(
    'DELIVERY FIGHT'), 'Titulo'] = 'DELIVERY FIGHT! – A LUTA CONTRA OS PATROES SEM ROS'

df.loc[df['Titulo'].str.contains(
    'A ARTE DE VIVER PARA AS NOVAS GERACOES'), 'Titulo'] = 'A ARTE DE VIVER PARA AS NOVAS GERACOES'

df.loc[df['Titulo'].str.contains(
    'COMO VOCE PODE RIR'), 'Titulo'] = 'COMO VOCE PODE RIR'

# Pega as primeiras datas para cada Titulo
df = df.loc[df.groupby('Titulo')['Data'].idxmin()]

df['Inicial_V'] = df['Veneta'] - df['ME'] + df['MS']
df['Inicial_T'] = df['Terc'] - df['TE'] + df['TS']
df['Inicial_E'] = df['Eve'] - df['EE'] + df['ES']

# Lê cadastro de produtos (ISBN)
pro = pd.read_excel(PROD, sheet_name=0, usecols='A,B',
                    names=['ISBN', 'Titulo'], dtype=str).dropna()

df = df.merge(pro, how='left', on=['Titulo'], validate='one_to_one')

print(df[df['ISBN'].isna()])

df.dropna(subset=['ISBN'], inplace=True)

dia = pd.read_pickle(DIARIO)[['Saldo_V', 'Saldo_T', 'Saldo_E']]

dia = dia[dia.index.get_level_values(
    1) == DT_CORTE].reset_index(level=1, drop=True)

df = df.merge(dia, how='left', on='ISBN', validate='one_to_one')

filtro = (df['Inicial_V'] != df['Saldo_V']) | \
         (df['Inicial_T'] != df['Saldo_T']) | \
         (df['Inicial_E'] != df['Saldo_E'])

print(df.shape)
print('\n', df.loc[filtro, ['Titulo', 'ISBN', 'Inicial_V', 'Saldo_V',
      'Inicial_T', 'Saldo_T', 'Inicial_E', 'Saldo_E']])
