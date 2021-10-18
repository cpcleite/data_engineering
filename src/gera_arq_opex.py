# -*- coding: utf-8 -*-
"""
Lê os dados da OPEX e gera o arquivo opex.pkl

Created on Wed Aug 11 16:07:00 2021

@author: Celso Leite
"""
import pandas as pd


def gera_opex():
    PATH_D = 'C:\\Users\\cpcle\\OneDrive\\Documentos\\Celso\\Python\\veneta-dash\\data\\'

    OPEX = PATH_D + 'opex.pkl'

    df = " (91.470, 14)(73.493,66)(86.260,59)(70.596,58)(65.403,73)(120.298, 04)(40.307, 45)(103.117, 91)(104.869, 61)(93.488, 24)(240.250, 98)(79.195, 22)(95.329, 22)(86.737, 57)(94.105, 39)(112.781, 06)(153.649, 89)(97.052, 59)(109.838, 41)(115.973, 48)"

    df = df.replace(' ', '').replace('(', '').replace('.', '')\
        .replace(',', '.').replace(')', ' ')

    df = [float(x) for x in df.split()]
    df = pd.DataFrame(df, columns=['OPEX'])
    df.index = pd.date_range('2020-01-01', periods=df.shape[0], freq='MS')
    df.index.name = 'Emissao'
    df.to_pickle(OPEX)


if __name__ == '__main__':
    gera_opex()
