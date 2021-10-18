# -*- coding: utf-8 -*-
"""
Import Invoices from Access File.

Created on Thu Oct 22 11:17:59 2020

@author: Celso Leite
"""

import urllib
from sqlalchemy import create_engine
import pandas as pd

# File Paths
pref = "C:/Users/cpcle/OneDrive/Documentos/Celso/Veneta/"
pref2 = "C:/Users/cpcle/OneDrive/Documentos/Celso/Python/veneta-dash/data/"


def main(salva=True):
    # Connection string to Access Database
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + pref + r'NFe.accdb;'
        r'ExtendedAnsiSQL=1;'
    )

    connection_uri = \
        f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}"

    # Create Engine
    engine = create_engine(connection_uri)

    # Read Notas Query
    with engine.begin() as conn:
        Notas = pd.read_sql('SELECT * FROM Notas', conn)

    Notas.drop(columns=['MesesLanc', 'CMV', 'CDL',
                        'MesesLanc', 'Comissao', 'TipoLoja'], inplace=True)

    # Write to pickle and csv files
    if salva:
        Notas.to_pickle(pref2 + 'Notas.pkl')
        # Notas.to_csv(pref2 + 'Notas.csv')

    return Notas


if __name__ == '__main__':
    main(True)
