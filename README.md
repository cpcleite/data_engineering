# data_engineering
Data Engineering in Python

This project contais several routines responsible for scraping text and excel files containing data from our ERP (Winbooks).
The Pipeline collects all data, check consistency and save it in some csv and pickle files. CSV files are use to generate Excel Reports via Dynamic Tables for data consistency detailing. Pickle Files are used by other Python applications. Mainly in Dashboard Applications.


### 1. src/atualiza_dados.py

This is the main Pipeline routine which starts each step of consolidation and consistency.

### 2. img/*.png

These images show how several routines are connected and which files they create.
