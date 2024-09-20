import os

import pandas as pd
import psycopg2

def remove_stdin_copy_statement(sql_script: str) -> str:
    """
    Remove the STDIN COPY statement from the SQL script

    - Parameters
    ----------
    sql_script : str

    - Returns
    -------
    str
    """
    copy_statement_count = sql_script.count('COPY')

    for _ in range(copy_statement_count):
        copy_index = sql_script.find('COPY')
        copy_end_index = sql_script.find('\\.', copy_index) + len('\\.')

        if copy_index != -1 and copy_end_index != -1:
            sql_script = sql_script[:copy_index] + sql_script[copy_end_index:]

    return sql_script

def import_postgresql_schema(path: str) -> None:
    """
    Import the PostgreSQL schema
    """
    connection = psycopg2.connect(
        host=os.getenv('DATABASE_HOST'),
        port=os.getenv('DATABASE_PORT', '5432'),
        user=os.getenv('DATABASE_USER', 'postgres'),
        password=os.getenv('DATABASE_PASSWORD', 'admin'),
        dbname=os.getenv('DATABASE_NAME', 'clientes')
    )

    cursor = connection.cursor()

    with open(path, 'r', encoding='UTF-8') as file:
        sql_script = file.read()

    sql_script = remove_stdin_copy_statement(sql_script)

    cursor.execute(sql_script)

    connection.commit()

    cursor.close()


def load_data_from_excel_sheets(path: str) -> pd.DataFrame:
    """
    Load data from an Excel file

    - Parameters
    ----------
    path : str

    - Returns
    -------
    pd.DataFrame

    """
    # Load data
    data = pd.read_excel(path)

    return data

def ingestion() -> pd.DataFrame:
    """Load data from the source and make data ingestion"""
    # Load data
    data = load_data_from_excel_sheets('../data/dados_importacao.xlsx')

    return data
