import pandas as pd

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
