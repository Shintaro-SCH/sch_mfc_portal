import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
GAS_TABLE = BASE_DIR / "spec_tables" / "gas_code_table.xlsx"

gas_df = pd.read_excel(GAS_TABLE)

def generate_cord(gas, flow):

    if pd.isna(gas) or pd.isna(flow):
        return None

    gas = str(gas).strip().upper()

    row = gas_df[gas_df["gas"].str.upper() == gas]

    if row.empty:
        return None

    code = str(row.iloc[0]["code"]).zfill(4)

    try:
        flow = float(flow)
    except:
        return None

    if flow < 1000:

        flow_part = str(int(flow)).zfill(3)
        return f"{code}{flow_part}C"

    else:

        liters = int(flow / 1000)
        flow_part = str(liters).zfill(3)
        return f"{code}{flow_part}L"