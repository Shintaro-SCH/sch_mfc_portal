import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
BIN_TABLE = BASE_DIR / "spec_tables" / "gf125_bin_size_table.xlsx"

bin_df = pd.read_excel(BIN_TABLE)

def calc_bin_size(gas, flow):

    if pd.isna(gas) or pd.isna(flow):
        return None

    gas = str(gas).strip().upper()

    try:
        flow = float(flow)
    except:
        return None

    rows = bin_df[bin_df["gas"].str.upper() == gas]

    for _, r in rows.iterrows():

        if r["low"] <= flow <= r["high"]:
            return r["bin"]

    return None