import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
INV_PATH = BASE_DIR / "inventory" / "mfc_inventory_stock.xlsx"

inv_df = pd.read_excel(INV_PATH, usecols=[2,5])

inv_df.columns = ["model", "order_code"]

inv_df["model"] = inv_df["model"].astype(str).str.upper().str.strip()

def check_inventory(model):

    if model is None:
        return False, None

    model = str(model).upper().strip()

    row = inv_df[inv_df["model"] == model]

    if row.empty:
        return False, None

    return True, row.iloc[0]["order_code"]