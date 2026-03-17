import time
import re
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="SCH JAPAN MFC Portal", layout="wide")

# =========================================================
# Paths
# =========================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DB_DIR = BASE_DIR / "database"
SPEC_TABLE_DIR = BASE_DIR / "spec_tables"
INVENTORY_DIR = BASE_DIR / "inventory"

GF125_GF126_TABLE_PATH = SPEC_TABLE_DIR / "gf125_gf126_python_tables.xlsx"
GF120_GAS_CODE_TABLE_PATH = SPEC_TABLE_DIR / "gf120_gas_code_table.xlsx"
INVENTORY_PATH = INVENTORY_DIR / "mfc_inventory_stock.xlsx"

# =========================================================
# Login users
# =========================================================
USERS = {
    "nobuhiro_azuma@sch-japan.com": "sch.001",
    "iwasaki_tomo@sch-japan.com": "sch.002",
    "maeda_shintaro@sch-japan.com": "sch.003",
}

# =========================================================
# Inventory alias
# =========================================================
MODEL_TO_INV_ALIAS = {
    "GF125CXXC": "GF125CC",
    "GF125XSLC": "GF120SC",
    "GF125CXXO": "GF125CO",
}

# =========================================================
# Session state
# =========================================================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if "page_mode" not in st.session_state:
    st.session_state.page_mode = "dashboard"

# =========================================================
# Helpers
# =========================================================
def get_db_files():
    return list(DB_DIR.glob("*_mfc_master_database.xlsx"))


def norm_text(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip().upper()


def clean_disp(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if s.upper() in ["NONE", "NAN", "NAT"]:
        return ""
    return s


def normalize_gas_symbol(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip().upper().replace("/", "").replace(" ", "")


def pick_first_existing(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def pretty_fab_name(v) -> str:
    s = norm_text(v)
    mapping = {
        "JASM_FAB_23_A_MFC_MASTER_DATABASE": "JASM Fab23A",
        "JASM_FAB_23_B_MFC_MASTER_DATABASE": "JASM Fab23B",
        "SONY_IS_MFC_MASTER_DATABASE": "SONY IS",
        "SONY_CIS_2_MFC_MASTER_DATABASE": "SONY CIS-2",
    }
    return mapping.get(s, clean_disp(v))


def format_install_date(v) -> str:
    if pd.isna(v):
        return ""
    dt = pd.to_datetime(v, errors="coerce")
    if pd.isna(dt):
        return clean_disp(v)
    return dt.strftime("%Y-%m-%d")


def warranty_status(install_date_value):
    if pd.isna(install_date_value):
        return ("UNKNOWN", "#6B7280")

    dt = pd.to_datetime(install_date_value, errors="coerce")
    if pd.isna(dt):
        return ("UNKNOWN", "#6B7280")

    expire = dt + pd.DateOffset(years=1)
    now = pd.Timestamp.today().normalize()
    soon = now + pd.DateOffset(months=3)

    if expire < now:
        return ("EXPIRED", "#D32F2F")
    if expire <= soon:
        return ("NEAR EXPIRY", "#E0A800")
    return ("ACTIVE", "#2E8B57")


def safe_number(x):
    try:
        if pd.isna(x):
            return None
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None


def normalize_header_name(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().lower()
    s = s.replace("\n", " ").replace("\r", " ")
    s = s.replace("/", "")
    s = s.replace("\\", "")
    s = s.replace("_", "")
    s = s.replace("-", "")
    s = s.replace(" ", "")
    return s


def extract_numeric_from_text(x):
    if pd.isna(x):
        return None

    if isinstance(x, (int, float)) and not isinstance(x, bool):
        try:
            return float(x)
        except Exception:
            return None

    s = str(x).strip().replace(",", "")
    if s == "":
        return None

    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None

    try:
        return float(m.group(1))
    except Exception:
        return None


def find_db_file_path_from_stem(db_stem: str):
    if not db_stem:
        return None

    for p in get_db_files():
        if p.stem == db_stem:
            return p

    return None


@st.cache_data
def load_original_sheet(db_stem: str, sheet_name: str) -> pd.DataFrame:
    file_path = find_db_file_path_from_stem(db_stem)
    if file_path is None or not sheet_name:
        return pd.DataFrame()

    try:
        xls = pd.ExcelFile(file_path)
        if sheet_name not in xls.sheet_names:
            return pd.DataFrame()
        return pd.read_excel(file_path, sheet_name=sheet_name, header=None, dtype=object)
    except Exception:
        return pd.DataFrame()


def load_original_sheet_candidates(db_stem: str, candidates) -> pd.DataFrame:
    file_path = find_db_file_path_from_stem(db_stem)
    if file_path is None:
        return pd.DataFrame()

    try:
        xls = pd.ExcelFile(file_path)
        for sheet_name in candidates:
            s = clean_disp(sheet_name)
            if not s:
                continue
            if s in xls.sheet_names:
                try:
                    return pd.read_excel(file_path, sheet_name=s, header=None, dtype=object)
                except Exception:
                    continue
    except Exception:
        return pd.DataFrame()

    return pd.DataFrame()


# =========================================================
# GF125 / GF126 Python table
# =========================================================
@st.cache_data
def load_gf125_gf126_bin_table(sheet_name: str = "0C_bin_ranges") -> pd.DataFrame:
    if not GF125_GF126_TABLE_PATH.exists():
        return pd.DataFrame()

    try:
        df = pd.read_excel(GF125_GF126_TABLE_PATH, sheet_name=sheet_name)
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    df.columns = [str(c).strip().lower() for c in df.columns]

    symbol_col = pick_first_existing(df, ["gas_symbol", "gas symbol"])
    bin_col = pick_first_existing(df, ["bin", "bin_size"])
    low_col = pick_first_existing(df, ["low", "min"])
    high_col = pick_first_existing(df, ["high", "max"])

    if symbol_col is None or bin_col is None or low_col is None or high_col is None:
        return pd.DataFrame()

    out = df[[symbol_col, bin_col, low_col, high_col]].copy()
    out.columns = ["gas_symbol", "bin", "low", "high"]

    out["gas_symbol"] = out["gas_symbol"].apply(normalize_gas_symbol)
    out["bin"] = out["bin"].astype(str).str.strip().str.upper()
    out["low"] = pd.to_numeric(out["low"], errors="coerce")
    out["high"] = pd.to_numeric(out["high"], errors="coerce")

    return out.dropna(subset=["gas_symbol", "bin", "low", "high"])


def calc_bin_size(gas, flow, sheet_name: str = "0C_bin_ranges") -> str:
    df = load_gf125_gf126_bin_table(sheet_name=sheet_name)
    if df.empty:
        return ""

    gas_norm = normalize_gas_symbol(gas)
    flow_num = safe_number(flow)

    if gas_norm == "" or flow_num is None:
        return ""

    rows = df[df["gas_symbol"] == gas_norm]
    if rows.empty:
        return ""

    hit = rows[(rows["low"] <= flow_num) & (flow_num <= rows["high"])]
    if hit.empty:
        return ""

    return clean_disp(hit.iloc[0]["bin"])


# =========================================================
# GF120 gas code table
# =========================================================
@st.cache_data
def load_gf120_gas_code_table() -> pd.DataFrame:
    if not GF120_GAS_CODE_TABLE_PATH.exists():
        return pd.DataFrame()

    try:
        xls = pd.ExcelFile(GF120_GAS_CODE_TABLE_PATH)
        sheet_name = "gas_codes" if "gas_codes" in xls.sheet_names else xls.sheet_names[0]
        df = pd.read_excel(GF120_GAS_CODE_TABLE_PATH, sheet_name=sheet_name)
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    df.columns = [str(c).strip().lower() for c in df.columns]

    symbol_col = pick_first_existing(df, ["gas_symbol", "gas symbol", "symbol"])
    code_col = pick_first_existing(df, ["gas_code", "gas code", "code"])

    if symbol_col is None or code_col is None:
        return pd.DataFrame()

    out = df[[symbol_col, code_col]].copy()
    out.columns = ["gas_symbol", "gas_code"]
    out["gas_symbol"] = out["gas_symbol"].apply(normalize_gas_symbol)
    out["gas_code"] = (
        out["gas_code"]
        .astype(str)
        .str.extract(r"(\d+)")[0]
        .fillna("")
        .str.zfill(4)
    )

    return out.dropna(subset=["gas_symbol", "gas_code"])


def calc_gf120_cord(gas, flow) -> str:
    gas_df = load_gf120_gas_code_table()
    if gas_df.empty:
        return ""

    gas_norm = normalize_gas_symbol(gas)
    flow_num = safe_number(flow)

    if gas_norm == "" or flow_num is None:
        return ""

    hit = gas_df[gas_df["gas_symbol"] == gas_norm]
    if hit.empty:
        return ""

    gas_code = str(hit.iloc[0]["gas_code"]).zfill(4)

    if flow_num < 1000:
        flow_part = f"{int(flow_num):03d}C"
    else:
        liters = int(flow_num / 1000)
        flow_part = f"{liters:03d}L"

    return gas_code + flow_part


def decode_gf120_cord(size_or_cord: str):
    s = clean_disp(size_or_cord).upper()

    if len(s) < 8:
        return "", ""

    gas_code = s[:4]
    flow_part = s[4:]

    gas_symbol = ""
    gas_df = load_gf120_gas_code_table()
    if not gas_df.empty:
        hit = gas_df[gas_df["gas_code"].astype(str).str.zfill(4) == gas_code]
        if not hit.empty:
            gas_symbol = clean_disp(hit.iloc[0]["gas_symbol"])

    flow_display = ""
    try:
        if flow_part.endswith("C"):
            flow_display = str(int(flow_part[:-1]))
        elif flow_part.endswith("L"):
            liters = int(flow_part[:-1])
            flow_display = str(liters * 1000)
    except Exception:
        flow_display = ""

    return gas_symbol, flow_display


def parse_gf120_model_string(model_str: str):
    """
    GF120 model文字列の両対応版

    対応例:
    1) GF120CXXC-0013005C-CXVOTX-XXXXAX-000
    2) GF120CXXC-0160200C

    戻り値:
    {
        "mfc_model": "...",
        "gas_code": "0160",
        "gas_symbol": "CH2F2",
        "flow": "200",
        "cord": "0160200C",
        "type": "..."
    }
    """
    s = clean_disp(model_str).upper()

    result = {
        "mfc_model": "",
        "gas_code": "",
        "gas_symbol": "",
        "flow": "",
        "cord": "",
        "type": "",
    }

    if not s:
        return result

    parts = s.split("-")

    # 例1: GF120CXXC-0013005C-CXVOTX-XXXXAX-000
    if len(parts) >= 3:
        result["mfc_model"] = parts[0]
        result["cord"] = parts[1]
        result["type"] = parts[2]

    # 例2: GF120CXXC-0160200C
    elif len(parts) == 2:
        result["mfc_model"] = parts[0]
        result["cord"] = parts[1]

    else:
        return result

    cord = result["cord"]
    if len(cord) < 8:
        return result

    gas_code = cord[:4]
    flow_part = cord[4:]

    result["gas_code"] = gas_code

    gas_df = load_gf120_gas_code_table()
    if not gas_df.empty:
        hit = gas_df[gas_df["gas_code"].astype(str).str.zfill(4) == gas_code]
        if not hit.empty:
            result["gas_symbol"] = clean_disp(hit.iloc[0]["gas_symbol"])

    try:
        if flow_part.endswith("C"):
            result["flow"] = str(int(flow_part[:-1]))
        elif flow_part.endswith("L"):
            liters = int(flow_part[:-1])
            result["flow"] = str(liters * 1000)
    except Exception:
        result["flow"] = ""

    return result


def extract_gf120_raw_fields(row):
    """
    fallback用。
    source_sheet / section から元シート復元を試し、
    ダメなら serial で全シート横断。
    """
    db_stem = clean_disp(row.get("db_file", ""))
    source_sheet = clean_disp(row.get("source_sheet", ""))
    section_name = clean_disp(row.get("section", ""))
    serial_value = clean_disp(row.get("serial", "")).upper()

    raw_df = load_original_sheet_candidates(db_stem, [source_sheet, section_name])

    section_header_row_no = extract_numeric_from_text(row.get("section_header_row"))
    label_row_no = extract_numeric_from_text(row.get("label_row"))
    data_row_no = extract_numeric_from_text(row.get("data_row"))

    def restore_from_sheet(df: pd.DataFrame):
        if df.empty or data_row_no is None:
            return {"gas": "", "flow": None}

        data_idx = max(int(data_row_no) - 1, 0)
        if data_idx >= len(df):
            return {"gas": "", "flow": None}

        if section_header_row_no is not None:
            start_idx = max(int(section_header_row_no) - 1, 0)
        elif label_row_no is not None:
            start_idx = max(int(label_row_no) - 1, 0)
        else:
            start_idx = max(data_idx - 5, 0)

        end_idx = max(data_idx - 1, start_idx)
        value_row = df.iloc[data_idx]

        gas_candidates = {"gas", "gassymbol", "gastype", "gasspecies", "gasname"}
        range_candidates = {"range", "flow", "flowrate", "sccm"}

        best_header_map = {}
        best_score = -1

        for header_idx in range(start_idx, end_idx + 1):
            header_row = df.iloc[header_idx]

            header_map = {}
            for i in range(len(df.columns)):
                key = normalize_header_name(header_row.iloc[i])
                if key != "":
                    header_map[key] = value_row.iloc[i]

            keys = set(header_map.keys())

            score = 0
            if keys & gas_candidates:
                score += 3
            if keys & range_candidates:
                score += 3

            if score > best_score:
                best_score = score
                best_header_map = header_map

        def pick_value(candidates):
            for c in candidates:
                if c in best_header_map and clean_disp(best_header_map[c]) != "":
                    return best_header_map[c]
            return None

        gas_val = pick_value(["gas", "gassymbol", "gastype", "gasspecies", "gasname"])
        range_val = pick_value(["range", "flow", "flowrate", "sccm"])

        if clean_disp(gas_val) == "":
            gas_df = load_gf120_gas_code_table()
            valid_gases = set(gas_df["gas_symbol"].tolist()) if not gas_df.empty else set()

            for val in value_row.tolist():
                cand = normalize_gas_symbol(val)
                if cand in valid_gases:
                    gas_val = cand
                    break

        if range_val is None:
            numeric_candidates = []
            for val in value_row.tolist():
                num = extract_numeric_from_text(val)
                if num is not None and 0 < num <= 50000:
                    numeric_candidates.append(num)

            if numeric_candidates:
                range_val = min(numeric_candidates)

        return {
            "gas": normalize_gas_symbol(gas_val),
            "flow": extract_numeric_from_text(range_val),
        }

    restored = restore_from_sheet(raw_df)
    if restored["gas"] and restored["flow"] is not None:
        return restored

    if db_stem and serial_value:
        file_path = find_db_file_path_from_stem(db_stem)
        if file_path is not None:
            try:
                xls = pd.ExcelFile(file_path)
                gas_df = load_gf120_gas_code_table()
                valid_gases = set(gas_df["gas_symbol"].tolist()) if not gas_df.empty else set()

                for sheet in xls.sheet_names:
                    try:
                        df = pd.read_excel(file_path, sheet_name=sheet, header=None, dtype=object)
                    except Exception:
                        continue

                    for ridx in range(len(df)):
                        row_vals = df.iloc[ridx].astype(str).str.strip().str.upper().tolist()
                        if serial_value not in row_vals:
                            continue

                        gas_val = ""
                        flow_val = None

                        for val in df.iloc[ridx].tolist():
                            cand = normalize_gas_symbol(val)
                            if cand in valid_gases and gas_val == "":
                                gas_val = cand

                            num = extract_numeric_from_text(val)
                            if num is not None and 0 < num <= 50000:
                                if flow_val is None or num < flow_val:
                                    flow_val = num

                        if gas_val and flow_val is not None:
                            return {
                                "gas": gas_val,
                                "flow": flow_val,
                            }
            except Exception:
                pass

    return {"gas": "", "flow": None}


def get_effective_gas_flow(row):
    mfc_model_value = clean_disp(row.get("mfc_model", ""))
    model_norm = norm_text(mfc_model_value)

    if model_norm.startswith("GF120"):
        parsed = parse_gf120_model_string(row.get("model", ""))

        gas_from_model = clean_disp(parsed.get("gas_symbol", ""))
        flow_from_model = clean_disp(parsed.get("flow", ""))

        gas_from_db = clean_disp(row.get("gas", ""))
        flow_from_db = clean_disp(row.get("flow", ""))

        # 1) model文字列から両方取れたら最優先
        if gas_from_model and flow_from_model:
            return gas_from_model, flow_from_model

        # 2) DB列に値があればそれを使う
        if gas_from_db and flow_from_db:
            return gas_from_db, flow_from_db

        # 3) 片方ずつ補完
        gas_value = gas_from_model if gas_from_model else gas_from_db
        flow_value = flow_from_model if flow_from_model else flow_from_db

        if gas_value or flow_value:
            return gas_value, flow_value

        # 4) 最後にraw sheet fallback
        gf120_raw = extract_gf120_raw_fields(row)
        gas_value = clean_disp(gf120_raw.get("gas", ""))
        flow_raw = gf120_raw.get("flow", "")
        if flow_raw in [None, ""] or pd.isna(flow_raw):
            flow_value = ""
        else:
            try:
                flow_value = str(int(float(flow_raw)))
            except Exception:
                flow_value = clean_disp(flow_raw)

        return gas_value, flow_value

    gas_value = clean_disp(row.get("gas", ""))
    flow_value = clean_disp(row.get("flow", ""))
    return gas_value, flow_value


# =========================================================
# Inventory
# =========================================================
@st.cache_data
def load_inventory() -> pd.DataFrame:
    if not INVENTORY_PATH.exists():
        return pd.DataFrame()

    try:
        df = pd.read_excel(INVENTORY_PATH, usecols=[2, 5])
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    df.columns = ["order_code", "qty"]
    df["order_code"] = df["order_code"].astype(str).str.strip()
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)

    df = df[
        df["order_code"].notna()
        & (df["order_code"].astype(str).str.strip() != "")
        & (df["order_code"].astype(str).str.lower() != "nan")
    ].copy()

    split_df = df["order_code"].str.split("-", n=2, expand=True)
    df["inv_model"] = split_df[0].fillna("").astype(str).str.upper().str.strip()
    df["inv_size"] = split_df[1].fillna("").astype(str).str.upper().str.strip()
    df["inv_type"] = split_df[2].fillna("").astype(str).str.upper().str.strip()
    df["stock_mark"] = df["qty"].apply(lambda x: "○" if x > 0 else "×")

    return df


def convert_model_to_inventory_side(model: str) -> str:
    model_norm = norm_text(model).replace("_", "")
    return MODEL_TO_INV_ALIAS.get(model_norm, model_norm)


def build_order_code_key(model: str, size_or_cord: str, mfc_type: str):
    inv_model = convert_model_to_inventory_side(model)
    inv_size = norm_text(size_or_cord)
    inv_type = norm_text(mfc_type)
    return inv_model, inv_size, inv_type


def lookup_inventory(model: str, size_or_cord: str, mfc_type: str):
    inv_df = load_inventory()
    if inv_df.empty:
        return False, None, 0

    inv_model, inv_size, inv_type = build_order_code_key(model, size_or_cord, mfc_type)

    if inv_size == "":
        return False, None, 0

    hit = inv_df[
        (inv_df["inv_model"] == inv_model)
        & (inv_df["inv_size"] == inv_size)
        & (inv_df["inv_type"] == inv_type)
    ]
    if not hit.empty:
        total_qty = float(hit["qty"].sum())
        order_code = hit.iloc[0]["order_code"]
        return total_qty > 0, order_code, total_qty

    hit2 = inv_df[
        (inv_df["inv_model"] == inv_model)
        & (inv_df["inv_size"].astype(str).str.startswith(inv_size, na=False))
        & (inv_df["inv_type"] == inv_type)
    ]
    if not hit2.empty:
        total_qty = float(hit2["qty"].sum())
        order_code = hit2.iloc[0]["order_code"]
        return total_qty > 0, order_code, total_qty

    return False, None, 0


# =========================================================
# Master DB
# =========================================================
@st.cache_data
def load_master_data() -> pd.DataFrame:
    frames = []

    db_files = get_db_files()
    if not db_files:
        return pd.DataFrame()

    preferred_sheets = ["Import_Long_DB_Safe", "Import_Long"]

    for file in db_files:
        try:
            xls = pd.ExcelFile(file)
            target_sheet = None

            for s in preferred_sheets:
                if s in xls.sheet_names:
                    target_sheet = s
                    break

            if target_sheet is not None:
                df = pd.read_excel(file, sheet_name=target_sheet)
            else:
                tmp_frames = []
                for sheet_name in xls.sheet_names:
                    tmp = pd.read_excel(file, sheet_name=sheet_name)
                    tmp["source_sheet"] = sheet_name
                    tmp_frames.append(tmp)
                df = pd.concat(tmp_frames, ignore_index=True) if tmp_frames else pd.DataFrame()

            if df.empty:
                continue

            if "source_sheet" not in df.columns:
                df["source_sheet"] = target_sheet if target_sheet else ""

            df["db_file"] = file.stem
            frames.append(df)

        except Exception:
            continue

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "fab" not in df.columns:
        c = pick_first_existing(df, ["fab_name", "factory", "site"])
        if c:
            df["fab"] = df[c]

    if "section" not in df.columns:
        c = pick_first_existing(df, ["source_sheet", "sheet"])
        if c:
            df["section"] = df[c]

    if "tool_oem" not in df.columns:
        c = pick_first_existing(df, ["oem", "tool oem"])
        if c:
            df["tool_oem"] = df[c]

    if "equ_model" not in df.columns:
        c = pick_first_existing(df, ["equ._model", "equ model", "equipment_model"])
        if c:
            df["equ_model"] = df[c]

    if "mfc_model" not in df.columns:
        c = pick_first_existing(df, ["model", "mfc-model", "mfc model"])
        if c:
            df["mfc_model"] = df[c]

    if "type" not in df.columns:
        c = pick_first_existing(df, ["mfc_type"])
        if c:
            df["type"] = df[c]

    if "ch" not in df.columns:
        c = pick_first_existing(df, ["channel", "chamber"])
        if c:
            df["ch"] = df[c]

    if "flow" not in df.columns:
        c = pick_first_existing(df, ["range"])
        if c:
            df["flow"] = df[c]

    if "bin_size" not in df.columns:
        c = pick_first_existing(df, ["bin", "bin size"])
        if c:
            df["bin_size"] = df[c]

    if "serial" not in df.columns:
        c = pick_first_existing(df, ["serial_no", "serial number", "s/n", "sn"])
        if c:
            df["serial"] = df[c]

    if "install_date" not in df.columns:
        c = pick_first_existing(df, ["install date", "installdate"])
        if c:
            df["install_date"] = df[c]

    expected_cols = [
        "fab",
        "section",
        "tool_oem",
        "customer_id",
        "equ_model",
        "install_date",
        "mfc_model",
        "model",
        "bin_size",
        "type",
        "ch",
        "gas",
        "flow",
        "serial",
        "source_sheet",
        "db_file",
        "label_row",
        "data_row",
        "tool_id",
        "section_header_row",
    ]
    for c in expected_cols:
        if c not in df.columns:
            df[c] = None

    if "source_sheet" in df.columns:
        df["section"] = df["section"].fillna(df["source_sheet"])

    df["fab"] = df["fab"].fillna(df["db_file"])
    df["install_date"] = pd.to_datetime(df["install_date"], errors="coerce")
    df["flow_num"] = pd.to_numeric(df["flow"], errors="coerce")

    for c in [
        "fab",
        "section",
        "tool_oem",
        "customer_id",
        "equ_model",
        "mfc_model",
        "model",
        "bin_size",
        "type",
        "ch",
        "gas",
        "serial",
        "flow",
    ]:
        df[f"{c}_norm"] = df[c].apply(norm_text)

    return df


# =========================================================
# Spec helpers
# =========================================================
def resolve_size_or_cord(row) -> str:
    mfc_model = clean_disp(row.get("mfc_model", ""))
    model_norm = norm_text(mfc_model)

    if model_norm.startswith(("GF125", "GF126")):
        gas_value, flow_value = get_effective_gas_flow(row)
        return calc_bin_size(gas_value, flow_value)

    if model_norm.startswith("GF120"):
        parsed = parse_gf120_model_string(row.get("model", ""))
        cord = clean_disp(parsed.get("cord", ""))
        if cord:
            return cord

        gas_value, flow_value = get_effective_gas_flow(row)
        return calc_gf120_cord(gas_value, flow_value)

    return ""


def build_final_spec(row) -> str:
    parsed = parse_gf120_model_string(row.get("model", ""))
    model_from_model = clean_disp(parsed.get("mfc_model", ""))
    type_from_model = clean_disp(parsed.get("type", ""))
    cord_from_model = clean_disp(parsed.get("cord", ""))

    model = clean_disp(row.get("mfc_model", ""))
    mfc_type = clean_disp(row.get("type", ""))

    if norm_text(model).startswith("GF120"):
        final_model = model if model else model_from_model
        final_type = mfc_type if mfc_type else type_from_model
        final_cord = resolve_size_or_cord(row) or cord_from_model

        if final_model and final_cord and final_type:
            return f"{final_model}-{final_cord}-{final_type}"
        return ""

    size_or_cord = resolve_size_or_cord(row)
    if model and size_or_cord and mfc_type:
        return f"{model}-{size_or_cord}-{mfc_type}"

    return ""


# =========================================================
# UI: Login
# =========================================================
def login_screen():
    st.markdown(
        "<h1 style='text-align:center;'>SCH JAPAN MFC Portal</h1>",
        unsafe_allow_html=True,
    )

    email = st.text_input("Email")
    password = st.text_input("Password", type="password")

    if st.button("Login", width="stretch"):
        if email in USERS and USERS[email] == password:
            st.session_state.logged_in = True
            st.success("Login Successful")
            time.sleep(0.8)
            st.rerun()
        else:
            st.markdown(
                "<p style='color:red; font-weight:bold;'>Failed</p>",
                unsafe_allow_html=True,
            )


# =========================================================
# UI: Dashboard
# =========================================================
def dashboard(df: pd.DataFrame):
    st.title("Dashboard")

    total_mfc = len(df)
    expired = 0
    soon = 0

    for v in df["install_date"]:
        status, _ = warranty_status(v)
        if status == "EXPIRED":
            expired += 1
        elif status == "NEAR EXPIRY":
            soon += 1

    inv_df = load_inventory()
    inv_available = int((inv_df["qty"] > 0).sum()) if not inv_df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Installed MFC", total_mfc)
    c2.metric("Warranty Expired", expired)
    c3.metric("Warranty Soon", soon)
    c4.metric("Inventory Available", inv_available)

    st.divider()

    left, right = st.columns(2)

    with left:
        st.subheader("Fab Distribution")
        fab_counts = (
            df["fab"].fillna("UNKNOWN").apply(pretty_fab_name).value_counts()
            .rename_axis("Fab").reset_index(name="Count")
        )
        st.dataframe(fab_counts, width="stretch", hide_index=True)

    with right:
        st.subheader("Model Distribution")
        model_counts = (
            df["mfc_model"].fillna("UNKNOWN").astype(str).value_counts()
            .rename_axis("MFC Model").reset_index(name="Count")
        )
        st.dataframe(model_counts, width="stretch", hide_index=True)

    st.divider()
    st.subheader("Portal Access")

    b1, b2, b3, b4 = st.columns(4)

    if b1.button("Serial Search", width="stretch"):
        st.session_state.page_mode = "serial"
        st.rerun()

    if b2.button("Tool Search", width="stretch"):
        st.session_state.page_mode = "tool"
        st.rerun()

    if b3.button("Spec Search", width="stretch"):
        st.session_state.page_mode = "spec"
        st.rerun()

    if b4.button("Inventory List", width="stretch"):
        st.session_state.page_mode = "inventory"
        st.rerun()


# =========================================================
# UI: Serial Search
# =========================================================
def serial_search_page(df: pd.DataFrame):
    st.title("Serial Search")

    if st.button("← Back to Dashboard"):
        st.session_state.page_mode = "dashboard"
        st.rerun()

    serial_q = st.text_input("Serial Number")

    if not serial_q:
        return

    serial_norm = serial_q.strip().upper()

    work = df.copy()
    work["serial_norm_local"] = work["serial"].astype(str).str.strip().str.upper()
    result = work[work["serial_norm_local"] == serial_norm]

    if result.empty:
        st.error("Serial not found.")
        return

    r = result.iloc[0]

    mfc_model_value = clean_disp(r.get("mfc_model"))
    model_norm = norm_text(mfc_model_value)

    size_or_cord = resolve_size_or_cord(r)
    final_spec = build_final_spec(r)

    warranty_label, warranty_color = warranty_status(r.get("install_date"))

    fab_value = pretty_fab_name(r.get("fab", ""))
    section_value = clean_disp(r.get("section"))
    tool_value = clean_disp(r.get("tool_oem"))
    customer_value = clean_disp(r.get("customer_id"))
    eq_model_value = clean_disp(r.get("equ_model"))
    install_date = format_install_date(r.get("install_date"))

    type_value = clean_disp(r.get("type"))
    ch_value = clean_disp(r.get("ch"))

    gas_value, flow_value = get_effective_gas_flow(r)
    size_label = "Cord" if model_norm.startswith("GF120") else "Bin Size"

    has_stock, order_code, qty = lookup_inventory(
        mfc_model_value,
        size_or_cord,
        type_value,
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(
            f"""
            **Fab**: {fab_value}  
            **Section**: {section_value}  
            **Tool OEM**: {tool_value}  
            **Customer ID**: {customer_value}  
            **Equ. Model**: {eq_model_value}  
            **Install Date**: {install_date}  
            **Warranty**: <span style="color:{warranty_color}; font-weight:bold;">{warranty_label}</span>
            """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            f"""
            **MFC Model**: {mfc_model_value}  
            **{size_label}**: {clean_disp(size_or_cord)}  
            **Type**: {type_value}  
            **Final Spec**: {clean_disp(final_spec)}
            """
        )

        stock_label = "○" if has_stock else "×"

        if st.button(f"Stock: {stock_label}", key=f"serial_stock_{serial_norm}"):
            if has_stock:
                st.success(f"Order Code: {order_code} / Qty: {int(qty)}")
            else:
                st.error("在庫なし")

    with col3:
        st.markdown(
            f"""
            **CH**: {ch_value}  
            **Gas**: {gas_value}  
            **Flow**: {flow_value}
            """
        )

    with st.expander("Debug row data"):
        st.dataframe(result, width="stretch", hide_index=True)
        st.write("GF120 parsed model:", parse_gf120_model_string(r.get("model", "")))
        st.write("GF120 effective gas/flow:", get_effective_gas_flow(r))
        st.write("source_sheet:", r.get("source_sheet"))
        st.write("section:", r.get("section"))
        st.write("label_row:", r.get("label_row"))
        st.write("data_row:", r.get("data_row"))
        st.write("section_header_row:", r.get("section_header_row"))


# =========================================================
# UI: Tool Search
# =========================================================
def tool_search_page(df: pd.DataFrame):
    st.title("Tool Search")

    if st.button("← Back to Dashboard"):
        st.session_state.page_mode = "dashboard"
        st.rerun()

    work = df.copy()
    work["fab_pretty"] = work["fab"].apply(pretty_fab_name)

    fab_options = sorted([
        x for x in work["fab_pretty"].dropna().unique().tolist()
        if str(x).strip()
    ])
    if not fab_options:
        st.warning("Fab データがありません。")
        return

    fab_sel = st.selectbox("Fab", fab_options)

    work1 = work[work["fab_pretty"] == fab_sel]
    section_options = sorted([
        x for x in work1["section"].dropna().astype(str).unique().tolist()
        if str(x).strip()
    ])
    if not section_options:
        st.warning("Section データがありません。")
        return

    section_sel = st.selectbox("Section", section_options)

    work2 = work1[work1["section"].astype(str) == section_sel]

    customer_options = sorted([
        x for x in work2["customer_id"].dropna().astype(str).unique().tolist()
        if str(x).strip()
    ])
    if not customer_options:
        st.warning("Customer ID データがありません。")
        return

    customer_sel = st.selectbox("Customer ID", customer_options)

    result = work2[work2["customer_id"].astype(str) == customer_sel].copy()
    if result.empty:
        st.warning("該当データがありません。")
        return

    first = result.iloc[0]
    warranty_label, warranty_color = warranty_status(first.get("install_date"))

    st.markdown("### Tool Summary")
    st.markdown(
        f"""
        **Fab**: {fab_sel}  
        **Section**: {section_sel}  
        **Customer ID**: {customer_sel}  
        **Tool OEM**: {clean_disp(first.get('tool_oem', ''))}  
        **Equ. Model**: {clean_disp(first.get('equ_model', ''))}  
        **Install Date**: {format_install_date(first.get('install_date', ''))}  
        **Warranty**: <span style="color:{warranty_color}; font-weight:bold;">{warranty_label}</span>
        """,
        unsafe_allow_html=True,
    )

    table_rows = []
    for _, row in result.iterrows():
        mfc_model_value = clean_disp(row.get("mfc_model", ""))
        size_or_cord = resolve_size_or_cord(row)
        final_spec = build_final_spec(row)
        gas_value, flow_value = get_effective_gas_flow(row)

        has_stock, order_code, qty = lookup_inventory(
            row.get("mfc_model", ""),
            size_or_cord,
            row.get("type", ""),
        )

        table_rows.append({
            "MFC Model": mfc_model_value,
            "Bin/Cord": clean_disp(size_or_cord),
            "Type": clean_disp(row.get("type", "")),
            "Gas": clean_disp(gas_value),
            "Flow": clean_disp(flow_value),
            "CH": clean_disp(row.get("ch", "")),
            "Serial": clean_disp(row.get("serial", "")),
            "Final Spec": clean_disp(final_spec),
            "Stock": "○" if has_stock else "×",
            "Order Code": clean_disp(order_code),
            "Qty": int(qty) if qty else 0,
        })

    show_df = pd.DataFrame(table_rows)
    st.markdown("### Installed MFC List")
    st.dataframe(show_df, width="stretch", hide_index=True)


# =========================================================
# UI: Spec Search
# =========================================================
def spec_search_page(df: pd.DataFrame):
    st.title("Spec Search")

    if st.button("← Back to Dashboard"):
        st.session_state.page_mode = "dashboard"
        st.rerun()

    model_tab = st.radio("MFC Model", ["GF120", "GF125", "GF126"], horizontal=True)

    work = df.copy()
    gas_sel = ""

    if model_tab in ["GF125", "GF126"]:
        gas_options = sorted([
            normalize_gas_symbol(x)
            for x in work["gas"].dropna().astype(str).unique().tolist()
            if str(x).strip()
        ])
        gas_options = [x for x in gas_options if x]
        gas_sel = st.selectbox("Gas Symbol", gas_options) if gas_options else ""
    else:
        gas_mode = st.radio("Gas Input", ["Gas Symbol", "Gas Code"], horizontal=True)

        gas_code_df = load_gf120_gas_code_table()

        if gas_mode == "Gas Symbol":
            gas_options = (
                sorted(gas_code_df["gas_symbol"].dropna().astype(str).unique().tolist())
                if not gas_code_df.empty else []
            )
            gas_sel = st.selectbox("Gas Symbol", gas_options) if gas_options else ""
        else:
            gas_code_options = (
                sorted(gas_code_df["gas_code"].dropna().astype(str).unique().tolist())
                if not gas_code_df.empty else []
            )
            gas_code_sel = st.selectbox("Gas Code", gas_code_options) if gas_code_options else ""

            if gas_code_sel and not gas_code_df.empty:
                hit = gas_code_df[gas_code_df["gas_code"] == gas_code_sel]
                gas_sel = hit.iloc[0]["gas_symbol"] if not hit.empty else ""

    flow_sel = st.number_input("Flow", min_value=0.0, value=0.0, step=1.0)

    type_options = sorted([
        x for x in work["type"].dropna().astype(str).unique().tolist()
        if str(x).strip()
    ])
    type_sel = st.selectbox("Type", type_options) if type_options else ""

    if st.button("Search Spec", width="stretch"):
        if not gas_sel or not type_sel or flow_sel <= 0:
            st.warning("MFC Model / Gas / Flow / Type を指定してください。")
            return

        if model_tab in ["GF125", "GF126"]:
            size_or_cord = calc_bin_size(gas_sel, flow_sel, sheet_name="0C_bin_ranges")
            size_label = "Bin Size"
        else:
            size_or_cord = calc_gf120_cord(gas_sel, flow_sel)
            size_label = "Cord"

        if not size_or_cord:
            if model_tab == "GF120":
                st.error("該当する Cord を特定できませんでした。")
            else:
                st.error("該当する Bin Size を特定できませんでした。")
            return

        result = df.copy()
        result = result[result["mfc_model_norm"].str.startswith(model_tab, na=False)]
        result = result[result["type_norm"] == norm_text(type_sel)]

        if model_tab in ["GF125", "GF126"]:
            result["effective_gas"] = result["gas"].apply(normalize_gas_symbol)
            result["resolved_size"] = result.apply(resolve_size_or_cord, axis=1)
            result = result[result["effective_gas"].apply(norm_text) == norm_text(gas_sel)]
            result = result[result["resolved_size"].apply(norm_text) == norm_text(size_or_cord)]
        else:
            result["effective_gas"] = result.apply(lambda r: get_effective_gas_flow(r)[0], axis=1)
            result["resolved_size"] = result.apply(resolve_size_or_cord, axis=1)
            result = result[result["effective_gas"].apply(norm_text) == norm_text(gas_sel)]
            result = result[result["resolved_size"].apply(norm_text) == norm_text(size_or_cord)]

        if result.empty:
            final_spec = f"{model_tab}-{size_or_cord}-{type_sel}"
        else:
            final_spec = build_final_spec(result.iloc[0])

        st.markdown("### Final Spec")
        st.code(final_spec)

        has_stock, order_code, qty = lookup_inventory(model_tab, size_or_cord, type_sel)
        stock_mark = "○" if has_stock else "×"

        if st.button(f"Stock: {stock_mark}", key=f"spec_stock_{final_spec}"):
            if has_stock:
                st.success(f"Order Code: {order_code} / Qty: {int(qty)}")
            else:
                st.error("在庫なし")

        st.markdown("### Search Result")
        st.markdown(
            f"""
            **MFC Model**: {model_tab}  
            **Gas Symbol**: {gas_sel}  
            **Flow**: {flow_sel:.0f}  
            **Type**: {type_sel}  
            **{size_label}**: {size_or_cord}
            """
        )

        if result.empty:
            st.info("DB内に搭載実績はまだありません。")
            return

        result = result.copy()
        result["fab_pretty"] = result["fab"].apply(pretty_fab_name)
        result["Final Spec"] = result.apply(build_final_spec, axis=1)

        summary = (
            result.groupby(["fab_pretty", "section"], dropna=False)
            .size()
            .reset_index(name="Count")
            .rename(columns={"fab_pretty": "Fab", "section": "Section"})
            .sort_values(["Fab", "Section"])
        )

        st.markdown("### Installed Base")
        st.dataframe(summary, width="stretch", hide_index=True)

        detail_rows = []
        for _, row in result.iterrows():
            gas_value, flow_value = get_effective_gas_flow(row)

            detail_rows.append({
                "Fab": pretty_fab_name(row.get("fab", "")),
                "Section": clean_disp(row.get("section", "")),
                "Customer ID": clean_disp(row.get("customer_id", "")),
                "Tool OEM": clean_disp(row.get("tool_oem", "")),
                "Equ. Model": clean_disp(row.get("equ_model", "")),
                "MFC Model": clean_disp(row.get("mfc_model", "")),
                "Gas": clean_disp(gas_value),
                "Flow": clean_disp(flow_value),
                "Type": clean_disp(row.get("type", "")),
                size_label: clean_disp(row.get("resolved_size", "")),
                "Final Spec": clean_disp(row.get("Final Spec", "")),
                "Serial": clean_disp(row.get("serial", "")),
            })

        detail_show = pd.DataFrame(detail_rows)

        st.markdown("### Installed Detail")
        st.dataframe(detail_show, width="stretch", hide_index=True)


# =========================================================
# UI: Inventory List
# =========================================================
def inventory_list_page():
    st.title("Inventory List")

    if st.button("← Back to Dashboard"):
        st.session_state.page_mode = "dashboard"
        st.rerun()

    inv_df = load_inventory()
    if inv_df.empty:
        st.warning("在庫表が読めません。")
        return

    view_df = inv_df.copy().rename(columns={
        "order_code": "Order Code",
        "inv_model": "Model",
        "inv_size": "Bin/Cord",
        "inv_type": "Type",
        "qty": "Qty",
        "stock_mark": "Stock",
    })

    show_cols = ["Order Code", "Model", "Bin/Cord", "Type", "Qty", "Stock"]
    st.dataframe(view_df[show_cols], width="stretch", hide_index=True)


# =========================================================
# Main
# =========================================================
if not st.session_state.logged_in:
    login_screen()
else:
    df = load_master_data()

    if df.empty:
        st.error(
            f"DB file not found or unreadable.\n\n"
            f"Place *_mfc_master_database.xlsx in:\n{DB_DIR}"
        )
        st.stop()

    mode = st.session_state.page_mode

    if mode == "dashboard":
        dashboard(df)
    elif mode == "serial":
        serial_search_page(df)
    elif mode == "tool":
        tool_search_page(df)
    elif mode == "spec":
        spec_search_page(df)
    elif mode == "inventory":
        inventory_list_page()