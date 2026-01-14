# -*- coding: utf-8 -*-
"""
Andreani | Gestión logística (v1.54)
-----------------------------------
Fix PRO (v1.54)
- Unifica region_key (CP Master + Matriz Andreani + Auditor) para evitar "SIN TARIFA" por mismatch de etiquetas.
- Lookup de tarifa robusto: soporta excedente por kg (Exc) cuando el peso supera el último tramo.
- Parser PDF Andreani estable: agrupa SGD/DISD y guía por bloque (funciona con formato "Servicio de transporte ... / Nro. de Envío: ...").
- Mantiene modo simulación + aplicar (con backup) y persistencia local en ./data.

Requisitos:
- streamlit, pandas, openpyxl, pyyaml, pdfplumber
"""

from __future__ import annotations

import io
import os
import re
import json
import math
import shutil
import unicodedata
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import pdfplumber
import yaml

# =========================
# Paths / Persistencia
# =========================
DATA_DIR = "data"
BACKUP_DIR = os.path.join(DATA_DIR, "backups")

CP_MASTER_PATH = os.path.join(DATA_DIR, "cp_master.pkl")
CATALOG_PATH = os.path.join(DATA_DIR, "catalog.pkl")
SALES_PATH = os.path.join(DATA_DIR, "sales.pkl")

MATRIX_DIR = os.path.join(DATA_DIR, "matrices")
REGISTRY_PATH = os.path.join(DATA_DIR, "matrices_registry.json")

AUDIT_LOG_PATH = os.path.join(DATA_DIR, "audit_log.jsonl")

APP_DIR = os.path.dirname(os.path.abspath(__file__))

TPL_CP = os.path.join(APP_DIR, "template_cp_master.xlsx")
TPL_CATALOG = os.path.join(APP_DIR, "template_catalogo.xlsx")
TPL_SALES = os.path.join(APP_DIR, "template_ventas.xlsx")
TPL_AND = os.path.join(APP_DIR, "template_matriz_andreani.xlsx")
TPL_FREE = os.path.join(APP_DIR, "template_free_shipping_cps.xlsx")
TPL_CONFIG = os.path.join(APP_DIR, "config.yaml")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(MATRIX_DIR, exist_ok=True)


# =========================
# Utilidades base
# =========================
def iso_now() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def today() -> dt.date:
    return dt.date.today()


def norm_text(s: Any) -> str:
    """
    Normaliza textos para comparaciones robustas:
    - lower + strip
    - quita tildes
    - reemplaza puntuación por espacios
    - colapsa espacios
    """
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def read_excel_best_sheet(file_obj, *, prefer_sheets=None) -> Tuple[pd.DataFrame, str]:
    """Lee un Excel eligiendo la hoja correcta automáticamente.
    Preferimos hojas tipo MATRIZ_RAW / MATRIZ / etc. para evitar leer INSTRUCCIONES por error.
    Devuelve (df, sheet_name).
    """
    prefer_sheets = prefer_sheets or []
    try:
        xls = pd.ExcelFile(file_obj, engine="openpyxl")
        sheets = list(xls.sheet_names)
        # 1) match exact prefers (normalizado)
        prefers_norm = [norm_text(s) for s in prefer_sheets]
        for s in sheets:
            if norm_text(s) in prefers_norm:
                return pd.read_excel(xls, sheet_name=s), s
        # 2) heurística: primera hoja que contenga 'matriz'
        for s in sheets:
            if "matriz" in norm_text(s):
                return pd.read_excel(xls, sheet_name=s), s
        # 3) fallback: primera hoja
        return pd.read_excel(xls, sheet_name=sheets[0]), sheets[0]
    except Exception:
        # fallback ultra-safe
        return pd.read_excel(file_obj, engine="openpyxl"), ""

def provinces_ar() -> list[str]:
    return [
        "Buenos Aires",
        "CABA",
        "Catamarca",
        "Chaco",
        "Chubut",
        "Córdoba",
        "Corrientes",
        "Entre Ríos",
        "Formosa",
        "Jujuy",
        "La Pampa",
        "La Rioja",
        "Mendoza",
        "Misiones",
        "Neuquén",
        "Río Negro",
        "Salta",
        "San Juan",
        "San Luis",
        "Santa Cruz",
        "Santa Fe",
        "Santiago del Estero",
        "Tierra del Fuego",
        "Tucumán",
    ]


def list_subregions_from_cp_master(cp_master: "pd.DataFrame") -> list[str]:
    """Sub_region tal como está en tu CP Master (sin inventar)."""
    if cp_master is None or cp_master.empty or "sub_region" not in cp_master.columns:
        return []
    vals = (
        cp_master["sub_region"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )
    vals = [v for v in vals if v]  # no vacíos
    vals.sort()
    return vals


def parse_cp_to_int(cp: Any) -> Optional[int]:
    """Convierte CP a entero (tolerante a Excel / strings con separadores / CP con letras)."""
    if cp is None:
        return None
    if isinstance(cp, float) and math.isnan(cp):
        return None

    # ints (incluye numpy ints)
    try:
        if isinstance(cp, (int,)) and not isinstance(cp, bool):
            return int(cp)
    except Exception:
        pass

    # floats típicos de Excel
    if isinstance(cp, float):
        if float(cp).is_integer():
            return int(cp)

    s = str(cp).strip().upper()
    if re.fullmatch(r"\d+\.0+", s):
        return int(s.split(".")[0])

    digits = re.findall(r"\d+", s)
    if not digits:
        return None

    # Evitar 8340.0 -> 83400 si viniera dividido en grupos
    if "." in s and len(digits) > 1 and all(set(d) == {"0"} for d in digits[1:]):
        return int(digits[0])

    return int("".join(digits))



def within_tol(actual: Any, expected: Any, *, abs_tol: float = 0.0, pct_tol: float = 0.0) -> bool:
    """True si |actual-expected| <= max(abs_tol, pct_tol*expected). pct_tol en [0..1]."""
    if actual is None or expected is None:
        return False
    try:
        a = float(actual)
        e = float(expected)
    except Exception:
        return False
    if isinstance(e, float) and math.isnan(e):
        return False
    if e == 0:
        return abs(a - e) <= float(abs_tol)
    band = max(float(abs_tol), float(pct_tol) * abs(e))
    return abs(a - e) <= band


def tol_band(expected: Any, *, abs_tol: float = 0.0, pct_tol: float = 0.0) -> float:
    """Ancho de tolerancia permitido para un expected dado."""
    try:
        e = float(expected)
    except Exception:
        return float(abs_tol)
    if isinstance(e, float) and math.isnan(e):
        return float(abs_tol)
    return max(float(abs_tol), float(pct_tol) * abs(e))

def ar_money_to_float(x: Any) -> Optional[float]:
    """Convierte '93.263,21' -> 93263.21"""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    s = str(x).strip()
    if not s:
        return None
    s = re.sub(r"[^0-9\.,\-]", "", s)
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def normalize_guia(val: Any) -> Optional[str]:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    if isinstance(val, int) and not isinstance(val, bool):
        return str(int(val))
    if isinstance(val, float):
        if math.isfinite(val) and abs(val - round(val)) < 1e-6:
            return str(int(round(val)))
        return str(val).strip()

    s = str(val).strip()
    if not s:
        return None

    # Evitar notación científica textual
    if re.search(r"[eE]\+?\d+", s):
        # no intentamos arreglarla acá: debe venir como texto desde Excel
        return s

    if re.match(r"^\d+\.0+$", s):
        s = s.split(".")[0]
    return s


def to_excel_bytes(df: pd.DataFrame, sheet: str = "data") -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name=sheet[:31])
    return out.getvalue()


def safe_show_df(df: pd.DataFrame, *, label: str = "", max_rows: int = 2000) -> None:
    if df is None or len(df) == 0:
        st.info("No hay datos para mostrar.")
        return

    view = df.copy()
    try:
        view = view.reset_index(drop=True)
    except Exception:
        pass

    for c in list(view.columns):
        s = view[c]
        if pd.api.types.is_datetime64_any_dtype(s):
            view[c] = pd.to_datetime(s, errors="coerce").dt.strftime("%Y-%m-%d")
        elif pd.api.types.is_object_dtype(s):
            view[c] = s.fillna("").astype(str)

    truncated = len(view) > max_rows
    if truncated:
        st.dataframe(view.head(max_rows), use_container_width=True)
        st.warning(f"Mostrando primeras {max_rows} filas (por estabilidad).")
    else:
        st.dataframe(view, use_container_width=True)

    try:
        st.download_button(
            f"Descargar CSV ({label or 'tabla'})",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=f"{(label or 'tabla').replace(' ', '_').lower()}.csv",
            mime="text/csv",
        )
    except Exception:
        pass


# =========================
# Config
# =========================
DEFAULT_CONFIG_YAML = """app:
  tolerance_weight_kg: 0.01
  tolerance_tariff_ars: 1.0

rounding:
  enabled: true
  decimals: 0
  mode: round

sgd:
  fixed_ars: 5378.0
  threshold_declared_ars: 500000.0
  excess_rate: 0.01
  when_missing_declared: fixed_only

tax:
  iva_rate: 0.21

zones:
  origin_province: "Salta"
  tierra_del_fuego_province: "Tierra del Fuego"
  patagonia_provinces: ["Neuquen","Río Negro","Chubut","Santa Cruz","Tierra del Fuego"]
  capital_keywords_by_province: {}
"""


def load_config(uploaded: Optional[io.BytesIO]) -> Dict[str, Any]:
    if uploaded is None:
        # si existe config.yaml en el repo, úsalo; si no, default.
        if os.path.exists(TPL_CONFIG):
            try:
                with open(TPL_CONFIG, "r", encoding="utf-8") as f:
                    return yaml.safe_load(f.read())
            except Exception:
                return yaml.safe_load(DEFAULT_CONFIG_YAML)
        return yaml.safe_load(DEFAULT_CONFIG_YAML)

    raw = uploaded.read().decode("utf-8")
    return yaml.safe_load(raw)


# =========================
# Backup / Persistencia
# =========================
def backup_file(path: str, label: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    ext = os.path.splitext(path)[1]
    bpath = os.path.join(BACKUP_DIR, f"{label}_{ts}{ext}")
    shutil.copy2(path, bpath)
    return bpath


def save_pickle(path: str, obj: Any) -> None:
    pd.to_pickle(obj, path)


def load_pickle(path: str) -> Any:
    return pd.read_pickle(path)


def audit_log(action: str, payload: Dict[str, Any]) -> None:
    rec = {"ts": iso_now(), "action": action, "payload": payload}
    with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")


# =========================
# Region key (FIX PRO)
# =========================
def normalize_region_key(raw: Any) -> Optional[str]:
    """
    Unifica claves de región para matchear con Matriz Andreani (Region ME1):
    Ejemplos:
    - 'PATAGONIA I | PAT I 64' -> 'PAT I 64'
    - 'INTERIOR II IN II 74'   -> 'IN II 74'
    - 'LOCAL LOC 53'           -> 'LOC 53'
    - 'SIN REGIÓN ...'         -> None
    """
    if raw is None or (isinstance(raw, float) and math.isnan(raw)):
        return None
    s = str(raw).strip()
    if not s:
        return None

    # si viene "base | sub" quedate con la sub (derecha)
    if "|" in s:
        s = s.split("|")[-1].strip()

    s = re.sub(r"\s+", " ", s).strip().upper()

    if s.startswith("SIN REGION") or s.startswith("SIN REGIÓN"):
        return None
    if "TIERRA" in s and "FUEGO" in s:
        return "TIERRA DEL FUEGO"

    m = re.search(r"\b(PAT|IN)\s*(I{1,2}|1|2)\s*(\d+)\b", s)
    if m:
        band = m.group(2)
        band = "I" if band == "1" else ("II" if band == "2" else band)
        return f"{m.group(1)} {band} {int(m.group(3))}"

    m = re.search(r"\bLOC\s*(\d+)\b", s)
    if m:
        return f"LOC {int(m.group(1))}"

    # fallback: deja lo que venga
    return s


# =========================
# Normalizadores de datasets
# =========================
def normalize_catalog(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    warnings: List[str] = []
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    required = {"sku", "producto", "peso_aforado_kg"}
    missing = sorted(list(required - set(df.columns)))
    if missing:
        raise ValueError(f"Catálogo: faltan columnas {missing}. Requeridas: {sorted(required)}")

    df["sku"] = df["sku"].astype(str).str.strip()
    df["producto"] = df["producto"].astype(str).str.strip()
    df["peso_aforado_kg"] = pd.to_numeric(df["peso_aforado_kg"], errors="coerce")

    if df["peso_aforado_kg"].isna().any():
        raise ValueError("Catálogo: hay filas con peso_aforado_kg inválido/vacío.")
    if (df["peso_aforado_kg"] <= 0).any():
        warnings.append("Catálogo: hay SKUs con peso_aforado_kg <= 0 (revisar).")
    if df["sku"].duplicated().any():
        warnings.append("Catálogo: hay SKUs duplicados (se toma la última ocurrencia al auditar).")

    return df.reset_index(drop=True), warnings


def normalize_sales(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str], pd.DataFrame]:
    """
    Normaliza Ventas SIN frenar por CP inválido.
    Devuelve:
      - norm: filas válidas (se importan)
      - warns: warnings
      - dropped: filas descartadas (ej: sin guia o sin sku), con motivo
    Requeridas: guia, cp, sku, qty
    Opcionales: fecha_envio (YYYY-MM-DD), valor_declarado_ars
    """
    warns: List[str] = []
    df = df.copy()

    # --- rename tolerante ---
    rename = {}
    for c in list(df.columns):
        nc = norm_text(c)
        if nc in {"guia", "nro guia", "numero guia", "tracking", "tracking_id"}:
            rename[c] = "guia"
        elif nc in {"cp", "codigo postal", "codigopostal", "cod postal", "codpostal"}:
            rename[c] = "cp"
        elif nc in {"sku", "producto", "item", "codigo sku"}:
            rename[c] = "sku"
        elif nc in {"qty", "cantidad", "cant"}:
            rename[c] = "qty"
        elif nc in {"fecha_envio", "fecha envio", "fecha"}:
            rename[c] = "fecha_envio"
        elif nc in {"valor_declarado_ars", "valor declarado", "valor_declarado", "declarado"}:
            rename[c] = "valor_declarado_ars"

    if rename:
        df = df.rename(columns=rename)

    required = {"guia", "cp", "sku", "qty"}
    missing = sorted(list(required - set(df.columns)))
    if missing:
        raise ValueError(f"Ventas: faltan columnas {missing}. Requeridas: {sorted(required)}")

    # --- tipos base ---
    df["guia"] = df["guia"].astype("string").str.strip()
    df["sku"] = df["sku"].astype("string").str.strip()

    # qty numérica
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    bad_qty = df["qty"].isna() | (df["qty"] <= 0)
    if bad_qty.any():
        warns.append(f"Ventas: {int(bad_qty.sum())} filas con qty inválida (se setean a 1).")
        df.loc[bad_qty, "qty"] = 1.0

    # --- CP: NO frenar, solo marcar ---
    df["cp_raw"] = df["cp"]
    df["cp_int"] = df["cp"].apply(parse_cp_to_int)
    df["flag_cp_invalido"] = df["cp_int"].isna()

    if df["flag_cp_invalido"].any():
        warns.append(
            f"Ventas: {int(df['flag_cp_invalido'].sum())} filas con CP inválido/vacío "
            "(se cargan igual; auditoría marcará SIN REGIÓN)."
        )

    # --- fecha_envio opcional ---
    if "fecha_envio" in df.columns:
        df["fecha_envio"] = pd.to_datetime(df["fecha_envio"], errors="coerce").dt.date
        if df["fecha_envio"].isna().any():
            warns.append(
                f"Ventas: {int(df['fecha_envio'].isna().sum())} filas sin fecha_envio válida "
                "(se usará fallback en auditoría)."
            )
    else:
        df["fecha_envio"] = None
        warns.append("Ventas: falta columna fecha_envio (opcional). Auditoría usará fallback.")

    # --- valor declarado opcional ---
    if "valor_declarado_ars" in df.columns:
        df["valor_declarado_ars"] = pd.to_numeric(df["valor_declarado_ars"], errors="coerce")
    else:
        df["valor_declarado_ars"] = None
        warns.append("Ventas: falta valor_declarado_ars (opcional). Seguro (SGD) se calculará solo con fijo.")

    # --- separar descartadas (sin guia o sin sku) ---
    bad_guia = df["guia"].isna() | (df["guia"].astype("string").str.len() == 0)
    bad_sku = df["sku"].isna() | (df["sku"].astype("string").str.len() == 0)
    bad_core = bad_guia | bad_sku

    dropped = df[bad_core].copy()

    # SIEMPRE existe drop_reason, incluso si dropped está vacío (clave para que no rompa el reintento)
    dropped["drop_reason"] = ""

    if not dropped.empty:
        dropped.loc[bad_guia, "drop_reason"] += "SIN_GUIA; "
        dropped.loc[bad_sku, "drop_reason"] += "SIN_SKU; "
        dropped["drop_reason"] = dropped["drop_reason"].str.strip()
        warns.append(
            f"Ventas: {int(len(dropped))} filas sin guia o sin sku se eliminaron (descargables)."
        )

    # --- norm: lo que queda ---
    norm = df[~bad_core].copy()

    out_cols = [
        "guia",
        "cp",
        "cp_int",
        "sku",
        "qty",
        "fecha_envio",
        "valor_declarado_ars",
        "flag_cp_invalido",
    ]

    # columnas para exportar dropped (todo lo original + drop_reason al final)
    drop_cols = [c for c in df.columns if c != "drop_reason"]

    dropped_out = dropped.copy()

    # ✅ asegurar columna siempre, incluso si dropped_out está vacío
    if "drop_reason" not in dropped_out.columns:
        dropped_out["drop_reason"] = ""

    # ✅ asegurar que existan todas las columnas esperadas sin explotar
    dropped_out = dropped_out.reindex(columns=drop_cols + ["drop_reason"])

    return (
        norm[out_cols].reset_index(drop=True),
        warns,
        dropped_out.reset_index(drop=True),
    )





def normalize_cp_master(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Formato objetivo (igual a CP MASTER 26.xlsx):
      - CP
      - Provincia
      - Localidad
      - region_base
      - sub_region

    Calculadas por la app:
      - CP_int
      - region_key (normalizada desde sub_region)
    """
    warnings: List[str] = []
    df = df.copy()

    # Renombrado tolerante por si vienen variaciones de header
    rename: Dict[str, str] = {}
    for c in list(df.columns):
        nc = norm_text(c)
        if nc in {"cp", "codigo postal", "codigopostal", "cod postal", "codpostal", "cpostal"}:
            rename[c] = "CP"
        elif nc in {"provincia", "prov", "state", "estado"}:
            rename[c] = "Provincia"
        elif nc in {"localidad", "ciudad", "local"}:
            rename[c] = "Localidad"
        elif nc in {"region base", "region_base", "regionbase"}:
            rename[c] = "region_base"
        elif nc in {"sub region", "sub_region", "subregion", "sub region me1"}:
            rename[c] = "sub_region"

    if rename:
        df = df.rename(columns=rename)

    required = {"CP", "Provincia", "Localidad", "region_base", "sub_region"}
    missing = sorted(list(required - set(df.columns)))
    if missing:
        raise ValueError(f"CP Master: faltan columnas {missing}. Debe ser EXACTO: {sorted(required)}")

    # CP_int
    df["CP_int"] = df["CP"].apply(parse_cp_to_int)
    if df["CP_int"].isna().any():
        bad = df[df["CP_int"].isna()][["CP"]].head(20).to_dict(orient="records")
        raise ValueError(f"CP Master: hay CP inválidos (primeros 20): {bad}")

    # region_key: SOLO desde sub_region (porque querés que sea tal cual)
    df["region_key"] = df["sub_region"].apply(normalize_region_key)

    # Validación: si sub_region viene vacío, avisamos (no frenamos, pero queda sin tarifar)
    if df["region_key"].isna().any():
        warnings.append("CP Master: hay CPs con sub_region vacío o inválido → esos envíos no van a tarifar (SIN REGIÓN).")

    # Duplicados de CP
    if df["CP_int"].duplicated().any():
        dups = df[df["CP_int"].duplicated(keep=False)].sort_values("CP_int")[["CP", "Provincia", "Localidad", "CP_int"]].head(30)
        raise ValueError(f"CP Master: hay CP repetidos (primeros):\n{dups.to_string(index=False)}")

    # Output CANON: tus 5 columnas + calculadas
    out_cols = ["CP", "Provincia", "Localidad", "region_base", "sub_region", "CP_int", "region_key"]
    return df[out_cols].reset_index(drop=True), warnings



# =========================
# Matriz Andreani (normalizada)
# =========================
def parse_weight_band(col: str) -> Optional[Tuple[float, float]]:
    m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", str(col))
    if not m:
        return None
    return float(m.group(1)), float(m.group(2))


def normalize_andreani_matrix(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Espera formato típico:
      - 'Region ME1' (o similar)
      - columnas '0-1', '1-5', etc
      - 'Exc' (excedente por kg)
    Devuelve long:
      region_key, w_from, w_to, cost, exc_per_kg
    """
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    if "Region ME1" not in df.columns:
        # intentamos encontrar alguna variante
        cand = [c for c in df.columns if norm_text(c) in {"region me1", "regionme1", "region"}]
        if cand:
            df = df.rename(columns={cand[0]: "Region ME1"})
        else:
            raise ValueError("Matriz Andreani: falta columna 'Region ME1'.")

    band_cols = [c for c in df.columns if parse_weight_band(c)]
    if not band_cols:
        raise ValueError("Matriz Andreani: no detecté columnas de bandas de peso tipo '0-1', '1-5', etc.")
    if "Exc" not in df.columns:
        raise ValueError("Matriz Andreani: falta columna 'Exc' (excedente por kg).")

    
    # Seguro (SGD) — opcional (se calcula como: fijo + pct*max(0, valor_declarado - umbral))
    # Permitimos variantes de nombre por robustez.
    def _colpick(canon: str, aliases: List[str]) -> Optional[str]:
        if canon in df.columns:
            return canon
        for a in aliases:
            cand = [c for c in df.columns if norm_text(c) == norm_text(a)]
            if cand:
                return cand[0]
        return None

    sgd_fijo_col = _colpick("SGD_FIJO", ["sgd fijo", "seguro fijo", "sgd_fijo"])
    sgd_umbral_col = _colpick("SGD_UMBRAL", ["sgd umbral", "umbral seguro", "sgd_umbral", "seguro umbral"])
    sgd_pct_col = _colpick("SGD_PCT_EXCESO", ["sgd pct exceso", "sgd_pct_exceso", "pct exceso seguro", "seguro pct exceso", "sgd % exceso"])
    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        region_raw = r.get("Region ME1")
        region_key = normalize_region_key(region_raw) or str(region_raw).strip().upper()
        exc = pd.to_numeric(r.get("Exc"), errors="coerce")

        sgd_fijo = pd.to_numeric(r.get(sgd_fijo_col), errors="coerce") if sgd_fijo_col else float("nan")
        sgd_umbral = pd.to_numeric(r.get(sgd_umbral_col), errors="coerce") if sgd_umbral_col else float("nan")
        sgd_pct_exceso = pd.to_numeric(r.get(sgd_pct_col), errors="coerce") if sgd_pct_col else float("nan")

        for bc in band_cols:
            b = parse_weight_band(bc)
            if not b:
                continue
            w1, w2 = b
            cost = pd.to_numeric(r.get(bc), errors="coerce")
            if pd.isna(cost):
                continue
            rows.append(
                {
                    "region_key": region_key,
                    "w_from": float(w1),
                    "w_to": float(w2),
                    "cost": float(cost),
                    "exc_per_kg": None if pd.isna(exc) else float(exc),
                    "sgd_fijo": None if pd.isna(sgd_fijo) else float(sgd_fijo),
                    "sgd_umbral": None if pd.isna(sgd_umbral) else float(sgd_umbral),
                    "sgd_pct_exceso": None if pd.isna(sgd_pct_exceso) else float(sgd_pct_exceso),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        raise ValueError("Matriz Andreani: quedó vacía al normalizar.")

    meta = {
        "regions": sorted(out["region_key"].dropna().unique().tolist()),
        "bands": sorted({(a, b) for a, b in zip(out["w_from"], out["w_to"])}),
        "band_cols": band_cols,
    }
    return out.reset_index(drop=True), meta



def tariff_lookup(
    matrix_long: pd.DataFrame,
    *,
    region_key: str,
    kg: float,
    valor_declarado_ars: Optional[float] = None,
) -> Tuple[Optional[float], Optional[float], Optional[float], str, str]:
    """
    Devuelve:
      (disd_esperado, sgd_esperado, exc_used, status, sgd_status)

    DISD:
      - Si kg cae dentro de una banda: cost
      - Si kg excede el máximo w_to: cost(max_band) + exc_per_kg*(kg - w_to_max) si existe exc
      - Si no hay region: status SIN_REGION_EN_MATRIZ

    SGD (Seguro):
      - Se toma de la matriz por región (si existe): sgd_fijo, sgd_umbral, sgd_pct_exceso
      - Fórmula: sgd_fijo + sgd_pct_exceso * max(0, valor_declarado - sgd_umbral)
      - Si no hay valor_declarado: usa solo sgd_fijo (marca sgd_status)
    """
    if not region_key:
        return None, None, None, "SIN_REGION", "SGD_NO_APLICA"

    if "region_key" not in matrix_long.columns:
        return None, None, None, "MATRIZ_SIN_REGION_KEY", "SGD_NO_APLICA"

    m = matrix_long[matrix_long["region_key"] == region_key].copy()
    if m.empty:
        return None, None, None, "SIN_REGION_EN_MATRIZ", "SGD_NO_APLICA"

    # -----------------
    # Seguro (SGD)
    # -----------------
    sgd_fijo = m["sgd_fijo"].iloc[0] if "sgd_fijo" in m.columns else None
    sgd_umbral = m["sgd_umbral"].iloc[0] if "sgd_umbral" in m.columns else None
    sgd_pct = m["sgd_pct_exceso"].iloc[0] if "sgd_pct_exceso" in m.columns else None

    # Normalización numérica
    def _to_float(x):
        try:
            if x is None or (isinstance(x, float) and math.isnan(x)):
                return None
            return float(x)
        except Exception:
            return None

    sgd_fijo = _to_float(sgd_fijo)
    sgd_umbral = _to_float(sgd_umbral) if sgd_umbral is not None else 0.0
    sgd_pct = _to_float(sgd_pct) if sgd_pct is not None else 0.0

    sgd_status = "SGD_OK"
    if sgd_fijo is None:
        sgd_esperado = None
        sgd_status = "SGD_SIN_CONFIG"
    else:
        vd = _to_float(valor_declarado_ars)
        if vd is None:
            sgd_esperado = float(sgd_fijo)
            # Si hay pct configurado pero no hay VD, avisamos (igual calculamos fijo)
            if sgd_pct and sgd_pct != 0.0:
                sgd_status = "SGD_SOLO_FIJO_SIN_VD"
            else:
                sgd_status = "SGD_FIJO"
        else:
            exceso = max(0.0, float(vd) - float(sgd_umbral or 0.0))
            sgd_esperado = float(sgd_fijo) + float(sgd_pct or 0.0) * exceso
            sgd_status = "SGD_OK"

    # -----------------
    # DISD (bandas)
    # -----------------
    kg = float(kg)

    # dentro de banda
    inside = m[(m["w_from"] <= kg) & (kg <= m["w_to"])].sort_values(["w_to", "w_from"], ascending=[True, True])
    if not inside.empty:
        row = inside.iloc[0]
        return float(row["cost"]), sgd_esperado, None, "OK", sgd_status

    # excede máximo
    max_row = m.sort_values(["w_to", "w_from"], ascending=[False, False]).iloc[0]
    w_to = float(max_row["w_to"])
    base_cost = float(max_row["cost"])
    exc = max_row.get("exc_per_kg", None)
    if kg > w_to:
        if exc is None or (isinstance(exc, float) and math.isnan(exc)):
            return None, sgd_esperado, None, "SIN_BANDA_SIN_EXC", sgd_status
        extra = float(exc) * (kg - w_to)
        return base_cost + extra, sgd_esperado, float(exc), "OK_EXCEDENTE", sgd_status

    # por debajo del mínimo: tomar mínima banda
    min_row = m.sort_values(["w_from", "w_to"], ascending=[True, True]).iloc[0]
    return float(min_row["cost"]), sgd_esperado, None, "OK_UNDER_MIN", sgd_status

# =========================
# Matrices Registry (simple)
# =========================
def load_registry() -> Dict[str, Any]:
    if not os.path.exists(REGISTRY_PATH):
        return {"matrices": []}

    try:
        with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Formato legacy: lista directa
        if isinstance(data, list):
            return {"matrices": data}

        # Formato nuevo: dict con key matrices
        if isinstance(data, dict):
            mats = data.get("matrices", [])
            return {"matrices": mats if isinstance(mats, list) else []}

        return {"matrices": []}

    except Exception:
        return {"matrices": []}

def save_registry(reg: Dict[str, Any]) -> None:
    mats = reg.get("matrices", [])
    if not isinstance(mats, list):
        mats = []
    with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
        json.dump({"matrices": mats}, f, ensure_ascii=False, indent=2)

def _normalize_registry_entry(e: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(e, dict):
        return {}

    out = dict(e)

    # path legacy -> file_path
    if "file_path" not in out and "path" in out:
        out["file_path"] = out["path"]

    out["name"] = str(out.get("name", "")).strip()
    out["status"] = str(out.get("status", "")).strip().upper()

    # marketplace real
    mp = out.get("marketplace", "")
    out["marketplace"] = str(mp).strip().lower()

    # kind real (RAW / NORMALIZADA)
    out["kind"] = str(out.get("kind", "")).strip().upper()

    out["updated_at"] = out.get("updated_at") or out.get("created_at") or ""

    # normalizar path (Windows)
    fp = out.get("file_path")
    if fp:
        fp = os.path.normpath(fp)
        if not os.path.isabs(fp):
            fp = os.path.normpath(os.path.join(APP_DIR, fp))
        out["file_path"] = fp

    return out


def upsert_matrix_version(
    *,
    name: str,
    kind: str,          # en tu esquema: "RAW" o "NORMALIZADA"
    status: str,        # "DRAFT" / "PUBLISHED"
    valid_from: Optional[str],
    valid_to: Optional[str],
    file_path: str,
    meta: Dict[str, Any],
    marketplace: str = "andreani",
) -> None:
    reg = load_registry()
    rows = reg.get("matrices", [])
    if not isinstance(rows, list):
        rows = []

    name = str(name).strip()
    kind = str(kind).strip().upper()
    status = str(status).strip().upper()
    marketplace = str(marketplace).strip().lower()

    # reemplaza por misma combinación marketplace + kind + name
    def _same(r: Any) -> bool:
        if not isinstance(r, dict):
            return False
        r_name = str(r.get("name", "")).strip()
        r_kind = str(r.get("kind", "")).strip().upper()
        r_mp = str(r.get("marketplace", r.get("mp", ""))).strip().lower()
        return (r_name == name) and (r_kind == kind) and (r_mp == marketplace)

    rows = [r for r in rows if not _same(r)]

    entry = {
        "name": name,
        "marketplace": marketplace,      # 👈 tu esquema real
        "kind": kind,                    # 👈 "RAW" / "NORMALIZADA"
        "status": status,                # 👈 "PUBLISHED" / "DRAFT"
        "valid_from": valid_from,
        "valid_to": valid_to,
        "path": file_path,               # 👈 legacy (tu archivo actual)
        "file_path": file_path,          # 👈 nuevo (compatible con el refactor)
        "meta": meta or {},
        "updated_at": iso_now(),
        "created_at": iso_now(),
    }

    rows.append(entry)

    # orden estable
    rows = sorted(
        rows,
        key=lambda r: (
            str(r.get("marketplace", "")),
            str(r.get("kind", "")),
            str(r.get("name", "")),
        ),
    )

    reg["matrices"] = rows
    save_registry(reg)


def pick_active_matrix(marketplace: str, ref_date: dt.date) -> Optional[Dict[str, Any]]:
    reg = load_registry()
    raw = reg.get("matrices", [])
    mats = [_normalize_registry_entry(x) for x in raw]
    mats = [m for m in mats if m]

    marketplace = str(marketplace).strip().lower()

    def _parse(d: Optional[str]) -> Optional[dt.date]:
        if not d:
            return None
        try:
            return dt.datetime.strptime(d, "%Y-%m-%d").date()
        except Exception:
            return None

    candidates = []
    for m in mats:
        if m.get("status") != "PUBLISHED":
            continue
        if m.get("marketplace") != marketplace:
            continue
        k = str(m.get("kind","")).strip().upper()
        if marketplace == "andreani":
            if k not in {"RAW","ANDREANI"}:
                continue
        else:
            if k != "RAW":
                continue
            continue

        vf = _parse(m.get("valid_from"))
        vt = _parse(m.get("valid_to"))
        if vf and ref_date < vf:
            continue
        if vt and ref_date > vt:
            continue

        fp = m.get("file_path")
        if not fp or not os.path.exists(fp):
            continue

        candidates.append(m)

    if not candidates:
        return None

    candidates.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    return candidates[0]


# =========================
# PDF Auditor (Andreani)
# =========================
RE_SERV = re.compile(
    r"^Servicio de transporte\s+(?P<svc>[A-Z]{3,4})\s+\d+\s+(?P<remito>[^ ]+)\s+(?P<fecha>\d{2}\.\d{2}\.\d{4})\s+(?P<bultos>\d+)\s+(?P<kg>[0-9\.,]+)\s+(?P<cant>[0-9\.,]+)\s+(?P<desc>[0-9\.,]+)\s+(?P<neto>[0-9\.,]+)\s*$"
)
RE_ENVIO = re.compile(r"Nro\.?\s*de\s*Envío:\s*(\d{10,})", re.IGNORECASE)


def parse_pdf_shipments(pdf_bytes: bytes) -> pd.DataFrame:
    """Extrae por guía: fecha_factura, bultos, kg, disd, sgd."""
    rows: List[Dict[str, Any]] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        lines: List[str] = []
        for p in pdf.pages:
            t = p.extract_text() or ""
            lines += t.splitlines()

    cur = {"disd": None, "sgd": None, "bultos": None, "kg": None, "fecha": None}

    for l in lines:
        l = (l or "").strip()
        m = RE_SERV.match(l)
        if m:
            svc = m.group("svc").upper()
            fecha = dt.datetime.strptime(m.group("fecha"), "%d.%m.%Y").date()
            bultos = int(m.group("bultos"))
            kg = ar_money_to_float(m.group("kg"))
            neto = ar_money_to_float(m.group("neto"))
            cur["bultos"] = bultos
            cur["kg"] = kg
            cur["fecha"] = fecha
            if svc == "DISD":
                cur["disd"] = neto
            elif svc == "SGD":
                cur["sgd"] = neto
            continue

        m2 = RE_ENVIO.search(l)
        if m2:
            guia = normalize_guia(m2.group(1))
            rows.append(
                {
                    "guia": guia,
                    "fecha_factura": cur["fecha"],
                    "bultos_factura": cur["bultos"],
                    "kg_factura": cur["kg"],
                    "disd_factura": cur["disd"],
                    "sgd_factura": cur["sgd"],
                }
            )
            cur = {"disd": None, "sgd": None, "bultos": None, "kg": None, "fecha": None}

    out = pd.DataFrame(rows)
    out = out.dropna(subset=["guia"]).reset_index(drop=True)
    return out


# =========================
# UI
# =========================
st.set_page_config(page_title="Andreani | Gestión logística", layout="wide")
st.title("Andreani | Gestión logística (v1.54) — Fix PRO")

with st.sidebar:
    st.header("Navegación")
    page = st.radio(
        "Módulo",
        [
            "Estado",
            "CP Master",
            "Catálogo",
            "Ventas",
            "Matriz Andreani",
            "Auditor Facturas",
            "Audit Trail",
        ],
        index=5,
    )

    st.divider()
    st.subheader("Config (opcional)")
    cfg_upload = st.file_uploader("config.yaml", type=["yaml", "yml"])
    config = load_config(cfg_upload)

    st.caption("Tip: si no cargás config, se usa el config.yaml del repo o defaults.")

# Helpers de carga actual
def get_cp_master() -> Optional[pd.DataFrame]:
    return load_pickle(CP_MASTER_PATH) if os.path.exists(CP_MASTER_PATH) else None

def get_catalog() -> Optional[pd.DataFrame]:
    return load_pickle(CATALOG_PATH) if os.path.exists(CATALOG_PATH) else None

def get_sales() -> Optional[pd.DataFrame]:
    return load_pickle(SALES_PATH) if os.path.exists(SALES_PATH) else None


# =========================
# Estado
# =========================
if page == "Estado":
    st.subheader("Estado de datos en disco")

    rows = []
    for label, path in [
        ("CP master", CP_MASTER_PATH),
        ("Catálogo", CATALOG_PATH),
        ("Ventas", SALES_PATH),
        ("Registry matrices", REGISTRY_PATH),
        ("Audit trail", AUDIT_LOG_PATH),
        ("Backups", BACKUP_DIR),
    ]:
        exists = os.path.exists(path)
        size_kb = round(os.path.getsize(path) / 1024, 1) if exists and os.path.isfile(path) else 0.0
        mod = dt.datetime.fromtimestamp(os.path.getmtime(path)).isoformat(timespec="seconds") if exists else ""
        rows.append({"Recurso": label, "Ruta": path, "Existe": exists, "Tamaño (KB)": size_kb, "Modificado": mod})

    safe_show_df(pd.DataFrame(rows), label="estado")

    st.divider()
    st.subheader("Templates")
    cols = st.columns(4)
    for i, (label, p) in enumerate(
        [
            ("Template CP Master", TPL_CP),
            ("Template Catálogo", TPL_CATALOG),
            ("Template Ventas", TPL_SALES),
            ("Template Matriz Andreani", TPL_AND),
        ]
    ):
        with cols[i]:
            if os.path.exists(p):
                st.download_button(label, data=open(p, "rb").read(), file_name=os.path.basename(p))
            else:
                st.caption(f"No encontré {os.path.basename(p)} en el repo.")

# =========================
# CP Master (UI PRO: buscar + editar + incremental import)
# =========================
if page == "CP Master":
    st.subheader("CP Master — Operación diaria (buscar / editar / sumar CPs)")

    cur = get_cp_master()
    if cur is None:
        cur = pd.DataFrame(columns=["CP", "Provincia", "Localidad", "region_base", "sub_region", "region", "region_key", "CP_int"])

    # --- Normalizar tipos base para editor ---
    cur = cur.copy()
    if "CP_int" not in cur.columns:
        cur["CP_int"] = cur["CP"].apply(parse_cp_to_int)

    # Alias consistente para el resto del módulo
    cp_master = cur

    # -------------------------
    # Barra de herramientas
    # -------------------------
    c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
    with c1:
        q = st.text_input("Buscar (CP / provincia / localidad / región)", value="")
    with c2:
        only_missing = st.checkbox("Solo CPs sin region_key", value=False)
    with c3:
        only_not_found = st.checkbox("Solo CPs con datos incompletos (prov/localidad)", value=False)
    with c4:
        st.metric("CPs", f"{len(cur):,}")

    view = cur.copy()

    if q.strip():
        nq = norm_text(q)
        mask = (
            view["CP"].astype(str).str.contains(q.strip(), na=False)
            | view["Provincia"].astype(str).apply(norm_text).str.contains(nq, na=False)
            | view["Localidad"].astype(str).apply(norm_text).str.contains(nq, na=False)
            | view["region_key"].astype(str).apply(norm_text).str.contains(nq, na=False)
        )
        view = view[mask]

    if only_missing:
        view = view[view["region_key"].isna() | (view["region_key"].astype(str).str.strip() == "")]

    if only_not_found:
        view = view[
            (view["Provincia"].isna() | (view["Provincia"].astype(str).str.strip() == ""))
            | (view["Localidad"].isna() | (view["Localidad"].astype(str).str.strip() == ""))
        ]

    st.caption("Tip: editás inline y recién se guarda cuando tocás **Aplicar cambios**.")

    # -------------------------
    # Editor inline (st.data_editor)
    # -------------------------
    # Columnas que sí querés editar a mano:
    editable_cols = ["CP", "Provincia", "Localidad", "region_base", "sub_region"]
    show_cols = editable_cols  # ← SOLO lo del Excel

    edited = st.data_editor(
        view[show_cols],
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
    )

    # Recalcular (no mostrar) claves internas
    edited_calc = edited.copy()
    edited_calc["CP_int"] = edited_calc["CP"].apply(parse_cp_to_int)
    edited_calc["region_key"] = edited_calc["sub_region"].apply(normalize_region_key)


    # Recalcular claves luego de edición
    def _recalc(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["CP_int"] = df["CP"].apply(parse_cp_to_int)
        df["region_key"] = df["sub_region"].apply(normalize_region_key)
        missing = df["region_key"].isna()
        if "region" in df.columns:
            df.loc[missing, "region_key"] = df.loc[missing, "region"].apply(normalize_region_key)
        return df

    edited = _recalc(edited)


    # -------------------------
    # Guardar edición inline (del editor)
    # -------------------------
    st.subheader("Guardar edición inline")
    st.caption("Aplica SOLO los cambios de la tabla visible (filtrada), sin tocar el resto del dataset.")

    confirm_inline = st.checkbox("Confirmo que quiero guardar los cambios del editor.", key="cp_inline_confirm")

    if st.button("Aplicar cambios (guardar CP Master)", disabled=not confirm_inline, key="cp_inline_apply"):
        base = cp_master.copy()
        if "CP_int" not in base.columns:
            base["CP_int"] = base["CP"].apply(parse_cp_to_int)
        if "region_key" not in base.columns:
            base["region_key"] = base.get("sub_region", pd.Series([None]*len(base))).apply(normalize_region_key)

        # Recalcular claves en lo editado
        upd = _recalc(edited_calc)

        # Mantener solo filas con CP válido
        upd = upd[upd["CP_int"].notna()].copy()
        if upd.empty:
            st.error("No hay filas válidas para guardar (CP inválidos).")
        else:
            # Asegurar columnas mínimas en base
            for c in ["Provincia", "Localidad", "region_base", "sub_region", "region_key"]:
                if c not in base.columns:
                    base[c] = ""

            added = 0
            updated = 0

            # Índice por CP_int para update rápido
            base_idx = base.set_index("CP_int", drop=False)

            for _, r in upd.iterrows():
                cp_i = int(r["CP_int"])
                payload = {
                    "CP": cp_i,
                    "CP_int": cp_i,
                    "Provincia": str(r.get("Provincia", "")).strip(),
                    "Localidad": str(r.get("Localidad", "")).strip(),
                    "region_base": str(r.get("region_base", "")).strip(),
                    "sub_region": str(r.get("sub_region", "")).strip(),
                    "region_key": r.get("region_key"),
                }

                if cp_i in base_idx.index:
                    mask = base["CP_int"] == cp_i
                    for k, v in payload.items():
                        if k in base.columns:
                            base.loc[mask, k] = v
                    updated += 1
                else:
                    base = pd.concat([base, pd.DataFrame([payload])], ignore_index=True)
                    added += 1

            base = base.sort_values("CP_int").reset_index(drop=True)

            backup_file(CP_MASTER_PATH, "cp_master")
            save_pickle(CP_MASTER_PATH, base)
            audit_log("cp_master_apply_inline", {"rows": int(len(base)), "added": int(added), "updated": int(updated)})
            st.success(f"Listo: CP Master guardado. (+{added} nuevos, {updated} actualizados)")
            st.rerun()

    st.divider()

    # -------------------------
    # Agregar CP manual (rápido)
    # -------------------------
    st.subheader("Agregar CP manual (rápido)")

    # listas para dropdown
    prov_list = provinces_ar()
    sub_list = list_subregions_from_cp_master(cp_master)

    c1, c2, c3, c4 = st.columns([1, 2, 3, 2])

    with c1:
        cp_in = st.text_input("CP", value="", placeholder="Ej: 3089")

    with c2:
        # Dropdown Provincia
        prov_in = st.selectbox(
            "Provincia",
            options=[""] + prov_list,
            index=0,
        )

    with c3:
        loc_in = st.text_input("Localidad", value="", placeholder="Ej: San José Del Rincón")

    with c4:
        # Dropdown Sub región (tomada de CP Master)
        sub_opts = [""] + sub_list
        sub_in = st.selectbox(
            "Sub región (ej: PAT I 64 / LOC 53)",
            options=sub_opts,
            index=0,
        )

    # (Opcional) permitir escribir sub_region nueva si no existe aún
    allow_custom_sub = st.checkbox("Permitir escribir sub_región nueva (si no está en la lista)", value=False)
    if allow_custom_sub:
        sub_in_custom = st.text_input("Sub región (manual)", value=sub_in or "")
        sub_in = sub_in_custom.strip()

    if st.button("Agregar / Actualizar este CP"):
        cp_int = parse_cp_to_int(cp_in)

        if cp_int is None:
            st.error("CP inválido. Usá un número (ej: 3089 / 9033).")
            st.stop()

        if not prov_in:
            st.error("Elegí una Provincia.")
            st.stop()

        if not loc_in.strip():
            st.error("Completá Localidad.")
            st.stop()

        if not sub_in or not str(sub_in).strip():
            st.error("Elegí una Sub región.")
            st.stop()

        # Normalización mínima (respetamos tu formato: 'IN I14' / 'PAT I 64' / 'LOC 53' tal cual)
        row = {
            "CP": int(cp_int),
            "CP_int": int(cp_int),
            "Provincia": str(prov_in).strip(),
            "Localidad": str(loc_in).strip(),
            "region_base": "",
            "sub_region": str(sub_in).strip(),
            "region_key": normalize_region_key(str(sub_in).strip()) if str(sub_in).strip() else None,
        }

        # upsert en cp_master
        base = cp_master.copy()
        if (base["CP_int"] == int(cp_int)).any():
            base.loc[base["CP_int"] == int(cp_int), ["Provincia", "Localidad", "sub_region"]] = [
                row["Provincia"],
                row["Localidad"],
                row["sub_region"],
            ]
            st.success(f"CP {cp_int} actualizado.")
        else:
            base = pd.concat([base, pd.DataFrame([row])], ignore_index=True)
            st.success(f"CP {cp_int} agregado.")

        # guardar
        backup_file(CP_MASTER_PATH, "cp_master")
        save_pickle(CP_MASTER_PATH, base)
        audit_log("cp_master_upsert_manual", {"cp": int(cp_int), "provincia": row["Provincia"], "sub_region": row["sub_region"]})
        st.rerun()


    # -------------------------
    # Import incremental (UPSERT por CP)
    # -------------------------
    st.subheader("Import incremental (sumar/actualizar CPs desde un archivito)")

    mode = st.radio(
        "Modo de import",
        ["Incremental (recomendado)", "Reemplazar todo (peligroso)"],
        index=0,
        horizontal=True,
    )

    up = st.file_uploader("Subí CP Master parcial (xlsx/csv)", type=["xlsx", "xls", "csv"])
    if up:
        raw = pd.read_csv(up) if up.name.lower().endswith(".csv") else pd.read_excel(up, engine="openpyxl")
        norm, warns = normalize_cp_master(raw)

        for w in warns:
            st.warning(w)

        st.write("Preview del import normalizado")
        safe_show_df(norm.head(2000), label="cp_master_import_preview")

        base = cur.copy()
        base["CP_int"] = base["CP"].apply(parse_cp_to_int)

        if mode.startswith("Reemplazar"):
            merged = norm.copy()
            added = len(merged)
            updated = 0
            same = 0
        else:
            # UPSERT por CP_int
            base_idx = base.set_index("CP_int", drop=False)
            norm_idx = norm.set_index("CP_int", drop=False)

            added_keys = [k for k in norm_idx.index if k not in base_idx.index]
            common_keys = [k for k in norm_idx.index if k in base_idx.index]

            # contar cambios reales
            updated = 0
            same = 0
            cols_cmp = ["Provincia", "Localidad", "region_base", "sub_region", "region", "region_key", "CP"]
            for k in common_keys:
                a = base_idx.loc[k, cols_cmp].astype(str).fillna("").to_list()
                b = norm_idx.loc[k, cols_cmp].astype(str).fillna("").to_list()
                if a == b:
                    same += 1
                else:
                    updated += 1

            # aplicar upsert
            merged = base_idx.copy()
            # actualiza existentes
            for k in common_keys:
                merged.loc[k, norm_idx.columns] = norm_idx.loc[k, norm_idx.columns]
            # agrega nuevos
            if added_keys:
                merged = pd.concat([merged, norm_idx.loc[added_keys]], axis=0)

            merged = merged.reset_index(drop=True)

            added = len(added_keys)

        st.write("Plan de cambios")
        cA, cB, cC, cD = st.columns(4)
        cA.metric("Actual (filas)", f"{len(base):,}")
        cB.metric("Import (filas)", f"{len(norm):,}")
        cC.metric("Agrega", f"{added:,}")
        cD.metric("Actualiza", f"{updated:,}")

        if mode.startswith("Incremental"):
            st.caption(f"Sin cambios (mismos valores): {same:,}")

        st.write("Preview después del merge")
        safe_show_df(merged.head(2000), label="cp_master_merged_preview")

        confirm = st.checkbox("Confirmo aplicar estos cambios en disco (con backup).")
        if st.button("Aplicar cambios (guardar CP Master)", disabled=not confirm):
            backup_file(CP_MASTER_PATH, "cp_master")
            save_pickle(CP_MASTER_PATH, merged)
            audit_log("cp_master_apply", {"mode": mode, "import_file": up.name, "rows": len(merged), "added": int(added), "updated": int(updated)})
            st.success("Listo: CP Master actualizado y guardado.")


# =========================
# Catálogo
# =========================
if page == "Catálogo":
    st.subheader("Catálogo — Import / Simular / Aplicar")
    cur = get_catalog()
    if cur is not None:
        st.caption(f"Dataset actual: {len(cur):,} filas")
        safe_show_df(cur, label="catalog_actual")

    # -------------------------
    # Carga manual (1 SKU)
    # -------------------------
    with st.expander("Agregar SKU manual (rápido)", expanded=False):
        st.caption("Agrega o actualiza un SKU del catálogo (sirve para destrabar auditorías sin reimportar todo).")
        c1, c2, c3 = st.columns([2, 4, 2])
        with c1:
            manual_sku = st.text_input("SKU", value="", placeholder="Ej: 171-0122", key="cat_manual_sku")
        with c2:
            manual_prod = st.text_input("Producto", value="", placeholder="Nombre producto", key="cat_manual_prod")
        with c3:
            manual_peso = st.number_input("Peso aforado (kg)", min_value=0.0, value=0.0, step=0.1, key="cat_manual_peso")

        mode = st.radio("Acción", ["Agregar", "Reemplazar (mismo SKU)"], horizontal=True, index=1, key="cat_manual_mode")

        if st.button("Guardar SKU manual", key="cat_manual_save"):
            sku_clean = str(manual_sku or "").strip()
            prod_clean = str(manual_prod or "").strip()

            if not sku_clean:
                st.error("Falta SKU.")
                st.stop()
            if not prod_clean:
                st.error("Falta Producto.")
                st.stop()
            if manual_peso is None or float(manual_peso) <= 0:
                st.error("Peso aforado debe ser > 0.")
                st.stop()

            one = pd.DataFrame([{
                "sku": sku_clean,
                "producto": prod_clean,
                "peso_aforado_kg": float(manual_peso),
            }])

            try:
                n1, w1 = normalize_catalog(one)
            except Exception as e:
                st.error(f"No pude guardar el SKU manual. Motivo: {e}")
                st.stop()

            row = n1.iloc[0].to_dict()
            cat_cur = cur.copy() if cur is not None else pd.DataFrame(columns=n1.columns)

            if len(cat_cur) and "sku" in cat_cur.columns:
                mask = cat_cur["sku"].astype(str).str.strip() == row["sku"]
            else:
                mask = pd.Series([False]*len(cat_cur))

            exists = bool(mask.any()) if len(cat_cur) else False

            if exists and mode.startswith("Reemplazar"):
                first_idx = cat_cur.index[mask][0]
                for col in n1.columns:
                    cat_cur.at[first_idx, col] = row.get(col)
                action_done = "actualizado"
            else:
                cat_cur = pd.concat([cat_cur, n1], ignore_index=True)
                action_done = "agregado"

            backup_file(CATALOG_PATH, "catalog")
            save_pickle(CATALOG_PATH, cat_cur.reset_index(drop=True))
            audit_log("catalog_manual_upsert", {"sku": row["sku"], "action": action_done})
            st.success(f"SKU {action_done} OK ✅")
            st.rerun()

    up = st.file_uploader("Subí Catálogo (xlsx/csv)", type=["xlsx", "xls", "csv"])
    if up:
        if up.name.lower().endswith(".csv"):
            raw = pd.read_csv(up)
        else:
            raw = pd.read_excel(up, engine="openpyxl")

        norm, warns = normalize_catalog(raw)
        st.success(f"OK: Catálogo normalizado ({len(norm):,} filas)")
        for w in warns:
            st.warning(w)

        safe_show_df(norm, label="catalog_preview")
        st.download_button("Descargar preview (Excel)", data=to_excel_bytes(norm, "catalog"), file_name="catalog_normalizado.xlsx")

        confirm = st.checkbox("Confirmo que quiero aplicar cambios (guardar en disco).")
        if st.button("Aplicar cambios", disabled=not confirm):
            backup_file(CATALOG_PATH, "catalog")
            save_pickle(CATALOG_PATH, norm)
            audit_log("catalog_apply", {"rows": len(norm), "file": up.name})
            st.success("Listo: Catálogo guardado.")

# =========================
# Ventas
# =========================
if page == "Ventas":
    st.subheader("Ventas — Import / Simular / Aplicar")

    cur = get_sales()
    if cur is not None:
        st.caption(f"Dataset actual: {len(cur):,} filas")
        safe_show_df(cur, label="ventas_actual")

    # -------------------------
    # Carga manual (1 venta)
    # -------------------------
    with st.expander("Agregar venta manual (rápido)", expanded=False):
        st.caption("Agrega o actualiza una línea de venta (guía + SKU). Útil para corregir rápido sin reimportar todo.")
        c1, c2, c3, c4 = st.columns([2, 1, 2, 1])
        with c1:
            manual_guia = st.text_input("Guía", value="", placeholder="Ej: 400400003136148")
        with c2:
            manual_cp = st.text_input("CP", value="", placeholder="Ej: 9033")
        with c3:
            manual_sku = st.text_input("SKU", value="", placeholder="Ej: 171-0122")
        with c4:
            manual_qty = st.number_input("Qty", min_value=1, value=1, step=1)

        c5, c6 = st.columns([1, 1])
        with c5:
            manual_fecha_envio = st.date_input("Fecha envío (opcional)", value=None)
        with c6:
            manual_valor_decl = st.number_input("Valor declarado ARS (opcional)", min_value=0.0, value=0.0, step=1000.0)

        mode = st.radio("Acción", ["Agregar", "Reemplazar (misma guía+SKU)"], horizontal=True, index=1)

        if st.button("Guardar venta manual"):
            guia_clean = normalize_guia(manual_guia)
            sku_clean = str(manual_sku or "").strip()
            cp_raw = str(manual_cp or "").strip()

            if not guia_clean:
                st.error("Falta Guía.")
                st.stop()
            if not sku_clean:
                st.error("Falta SKU.")
                st.stop()

            # Armamos DF de 1 fila y reutilizamos normalizador (mismas reglas que import)
            one = pd.DataFrame([{
                "guia": guia_clean,
                "cp": cp_raw,
                "sku": sku_clean,
                "qty": int(manual_qty),
                "fecha_envio": manual_fecha_envio,
                "valor_declarado_ars": float(manual_valor_decl) if manual_valor_decl else None,
            }])

            try:
                n1, w1, d1 = normalize_sales(one)
            except Exception as e:
                st.error(f"No pude guardar la venta manual. Motivo: {e}")
                st.stop()

            if d1 is not None and not d1.empty:
                st.error("No se guardó: la fila quedó descartada (sin guía o sin SKU).")
                safe_show_df(d1, label="venta_manual_descartada")
                st.stop()

            row = n1.iloc[0].to_dict()

            sales_cur = cur.copy() if cur is not None else pd.DataFrame(columns=n1.columns)

            mask = (sales_cur.get("guia", pd.Series(dtype="string")) == row["guia"]) & (sales_cur.get("sku", pd.Series(dtype="string")) == row["sku"])
            exists = bool(mask.any()) if len(sales_cur) else False

            if exists and mode.startswith("Reemplazar"):
                # reemplaza la primera coincidencia (dejamos 1 línea por guía+SKU)
                first_idx = sales_cur.index[mask][0]
                for col in n1.columns:
                    sales_cur.at[first_idx, col] = row.get(col)
                # si hubiese más duplicados, los dejamos tal cual (puede ser a propósito); opcional: eliminarlos
                action_done = "actualizada"
            else:
                sales_cur = pd.concat([sales_cur, n1], ignore_index=True)
                action_done = "agregada"

            backup_file(SALES_PATH, "sales")
            save_pickle(SALES_PATH, sales_cur.reset_index(drop=True))
            audit_log("sales_manual_upsert", {"guia": row["guia"], "sku": row["sku"], "action": action_done})
            st.success(f"Venta {action_done} OK ✅")
            st.rerun()

    up = st.file_uploader("Subí Ventas (xlsx/csv)", type=["xlsx", "xls", "csv"])
    if up:
        # -------------------------
        # Lectura robusta (Excel con múltiples hojas)
        # -------------------------
        if up.name.lower().endswith(".csv"):
            raw = pd.read_csv(
                up,
                dtype={"guia": "string"},
                encoding_errors="ignore",
            )
            detected_sheet = "CSV"
        else:
            xls = pd.read_excel(up, sheet_name=None, engine="openpyxl")

            if "VENTAS" in xls:
                raw = xls["VENTAS"]
                detected_sheet = "VENTAS"
            else:
                required = {"guia", "cp", "sku", "qty"}
                raw = None
                detected_sheet = None

                for sh, df in xls.items():
                    cols = {str(c).strip().lower() for c in df.columns}
                    if required.issubset(cols):
                        raw = df
                        detected_sheet = sh
                        break

                if raw is None:
                    detected_sheet = next(iter(xls.keys()))
                    raw = xls[detected_sheet]

        st.caption(f"Hoja detectada: {detected_sheet}")

        # -------------------------
        # Normalización
        # -------------------------
        try:
            norm, warns, dropped = normalize_sales(raw)
        except Exception as e:
            st.error(f"No pude normalizar Ventas. Motivo: {e}")
            st.info("Tip: la hoja debe contener columnas: guia, cp, sku, qty (+ opcional fecha_envio, valor_declarado_ars).")
            st.stop()

        st.success(f"OK: Ventas normalizadas ({len(norm):,} filas)")
        for w in warns:
            st.warning(w)

        # -------------------------
        # Preview + descarga
        # -------------------------
        safe_show_df(norm, label="ventas_preview")
        st.download_button(
            "Descargar preview (Excel)",
            data=to_excel_bytes(norm, "ventas"),
            file_name="ventas_normalizadas.xlsx",
        )

        # -----------------------------------
        # Descargar TODO lo que tenga pendientes (para corregir y reimportar)
        # -----------------------------------
        def _build_sales_errors(df: pd.DataFrame) -> pd.DataFrame:
            out = df.copy()
            out["error_reason"] = ""

            if "flag_cp_invalido" in out.columns:
                out.loc[out["flag_cp_invalido"], "error_reason"] += "CP_INVALIDO; "

            if "fecha_envio" in out.columns:
                out.loc[out["fecha_envio"].isna(), "error_reason"] += "FECHA_ENVIO_INVALIDA; "

            if "valor_declarado_ars" in out.columns:
                out.loc[out["valor_declarado_ars"].isna(), "error_reason"] += "SIN_VALOR_DECLARADO; "

            out = out[out["error_reason"].str.len() > 0].copy()
            out["error_reason"] = out["error_reason"].str.strip()

            prefer = [
                "error_reason",
                "guia",
                "cp",
                "cp_int",
                "sku",
                "qty",
                "fecha_envio",
                "valor_declarado_ars",
                "flag_cp_invalido",
            ]
            cols = [c for c in prefer if c in out.columns] + [c for c in out.columns if c not in prefer]
            return out[cols]

        errors_df = _build_sales_errors(norm)

        if not errors_df.empty:
            st.warning(f"Pendientes detectados: {len(errors_df):,} filas. Descargalos, corregí y reimportá.")
            safe_show_df(errors_df, label="ventas_errores")

            st.download_button(
                "Descargar PENDIENTES (Excel)",
                data=to_excel_bytes(errors_df, "pendientes_ventas"),
                file_name="ventas_pendientes.xlsx",
            )
            st.download_button(
                "Descargar PENDIENTES (CSV)",
                data=errors_df.to_csv(index=False).encode("utf-8"),
                file_name="ventas_pendientes.csv",
                mime="text/csv",
            )
        else:
            st.success("No se detectaron pendientes en Ventas ✅")

        # -----------------------------------
        # Filas eliminadas (sin guia / sin sku) — EDITABLES y reintegrables
        # -----------------------------------
        if dropped is not None and not dropped.empty:
            st.error(
                f"Filas eliminadas: {len(dropped):,} (no se importan porque faltan datos clave). "
                "Podés editarlas acá y reintentar incluirlas."
            )

            # Mostramos solo lo útil para corregir (y evitamos ruido)
            editable_cols = ["guia", "cp", "sku", "qty", "fecha_envio", "valor_declarado_ars"]
            view_cols = [c for c in editable_cols if c in dropped.columns]
            dropped_view = dropped.copy()

            # Si alguna no existe, la creamos para que el editor no rompa
            for c in editable_cols:
                if c not in dropped_view.columns:
                    dropped_view[c] = None

            st.caption("Editá los campos faltantes (ej: sku/guia/cp). Luego podés reintentar incluirlas.")
            dropped_edit = st.data_editor(
                dropped_view[editable_cols],
                use_container_width=True,
                num_rows="dynamic",
                hide_index=True,
                key="ventas_dropped_editor",
            )

            c1, c2, c3 = st.columns([1, 1, 2])

            with c1:
                st.download_button(
                    "Descargar FILAS ELIMINADAS (Excel)",
                    data=to_excel_bytes(dropped, "filas_eliminadas"),
                    file_name="ventas_filas_eliminadas.xlsx",
                )

            with c2:
                discard = st.button("Descartar eliminadas y seguir", type="secondary")

            with c3:
                retry = st.button("Reintentar incluir editadas", type="primary")

            if discard:
                st.info("OK. Se descartan las eliminadas. Podés aplicar cambios con el dataset normalizado.")
                # No hacemos nada: norm queda tal cual
                # (Si querés, acá podríamos setear dropped vacío)
                # st.rerun()

            if retry:
                # Armamos un RAW reparado: norm (en formato canónico) + dropped_edit (editado)
                # Importante: usamos nombres esperados por normalize_sales: guia, cp, sku, qty, fecha_envio, valor_declarado_ars
                reparadas_raw = dropped_edit.copy()

                # concatenamos con norm "base", pero norm ya está normalizado (tiene cp_int/flag)
                # Necesitamos reconstruir un raw simple desde norm
                base_raw = norm.copy()
                base_raw = base_raw.rename(columns={"cp_int": "cp_int"})  # no-op, por claridad
                # mantener columnas que normalize_sales entiende
                base_raw = base_raw[["guia", "cp", "sku", "qty", "fecha_envio", "valor_declarado_ars"]].copy()

                raw_merged = pd.concat([base_raw, reparadas_raw], ignore_index=True)

                # Re-normalizamos TODO junto
                try:
                    norm2, warns2, dropped2 = normalize_sales(raw_merged)
                except Exception as e:
                    st.error(f"No pude reintentar incluir. Motivo: {e}")
                    st.stop()

                st.success(f"Reintento OK. Ahora importables: {len(norm2):,} filas. Eliminadas restantes: {len(dropped2):,}.")
                for w in warns2:
                    st.warning(w)

                # Reemplazamos en memoria lo que se muestra a partir de ahora
                norm = norm2
                dropped = dropped2

                # Mostrar preview actualizado
                safe_show_df(norm, label="ventas_preview_reintento")
                st.download_button(
                    "Descargar preview actualizado (Excel)",
                    data=to_excel_bytes(norm, "ventas_reintento"),
                    file_name="ventas_normalizadas_actualizadas.xlsx",
                )

                # Si quedaron eliminadas, mostrarlas (y se pueden volver a editar)
                if dropped is not None and not dropped.empty:
                    st.error(f"Quedan {len(dropped):,} filas aún eliminadas (faltan datos clave). Podés seguir editando y reintentar.")
                    safe_show_df(dropped, label="ventas_dropped_restantes")


        # -------------------------
        # Aplicar
        # -------------------------
        confirm = st.checkbox("Confirmo que quiero aplicar cambios (guardar en disco).")
        if st.button("Aplicar cambios", disabled=not confirm):
            backup_file(SALES_PATH, "sales")
            save_pickle(SALES_PATH, norm)
            audit_log("sales_apply", {"rows": int(len(norm)), "file": up.name})
            st.success("Listo: Ventas guardadas.")
            st.rerun()

# =========================
# Matriz Andreani
# =========================
if page == "Matriz Andreani":
    st.subheader("Matriz Andreani — Import / Publicar / Usar en auditoría")

    reg = load_registry()
    mats = [m for m in reg.get("matrices", [])
            if (str(m.get("marketplace","")).lower() == "andreani" or str(m.get("kind","")).lower() == "andreani")
            ]
    if mats:
        st.write("Matrices registradas (Andreani)")
        safe_show_df(pd.DataFrame(mats), label="registry_andreani")

    up = st.file_uploader("Subí Matriz Andreani (xlsx)", type=["xlsx", "xls"])
    if up:
        raw, sheet = read_excel_best_sheet(up, prefer_sheets=["MATRIZ_RAW","MATRIZ RAW","MATRIZ"])
        if sheet:
            st.caption(f"Hoja detectada: {sheet}")
        norm, meta = normalize_andreani_matrix(raw)

        st.success(f"OK: matriz normalizada ({len(norm):,} filas / {len(meta.get('regions', []))} regiones)")
        st.write("Preview normalizado (long)")
        safe_show_df(norm.head(2000), label="matriz_andreani_preview")

        st.download_button("Descargar preview (Excel)", data=to_excel_bytes(norm, "matrix_andreani"), file_name="matriz_andreani_normalizada.xlsx")

        st.divider()
        st.write("Guardar versión")
        name = st.text_input("Nombre versión (ej: 'Andreani diciembre')", value=os.path.splitext(up.name)[0])
        status = st.selectbox("Estado", ["DRAFT", "PUBLISHED"], index=1)
        c1, c2 = st.columns(2)
        with c1:
            valid_from = st.date_input("Vigente desde", value=today())
        with c2:
            valid_to = st.date_input("Vigente hasta (opcional)", value=None)

        confirm = st.checkbox("Confirmo guardar esta versión en disco.")
        if st.button("Guardar versión", disabled=not confirm):
            # persistir pkl
            file_path = os.path.join(MATRIX_DIR, f"andreani__{re.sub(r'[^a-zA-Z0-9_-]+','_',name).lower()}.pkl")
            backup_file(file_path, f"matrix_andreani_{name}")
            save_pickle(file_path, norm)

            upsert_matrix_version(
                name=name.strip(),
                kind="andreani",
                status=status,
                valid_from=valid_from.strftime("%Y-%m-%d") if valid_from else None,
                valid_to=valid_to.strftime("%Y-%m-%d") if valid_to else None,
                file_path=file_path,
                meta=meta,
            )
            audit_log("matrix_andreani_save", {"name": name, "status": status, "file": up.name})
            st.success("Listo: matriz guardada y registrada.")

# =========================
# Auditor Facturas (PRO: matriz por guía + outputs por paso)
# =========================
if page == "Auditor Facturas":
    st.subheader("Auditor de facturas (PDF) — Andreani (PRO por guía)")

    cp_master = get_cp_master()
    catalog = get_catalog()
    sales = get_sales()

    missing = []
    if cp_master is None:
        missing.append("CP Master")
    if catalog is None:
        missing.append("Catálogo")
    if sales is None:
        missing.append("Ventas")
    if missing:
        st.error(f"Faltan datasets requeridos: {', '.join(missing)}. Cargalos primero.")
        st.stop()

    # Fallback manual (si falta fecha_envio / fecha_factura)
    ref_date = st.date_input("Fecha de referencia (fallback si falta fecha_envío)", value=today())
    st.caption("Tolerancias: evita falsos positivos por redondeos/ajustes. Elegí % **o** $ (uno solo).")

    t1, t2, t3, t4 = st.columns([1, 1, 1, 1])

    # DISD
    with t1:
        disd_mode = st.radio("DISD tolerancia", ["$", "%"], horizontal=True, index=0, key="disd_mode")
    with t2:
        if disd_mode == "$":
            disd_abs_tol = st.number_input("DISD ±$ (tolerancia)", min_value=0.0, value=50.0, step=10.0, key="disd_abs")
            disd_pct_tol = 0.0
        else:
            disd_pct = st.number_input("DISD ±% (tolerancia)", min_value=0.0, value=0.5, step=0.1, key="disd_pct")
            disd_pct_tol = float(disd_pct) / 100.0
            disd_abs_tol = 0.0

    # SGD / Seguro declarado
    with t3:
        sgd_mode = st.radio("SGD tolerancia", ["$", "%"], horizontal=True, index=0, key="sgd_mode")
    with t4:
        if sgd_mode == "$":
            sgd_abs_tol = st.number_input("SGD ±$ (tolerancia)", min_value=0.0, value=50.0, step=10.0, key="sgd_abs")
            sgd_pct_tol = 0.0
        else:
            sgd_pct = st.number_input("SGD ±% (tolerancia)", min_value=0.0, value=0.5, step=0.1, key="sgd_pct")
            sgd_pct_tol = float(sgd_pct) / 100.0
            sgd_abs_tol = 0.0


    pdfs = st.file_uploader("Subí factura(s) PDF", type=["pdf"], accept_multiple_files=True)
    if not pdfs:
        st.info("Subí al menos un PDF para auditar.")
        st.stop()

    # Índices
    cp_idx = cp_master.set_index("CP_int", drop=False)
    catalog_idx = catalog.drop_duplicates(subset=["sku"], keep="last").set_index("sku", drop=False)

    sales_norm = sales.copy()
    sales_norm["guia"] = sales_norm["guia"].apply(normalize_guia)

    # --- Cache de matrices en memoria para performance ---
    matrix_cache: Dict[str, pd.DataFrame] = {}

    def _get_matrix_for_date(base_date: dt.date) -> Tuple[Optional[Dict[str, Any]], Optional[pd.DataFrame]]:
        """Devuelve (mrec, matrix_long) para marketplace=andreani según base_date (por guía)."""
        mrec = pick_active_matrix("andreani", base_date)
        if not mrec:
            return None, None
        fp = mrec.get("file_path")
        if not fp:
            return mrec, None
        if fp not in matrix_cache:
            matrix_cache[fp] = load_pickle(fp)
        return mrec, matrix_cache[fp]

    def _map_cp(cp_int: Any) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """
        Devuelve:
        provincia, localidad, sub_region, region_key
        - region_key se calcula SOLO desde sub_region (tu estándar)
        """
        if cp_int is None or (isinstance(cp_int, float) and math.isnan(cp_int)):
            return None, None, None, None
        try:
            cp_int = int(cp_int)
        except Exception:
            return None, None, None, None

        if cp_int not in cp_idx.index:
            return None, None, None, None

        r = cp_idx.loc[cp_int]
        prov = r.get("Provincia")
        loc = r.get("Localidad")

        sub = r.get("sub_region")
        rk = normalize_region_key(sub) if sub else None

        return prov, loc, sub, rk


    # --- Helpers de peso esperado desde catálogo (sku*qty) ---
    def _line_weight(row) -> float:
        sku = str(row.get("sku", "")).strip()
        qty = float(row.get("qty", 1) or 1)
        if sku in catalog_idx.index:
            w = catalog_idx.loc[sku, "peso_aforado_kg"]
            try:
                return float(w) * qty
            except Exception:
                return 0.0
        return 0.0

    results_all: List[pd.DataFrame] = []
    step1_all: List[pd.DataFrame] = []
    step2_all: List[pd.DataFrame] = []
    step3_all: List[pd.DataFrame] = []
    step4_all: List[pd.DataFrame] = []
    step5_all: List[pd.DataFrame] = []

    for up in pdfs:
        # -----------------
        # Paso 1: PDF → envíos
        # -----------------
        ship = parse_pdf_shipments(up.read())
        ship["source_pdf"] = up.name
        step1_all.append(ship.copy())

        if ship.empty:
            st.warning(f"No pude extraer envíos del PDF: {up.name}")
            continue

        # -----------------
        # Paso 2: Ventas → CP + fecha_envio + kg_esperado
        # -----------------
        s = sales_norm.dropna(subset=["guia"]).copy()
        s["line_weight"] = s.apply(_line_weight, axis=1)
        by_guia = s.groupby("guia", as_index=False).agg(
            cp_int=("cp_int", "first"),
            fecha_envio=("fecha_envio", "first"),
            valor_declarado_ars=("valor_declarado_ars", "first"),
            kg_esperado=("line_weight", "sum"),
        )
        step2 = ship.merge(by_guia, on="guia", how="left", indicator=True)
        step2["flag_sin_venta"] = step2["_merge"].ne("both")
        step2 = step2.drop(columns=["_merge"])
        step2_all.append(step2.copy())

        # -----------------
        # Paso 3: CP master → región/subregión (region_key)
        # -----------------
        mapped = step2["cp_int"].apply(_map_cp)
        step3 = step2.copy()
        step3["provincia"] = mapped.apply(lambda x: x[0])
        step3["localidad"] = mapped.apply(lambda x: x[1])

        # Si _map_cp devuelve: (provincia, localidad, sub_region, region_key)
        step3["sub_region"] = mapped.apply(lambda x: x[2])
        step3["region_key"] = mapped.apply(lambda x: x[3])

        step3["flag_cp_no_encontrado"] = step3["cp_int"].notna() & step3["provincia"].isna()
        step3["flag_sin_region"] = step3["region_key"].isna()
        step3_all.append(step3.copy())

        # -----------------
        # Paso 4: seleccionar matriz por guía según fecha_envio (fallback)
        # -----------------
        def _pick_base_date(row) -> Tuple[dt.date, str]:
            fe = row.get("fecha_envio")
            ff = row.get("fecha_factura")
            if isinstance(fe, dt.date) and not pd.isna(fe):
                return fe, "ventas.fecha_envio"
            if isinstance(ff, dt.date) and not pd.isna(ff):
                return ff, "pdf.fecha_factura"
            return ref_date, "manual.ref_date"

        base = step3.apply(_pick_base_date, axis=1)
        step4 = step3.copy()
        step4["fecha_base"] = base.apply(lambda x: x[0])
        step4["fecha_base_source"] = base.apply(lambda x: x[1])

        # matriz por fila (guardamos name/path)
        def _matrix_meta(row) -> Tuple[Optional[str], Optional[str]]:
            mrec, _ = _get_matrix_for_date(row["fecha_base"])
            if not mrec:
                return None, None
            return mrec.get("name"), mrec.get("file_path")

        mm = step4.apply(_matrix_meta, axis=1)
        step4["matrix_name_usada"] = mm.apply(lambda x: x[0])
        step4["matrix_path_usada"] = mm.apply(lambda x: x[1])
        step4["flag_sin_matriz"] = step4["matrix_name_usada"].isna()
        step4_all.append(step4.copy())

        # -----------------
        # Paso 5: tarifa esperada con (region_key + kg_factura) en matriz RAW seleccionada
        # -----------------
        def _expected_from_matrix(row) -> Tuple[Optional[float], Optional[float], str, str]:
            if row.get("flag_sin_matriz"):
                return None, None, "SIN MATRIZ (no hay PUBLISHED vigente)", "SGD_NO_APLICA"
            rk = row.get("region_key")
            if not rk:
                return None, None, "SIN REGIÓN (CP no mapea)", "SGD_NO_APLICA"

            kg_fact = row.get("kg_factura")
            if kg_fact is None or (isinstance(kg_fact, float) and math.isnan(kg_fact)) or kg_fact <= 0:
                return None, None, "SIN PESO FACTURA", "SGD_NO_APLICA"

            mrec, mdf = _get_matrix_for_date(row["fecha_base"])
            if mdf is None:
                return None, None, "SIN MATRIZ (archivo no cargó)", "SGD_NO_APLICA"

            disd_val, sgd_val, exc, status, sgd_status = tariff_lookup(
                mdf,
                region_key=str(rk),
                kg=float(kg_fact),
                valor_declarado_ars=row.get("valor_declarado_ars"),
            )

            # DISD (principal)
            if disd_val is None or (isinstance(disd_val, float) and math.isnan(disd_val)):
                if status == "MATRIZ_SIN_REGION_KEY":
                    return None, sgd_val, "SIN TARIFA (matriz sin region_key)", sgd_status
                if status == "SIN_REGION_EN_MATRIZ":
                    return None, sgd_val, "SIN TARIFA (región no existe en matriz)", sgd_status
                if status == "SIN_BANDA_SIN_EXC":
                    return None, sgd_val, "SIN TARIFA (kg excede y no hay Exc)", sgd_status
                return None, sgd_val, "SIN TARIFA (no matchea banda)", sgd_status

            if status == "OK_EXCEDENTE":
                return float(disd_val), sgd_val, "OK (EXCEDENTE)", sgd_status

            # OK / OK_UNDER_MIN / etc
            return float(disd_val), sgd_val, "OK", sgd_status

        exp = step4.apply(_expected_from_matrix, axis=1)
        step5 = step4.copy()
        step5["disd_esperado"] = exp.apply(lambda x: x[0])
        step5["sgd_esperado"] = exp.apply(lambda x: x[1])
        step5["tarifa_status"] = exp.apply(lambda x: x[2])
        step5["sgd_status"] = exp.apply(lambda x: x[3])
        step5_all.append(step5.copy())

        # -----------------
        # Paso 6: comparar pesos (kg_factura vs kg_esperado catálogo)
        # -----------------
        out = step5.copy()

        out["delta_kg"] = out["kg_factura"] - out["kg_esperado"]
        out["delta_disd"] = out["disd_factura"] - out["disd_esperado"]
        out["delta_sgd"] = out["sgd_factura"] - out["sgd_esperado"]
        # Tolerancias: bandas permitidas + flags OK
        out["disd_band"] = out["disd_esperado"].apply(lambda e: tol_band(e, abs_tol=disd_abs_tol, pct_tol=disd_pct_tol))
        out["sgd_band"] = out["sgd_esperado"].apply(lambda e: tol_band(e, abs_tol=sgd_abs_tol, pct_tol=sgd_pct_tol))
        out["disd_ok"] = out.apply(lambda r: within_tol(r.get("disd_factura"), r.get("disd_esperado"), abs_tol=disd_abs_tol, pct_tol=disd_pct_tol), axis=1)
        out["sgd_ok"] = out.apply(lambda r: within_tol(r.get("sgd_factura"), r.get("sgd_esperado"), abs_tol=sgd_abs_tol, pct_tol=sgd_pct_tol), axis=1)

        # Estado final: prioriza fallas de data / matching + aplica tolerancia
        def _final_state(r) -> str:
            if r.get("flag_sin_venta"):
                return "SIN VENTA"
            if r.get("flag_cp_no_encontrado"):
                return "CP NO ENCONTRADO"
            if r.get("flag_sin_region"):
                return "SIN REGIÓN"
            if r.get("flag_sin_matriz"):
                return "SIN MATRIZ"

            disd_exp = r.get("disd_esperado")
            if disd_exp is None or (isinstance(disd_exp, float) and math.isnan(disd_exp)):
                return r.get("tarifa_status") or "SIN TARIFA"

            disd_ok = within_tol(r.get("disd_factura"), disd_exp, abs_tol=disd_abs_tol, pct_tol=disd_pct_tol)
            sgd_ok = within_tol(r.get("sgd_factura"), r.get("sgd_esperado"), abs_tol=sgd_abs_tol, pct_tol=sgd_pct_tol)

            if disd_ok and sgd_ok:
                return "OK (tolerancia)"
            if (not disd_ok) and sgd_ok:
                return "DIF TARIFA"
            if disd_ok and (not sgd_ok):
                return "DIF SEGURO"
            return "DIF TARIFA+SEGURO"

        out["estado_final"] = out.apply(_final_state, axis=1)

        # columnas finales
        cols = [
            "estado_final",
            "tarifa_status",
            "guia",
            "source_pdf",
            "fecha_factura",
            "fecha_envio",
            "valor_declarado_ars",
            "fecha_base",
            "fecha_base_source",
            "matrix_name_usada",
            "cp_int",
            "provincia",
            "localidad",
            "region_key",
            "bultos_factura",
            "kg_factura",
            "kg_esperado",
            "delta_kg",
            "disd_factura",
            "disd_esperado",
            "delta_disd",
            "sgd_factura",
            "sgd_esperado",
            "delta_sgd",
            "disd_ok",
            "disd_band",
            "sgd_ok",
            "sgd_band",
            "sgd_status",
            "flag_sin_venta",
            "flag_cp_no_encontrado",
            "flag_sin_region",
            "flag_sin_matriz",
        ]
        out = out[cols].rename(columns={"cp_int": "cp"})
        results_all.append(out)

    if not results_all:
        st.error("No se generaron resultados (no pude parsear ningún PDF).")
        st.stop()

    # ==================
    # OUTPUTS por paso
    # ==================
    st.divider()
    st.subheader("Resultados por paso (debug transparente)")

    with st.expander("Paso 1 — PDF parseado (envíos encontrados)", expanded=False):
        df1 = pd.concat(step1_all, ignore_index=True) if step1_all else pd.DataFrame()
        safe_show_df(df1, label="paso1_pdf_parse")

    with st.expander("Paso 2 — Cruce con Ventas (CP, fecha_envío, kg_esperado)", expanded=False):
        df2 = pd.concat(step2_all, ignore_index=True) if step2_all else pd.DataFrame()
        safe_show_df(df2, label="paso2_ventas_merge")

    with st.expander("Paso 3 — CP Master (provincia/localidad/region_key)", expanded=False):
        df3 = pd.concat(step3_all, ignore_index=True) if step3_all else pd.DataFrame()
        safe_show_df(df3, label="paso3_cp_master")

    with st.expander("Paso 4 — Selección de matriz por guía (según fecha)", expanded=False):
        df4 = pd.concat(step4_all, ignore_index=True) if step4_all else pd.DataFrame()
        safe_show_df(df4[["guia","source_pdf","fecha_envio","fecha_factura","fecha_base","fecha_base_source","matrix_name_usada","flag_sin_matriz"]], label="paso4_matriz_por_guia")

        # resumen matrices usadas
        if not df4.empty:
            summary = df4.groupby(["matrix_name_usada"], dropna=False).size().reset_index(name="envios")
            st.write("Resumen de matrices usadas")
            safe_show_df(summary, label="resumen_matrices_usadas")

    with st.expander("Paso 5 — Tarifa esperada (region_key + kg_factura → matriz RAW)", expanded=False):
        df5 = pd.concat(step5_all, ignore_index=True) if step5_all else pd.DataFrame()
        safe_show_df(df5[["guia","region_key","kg_factura","valor_declarado_ars","matrix_name_usada","disd_esperado","tarifa_status","sgd_esperado","sgd_status"]], label="paso5_tarifa")

    # ==================
    # Resultado final
    # ==================
    res = pd.concat(results_all, ignore_index=True)

    st.divider()
    st.subheader("Resultado final (auditoría)")

    safe_show_df(res, label="auditoria_result_final")

    st.download_button(
        "Descargar auditoría (Excel)",
        data=to_excel_bytes(res, "auditoria"),
        file_name=f"auditoria_andreani_{today().isoformat()}.xlsx",
    )
# =========================
# Audit trail
# =========================
if page == "Audit Trail":
    st.subheader("Audit trail (últimos eventos)")
    if not os.path.exists(AUDIT_LOG_PATH):
        st.info("Aún no hay audit log.")
    else:
        with open(AUDIT_LOG_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()[-300:]
        rows = [json.loads(x) for x in lines if x.strip()]
        df = pd.json_normalize(rows).sort_values("ts", ascending=False) if rows else pd.DataFrame()
        safe_show_df(df, label="audit_trail")