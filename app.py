# -*- coding: utf-8 -*-
"""
Andreani | App (v1.41)
--------------------
✅ v1.41: UI de auditoría más legible (deltas + tramos + filtros), menos ruido en pantalla

- Cada import (CP Master / Catálogo / Ventas / Matriz) se trabaja como:
  1) Subís archivo -> Preview + Validación + Sanity-check
  2) Se genera un "Plan de cambios" (qué se va a guardar y cómo impacta)
  3) Podés:
     - Simular (no guarda)
     - Aplicar (guarda, con backup)
     - Exportar el preview normalizado a Excel/CSV

- Visualización:
  - Se ve el dataset actual completo (paginado) + métricas
  - Se puede descargar el dataset actual
  - Se puede descargar el preview normalizado (lo que se va a guardar)

Persistencia local en ./data (igual que antes).
"""

from __future__ import annotations


# =========================
# Reglas de región Andreani
# =========================
# Patagonia vs Interior (según tu criterio operativo)
PATAGONIA_PROVS_FALLBACK = {
    "LA PAMPA",
    "NEUQUEN",
    "RIO NEGRO",
    "CHUBUT",
    "SANTA CRUZ",
    "TIERRA DEL FUEGO",
    "TIERRA DEL FUEGO, ANTARTIDA E ISLAS DEL ATLANTICO SUR",
}

# Capital de provincia => banda I | Interior => banda II.
# Match por "contiene" contra Localidad (CP master). Ajustable si Andreani redefine.
CAPITAL_TOKENS_BY_PROV_FALLBACK = {
    "BUENOS AIRES": ["LA PLATA"],
    "CATAMARCA": ["SAN FERNANDO DEL VALLE DE CATAMARCA"],
    "CHACO": ["RESISTENCIA"],
    "CHUBUT": ["RAWSON"],
    "CORDOBA": ["CORDOBA"],
    "CORRIENTES": ["CORRIENTES"],
    "ENTRE RIOS": ["PARANA"],
    "FORMOSA": ["FORMOSA"],
    "JUJUY": ["SAN SALVADOR DE JUJUY"],
    "LA PAMPA": ["SANTA ROSA"],
    "LA RIOJA": ["LA RIOJA"],
    "MENDOZA": ["MENDOZA"],
    "MISIONES": ["POSADAS"],
    "NEUQUEN": ["NEUQUEN"],
    "RIO NEGRO": ["VIEDMA"],
    "SALTA": ["SALTA"],
    "SAN JUAN": ["SAN JUAN"],
    "SAN LUIS": ["SAN LUIS"],
    "SANTA CRUZ": ["RIO GALLEGOS"],
    "SANTA FE": ["SANTA FE"],
    "SANTIAGO DEL ESTERO": ["SANTIAGO DEL ESTERO"],
    "TIERRA DEL FUEGO": ["USHUAIA"],
    "TUCUMAN": ["SAN MIGUEL DE TUCUMAN"],
    # CABA (Capital Federal): siempre "capital"
    "CAPITAL FEDERAL": ["CABA"],
}

import io, os, json, math, re, shutil, unicodedata
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
from numbers import Integral
from decimal import Decimal, InvalidOperation

import pandas as pd
import streamlit as st
def safe_show_df(df: pd.DataFrame, *, label: str = "", max_rows: int = 2000, use_container_width: bool = True):
    """
    Render defensivo para evitar crashes del front (React error #185) por dtypes mixtos/raros.
    - Normaliza fechas a string ISO.
    - Convierte object a string.
    - Limita filas (descarga CSV para ver todo).
    """
    if df is None:
        st.info("No hay datos para mostrar.")
        return

    try:
        view = df.copy()

        # Reset index para evitar indices raros (MultiIndex / duplicados)
        try:
            view = view.reset_index(drop=True)
        except Exception:
            pass

        # Normalización por tipo
        for c in list(view.columns):
            s = view[c]
            # datetimes
            if pd.api.types.is_datetime64_any_dtype(s):
                view[c] = pd.to_datetime(s, errors="coerce").dt.strftime("%Y-%m-%d")
                continue
            # timedeltas/periods
            if pd.api.types.is_timedelta64_dtype(s) or pd.api.types.is_period_dtype(s):
                view[c] = s.astype(str)
                continue
            # objects: a string, pero preservando NaN como vacío
            if pd.api.types.is_object_dtype(s):
                view[c] = s.fillna("").astype(str)
                continue

        # Limite para UI
        truncated = False
        if len(view) > max_rows:
            truncated = True
            view_small = view.head(max_rows).copy()
        else:
            view_small = view

        st.dataframe(view_small, use_container_width=use_container_width)

        if truncated:
            st.warning(f"Mostrando solo las primeras {max_rows} filas para estabilidad. Podés descargar el CSV completo abajo.")
        # Descarga CSV (siempre)
        try:
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                f"Descargar CSV ({label or 'tabla'})",
                data=csv,
                file_name=f"{(label or 'tabla').replace(' ', '_').lower()}.csv",
                mime="text/csv",
            )
        except Exception:
            pass

    except Exception as e:
        st.error(f"No pude renderizar la tabla en pantalla ({label}). Error: {e}")
        # Como fallback, mostrar texto
        try:
            st.text(str(df.head(50)))
        except Exception:
            pass

import pdfplumber
import yaml

DATA_DIR = "data"
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
REGISTRY_PATH = os.path.join(DATA_DIR, "matrices_registry.json")
AUDIT_LOG_PATH = os.path.join(DATA_DIR, "audit_log.jsonl")
CATALOG_PATH = os.path.join(DATA_DIR, "catalog.pkl")
SALES_PATH = os.path.join(DATA_DIR, "sales.pkl")
CP_MASTER_PATH = os.path.join(DATA_DIR, "cp_master.pkl")
FREE_SHIP_PATH = os.path.join(DATA_DIR, "free_shipping_cps.json")

APP_DIR = os.path.dirname(os.path.abspath(__file__))
TPL_CONFIG = os.path.join(APP_DIR, "config.yaml")
TPL_CP = os.path.join(APP_DIR, "template_cp_master.xlsx")
TPL_CATALOG = os.path.join(APP_DIR, "template_catalogo.xlsx")
TPL_SALES = os.path.join(APP_DIR, "template_ventas.xlsx")
TPL_ME1 = os.path.join(APP_DIR, "template_matriz_me1.xlsx")
TPL_BNA = os.path.join(APP_DIR, "template_matriz_bna.xlsx")
TPL_AND = os.path.join(APP_DIR, "template_matriz_andreani.xlsx")
TPL_FREE = os.path.join(APP_DIR, "template_free_shipping_cps.xlsx")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

# -----------------------------
# Utilidades base
# -----------------------------
def today() -> dt.date:
    return dt.date.today()

def iso_now() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")

def file_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def norm_text(s: Any) -> str:
    """Normaliza textos (provincias/localidades) para comparaciones robustas.

    - lower + strip
    - quita tildes/diacríticos
    - reemplaza puntuación por espacios
    - colapsa espacios
    """
    if s is None:
        return ""
    s = str(s).strip().lower()
    # Remover tildes/diacríticos: "San Martín" -> "san martin"
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    # Puntuación fuera (ej: "C.A.B.A." -> "c a b a")
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def cp_to_int(cp: Any) -> Optional[int]:
    """Convierte CP a entero (tolerante a formatos típicos de Argentina).

    Soporta:
    - int / numpy.int64 / etc.
    - float de Excel (8340.0) sin transformarlo en 83400
    - strings: '1071', '1.071', 'A4400', 'U8340'
    """
    if cp is None:
        return None
    if isinstance(cp, float) and math.isnan(cp):
        return None

    # Números enteros (incluye numpy/pandas ints vía Integral)
    if isinstance(cp, Integral):
        return int(cp)

    # Floats típicos de Excel
    if isinstance(cp, float):
        if float(cp).is_integer():
            return int(cp)

    s = str(cp).strip().upper()

    # Caso clásico: '8340.0' / '1071.00'
    if re.fullmatch(r"\d+\.0+", s):
        return int(s.split(".")[0])

    digits = re.findall(r"\d+", s)
    if not digits:
        return None

    # Si venía como '8340.0' y por algún motivo no matcheó arriba, evitamos '83400'
    if "." in s and len(digits) > 1 and all(set(d) == {"0"} for d in digits[1:]):
        return int(digits[0])

    return int("".join(digits))

def parse_cp_to_int(cp: Any) -> Optional[int]:
    # Backwards-compat: versiones anteriores usaban este nombre
    return cp_to_int(cp)


def ar_money_to_float(x: Any) -> Optional[float]:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    s = str(x).strip()
    if not s:
        return None
    s = re.sub(r"[^0-9\.,\-]", "", s)
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

def apply_rounding(config: Dict[str, Any], v: Optional[float]) -> Optional[float]:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    rnd = (config.get("rounding") or {})
    if not rnd.get("enabled", True):
        return float(v)
    decimals = int(rnd.get("decimals", 0))
    mode = str(rnd.get("mode", "round")).lower()
    factor = 10 ** decimals
    if mode == "ceil":
        return math.ceil(float(v) * factor) / factor
    return round(float(v), decimals)


# =========================
# PDF parsing helpers (v1.41)
# =========================
RE_GUIA_LINE = re.compile(r"(?:nro\.?\s*de\s*env[ií]o)\s*:?\s*(\d{10,})", re.IGNORECASE)
RE_SERVICIO_FECHA = re.compile(r"Servicio de transporte .*?(\d{2}[\./]\d{2}[\./]\d{4})", re.IGNORECASE)
RE_ANY_FECHA = re.compile(r"(\d{2}[\./]\d{2}[\./]\d{4})")

def _parse_ddmmyyyy(s: str):
    """Parsea DD.MM.YYYY o DD/MM/YYYY -> date. Devuelve None si falla."""
    try:
        s = s.strip()
        s = s.replace("/", ".")
        return dt.datetime.strptime(s, "%d.%m.%Y").date()
    except Exception:
        return None

def pick_fecha_around(lines, i, window=8):
    """
    Busca una fecha cercana a la línea de la guía.
    Prioriza fechas en líneas 'Servicio de transporte ... <fecha>'.
    """
    lo = max(0, i - window)
    hi = min(len(lines), i + window + 1)

    candidates = []
    for j in range(lo, hi):
        m = RE_SERVICIO_FECHA.search(lines[j])
        if m:
            d = _parse_ddmmyyyy(m.group(1))
            if d:
                candidates.append((abs(j - i), d))
    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]

    candidates = []
    for j in range(lo, hi):
        m = RE_ANY_FECHA.search(lines[j])
        if m:
            d = _parse_ddmmyyyy(m.group(1))
            if d:
                candidates.append((abs(j - i), d))
    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]
    return None

def normalize_guia(val: Any) -> Optional[str]:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    if isinstance(val, (int,)) and not isinstance(val, bool):
        return str(int(val))
    if isinstance(val, (float,)):
        if math.isfinite(val) and abs(val - round(val)) < 1e-6:
            return str(int(round(val)))
        s = format(val, ".0f") if abs(val) > 1e12 else str(val)
        return s.strip()
    s = str(val).strip()
    if not s:
        return None
    try:
        if re.search(r"[eE]\+?\d+", s):
            d = Decimal(s)
            return format(d.quantize(Decimal("1")), "f")
    except (InvalidOperation, ValueError):
        pass
    if re.match(r"^\d+\.0+$", s):
        s = s.split(".")[0]
    return s

def to_excel_bytes(df: pd.DataFrame, sheet: str="data") -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name=sheet[:31])
    return out.getvalue()

# -----------------------------
# Backup + borrar + restore
# -----------------------------
def backup_file(path: str, label: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    ext = os.path.splitext(path)[1]
    bpath = os.path.join(BACKUP_DIR, f"{label}_{ts}{ext}")
    shutil.copy2(path, bpath)
    return bpath

def last_backup_for(label: str) -> Optional[str]:
    cand = [os.path.join(BACKUP_DIR, f) for f in os.listdir(BACKUP_DIR) if f.startswith(label + "_")]
    if not cand:
        return None
    cand.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return cand[0]

def delete_file(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)

def restore_backup(label: str, target_path: str) -> bool:
    b = last_backup_for(label)
    if not b:
        return False
    shutil.copy2(b, target_path)
    return True

# -----------------------------
# Config base
# -----------------------------
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
        return yaml.safe_load(DEFAULT_CONFIG_YAML)
    raw = uploaded.read().decode("utf-8")
    return yaml.safe_load(raw)

# -----------------------------
# Audit trail
# -----------------------------
def audit_log(action: str, actor: str, payload: Dict[str, Any]) -> None:
    rec = {"ts": iso_now(), "action": action, "actor": actor, "payload": payload}
    with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def read_audit_tail(n: int = 300) -> pd.DataFrame:
    if not os.path.exists(AUDIT_LOG_PATH):
        return pd.DataFrame()
    with open(AUDIT_LOG_PATH, "r", encoding="utf-8") as f:
        lines = f.readlines()[-n:]
    rows = [json.loads(x) for x in lines if x.strip()]
    return pd.json_normalize(rows).sort_values("ts", ascending=False) if rows else pd.DataFrame()

# -----------------------------
# Persistencia datasets
# -----------------------------
def save_pickle(path: str, obj: Any) -> None:
    pd.to_pickle(obj, path)

def load_pickle(path: str) -> Any:
    return pd.read_pickle(path)

def get_cp_master() -> Optional[pd.DataFrame]:
    return load_pickle(CP_MASTER_PATH) if os.path.exists(CP_MASTER_PATH) else None

def set_cp_master(df: pd.DataFrame) -> None:
    save_pickle(CP_MASTER_PATH, df)

def get_catalog() -> Optional[pd.DataFrame]:
    return load_pickle(CATALOG_PATH) if os.path.exists(CATALOG_PATH) else None

def set_catalog(df: pd.DataFrame) -> None:
    save_pickle(CATALOG_PATH, df)

def get_sales() -> Optional[pd.DataFrame]:
    return load_pickle(SALES_PATH) if os.path.exists(SALES_PATH) else None

def set_sales(df: pd.DataFrame) -> None:
    save_pickle(SALES_PATH, df)

def default_free_ship() -> Dict[str, List[str]]:
    return {
        "Capital": ["A4193","A4400","A4401","A4402","A4404","A4406","A4408","A4410","A4412","A4414"],
        "Cerrillos": ["A4126","A4400","A4401","A4403","A4421"]
    }

def get_free_ship() -> Dict[str, List[str]]:
    if os.path.exists(FREE_SHIP_PATH):
        try:
            with open(FREE_SHIP_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default_free_ship()
    return default_free_ship()

def save_free_ship(d: Dict[str, List[str]]) -> None:
    with open(FREE_SHIP_PATH, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)

def memory_status() -> pd.DataFrame:
    rows = []
    for label, path in [
        ("CP master", CP_MASTER_PATH),
        ("Catálogo", CATALOG_PATH),
        ("Ventas", SALES_PATH),
        ("Excepciones envío gratis", FREE_SHIP_PATH),
        ("Registry matrices", REGISTRY_PATH),
        ("Audit trail", AUDIT_LOG_PATH),
        ("Backups", BACKUP_DIR),
    ]:
        exists = os.path.exists(path)
        size = 0.0
        if exists and os.path.isfile(path):
            size = round(os.path.getsize(path)/1024, 1)
        rows.append({
            "Recurso": label,
            "Ruta": path,
            "Existe": exists,
            "Tamaño (KB)": size,
            "Modificado": dt.datetime.fromtimestamp(os.path.getmtime(path)).isoformat(timespec="seconds") if exists else "",
        })
    return pd.DataFrame(rows)

# -----------------------------
# Import helpers
# -----------------------------
def read_excel_safe(uploaded, converters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    return pd.read_excel(uploaded, engine="openpyxl", converters=converters)

def normalize_catalog(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    warnings = []
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    required = {"sku","producto","peso_aforado_kg"}
    missing = sorted(list(required - set(df.columns)))
    if missing:
        raise ValueError(f"Faltan columnas: {missing}. Requeridas: {sorted(required)}")
    df["sku"] = df["sku"].astype(str).str.strip()
    df["producto"] = df["producto"].astype(str).str.strip()
    df["peso_aforado_kg"] = pd.to_numeric(df["peso_aforado_kg"], errors="coerce")
    if df["peso_aforado_kg"].isna().any():
        raise ValueError("Hay filas con peso_aforado_kg inválido/vacío.")
    if (df["peso_aforado_kg"] <= 0).any():
        warnings.append("Hay SKUs con peso_aforado_kg <= 0 (revisar).")
    if df["sku"].duplicated().any():
        warnings.append("Hay SKUs repetidos en el catálogo (se toma la última ocurrencia al auditar).")
    return df.reset_index(drop=True), warnings

def normalize_sales(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Normaliza el import de Ventas.
    - fecha_envio es OPCIONAL: si falta o es inválida, queda vacía (NaT) y se emite un aviso.
    - El auditor prioriza fecha_envio (si existe) y, si no, usa la fecha del PDF por guía.
    """
    warnings: List[str] = []
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    required = {"guia","cp","sku","qty"}
    missing = sorted(list(required - set(df.columns)))
    if missing:
        raise ValueError(f"Faltan columnas: {missing}. Requeridas: {sorted(required)}")

    df["guia"] = df["guia"].apply(normalize_guia)
    if df["guia"].isna().any():
        raise ValueError("Hay filas con guia vacía/ilegible.")
    if df["guia"].astype(str).str.contains(r"[eE]\+").any():
        raise ValueError("Hay guías en notación científica. Exportá guías como TEXTO o usá la plantilla.")

    df["sku"] = df["sku"].astype(str).str.strip()
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(1).astype(float)

    # fecha_envio (opcional)
    if "fecha_envio" in df.columns:
        raw_missing = df["fecha_envio"].isna().sum()
        df["fecha_envio"] = pd.to_datetime(df["fecha_envio"], errors="coerce").dt.date
        bad = df["fecha_envio"].isna().sum()
        if bad > 0 and (len(df) > 0):
            warnings.append("Aviso: hay filas con fecha_envio inválida o vacía. Se seguirá igual y la auditoría usará la fecha del PDF para esas guías.")
        # Limpieza: fechas muy viejas (ej. 1970/1980) las anulamos.
        try:
            too_old = df["fecha_envio"].apply(lambda d: (d is not None) and (not pd.isna(d)) and (d < dt.date(2000,1,1)))
            if too_old.any():
                df.loc[too_old, "fecha_envio"] = pd.NaT
                warnings.append("Aviso: se detectaron fechas muy viejas en fecha_envio y se anularon (NaT).")
        except Exception:
            pass
    else:
        df["fecha_envio"] = pd.NaT
        warnings.append("Aviso: no se cargó fecha_envio en ventas (opcional). La auditoría usará la fecha del PDF.")

    df["cp"] = df["cp"].astype(str).str.strip()
    df["cp_int"] = df["cp"].apply(parse_cp_to_int)
    if df["cp_int"].isna().any():
        raise ValueError("Hay filas con CP inválido (ej: A4400, 4400, etc.).")

    # dedupe: si te pasan varias líneas por guía/sku, lo dejamos tal cual (la auditoría agrupa después)
    return df, warnings



def normalize_cp_master(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    warnings=[]
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    required = {"CP", "Provincia", "Localidad"}
    missing = sorted(list(required - set(df.columns)))
    if missing:
        raise ValueError(f"Faltan columnas: {missing}. Requeridas: {sorted(required)}")
    df["CP_int"] = df["CP"].apply(parse_cp_to_int)
    df = df.dropna(subset=["CP_int"]).copy()
    df["CP_int"] = df["CP_int"].astype(int)
    if df["Provincia"].isna().any() or df["Localidad"].isna().any():
        raise ValueError("Hay filas con Provincia/Localidad vacías.")
    if df["CP_int"].duplicated().any():
        warnings.append("Hay CP repetidos en CP Master (se toma el primero).")
    return df.reset_index(drop=True), warnings

def sales_sanity(df: pd.DataFrame) -> Dict[str, Any]:
    return {
        "filas": int(len(df)),
        "guias_unicas": int(df["guia"].nunique()),
        "fecha_min": str(min(df["fecha_envio"])) if len(df) else "",
        "fecha_max": str(max(df["fecha_envio"])) if len(df) else "",
        "ejemplo_guias": df["guia"].head(8).tolist(),
        "ejemplo_cp": df["cp"].head(8).tolist(),
        "ejemplo_skus": df["sku"].head(8).tolist(),
    }

# -----------------------------
# Matrices (versionado básico, sin cambios grandes respecto v1.35)
# -----------------------------
def parse_weight_band(col: str) -> Optional[Tuple[float,float]]:
    m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", str(col))
    if not m:
        return None
    return float(m.group(1)), float(m.group(2))

SCHEMA_ML = {
    "CP Inicio": "cp_from",
    "CP Fin": "cp_to",
    "Peso Mínimo (kg)": "w_min",
    "Peso Máximo (kg)": "w_max",
    "Valor Flete Peso ($)": "base_cost",
    "Valor p/ kg excedente ($)": "extra_per_kg",
}
SCHEMA_BNA = {
    "zip_from": "cp_from",
    "zip_to": "cp_to",
    "weight_min": "w_min",
    "weight_max": "w_max",
    "price": "base_cost",
}

def normalize_tariffs_any(df: pd.DataFrame) -> Tuple[str, pd.DataFrame, Dict[str,Any]]:
    cols = [c.strip() for c in df.columns]
    df = df.copy(); df.columns = cols

    if "Region ME1" in df.columns and any(re.match(r"^\s*\d+\s*-\s*\d+\s*$", c) for c in df.columns):
        band_cols = [c for c in df.columns if parse_weight_band(c)]
        if "Exc" not in df.columns:
            raise ValueError("Formato Andreani: falta columna 'Exc'.")
        out_rows = []
        for region, g in df.groupby("Region ME1", sort=False):
            g = g.reset_index(drop=True)
            for i, row in g.iterrows():
                tier_id = i + 1
                exc = pd.to_numeric(row.get("Exc"), errors="coerce")
                for bc in band_cols:
                    w1, w2 = parse_weight_band(bc)
                    cost = pd.to_numeric(row.get(bc), errors="coerce")
                    if pd.isna(cost):
                        continue
                    out_rows.append({
                        "region": str(region).strip().upper(),
                        "tier_id": int(tier_id),
                        "w_from": float(w1),
                        "w_to": float(w2),
                        "cost": float(cost),
                        "exc_per_kg": None if pd.isna(exc) else float(exc),
                    })
        norm = pd.DataFrame(out_rows)
        if norm.empty:
            raise ValueError("No pude normalizar la matriz Andreani (sin datos).")
        return "andreani_region", norm, {"regions": sorted(norm["region"].unique().tolist()), "band_cols": band_cols}

# -----------------------------
# Auditoría de facturas Andreani (PDF)
# -----------------------------
def ar_num(x: str) -> Optional[float]:
    """Convierte números AR (46.241,40) a float."""
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def parse_date_ddmmyyyy_dots(s: str) -> Optional[dt.date]:
    """Parsea fechas en formato DD.MM.YYYY o DD/MM/YYYY y devuelve date.

    Nota: Andreani usa DD.MM.YYYY en el detalle (ej: 03.11.2025).
    """
    m = re.search(r"(\d{2})[\./](\d{2})[\./](\d{4})", str(s))
    if not m:
        return None
    d, mo, y = map(int, m.groups())
    try:
        return dt.date(y, mo, d)
    except Exception:
        return None
def parse_invoice_pdf_bytes(pdf_bytes: bytes) -> pd.DataFrame:
    """
    Parser por texto (pdfplumber) para facturas Andreani.
    Robusto a saltos de página: asocia líneas de servicio a la guía por bloque entre guías.

    Extrae por guía:
      - fecha_factura (fecha de envío del renglón 'Servicio de transporte ... <fecha>')
      - bultos_factura
      - kg_factura
      - disd_factura (Imp. Neto ARS de DISD)
      - sgd_factura (Imp. Neto ARS de SGD)
      - invoice_issue_date (header 'Fecha:')
    """
    try:
        import pdfplumber  # type: ignore
    except Exception as e:
        raise RuntimeError("Falta dependencia pdfplumber. Instalá con: pip install pdfplumber") from e

    pages_text = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for p in pdf.pages:
            pages_text.append(p.extract_text() or "")

    # Armamos líneas preservando cortes de página (útil para debug)
    lines = []
    for pi, txt in enumerate(pages_text):
        for raw in txt.splitlines():
            ln = " ".join(str(raw).replace("\xa0", " ").split()).strip()
            if ln:
                lines.append(ln)
        if pi < len(pages_text)-1:
            lines.append("<<PAGE_BREAK>>")

    # Fecha de emisión de factura (header) — buscar en primeras páginas
    invoice_issue_date = None
    for ln in lines[:250]:
        mh = re.search(r"\bFecha:\s*(\d{2}[\./]\d{2}[\./]\d{4})\b", ln)
        if mh:
            invoice_issue_date = parse_date_ddmmyyyy_dots(mh.group(1).replace("/","."))
            if invoice_issue_date:
                break

    date_token_re = re.compile(r"\b(\d{2}[\./]\d{2}[\./]\d{4})\b")
    money_token_re = re.compile(r"^\d{1,3}(?:\.\d{3})*,\d{2}$")
    guia_re = re.compile(r"(?:Nro\.?\s*de\s*Envío)\s*:?\s*(\d{10,})", re.IGNORECASE)

    def parse_float_ar(tok: str):
        try:
            return float(tok.replace(".","").replace(",", "."))
        except Exception:
            return None

    def parse_service_line(ln: str):
        if "Servicio de transporte" not in ln:
            return None
        if "<<PAGE_BREAK>>" in ln:
            return None
        svc = None
        if " DISD" in (" "+ln):
            svc = "DISD"
        elif " SGD" in (" "+ln):
            svc = "SGD"
        else:
            return None

        md = date_token_re.search(ln)
        d = None
        if md:
            d = parse_date_ddmmyyyy_dots(md.group(1).replace("/","."))

        toks = ln.split()
        date_idx = None
        for i,t in enumerate(toks):
            if date_token_re.fullmatch(t):
                date_idx = i
                break

        bultos = None
        kg = None
        if date_idx is not None:
            if date_idx+1 < len(toks) and toks[date_idx+1].isdigit():
                bultos = int(toks[date_idx+1])
            if date_idx+2 < len(toks):
                kg = parse_float_ar(toks[date_idx+2])
            if kg is None:
                for t in toks[date_idx+1: min(len(toks), date_idx+12)]:
                    if re.search(r"\d+,\d+", t):
                        kg = parse_float_ar(t)
                        if kg is not None:
                            break

        imp = None
        for t in reversed(toks[-10:]):
            if money_token_re.match(t):
                try:
                    imp = ar_num(t)
                except Exception:
                    imp = None
                break

        return {"svc": svc, "fecha": d, "bultos": bultos, "kg": kg, "imp": imp}

    # indexar líneas
    guide_marks = []
    service_marks = []
    for i, ln in enumerate(lines):
        mg = guia_re.search(ln)
        if mg:
            guide_marks.append((i, normalize_guia(mg.group(1))))
        svc = parse_service_line(ln)
        if svc:
            service_marks.append((i, svc))

    if not guide_marks:
        return pd.DataFrame()

    # para lookup rápido: services por índice
    svc_by_i = {i: s for i, s in service_marks}

    rows = []
    for gi, guia in guide_marks:
        prev_gi = -1
        next_gi = len(lines)
        # find previous/next guide indices
        for j in range(len(guide_marks)):
            if guide_marks[j][0] == gi:
                if j > 0:
                    prev_gi = guide_marks[j-1][0]
                if j < len(guide_marks)-1:
                    next_gi = guide_marks[j+1][0]
                break

        # prefer services BEFORE guide (usual case)
        candidate_idx = [i for i in svc_by_i.keys() if prev_gi < i < gi]
        # if none, take AFTER guide until next guide (page-break weirdness)
        if len(candidate_idx) == 0:
            candidate_idx = [i for i in svc_by_i.keys() if gi < i < next_gi]

        # sort in document order
        candidate_idx.sort()

        fecha = None
        bultos = None
        kg = None
        disd = 0.0
        sgd = 0.0
        have_any = False

        for i in candidate_idx:
            s = svc_by_i[i]
            have_any = True
            if s.get("fecha") and not fecha:
                fecha = s["fecha"]
            if s.get("bultos") is not None and bultos is None:
                bultos = s["bultos"]
            if s.get("kg") is not None and kg is None:
                kg = s["kg"]
            if s.get("imp") is not None:
                if s["svc"] == "DISD":
                    disd += float(s["imp"])
                else:
                    sgd += float(s["imp"])

        rows.append({
            "guia": guia,
            "fecha_factura": fecha,
            "bultos_factura": bultos,
            "kg_factura": kg,
            "disd_factura": disd if have_any else None,
            "sgd_factura": sgd if have_any else None,
            "invoice_issue_date": invoice_issue_date,
        })

    df = pd.DataFrame(rows)
    # de-dupe por guía
    df["_score"] = (
        df["fecha_factura"].notna().astype(int) * 10 +
        df["kg_factura"].notna().astype(int) * 5 +
        df["disd_factura"].notna().astype(int) * 3 +
        df["sgd_factura"].notna().astype(int) * 2
    )
    df = df.sort_values(["guia","_score"], ascending=[True,False]).drop_duplicates(subset=["guia"]).drop(columns=["_score"])
    return df




def normalize_cp_master_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deja CP master listo para lookup:
      - Acepta CP con separadores (ej. 1,071) y CPA alfanumérico (A4400)
      - Genera CP_int (entero) para matching rápido
      - Estandariza columnas Provincia / Localidad
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # CP
    cp_col = None
    for c in ["CP", "cp", "codigo_postal", "codigo postal", "postal_code", "postal code"]:
        if c in df.columns:
            cp_col = c
            break
    if cp_col is None:
        return df

    # provincia/localidad
    prov_col = None
    for c in ["Provincia", "provincia", "PROVINCIA"]:
        if c in df.columns:
            prov_col = c
            break
    loc_col = None
    for c in ["Localidad", "localidad", "LOCALIDAD"]:
        if c in df.columns:
            loc_col = c
            break

    df["CP"] = df[cp_col].astype(str).str.strip()
    df["CP_int"] = df["CP"].apply(cp_to_int).astype("Int64")
    if prov_col and prov_col != "Provincia":
        df = df.rename(columns={prov_col: "Provincia"})
    if loc_col and loc_col != "Localidad":
        df = df.rename(columns={loc_col: "Localidad"})

    df = df[df["CP_int"].notna()].copy()
    return df

def region_from_cp(cp: str, cp_master: pd.DataFrame):
    """
    Devuelve (region, provincia, localidad) para un CP.
    Regla operativa:
      - Patagonia vs Interior: por provincia (PATAGONIA_PROVS_FALLBACK)
      - Banda I (capital provincial) vs Banda II (interior): por Localidad vs tokens de capital.
    """
    if cp_master is None or len(cp_master) == 0:
        return (None, None, None)

    try:
        cp_int = cp_to_int(cp)
    except Exception:
        return (None, None, None)

    # cp_master puede venir con columnas en mayúscula/minúscula
    cols = {c.lower(): c for c in cp_master.columns}
    cp_col = cols.get("cp_int") or cols.get("cp") or cols.get("codigo_postal")
    prov_col = cols.get("provincia") or cols.get("prov")
    loc_col = cols.get("localidad") or cols.get("loc") or cols.get("ciudad")

    if cp_col is None or prov_col is None or loc_col is None:
        return (None, None, None)

    row = cp_master.loc[cp_master[cp_col] == cp_int]
    if row.empty:
        # fallback: si CP exacto no existe, no inventamos
        return (None, None, None)

    provincia = str(row.iloc[0][prov_col])
    localidad = str(row.iloc[0][loc_col])

    prov_norm = norm_text(provincia)
    loc_norm = norm_text(localidad)

    pat_set = globals().get("PATAGONIA_PROVS", None)
    if not pat_set:
        pat_set = PATAGONIA_PROVS_FALLBACK

    cap_dict = globals().get("CAPITAL_TOKENS_BY_PROV", None)
    if not cap_dict:
        cap_dict = CAPITAL_TOKENS_BY_PROV_FALLBACK

    is_capital = False
    if prov_norm == "CAPITAL FEDERAL":
        is_capital = True
    else:
        tokens = cap_dict.get(prov_norm, [])
        # match por contiene (token ya está en mayúsculas; loc_norm también)
        for t in tokens:
            if norm_text(t) in loc_norm:
                is_capital = True
                break

    prefix = "PATAGONIA" if prov_norm in pat_set else "INTERIOR"
    band = "I" if is_capital else "II"
    region = f"{prefix} {band}"

    return (region, provincia, localidad)



def expected_disd_from_raw(raw_matrix: pd.DataFrame, region: str, weight_kg: float) -> Optional[float]:
    """
    Esperado conservador: toma el MAX posible entre todas las filas (tier_id) para esa región.
    """
    if raw_matrix is None or raw_matrix.empty or region is None or pd.isna(weight_kg):
        return None
    df = raw_matrix.copy()
    df["region"] = df["region"].astype(str).str.upper()
    region = str(region).strip().upper()
    df = df[df["region"] == region].copy()
    if df.empty:
        return None

    # Para cada fila, computar costo según tramo o excedente
    max_w = df["w_to"].max()
    def cost_row(r):
        w = float(weight_kg)
        if w <= float(r["w_to"]):
            # buscar banda exacta en esa misma fila
            hit = df[(df.get("tier_id", -1) == r.get("tier_id", -1)) & (df["w_from"] <= w) & (w <= df["w_to"])]
            if not hit.empty:
                return float(hit.iloc[0]["cost"])
            return None
        # excedente: tomar última banda de esa fila + exc
        last = df[(df.get("tier_id",-1)==r.get("tier_id",-1)) & (df["w_to"]==max_w)]
        if last.empty:
            return None
        base = float(last.iloc[0]["cost"])
        exc = last.iloc[0].get("exc_per_kg")
        if pd.isna(exc) or exc is None:
            return base
        return base + float(exc) * max(0.0, (w - float(max_w)))

    vals = []
    for _, r in df.drop_duplicates(subset=["tier_id"]).iterrows():
        v = cost_row(r)
        if v is not None and not pd.isna(v):
            vals.append(float(v))
    if not vals:
        return None
    return float(max(vals))

def expected_sgd(valor_declarado: Optional[float], base_hasta: float, base_costo: float, excedente_pct: float) -> Optional[float]:
    if valor_declarado is None or pd.isna(valor_declarado):
        return None
    v = float(valor_declarado)
    if v <= base_hasta:
        return float(base_costo)
    return float(base_costo) + float(excedente_pct) * max(0.0, v - float(base_hasta))


    if set(SCHEMA_ML.keys()).issubset(set(df.columns)):
        out = df.rename(columns=SCHEMA_ML).copy()
        if "extra_per_kg" not in out.columns:
            out["extra_per_kg"] = None
        out["base_cost"] = out["base_cost"].apply(ar_money_to_float).astype(float)
        out["extra_per_kg"] = out["extra_per_kg"].apply(ar_money_to_float)
        out["cp_from"] = out["cp_from"].apply(parse_cp_to_int)
        out["cp_to"] = out["cp_to"].apply(parse_cp_to_int)
        out["w_min"] = pd.to_numeric(out["w_min"], errors="coerce").astype(float)
        out["w_max"] = pd.to_numeric(out["w_max"], errors="coerce").astype(float)
        out = out.dropna(subset=["cp_from","cp_to","w_min","w_max","base_cost"]).copy()
        out["cp_from"] = out["cp_from"].astype(int); out["cp_to"] = out["cp_to"].astype(int)
        return "cp_ranges", out.sort_values(["cp_from","cp_to","w_min","w_max"]).reset_index(drop=True), {}

    if set(SCHEMA_BNA.keys()).issubset(set(df.columns)):
        out = df.rename(columns=SCHEMA_BNA).copy()
        out["extra_per_kg"] = None
        out["base_cost"] = pd.to_numeric(out["base_cost"], errors="coerce").astype(float)
        out["cp_from"] = out["cp_from"].apply(parse_cp_to_int)
        out["cp_to"] = out["cp_to"].apply(parse_cp_to_int)
        out["w_min"] = pd.to_numeric(out["w_min"], errors="coerce").astype(float)
        out["w_max"] = pd.to_numeric(out["w_max"], errors="coerce").astype(float)
        out = out.dropna(subset=["cp_from","cp_to","w_min","w_max","base_cost"]).copy()
        out["cp_from"] = out["cp_from"].astype(int); out["cp_to"] = out["cp_to"].astype(int)
        return "cp_ranges", out.sort_values(["cp_from","cp_to","w_min","w_max"]).reset_index(drop=True), {}

    raise ValueError("Formato de matriz no reconocido.")

def validate_matrix(matrix_type: str, df_norm: pd.DataFrame) -> Dict[str, Any]:
    rep: Dict[str, Any] = {"errors": [], "warnings": [], "stats": {"rows": int(len(df_norm))}}
    if matrix_type == "cp_ranges":
        if (df_norm["cp_from"] > df_norm["cp_to"]).any():
            rep["errors"].append("Hay filas con cp_from > cp_to.")
        if (df_norm["w_min"] > df_norm["w_max"]).any():
            rep["errors"].append("Hay filas con w_min > w_max.")
        if df_norm["base_cost"].isna().any():
            rep["errors"].append("Hay filas con base_cost vacío.")
    else:
        if (df_norm["w_from"] > df_norm["w_to"]).any():
            rep["errors"].append("Hay filas con w_from > w_to.")
    return rep


# -----------------------------
# Registry helpers (editar / publicar / duplicar / borrar)
# -----------------------------
def sanitize_name_for_file(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "_", str(name))

def _find_registry_index(reg: List[Dict[str, Any]], name: str) -> Optional[int]:
    for i, e in enumerate(reg):
        if e.get("name") == name:
            return i
    return None

def update_matrix_entry(name: str, actor: str, updates: Dict[str, Any]) -> None:
    """
    Edita metadata de una versión (status / vigencia / notas, etc).
    Guarda diff (antes/después) en audit trail.
    """
    reg = ensure_registry_defaults(load_registry())
    idx = _find_registry_index(reg, name)
    if idx is None:
        raise ValueError(f"No existe la versión: {name}")
    before = json.loads(json.dumps(reg[idx], ensure_ascii=False))
    for k, v in updates.items():
        reg[idx][k] = v
    reg[idx]["updated_at"] = iso_now()
    save_registry(reg)
    after = json.loads(json.dumps(reg[idx], ensure_ascii=False))
    audit_log("matrix_update", actor, {"name": name, "before": before, "after": after})
    load_matrix_from_disk.clear()

def duplicate_matrix_version(src_name: str, new_name: str, actor: str, tweaks: Dict[str, Any]) -> None:
    """
    Duplica una versión: copia el .pkl y crea nueva entrada en registry.
    Por default crea DRAFT (salvo que tweaks lo cambie).
    """
    reg = ensure_registry_defaults(load_registry())
    idx = _find_registry_index(reg, src_name)
    if idx is None:
        raise ValueError(f"No existe la versión origen: {src_name}")
    if _find_registry_index(reg, new_name) is not None:
        raise ValueError("Ya existe una versión con ese nombre.")
    src = reg[idx]
    df = load_matrix_from_disk(src["path"])
    new_path = save_matrix_to_disk(new_name, df)

    created_at = iso_now()
    entry = {
        "name": new_name,
        "marketplace": src.get("marketplace"),
        "kind": src.get("kind", "RAW"),
        "valid_from": src.get("valid_from"),
        "valid_to": src.get("valid_to"),
        "created_at": created_at,
        "updated_at": created_at,
        "path": new_path,
        "status": "DRAFT",
        "actor": actor,
        "meta": json.loads(json.dumps(src.get("meta") or {}, ensure_ascii=False)),
    }
    for k, v in (tweaks or {}).items():
        if k == "meta" and isinstance(v, dict):
            entry["meta"].update(v)
        else:
            entry[k] = v
    reg.append(entry)
    save_registry(reg)
    audit_log("matrix_duplicate", actor, {"src": src_name, "new": new_name, "tweaks": tweaks})
    load_matrix_from_disk.clear()

def delete_matrix_version(name: str, actor: str) -> None:
    """
    Elimina una versión SOLO si está en DRAFT. También borra el pkl.
    """
    reg = ensure_registry_defaults(load_registry())
    idx = _find_registry_index(reg, name)
    if idx is None:
        raise ValueError(f"No existe la versión: {name}")
    entry = reg[idx]
    if str(entry.get("status","")).upper() != "DRAFT":
        raise ValueError("Solo se puede eliminar una versión en DRAFT (para no romper auditoría).")
    try:
        if entry.get("path") and os.path.exists(entry["path"]):
            safe = sanitize_name_for_file(name)
            backup_file(entry["path"], f"matrixfile_{safe}")
            os.remove(entry["path"])
    except Exception:
        pass
    before = json.loads(json.dumps(entry, ensure_ascii=False))
    reg.pop(idx)
    save_registry(reg)
    audit_log("matrix_delete", actor, {"name": name, "before": before})
    load_matrix_from_disk.clear()


def rename_matrix_version(old_name: str, new_name: str, actor: str) -> None:
    """
    Renombra una versión: actualiza el registry y (si es posible) renombra el archivo .pkl.
    Mantiene el contenido idéntico, solo cambia identificador 'name' y path.
    """
    old_name = str(old_name).strip()
    new_name = str(new_name).strip()
    if not old_name or not new_name:
        raise ValueError("Nombre inválido.")
    if old_name == new_name:
        return

    reg = ensure_registry_defaults(load_registry())
    idx_old = _find_registry_index(reg, old_name)
    if idx_old is None:
        raise ValueError(f"No existe la versión: {old_name}")
    if _find_registry_index(reg, new_name) is not None:
        raise ValueError("Ya existe una versión con ese nombre.")

    before = json.loads(json.dumps(reg[idx_old], ensure_ascii=False))

    # Renombrar archivo: si la ruta actual es un .pkl dentro de DATA_DIR, lo movemos.
    old_path = reg[idx_old].get("path")
    new_path = old_path
    try:
        if old_path and os.path.exists(old_path):
            base_dir = os.path.dirname(old_path)
            ext = os.path.splitext(old_path)[1] or ".pkl"
            # archivo nuevo basado en nombre
            safe = sanitize_name_for_file(new_name)
            candidate = os.path.join(base_dir, f"{safe}{ext}")
            # evitar pisar
            if os.path.abspath(candidate) != os.path.abspath(old_path):
                # backup por seguridad
                backup_file(old_path, f"before_rename_{sanitize_name_for_file(old_name)}")
                os.replace(old_path, candidate)
                new_path = candidate
    except Exception:
        # si no se puede mover, igual renombramos la entrada y dejamos el path tal cual
        new_path = old_path

    reg[idx_old]["name"] = new_name
    reg[idx_old]["path"] = new_path
    reg[idx_old]["updated_at"] = iso_now()
    save_registry(reg)

    after = json.loads(json.dumps(reg[idx_old], ensure_ascii=False))
    audit_log("matrix_rename", actor, {"old": old_name, "new": new_name, "before": before, "after": after})
    load_matrix_from_disk.clear()

def published_only(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "status" not in df.columns:
        return df
    return df[df["status"].astype(str).str.upper() == "PUBLISHED"].copy()

def pick_active_published(marketplace: str, kind: str, when: dt.date) -> Optional[Dict[str, Any]]:
    """
    Devuelve la versión PUBLISHED vigente en 'when' (si hay varias, toma la más nueva).
    Regla dura: SOLO PUBLISHED.
    """
    inv = list_matrices(marketplace)
    if inv is None or inv.empty:
        return None
    if "kind" in inv.columns:
        inv = inv[inv["kind"].astype(str).str.upper() == str(kind).upper()].copy()
    inv = published_only(inv)
    if inv.empty:
        return None
    when_ts = pd.to_datetime(when)
    inv["vf"] = pd.to_datetime(inv.get("valid_from"), errors="coerce")
    inv["vt"] = pd.to_datetime(inv.get("valid_to"), errors="coerce")
    inv = inv[(inv["vf"] <= when_ts) & ((inv["vt"].isna()) | (inv["vt"] >= when_ts))].copy()
    if inv.empty:
        return None
    inv = inv.sort_values(["vf","created_at"], ascending=[False, False])
    return inv.iloc[0].to_dict()


# Registry

def load_registry() -> List[Dict[str, Any]]:
    if not os.path.exists(REGISTRY_PATH):
        return []
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_registry(reg: List[Dict[str, Any]]) -> None:
    with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
        json.dump(reg, f, ensure_ascii=False, indent=2, default=str)

def ensure_registry_defaults(reg: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    changed = False
    for e in reg:
        # kind: RAW vs NORMALIZADA (MAX por región + tramo)
        if "kind" not in e:
            e["kind"] = "RAW"  # compat: versiones viejas
            changed = True
        if "status" not in e:
            e["status"] = "DRAFT"; changed = True
        if "actor" not in e:
            e["actor"] = "unknown"; changed = True
        if "updated_at" not in e:
            e["updated_at"] = e.get("created_at"); changed = True
        if "meta" not in e or e["meta"] is None:
            e["meta"] = {}; changed = True
    if changed:
        save_registry(reg)
    return reg

def matrix_file_path(name: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_\-\.]", "_", name)
    return os.path.join(DATA_DIR, f"{safe}.pkl")

def save_matrix_to_disk(name: str, df_norm: pd.DataFrame) -> str:
    path = matrix_file_path(name)
    df_norm.to_pickle(path)
    return path

@st.cache_data(show_spinner=False)
def load_matrix_from_disk(path: str) -> pd.DataFrame:
    return pd.read_pickle(path)

def register_matrix(
    name: str,
    marketplace: str,
    valid_from: dt.date,
    valid_to: Optional[dt.date],
    df_norm: pd.DataFrame,
    actor: str,
    status: str,
    kind: str,
    meta: Dict[str, Any],
) -> None:
    reg = ensure_registry_defaults(load_registry())
    created_at = iso_now()
    path = save_matrix_to_disk(name, df_norm)
    reg.append({
        "name": name,
        "marketplace": marketplace,
        "kind": str(kind).strip().upper(),
        "valid_from": valid_from.isoformat(),
        "valid_to": valid_to.isoformat() if valid_to else None,
        "created_at": created_at,
        "updated_at": created_at,
        "path": path,
        "status": status,
        "actor": actor,
        "meta": meta or {},
    })
    save_registry(reg)
    load_matrix_from_disk.clear()
    audit_log("matrix_create", actor, {"name": name, "status": status, "kind": str(kind).strip().upper()})

def list_matrices(marketplace: Optional[str]=None) -> pd.DataFrame:
    reg = ensure_registry_defaults(load_registry())
    df = pd.DataFrame(reg)
    if df.empty:
        return df
    if marketplace is not None:
        df = df[df["marketplace"] == marketplace].copy()
    return df.sort_values(["marketplace","valid_from","created_at"], ascending=[True, True, False])

# -----------------------------
# UI helpers
# -----------------------------
def dataset_panel(title: str, df: Optional[pd.DataFrame], label: str, path: str) -> None:
    st.markdown(f"### Dataset actual: {title}")
    if df is None:
        st.info("No hay datos cargados todavía.")
        return
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Filas", len(df))
    if "guia" in df.columns:
        c2.metric("Guías únicas", int(df["guia"].nunique()))
    elif "sku" in df.columns:
        c2.metric("SKUs", int(df["sku"].nunique()))
    else:
        c2.metric("Columnas", len(df.columns))
    c3.metric("Última modificación", dt.datetime.fromtimestamp(os.path.getmtime(path)).isoformat(timespec="seconds") if (os.path.exists(path) and os.path.isfile(path)) else "")
    c4.metric("Backups", len([f for f in os.listdir(BACKUP_DIR) if f.startswith(label + "_")]))
    st.dataframe(df, use_container_width=True, height=520)
    st.download_button("Descargar dataset actual (CSV)", df.to_csv(index=False).encode("utf-8"), f"{label}_actual.csv", "text/csv")
    st.download_button("Descargar dataset actual (Excel)", to_excel_bytes(df, sheet=label), f"{label}_actual.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    colx,coly,colz = st.columns(3)
    with colx:
        if st.button(f"Borrar {title}", type="secondary"):
            backup_file(path, label)
            delete_file(path)
            audit_log(f"{label}_delete", actor, {})
            st.success("Borrado (backup creado). Recargá la página.")
    with coly:
        if st.button("Restaurar último backup"):
            ok = restore_backup(label, path)
            st.success("Restaurado. Recargá la página.") if ok else st.warning("No hay backups disponibles.")
    with colz:
        st.caption("Consejo: siempre simulá primero y recién después aplicá.")

def change_plan(old: Optional[pd.DataFrame], new: pd.DataFrame, key_cols: List[str]) -> pd.DataFrame:
    """
    Plan de cambios simple:
      - filas nuevas vs filas que desaparecerían, usando key_cols (si existen).
    """
    if old is None:
        return pd.DataFrame([{"tipo":"nuevo_dataset","detalle":f"Se cargan {len(new):,} filas (no existía dataset previo)."}])
    for k in key_cols:
        if k not in old.columns or k not in new.columns:
            return pd.DataFrame([{"tipo":"reemplazo_total","detalle":"Se reemplaza dataset completo (no se pudieron comparar keys)."}])

    old_keys = set(tuple(x) for x in old[key_cols].astype(str).values.tolist())
    new_keys = set(tuple(x) for x in new[key_cols].astype(str).values.tolist())
    add = len(new_keys - old_keys)
    rem = len(old_keys - new_keys)
    return pd.DataFrame([
        {"tipo":"reemplazo_total","detalle":f"Dataset actual: {len(old):,} filas. Nuevo: {len(new):,} filas."},
        {"tipo":"keys_nuevas", "detalle": f"Keys nuevas detectadas: {add:,} (sobre {len(new_keys):,} keys del nuevo)."},
        {"tipo":"keys_que_se_pierden", "detalle": f"Keys que desaparecerían: {rem:,} (sobre {len(old_keys):,} keys del actual)."},
    ])

# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="Andreani | Gestión logística (v1.42)", layout="wide")
st.title("Andreani | Gestión logística (v1.42) — Modo simulación")

with st.sidebar:
    st.header("Operación")
    actor = st.text_input("Operador", value="Fede")

    st.divider()
    st.header("Plantillas (descarga)")
    st.download_button("Config YAML (plantilla)", file_bytes(TPL_CONFIG), "config.yaml")
    st.download_button("CP Master (plantilla)", file_bytes(TPL_CP), "template_cp_master.xlsx")
    st.download_button("Catálogo (plantilla)", file_bytes(TPL_CATALOG), "template_catalogo.xlsx")
    st.download_button("Ventas (plantilla)", file_bytes(TPL_SALES), "template_ventas.xlsx")
    st.download_button("Matriz Andreani (Region ME1) (plantilla)", file_bytes(TPL_AND), "template_matriz_andreani.xlsx")
    st.download_button("Matriz ME1 (plantilla)", file_bytes(TPL_ME1), "template_matriz_me1.xlsx")
    st.download_button("Matriz BNA (plantilla)", file_bytes(TPL_BNA), "template_matriz_bna.xlsx")
    st.download_button("Free shipping CPs (plantilla)", file_bytes(TPL_FREE), "template_free_shipping_cps.xlsx")

    st.divider()
    page = st.radio("Módulos", ["Home", "CP Master", "Catálogo", "Ventas", "Matriz Andreani (madre)", "Auditor Facturas", "Audit Trail"], index=0)
    st.caption("Tip: Matrices → publicá (PUBLISHED) para que se usen en cálculos.")

    st.divider()
    cfg_file = st.file_uploader("Config YAML (opcional)", type=["yml","yaml"])
    st.caption("Tip: por default, TODO se puede simular sin escribir nada.")

config = load_config(cfg_file)

cp_master = get_cp_master()
catalog = get_catalog()
sales = get_sales()
reg_all = list_matrices(None)

# HOME
if page == "Home":
    st.subheader("Estado / Memoria")
    st.caption("Versión en ejecución: v1.35")
    with st.expander("Diagnóstico (para evitar confusiones de versión)", expanded=False):
        st.code(__file__)
        st.write("Si arriba dice v1.35, estás ejecutando otro app.py.")
        st.write("Recomendado en Windows: streamlit run app.py --server.fileWatcherType none")
    st.info("Regla dura: la app SOLO usa matrices con estado PUBLISHED para cálculos. DRAFT es borrador.")
    st.dataframe(memory_status(), use_container_width=True, height=280)

    st.markdown("### Visualización completa (datasets actuales)")
    with st.expander("Ver CP Master", expanded=False):
        if cp_master is None:
            st.info("Sin CP Master.")
        else:
            st.dataframe(cp_master, use_container_width=True, height=520)
    with st.expander("Ver Catálogo", expanded=False):
        if catalog is None:
            st.info("Sin catálogo.")
        else:
            st.dataframe(catalog, use_container_width=True, height=520)
    with st.expander("Ver Ventas", expanded=False):
        if sales is None:
            st.info("Sin ventas.")
        else:
            st.dataframe(sales, use_container_width=True, height=520)

# CP MASTER
if page == "CP Master":
    st.subheader("CP Master — Simular / Aplicar")

    st.caption("Tip: si tu archivo trae CP con separadores (ej. 1,071), la app lo normaliza a 1071 para el matching.")
    dataset_panel("CP Master", cp_master, "cp_master", CP_MASTER_PATH)

    st.divider()
    st.markdown("### Import (preview + validación)")
    up = st.file_uploader("Subir CP.xlsx", type=["xlsx"])
    if up is not None:
        try:
            df_raw = read_excel_safe(up, converters={"CP": str, "Provincia": str, "Localidad": str})
            df_preview, warns = normalize_cp_master(df_raw)
            st.success("Validación OK.")
            for w in warns: st.warning(w)
            st.dataframe(df_preview.head(200), use_container_width=True, height=520)

            st.markdown("#### Plan de cambios")
            st.dataframe(change_plan(cp_master, df_preview, key_cols=["CP_int"]), use_container_width=True)

            st.download_button("Descargar preview normalizado (Excel)", to_excel_bytes(df_preview, "cp_master_preview"),
                               "cp_master_preview.xlsx",
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.download_button("Descargar preview normalizado (CSV)", df_preview.to_csv(index=False).encode("utf-8"),
                               "cp_master_preview.csv", "text/csv")

            col1,col2 = st.columns(2)
            with col1:
                st.button("Simular (no guarda)", disabled=True, help="Ya estás simulando: el preview NO se guarda.")
            with col2:
                confirm = st.checkbox("Confirmo que el preview está OK y quiero aplicar (sobrescribe).", key="cp_confirm")
                if st.button("Aplicar cambios", type="primary", disabled=not confirm):
                    backup_file(CP_MASTER_PATH, "cp_master")
                    set_cp_master(df_preview)
                    audit_log("cp_master_set", actor, {"rows": int(len(df_preview))})
                    st.success("Aplicado. Recargá la página.")
        except Exception as e:
            st.error(f"Error: {e}")

# CATÁLOGO
if page == "Catálogo":
    st.subheader("Catálogo — Simular / Aplicar")
    dataset_panel("Catálogo", catalog, "catalog", CATALOG_PATH)

    st.divider()
    st.markdown("### Import (preview + validación)")
    up = st.file_uploader("Subir catálogo (xlsx/csv)", type=["xlsx","csv"])
    if up is not None:
        try:
            if up.name.lower().endswith(".csv"):
                df_raw = pd.read_csv(up)
            else:
                df_raw = read_excel_safe(up, converters={"sku": str, "producto": str})
            df_preview, warns = normalize_catalog(df_raw)
            st.success("Validación OK.")
            for w in warns: st.warning(w)
            st.dataframe(df_preview.head(300), use_container_width=True, height=520)

            st.markdown("#### Plan de cambios")
            st.dataframe(change_plan(catalog, df_preview, key_cols=["sku"]), use_container_width=True)

            st.download_button("Descargar preview normalizado (Excel)", to_excel_bytes(df_preview, "catalog_preview"),
                               "catalog_preview.xlsx",
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.download_button("Descargar preview normalizado (CSV)", df_preview.to_csv(index=False).encode("utf-8"),
                               "catalog_preview.csv", "text/csv")

            col1,col2 = st.columns(2)
            with col1:
                st.button("Simular (no guarda)", disabled=True, help="Ya estás simulando: el preview NO se guarda.")
            with col2:
                confirm = st.checkbox("Confirmo que el preview está OK y quiero aplicar (sobrescribe).", key="cat_confirm")
                if st.button("Aplicar cambios", type="primary", disabled=not confirm):
                    backup_file(CATALOG_PATH, "catalog")
                    set_catalog(df_preview)
                    audit_log("catalog_set", actor, {"rows": int(len(df_preview))})
                    st.success("Aplicado. Recargá la página.")
        except Exception as e:
            st.error(f"Error: {e}")

# VENTAS
if page == "Ventas":
    st.subheader("Ventas — Simular / Aplicar (visualización completa)")
    dataset_panel("Ventas", sales, "sales", SALES_PATH)

    st.divider()
    st.markdown("### Import (preview + validación + sanity-check)")
    up = st.file_uploader("Subir ventas (xlsx/csv)", type=["xlsx","csv"])
    if up is not None:
        try:
            if up.name.lower().endswith(".csv"):
                df_raw = pd.read_csv(up)
            else:
                df_raw = read_excel_safe(up, converters={"guia": str, "cp": str, "sku": str})
            df_preview, warns = normalize_sales(df_raw)

            cross_warns = []
            cat = get_catalog()
            if cat is None:
                cross_warns.append("No hay catálogo cargado: no puedo validar SKUs faltantes todavía.")
            else:
                missing_skus = sorted(set(df_preview["sku"]) - set(cat["sku"].astype(str)))
                if missing_skus:
                    cross_warns.append(f"Hay {len(missing_skus)} SKUs en ventas que NO existen en catálogo (ej: {missing_skus[:8]}).")

            st.success("Validación OK.")
            for w in warns: st.warning(w)
            for w in cross_warns: st.warning(w)

            st.markdown("#### Sanity-check")
            st.json(sales_sanity(df_preview))

            st.markdown("#### Preview normalizado")
            st.dataframe(df_preview.head(400), use_container_width=True, height=560)

            st.markdown("#### Plan de cambios")
            st.dataframe(change_plan(sales, df_preview, key_cols=["guia","sku","fecha_envio"]), use_container_width=True)

            st.download_button("Descargar preview normalizado (Excel)", to_excel_bytes(df_preview, "sales_preview"),
                               "sales_preview.xlsx",
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.download_button("Descargar preview normalizado (CSV)", df_preview.to_csv(index=False).encode("utf-8"),
                               "sales_preview.csv", "text/csv")

            col1,col2 = st.columns(2)
            with col1:
                st.button("Simular (no guarda)", disabled=True, help="Ya estás simulando: el preview NO se guarda.")
            with col2:
                confirm = st.checkbox("Confirmo que el preview está OK y quiero aplicar (sobrescribe).", key="sales_confirm")
                if st.button("Aplicar cambios", type="primary", disabled=not confirm):
                    backup_file(SALES_PATH, "sales")
                    set_sales(df_preview)
                    audit_log("sales_set", actor, {"rows": int(len(df_preview)), "guides": int(df_preview['guia'].nunique())})
                    st.success("Aplicado. Recargá la página.")
        except Exception as e:
            st.error(f"Error: {e}")

# MATRIZ

if page == "Matriz Andreani (madre)":
    st.subheader("Matriz Andreani (madre) — RAW vs NORMALIZADA (MAX)")
    st.caption("Acá elegís QUÉ estás cargando y la app lo guarda en el espacio correcto.")

    tipo = st.radio(
        "Tipo de matriz a registrar",
        ["RAW (original Andreani)", "NORMALIZADA (MAX por región + tramo de peso)"],
        index=0,
        help=(
            "RAW: se usa para auditoría de facturas (más fiel al universo de tarifas posibles).\n"
            "NORMALIZADA: se usa para emitir matrices de marketplaces (una sola tarifa por región y tramo)."
        ),
    )

    up = st.file_uploader("Subir matriz (xlsx)", type=["xlsx"])
    name_default = f"andreani_{today().isoformat()}_{'RAW' if tipo.startswith('RAW') else 'NORM'}"
    name = st.text_input("Nombre versión", value=name_default)
    valid_from = st.date_input("Vigente desde", value=today())
    has_end = st.checkbox("Tiene fecha de fin", value=True)
    valid_to = st.date_input("Vigente hasta", value=today().replace(day=28)) if has_end else None
    notes = st.text_area("Notas", placeholder="Ej: Vigente Ago-2025 a Nov-2025. Sin IVA / sin SGD.")
    status = st.selectbox("Estado", ["DRAFT","PUBLISHED"], index=0)

    st.divider()
    st.markdown("### Simulación (preview + validación)")

    if up is None:
        st.info("Subí un archivo para ver el preview y validar antes de registrar.")
    else:
        try:
            df_raw = read_excel_safe(up)

            # 1) Parse Andreani al formato de cálculo (con tier_id + tramos)
            matrix_type, norm_raw, meta_extra = normalize_tariffs_any(df_raw)

            if matrix_type != "andreani_region":
                st.error("Este módulo está pensado para la matriz Andreani en formato 'Region ME1' con tramos tipo '0-50'.")
                st.stop()

            # 2) Si es NORMALIZADA: consolidar por región + tramo de peso (ignorando tier_id)
            if tipo.startswith("NORMALIZADA"):
                # MAX costo y MAX exc por (región, w_from, w_to)
                norm = (
                    norm_raw
                    .groupby(["region","w_from","w_to"], as_index=False)
                    .agg({"cost":"max", "exc_per_kg":"max"})
                    .sort_values(["region","w_from","w_to"])
                    .reset_index(drop=True)
                )
                kind = "NORMALIZADA"
                st.success("Se generó la matriz NORMALIZADA usando MAX por región + tramo de peso.")
            else:
                # RAW: mantener el detalle con tier_id
                norm = norm_raw.copy()
                kind = "RAW"
                st.success("Se usará la matriz RAW (detalle por tier_id).")

            # Rounding
            if "cost" in norm.columns:
                norm["cost"] = norm["cost"].apply(lambda v: apply_rounding(config, v))

            # Validación
            rep = validate_matrix("andreani_region", norm if kind=="RAW" else norm.rename(columns={"w_from":"w_from","w_to":"w_to"}))
            if rep["errors"]:
                st.error("Errores: no se puede registrar.")
            st.json(rep)

            # Métricas de limpieza
            c1,c2,c3 = st.columns(3)
            c1.metric("Filas RAW parseadas", int(len(norm_raw)))
            c2.metric("Filas a registrar", int(len(norm)))
            c3.metric("Reducción", f"{(1 - (len(norm)/max(1,len(norm_raw))))*100:.1f}%")

            st.markdown("#### Preview")
            st.caption("Nota: `tier_id` es un ID técnico (orden de fila en el Excel). No representa bultos reales del envío.")
            st.dataframe(norm.head(500), use_container_width=True, height=560)

            # Export del preview
            st.download_button(
                "Descargar preview normalizado (Excel)",
                to_excel_bytes(norm, "preview"),
                f"{name.strip()}_preview.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # Aplicar (registrar)
            st.divider()
            st.markdown("### Registrar versión (escritura)")
            confirm = st.checkbox("Confirmo que el preview está OK y quiero registrar esta versión.", key="mat_confirm")
            if st.button("Registrar versión", type="primary", disabled=(len(rep["errors"])>0 or not confirm)):
                meta = {
                    "notes": notes,
                    "validation": rep,
                    "matrix_type": "andreani_region",
                    "kind_policy": "RAW=detalle por tier_id | NORMALIZADA=MAX por región+tramo",
                    **(meta_extra or {}),
                }
                register_matrix(
                    name=name.strip(),
                    marketplace="andreani",
                    valid_from=valid_from,
                    valid_to=valid_to if has_end else None,
                    df_norm=norm,
                    actor=actor,
                    status=status,
                    kind=kind,
                    meta=meta,
                )
                st.success(f"Versión registrada como {kind}.")
        except Exception as e:
            st.error(f"Error: {e}")

    st.divider()
    st.markdown("### Inventario — Andreani (diferenciado)")
    inv = list_matrices("andreani")
    if inv.empty:
        st.info("No hay matrices Andreani registradas todavía.")
    else:
        # Asegurar columna kind en viejas
        if "kind" not in inv.columns:
            inv["kind"] = "RAW"
        colA, colB = st.columns(2)
        with colA:
            st.markdown("#### RAW (para auditoría)")
            inv_raw = inv[inv["kind"].astype(str).str.upper() == "RAW"].copy()
            st.dataframe(inv_raw[["name","status","valid_from","valid_to","created_at","actor"]], use_container_width=True, height=260)
        with colB:
            st.markdown("#### NORMALIZADA (para emitir matrices)")
            inv_norm = inv[inv["kind"].astype(str).str.upper() == "NORMALIZADA"].copy()
            st.dataframe(inv_norm[["name","status","valid_from","valid_to","created_at","actor"]], use_container_width=True, height=260)

        st.markdown("#### Ver una versión")
        sel = st.selectbox("Seleccionar versión", inv["name"].tolist())
        row = inv[inv["name"] == sel].iloc[0].to_dict()
        dfm = load_matrix_from_disk(row["path"])
        st.caption(f"Tipo: {row.get('kind','')} | Vigencia: {row.get('valid_from','')} → {row.get('valid_to') or 'sin fin'} | Estado: {row.get('status')}")
        st.dataframe(dfm.head(500), use_container_width=True, height=520)

        st.divider()
        st.markdown("### Acciones por versión (gobernanza)")
        st.caption("Regla dura: la app SOLO usa matrices **PUBLISHED** para cálculos. DRAFT es borrador.")

        action_name = st.selectbox("Elegir versión para administrar", inv["name"].tolist(), key="admin_sel")
        row2 = inv[inv["name"] == action_name].iloc[0].to_dict()

        cA, cB, cC, cD = st.columns(4)
        cA.metric("Tipo", row2.get("kind",""))
        cB.metric("Estado", row2.get("status",""))
        cC.metric("Desde", row2.get("valid_from",""))
        cD.metric("Hasta", row2.get("valid_to") or "sin fin")

        st.divider()
        st.markdown("#### Renombrar versión")
        st.caption("Cambia el nombre de la versión (y si se puede, renombra el archivo en disco). No modifica tarifas.")
        with st.form("rename_form"):
            new_name = st.text_input("Nuevo nombre", value=str(action_name))
            do_rename = st.form_submit_button("Renombrar")
            if do_rename:
                try:
                    rename_matrix_version(action_name, new_name.strip(), actor)
                    st.success("Nombre actualizado.")
                except Exception as e:
                    st.error(f"No se pudo renombrar: {e}")

        desired_pub = st.toggle("PUBLISHED (oficial)", value=str(row2.get("status","")).upper()=="PUBLISHED")
        if desired_pub and str(row2.get("status","")).upper() != "PUBLISHED":
            if st.button("Publicar versión", type="primary"):
                update_matrix_entry(action_name, actor, {"status":"PUBLISHED"})
                st.success("Publicada. (Queda oficial para cálculos)")
        if (not desired_pub) and str(row2.get("status","")).upper() == "PUBLISHED":
            if st.button("Pasar a DRAFT (despublicar)", type="secondary"):
                update_matrix_entry(action_name, actor, {"status":"DRAFT"})
                st.warning("Despublicada. (No se usa para cálculos)")

        st.divider()
        st.markdown("#### Editar vigencia + notas")
        with st.form("edit_meta_form"):
            vf = st.date_input("Vigente desde", value=pd.to_datetime(row2.get("valid_from"), errors="coerce").date())
            has_end2 = st.checkbox("Tiene fecha de fin", value=row2.get("valid_to") is not None)
            vt = st.date_input("Vigente hasta", value=pd.to_datetime(row2.get("valid_to") or dt.date.today(), errors="coerce").date()) if has_end2 else None
            notes_prev = (row2.get("meta") or {}).get("notes","")
            notes2 = st.text_area("Notas", value=notes_prev)
            submit = st.form_submit_button("Guardar cambios de metadata")
            if submit:
                meta_new = dict(row2.get("meta") or {})
                meta_new["notes"] = notes2
                update_matrix_entry(action_name, actor, {
                    "valid_from": vf.isoformat(),
                    "valid_to": vt.isoformat() if vt else None,
                    "meta": meta_new
                })
                st.success("Metadata actualizada.")

        st.divider()
        st.markdown("#### Duplicar como nueva versión (recomendado para correcciones)")
        with st.form("dup_form"):
            new_name = st.text_input("Nombre nueva versión", value=f"{action_name}_v1.35")
            nvf = st.date_input("Nueva vigencia desde", value=pd.to_datetime(row2.get("valid_from"), errors="coerce").date())
            nhas_end = st.checkbox("Nueva vigencia con fin", value=row2.get("valid_to") is not None)
            nvt = st.date_input("Nueva vigencia hasta", value=pd.to_datetime(row2.get("valid_to") or dt.date.today(), errors="coerce").date()) if nhas_end else None
            nnotes = st.text_area("Notas nuevas (opcional)", value=f"Duplicada desde {action_name}.")
            keep_published = st.checkbox("Publicar automáticamente", value=False)
            dup_submit = st.form_submit_button("Crear duplicado")
            if dup_submit:
                tweaks = {
                    "valid_from": nvf.isoformat(),
                    "valid_to": nvt.isoformat() if nvt else None,
                    "status": "PUBLISHED" if keep_published else "DRAFT",
                    "meta": {"notes": nnotes},
                }
                duplicate_matrix_version(action_name, new_name.strip(), actor, tweaks)
                st.success("Duplicado creado.")

        st.divider()
        st.markdown("#### Eliminar (solo DRAFT)")
        if str(row2.get("status","")).upper() == "DRAFT":
            if st.button("Eliminar versión DRAFT", type="secondary"):
                delete_matrix_version(action_name, actor)
                st.success("Eliminada (y respaldada).")
        else:
            st.info("Solo se puede eliminar una versión en DRAFT (para no romper auditoría).")

        st.divider()
        st.markdown("### Matriz vigente hoy (SOLO PUBLISHED)")
        kind_pick = st.selectbox("Tipo", ["RAW","NORMALIZADA"], index=0, key="vig_kind")
        active = pick_active_published("andreani", kind_pick, today())
        if not active:
            st.error("No hay una matriz PUBLISHED vigente hoy para ese tipo. (Cálculo bloqueado hasta que publiques una)")
        else:
            st.success(f"Vigente hoy: {active['name']} ({active.get('valid_from')} → {active.get('valid_to') or 'sin fin'})")

# AUDIT TRAIL

if page == "Auditor Facturas":
    st.subheader("Auditor de Facturas Andreani — PDF vs Ventas (SOLO PUBLISHED)")
    st.caption("Objetivo: detectar tarifa inflada, peso inflado y SGD incorrecto. IVA se calcula aparte.")

    # Precondiciones
    missing = []
    if sales is None or sales.empty:
        missing.append("Ventas (importadas)")
    if catalog is None or catalog.empty:
        missing.append("Catálogo (SKU → peso_aforado_kg)")
    if cp_master is None or cp_master.empty:
        missing.append("CP Master (CP → provincia/localidad)")
    if missing:
        st.error("Faltan datasets para auditar: " + ", ".join(missing))
        st.info("Cargalos en sus módulos y volvé.")
        st.stop()

    st.divider()
    st.markdown("### Selección de matriz Andreani (RAW) por fecha — automática (SOLO PUBLISHED)")
    st.caption("No tenés que elegir una fecha a mano: la app usa la fecha de envío (Ventas) y, si falta, la fecha que viene en la factura PDF por guía.")

    # Cargamos inventario RAW publicado (para poder resolver por fecha)
    inv_all = list_matrices("andreani")
    inv_all = inv_all if inv_all is not None else pd.DataFrame([])
    if (not inv_all.empty) and ("kind" in inv_all.columns):
        inv_raw_pub = inv_all[inv_all["kind"].astype(str).str.upper()=="RAW"].copy()
    else:
        inv_raw_pub = inv_all.copy()
    inv_raw_pub = published_only(inv_raw_pub)

    if inv_raw_pub.empty:
        st.error("No hay matrices Andreani RAW en estado PUBLISHED. Publicá al menos una para auditar.")
        st.stop()

    inv_raw_pub["vf"] = pd.to_datetime(inv_raw_pub.get("valid_from"), errors="coerce")
    inv_raw_pub["vt"] = pd.to_datetime(inv_raw_pub.get("valid_to"), errors="coerce")

    def matrix_for_date(d):
        if d is None or pd.isna(d):
            return None
        when_ts = pd.to_datetime(d)
        hit = inv_raw_pub[(inv_raw_pub["vf"] <= when_ts) & ((inv_raw_pub["vt"].isna()) | (inv_raw_pub["vt"] >= when_ts))].copy()
        if hit.empty:
            return None
        hit = hit.sort_values(["vf","created_at"], ascending=[False, False])
        return hit.iloc[0].to_dict()

    st.info("Tip: si hay guías con fechas fuera de vigencia, el auditor las marca como 'SIN MATRIZ' y no inventa costo.")

    st.divider()
    st.markdown("### Subir factura(s) PDF (modo simulación)")
    pdfs = st.file_uploader("Subir factura(s) Andreani (PDF)", type=["pdf"], accept_multiple_files=True)
    # Tolerancias para flaggear diferencias (se usan SOLO en el reporte comparativo).
    tol_ars = st.number_input(
        "Tolerancia $ (para marcar 'sobreprecio')",
        min_value=0.0,
        value=1.0,
        step=1.0,
        format="%.2f",
    )
    tol_kg = st.number_input(
        "Tolerancia KG (para marcar 'peso inflado')",
        min_value=0.0,
        value=0.10,
        step=0.05,
        format="%.2f",
    )

    # Alias por compatibilidad: en versiones previas la tolerancia estaba como `tol_cost`.
    tol_cost = tol_ars

    # SGD config
    base_hasta = float(config.get("sgd_base_hasta", 500000))
    base_costo = float(config.get("sgd_base_costo", 5378))
    exced_pct = float(config.get("sgd_excedente_pct", 0.01))
    st.caption(f"SGD: ${base_costo:,.0f} hasta ${base_hasta:,.0f} + {exced_pct*100:.2f}% sobre excedente (configurable en YAML).")

    if not pdfs:
        st.info("Subí al menos un PDF para ver el reporte.")
        st.stop()

    # Parse PDFs
    inv_parts = []
    for f in pdfs:
        try:
            inv_parts.append(parse_invoice_pdf_bytes(f.getvalue()))
        except Exception as e:
            st.error(f"{f.name}: {e}")
            with st.expander(f"Debug PDF: {f.name} (primeras líneas)", expanded=False):
                try:
                    import pdfplumber, io
                    with pdfplumber.open(io.BytesIO(f.getvalue())) as _pdf:
                        _lines=[]
                        for _p in _pdf.pages:
                            _t=_p.extract_text() or ""
                            _lines.extend(_t.splitlines())
                    st.code("\n".join(_lines[:200]))
                except Exception as _e:
                    st.write(_e)
    if not inv_parts:
        st.stop()

    inv = pd.concat(inv_parts, ignore_index=True).drop_duplicates(subset=["guia"]).copy()
    st.write(f"Envíos extraídos de PDFs: **{len(inv)}**")

    # Build expected from ventas + catálogo
    cat_map = catalog.set_index("sku")["peso_aforado_kg"].to_dict()

    s = sales.copy()
    # peso por línea
    s["peso_linea"] = s.apply(lambda r: float(cat_map.get(r["sku"], 0.0)) * float(r["qty"]), axis=1)
    missing_sku = s[s["peso_linea"] == 0.0]["sku"].unique().tolist()
    if missing_sku:
        st.warning(f"Hay SKUs sin peso en catálogo (o peso=0): {missing_sku[:15]}{'...' if len(missing_sku)>15 else ''}")

    agg = (
        s.groupby("guia", as_index=False)
        .agg({
            "fecha_envio": "min",
            "cp": "first",
            "peso_linea": "sum",
        })
        .rename(columns={"peso_linea":"kg_esperado"})
    )

    # Map CP -> region/prov/loc
    agg[["region","provincia","localidad"]] = agg.apply(
        lambda r: pd.Series(region_from_cp(r["cp"], cp_master)),
        axis=1
    )

    # Merge
    rep = inv.merge(agg, on="guia", how="left", indicator=True)
    rep["flag_sin_venta"] = rep["_merge"] != "both"
    rep = rep.drop(columns=["_merge"])

    # Expected costs
    # Resolver matriz por guía según fecha (prioridad: fecha_envio de Ventas; fallback: fecha_factura del PDF)
    rep["fecha_base"] = rep["fecha_envio"]
    rep.loc[rep["fecha_base"].isna(), "fecha_base"] = rep.loc[rep["fecha_base"].isna(), "fecha_factura"]

    # Elegir versión/matriz por cada fecha única
    rep["matrix_name"] = rep["fecha_base"].apply(lambda d: (matrix_for_date(d) or {}).get("name") if pd.notna(d) else None)
    missing_dates = rep[rep["fecha_base"].isna()]["guia"].tolist()
    if missing_dates:
        st.warning(f"Hay {len(missing_dates)} guías sin fecha (ni en ventas ni en PDF). No se puede elegir tarifa para esas guías.")
        with st.expander("Ver guías sin fecha", expanded=False):
            st.write(missing_dates)

    missing_matrix = rep[(rep["fecha_base"].notna()) & (rep["matrix_name"].isna())]
    if not missing_matrix.empty:
        st.error("Hay guías con fecha pero SIN matriz RAW PUBLISHED vigente. No se calcula esperado para esas guías.")
        st.dataframe(missing_matrix[["guia","fecha_base","fecha_envio","fecha_factura"]].head(200), use_container_width=True)

    # Cache: cargar cada matriz una sola vez
    mat_cache = {}
    for nm in sorted([x for x in rep["matrix_name"].dropna().unique().tolist()]):
        entry = inv_raw_pub[inv_raw_pub["name"]==nm].iloc[0].to_dict()
        mat_cache[nm] = load_matrix_from_disk(entry["path"])

    # Resumen de matrices usadas
    used = rep["matrix_name"].value_counts(dropna=False).reset_index()
    used.columns = ["matrix_name","guias"]
    st.caption("Matrices RAW PUBLISHED utilizadas (por fecha):")
    st.dataframe(used, use_container_width=True, height=180)

    rep["disd_esperado_max"] = rep.apply(
        lambda r: expected_disd_from_raw(mat_cache.get(r.get("matrix_name")), r.get("region"), r.get("kg_esperado"))
        if (r.get("matrix_name") is not None) and (not pd.isna(r.get("kg_esperado"))) else None,
        axis=1
    )
    rep["delta_disd"] = (rep["disd_factura"] - rep["disd_esperado_max"])
    rep["flag_sobreprecio_disd"] = rep["delta_disd"].apply(lambda x: False if pd.isna(x) else (x > tol_ars))

    # Peso inflado: si kg_factura > kg_esperado + tol
    rep["delta_kg"] = rep["kg_factura"] - rep["kg_esperado"]
    rep["flag_peso_inflado"] = rep["delta_kg"].apply(lambda x: False if pd.isna(x) else (x > tol_kg))
    rep["flag_peso_ok"] = rep["delta_kg"].apply(lambda x: False if pd.isna(x) else (x <= tol_kg))

    # SGD esperado (si existe valor_declarado en ventas)
    if "valor_declarado" in s.columns:
        dec = s.groupby("guia", as_index=False)["valor_declarado"].sum()
        rep = rep.merge(dec, on="guia", how="left")
        rep["sgd_esperado"] = rep["valor_declarado"].apply(lambda v: expected_sgd(v, base_hasta, base_costo, exced_pct))
        rep["delta_sgd"] = rep["sgd_factura"] - rep["sgd_esperado"]
        rep["flag_sgd"] = rep["delta_sgd"].apply(lambda x: False if pd.isna(x) else (abs(x) > tol_ars))
    else:
        rep["valor_declarado"] = None
        rep["sgd_esperado"] = None
        rep["delta_sgd"] = None
        rep["flag_sgd"] = False
        st.info("Tip: si querés auditar SGD, agregá una columna 'valor_declarado' en Ventas (o lo hacemos como dataset aparte).")

    # Prioridad de flags
    def prio(r):
        if r.get("flag_sin_venta"):
            return "SIN VENTA (no matchea guía)"
        if r.get("matrix_name") is None and (not pd.isna(r.get("fecha_base"))):
            return "SIN MATRIZ (no hay RAW PUBLISHED vigente)"
        if pd.isna(r.get("region")):
            return "SIN REGIÓN (CP no mapea)"
        if pd.isna(r.get("disd_esperado_max")):
            return "SIN TARIFA (no se pudo matchear banda de peso en matriz)"
        if r.get("flag_sobreprecio_disd"):
            return "SOBREPRECIO DISD"
        if r.get("flag_peso_inflado"):
            return "PESO INFLADO"
        if r.get("flag_sgd"):
            return "SGD DIF"
        return "OK"

    rep["estado"] = rep.apply(prio, axis=1)

    st.divider()
    st.markdown("### Resultado")


    # =====================
    # Presentación (pro): separar "Factura" vs "Esperado" vs "Comparación"
    # =====================
    try:
        rep_view = rep.copy()
        if "disd_factura" in rep_view.columns or "sgd_factura" in rep_view.columns:
            rep_view["total_factura"] = pd.to_numeric(rep_view.get("disd_factura", 0), errors="coerce").fillna(0) + pd.to_numeric(rep_view.get("sgd_factura", 0), errors="coerce").fillna(0)
        else:
            rep_view["total_factura"] = None
        rep_view["total_esperado"] = pd.to_numeric(rep_view.get("disd_esperado_max", 0), errors="coerce").fillna(0) + pd.to_numeric(rep_view.get("sgd_esperado", 0), errors="coerce").fillna(0)

        # Tramos (bandas de peso) — útil para detectar saltos de rango (no micro-diferencias dentro del mismo tramo)
        def _band_label(region: str, kg: float, mdf: pd.DataFrame):
            try:
                if mdf is None or len(mdf) == 0 or region is None or pd.isna(kg):
                    return None
                region_key = str(region).upper().strip()
                sub = mdf[mdf["region"].astype(str).str.upper().str.strip() == region_key]
                if len(sub) == 0:
                    return None
                kgf = float(kg)
                hit = sub[(sub["w_from"] <= kgf) & (kgf <= sub["w_to"])]
                if len(hit) == 0:
                    return None
                r0 = hit.iloc[0]
                wf = int(float(r0["w_from"]))
                wt = int(float(r0["w_to"]))
                return f"{wf}-{wt}"
            except Exception:
                return None

        if active_raw_df is not None and len(active_raw_df) > 0:
            rep_view["tramo_factura"] = rep_view.apply(lambda r: _band_label(r.get("region"), r.get("kg_factura"), active_raw_df), axis=1)
            rep_view["tramo_esperado"] = rep_view.apply(lambda r: _band_label(r.get("region"), r.get("kg_esperado"), active_raw_df), axis=1)
            rep_view["salto_tramo"] = (rep_view["tramo_factura"].notna()) & (rep_view["tramo_esperado"].notna()) & (rep_view["tramo_factura"] != rep_view["tramo_esperado"])
        else:
            rep_view["tramo_factura"] = None
            rep_view["tramo_esperado"] = None
            rep_view["salto_tramo"] = False

        rep_view["delta_total"] = pd.to_numeric(rep_view.get("total_factura", 0), errors="coerce") - pd.to_numeric(rep_view.get("total_esperado", 0), errors="coerce")
        rep_view["flag_sobreprecio_total"] = rep_view["delta_total"].fillna(0) > float(tol_ars)
        rep_view["flag_peso_inflado_tramo"] = rep_view["salto_tramo"] & (pd.to_numeric(rep_view.get("delta_kg", 0), errors="coerce").fillna(0) > float(tol_kg))
        rep_view["flag_peso_menor_tramo"] = rep_view["salto_tramo"] & (pd.to_numeric(rep_view.get("delta_kg", 0), errors="coerce").fillna(0) < -float(tol_kg))


        
        # Sanitización para evitar bugs del front (React #185):
        # - Fuerza numéricos en columnas de costos/pesos
        # - Convierte fechas/datetimes a string (estable para el render)
        # - Elimina índices raros
        def _sanitize_for_table(df: pd.DataFrame) -> pd.DataFrame:
            if df is None or len(df) == 0:
                return df
            out = df.copy()

            # Numeric columns (safe coercion)
            for c in ["bultos_factura","kg_factura","kg_esperado","delta_kg",
                      "disd_factura","sgd_factura","total_factura",
                      "disd_esperado_max","sgd_esperado","total_esperado",
                      "delta_disd","delta_sgd","delta_total"]:
                if c in out.columns:
                    out[c] = pd.to_numeric(out[c], errors="coerce")

            # Datetime-like to ISO string (works for both datetime64 and python dates)
            for c in out.columns:
                s = out[c]
                if pd.api.types.is_datetime64_any_dtype(s):
                    out[c] = s.dt.strftime("%Y-%m-%d")
                else:
                    # object column with python date/datetime
                    try:
                        if s.dtype == "object":
                            sample = s.dropna().head(50)
                            if len(sample) > 0 and all(isinstance(x, (datetime.date, datetime.datetime, pd.Timestamp)) for x in sample):
                                out[c] = s.astype(str)
                    except Exception:
                        pass

            out = out.reset_index(drop=True)
            # Evita dtypes "object" ambiguos luego de fillna
            try:
                out = out.infer_objects(copy=False)
            except Exception:
                pass
            return out

        rep_show = _sanitize_for_table(rep_view)

        t1, t2, t3 = st.tabs(["Factura (PDF)", "Esperado (Base)", "Comparación"])
        with t1:
            cols = [c for c in [
                "guia","invoice_issue_date","fecha_factura","bultos_factura","kg_factura","disd_factura","sgd_factura","total_factura"
            ] if c in rep_view.columns]
            st.dataframe(rep_show[cols].rename(columns={
                "invoice_issue_date":"fecha_emision_factura",
                "fecha_factura":"fecha_envio_pdf",
            }), use_container_width=True, height=420)

        with t2:
            cols = [c for c in [
                "guia","fecha_base","matrix_name","region","cp","provincia","localidad","kg_esperado","disd_esperado_max","sgd_esperado","total_esperado"
            ] if c in rep_view.columns]
            st.dataframe(rep_show[cols], use_container_width=True, height=420)

        with t3:
            cols = [c for c in [
                "estado","guia","fecha_factura","fecha_base","matrix_name",
                "cp","provincia","localidad","region",
                "kg_factura","kg_esperado","tramo_factura","tramo_esperado","salto_tramo","delta_kg",
                "disd_factura","disd_esperado_max","delta_disd",
                "sgd_factura","sgd_esperado","delta_sgd",
                "total_factura","total_esperado","delta_total"
            ] if c in rep_view.columns]

            only_diff = st.checkbox("Mostrar solo envíos con diferencias / alertas", value=True, key="only_diff")
            view = rep_view[cols].copy()

            # criterio de diferencia: por estado o por deltas fuera de tolerancia
            if only_diff and "estado" in view.columns:
                # todo lo que no sea OK
                view = view[view["estado"].astype(str).str.upper().ne("OK")].copy()

            # Coloreo: rojo si sobreprecio / peso inflado (salto de banda), verde si es "a favor" (más liviano y cambia banda)
            def _band_for(region: str, kg: float, mdf: pd.DataFrame):
                if mdf is None or mdf.empty or region is None or kg is None or pd.isna(kg):
                    return None
                sub = mdf[mdf["region"].astype(str).str.upper() == str(region).upper()].copy()
                if sub.empty:
                    return None
                sub = sub.sort_values(["w_from","w_to"])
                hit = sub[(sub["w_from"] <= kg) & (kg <= sub["w_to"])]
                if hit.empty:
                    return None
                # id por índice ordenado
                return int(hit.iloc[0].name)

            # Matriz RAW activa (si existe) para inferir salto de banda
            active_raw_df = None
            try:
                # en el auditor ya existe active (o similar). Si no, queda None.
                if "active" in locals() and isinstance(active, dict) and "df" in active:
                    active_raw_df = active["df"]
                elif "active_raw" in locals() and isinstance(active_raw, dict) and "df" in active_raw:
                    active_raw_df = active_raw["df"]
            except Exception:
                active_raw_df = None

            try:
                def style_row(row):
                    # Colorea solo lo que importa para la auditoría (deltas + saltos de tramo)
                    styles = pd.Series('', index=row.index, dtype='object')
                    def _paint(col, val):
                        try:
                            fval = float(val)
                        except Exception:
                            return
                        if fval > float(tol_ars):
                            styles[col] = 'background-color: #fde2e2;'
                        elif fval < -float(tol_ars):
                            styles[col] = 'background-color: #e7f7ec;'
                
                    for col in ['delta_total', 'delta_disd', 'delta_sgd', 'delta_kg']:
                        if col in row.index:
                            _paint(col, row[col])
                
                    if 'salto_tramo' in row.index and bool(row.get('salto_tramo')):
                        for col in ['tramo_factura', 'tramo_esperado']:
                            if col in row.index:
                                styles[col] = 'background-color: #fff1cc;'
                    return styles
                
                styled = view.style.apply(style_row, axis=1)
                st.dataframe(styled, use_container_width=True, hide_index=True)
            except Exception:
                st.dataframe(view, use_container_width=True, hide_index=True)


    except Exception:
        # Fallback silencioso: si el render de tabs falla, evitamos ruido en la interfaz.
        pass


    cols = [
        "estado","guia","fecha_base","matrix_name","fecha_factura","fecha_envio","cp","provincia","localidad","region",
        "bultos_factura","kg_factura","kg_esperado","delta_kg",
        "disd_factura","disd_esperado_max","delta_disd",
        "sgd_factura","sgd_esperado","delta_sgd",
        "total_factura","flag_sin_venta"
    ]
    cols = [c for c in cols if c in rep.columns]
    st.dataframe(rep[cols].sort_values(["estado","guia"]), use_container_width=True, height=620)

    # KPIs (día a día)
    total_envios = int(len(rep))
    ok_cnt = int((rep["estado"] == "OK").sum())
    alert_cnt = int((rep["estado"] != "OK").sum())
    sin_venta_cnt = int((rep["estado"].astype(str).str.contains("SIN VENTA")).sum()) if "estado" in rep.columns else 0

    if "delta_total" in rep.columns:
        sobreprecio_total = float(pd.to_numeric(rep["delta_total"], errors="coerce").fillna(0).clip(lower=0).sum())
        sobreprecio_cnt = int((rep.get("flag_sobreprecio_total", False) == True).sum()) if "flag_sobreprecio_total" in rep.columns else int((rep["delta_total"].fillna(0) > float(tol_ars)).sum())
    else:
        sobreprecio_total = float(pd.to_numeric(rep.get("delta_disd", 0), errors="coerce").fillna(0).clip(lower=0).sum())
        sobreprecio_cnt = int((rep["estado"] == "SOBREPRECIO DISD").sum()) if "estado" in rep.columns else 0

    salto_tramo_cnt = int((rep.get("salto_tramo", False) == True).sum()) if "salto_tramo" in rep.columns else int((rep["estado"] == "PESO INFLADO").sum())

    r1c1, r1c2, r1c3, r1c4 = st.columns(4)
    r1c1.metric("Envíos", total_envios)
    r1c2.metric("OK", ok_cnt)
    r1c3.metric("Alertas", alert_cnt)
    r1c4.metric("Sobreprecio estimado ($)", f"{sobreprecio_total:,.2f}")

    r2c1, r2c2, r2c3, r2c4 = st.columns(4)
    r2c1.metric("Saltos de tramo", salto_tramo_cnt)
    r2c2.metric("Sobreprecio (casos)", sobreprecio_cnt)
    r2c3.metric("Sin venta", sin_venta_cnt)
        r2c4.metric("Tolerancias", f"${tol_ars} / {tol_kg}kg")

    with st.expander("Glosario (qué significa delta_*)", expanded=False):
        st.markdown("""
        - **delta_kg** = `kg_factura - kg_esperado` (positivo => te cobraron por más kg).
        - **delta_disd** = `disd_factura - disd_esperado_max`.
        - **delta_sgd** = `sgd_factura - sgd_esperado`.
        - **delta_total** = `total_factura - total_esperado` (tu número final para auditoría).
        - **salto_tramo**: el tramo (banda) de kg de la factura != el tramo esperado (esto sí importa).
        """)

    st.download_button(
        "Descargar reporte (Excel)",
        to_excel_bytes(rep[cols], "auditoria"),
        f"auditoria_andreani_{today().isoformat()}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


if page == "Audit Trail":
    st.subheader("Audit Trail")
    n = st.slider("Últimos N", 50, 2000, 400, 50)
    df = read_audit_tail(n)
    if df.empty:
        st.info("Sin eventos todavía.")
    else:
        st.dataframe(df, use_container_width=True, height=650)
        st.download_button("Descargar (CSV)", df.to_csv(index=False).encode("utf-8"), "audit_log.csv", "text/csv")
