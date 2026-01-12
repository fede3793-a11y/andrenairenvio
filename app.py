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


def normalize_sales(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    warnings: List[str] = []
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    required = {"guia", "cp", "sku", "qty"}
    missing = sorted(list(required - set(df.columns)))
    if missing:
        raise ValueError(f"Ventas: faltan columnas {missing}. Requeridas: {sorted(required)}")

    df["guia"] = df["guia"].apply(normalize_guia)
    if df["guia"].isna().any():
        raise ValueError("Ventas: hay filas con guía vacía/ilegible.")
    if df["guia"].astype(str).str.contains(r"[eE]\+").any():
        raise ValueError("Ventas: hay guías en notación científica. Exportalas como TEXTO o usá la plantilla.")

    df["sku"] = df["sku"].astype(str).str.strip()
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(1).astype(float)

    # fecha_envio opcional
    if "fecha_envio" in df.columns:
        df["fecha_envio"] = pd.to_datetime(df["fecha_envio"], errors="coerce").dt.date
        bad = df["fecha_envio"].isna().sum()
        if bad > 0:
            warnings.append("Ventas: hay filas con fecha_envio inválida o vacía (se ignora y auditor usa fecha del PDF).")
        # limpieza de fechas absurdas
        try:
            too_old = df["fecha_envio"].apply(lambda d: (d is not None) and (not pd.isna(d)) and (d < dt.date(2000, 1, 1)))
            if too_old.any():
                df.loc[too_old, "fecha_envio"] = pd.NaT
                warnings.append("Ventas: se detectaron fechas muy viejas en fecha_envio y se anularon (NaT).")
        except Exception:
            pass
    else:
        df["fecha_envio"] = pd.NaT
        warnings.append("Ventas: no se cargó fecha_envio (opcional). Auditor usa fecha del PDF.")

    df["cp"] = df["cp"].astype(str).str.strip()
    df["cp_int"] = df["cp"].apply(parse_cp_to_int)
    if df["cp_int"].isna().any():
        raise ValueError("Ventas: hay CP inválidos.")

    return df.reset_index(drop=True), warnings


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

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        region_raw = r.get("Region ME1")
        region_key = normalize_region_key(region_raw) or str(region_raw).strip().upper()
        exc = pd.to_numeric(r.get("Exc"), errors="coerce")

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


def tariff_lookup(matrix_long: pd.DataFrame, *, region_key: str, kg: float) -> Tuple[Optional[float], Optional[float], str]:
    """
    Devuelve:
      (expected_cost, exc_used, status)
    - Si kg cae dentro de una banda: cost
    - Si kg excede el máximo w_to: cost(max_band) + exc_per_kg*(kg - w_to_max) si existe exc
    - Si no hay region: status SIN_REGION_EN_MATRIZ
    """
    if not region_key:
        return None, None, "SIN_REGION"

    m = matrix_long[matrix_long["region_key"] == region_key].copy()
    if m.empty:
        return None, None, "SIN_REGION_EN_MATRIZ"

    kg = float(kg)
    # dentro de banda (inclusive)
    inside = m[(m["w_from"] <= kg) & (kg <= m["w_to"])]
    if not inside.empty:
        # si hubiera duplicados, tomamos el mínimo costo (conservador)
        row = inside.sort_values(["w_from", "w_to", "cost"], ascending=[True, True, True]).iloc[0]
        return float(row["cost"]), None, "OK"

    # si supera máximo: aplicar excedente
    max_row = m.sort_values(["w_to", "w_from"], ascending=[False, False]).iloc[0]
    w_to = float(max_row["w_to"])
    base_cost = float(max_row["cost"])
    exc = max_row["exc_per_kg"]
    if kg > w_to:
        if exc is None or (isinstance(exc, float) and math.isnan(exc)):
            return None, None, "SIN_BANDA_SIN_EXC"
        extra = float(exc) * (kg - w_to)
        return base_cost + extra, float(exc), "OK_EXCEDENTE"

    # si queda por debajo del mínimo: tomar la mínima banda
    min_row = m.sort_values(["w_from", "w_to"], ascending=[True, True]).iloc[0]
    return float(min_row["cost"]), None, "OK_UNDER_MIN"


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
        if m.get("kind") != "RAW":
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

    st.divider()

    # -------------------------
    # Agregar CP manual (rápido)
    # -------------------------
    st.subheader("Agregar CP manual (rápido)")

    a1, a2, a3, a4 = st.columns([1, 2, 2, 2])
    with a1:
        new_cp = st.text_input("CP", value="")
    with a2:
        new_prov = st.text_input("Provincia", value="")
    with a3:
        new_loc = st.text_input("Localidad", value="")
    with a4:
        new_sub = st.text_input("Sub región (ej: PAT I 64 / LOC 53)", value="")

    if st.button("Agregar / Actualizar este CP"):
        cp_int = parse_cp_to_int(new_cp)
        if cp_int is None:
            st.error("CP inválido.")
        else:
            rk = normalize_region_key(new_sub) if new_sub.strip() else None
            row = {
                "CP": str(new_cp).strip(),
                "Provincia": str(new_prov).strip(),
                "Localidad": str(new_loc).strip(),
                "region_base": None,
                "sub_region": str(new_sub).strip() if new_sub.strip() else None,
                "region": None,
                "CP_int": cp_int,
                "region_key": rk,
            }

            base = cur.copy()
            base["CP_int"] = base["CP"].apply(parse_cp_to_int)

            if (base["CP_int"] == cp_int).any():
                base.loc[base["CP_int"] == cp_int, list(row.keys())] = pd.Series(row)
                st.success(f"Actualizado CP {cp_int}.")
            else:
                base = pd.concat([base, pd.DataFrame([row])], ignore_index=True)
                st.success(f"Agregado CP {cp_int}.")

            # refrescar en memoria del módulo (no guarda todavía)
            cur = base

    st.divider()

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

    up = st.file_uploader("Subí Ventas (xlsx/csv)", type=["xlsx", "xls", "csv"])
    if up:
        if up.name.lower().endswith(".csv"):
            raw = pd.read_csv(up)
        else:
            raw = pd.read_excel(up, engine="openpyxl")

        norm, warns = normalize_sales(raw)
        st.success(f"OK: Ventas normalizadas ({len(norm):,} filas)")
        for w in warns:
            st.warning(w)

        safe_show_df(norm, label="ventas_preview")
        st.download_button("Descargar preview (Excel)", data=to_excel_bytes(norm, "ventas"), file_name="ventas_normalizadas.xlsx")

        confirm = st.checkbox("Confirmo que quiero aplicar cambios (guardar en disco).")
        if st.button("Aplicar cambios", disabled=not confirm):
            backup_file(SALES_PATH, "sales")
            save_pickle(SALES_PATH, norm)
            audit_log("sales_apply", {"rows": len(norm), "file": up.name})
            st.success("Listo: Ventas guardadas.")

# =========================
# Matriz Andreani
# =========================
if page == "Matriz Andreani":
    st.subheader("Matriz Andreani — Import / Publicar / Usar en auditoría")

    reg = load_registry()
    mats = [m for m in reg.get("matrices", []) if m.get("kind") == "andreani"]
    if mats:
        st.write("Matrices registradas (Andreani)")
        safe_show_df(pd.DataFrame(mats), label="registry_andreani")

    up = st.file_uploader("Subí Matriz Andreani (xlsx)", type=["xlsx", "xls"])
    if up:
        raw = pd.read_excel(up, engine="openpyxl")
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
        step3["region_key"] = mapped.apply(lambda x: x[2])
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
        def _expected_from_matrix(row) -> Tuple[Optional[float], str]:
            if row.get("flag_sin_matriz"):
                return None, "SIN MATRIZ (no hay PUBLISHED vigente)"
            rk = row.get("region_key")
            if not rk:
                return None, "SIN REGIÓN (CP no mapea)"

            kg_fact = row.get("kg_factura")
            if kg_fact is None or (isinstance(kg_fact, float) and math.isnan(kg_fact)) or kg_fact <= 0:
                return None, "SIN PESO FACTURA"

            mrec, mdf = _get_matrix_for_date(row["fecha_base"])
            if mdf is None:
                return None, "SIN MATRIZ (archivo no cargó)"

            val, exc, status = tariff_lookup(mdf, region_key=str(rk), kg=float(kg_fact))
            if val is None:
                if status == "SIN_REGION_EN_MATRIZ":
                    return None, "SIN TARIFA (región no existe en matriz)"
                if status == "SIN_BANDA_SIN_EXC":
                    return None, "SIN TARIFA (kg excede y no hay Exc)"
                return None, "SIN TARIFA (no matchea banda)"
            # si usó excedente, lo declaramos
            if status == "OK_EXCEDENTE":
                return float(val), "OK (EXCEDENTE)"
            return float(val), "OK"

        exp = step4.apply(_expected_from_matrix, axis=1)
        step5 = step4.copy()
        step5["disd_esperado"] = exp.apply(lambda x: x[0])
        step5["tarifa_status"] = exp.apply(lambda x: x[1])
        step5_all.append(step5.copy())

        # -----------------
        # Paso 6: comparar pesos (kg_factura vs kg_esperado catálogo)
        # -----------------
        out = step5.copy()

        out["delta_kg"] = out["kg_factura"] - out["kg_esperado"]
        out["delta_disd"] = out["disd_factura"] - out["disd_esperado"]

        # Estado final: prioriza fallas de data / matching
        def _final_state(r) -> str:
            if r.get("flag_sin_venta"):
                return "SIN VENTA"
            if r.get("flag_cp_no_encontrado"):
                return "CP NO ENCONTRADO"
            if r.get("flag_sin_region"):
                return "SIN REGIÓN"
            if r.get("flag_sin_matriz"):
                return "SIN MATRIZ"
            if not r.get("disd_esperado") or (isinstance(r.get("disd_esperado"), float) and math.isnan(r.get("disd_esperado"))):
                return r.get("tarifa_status") or "SIN TARIFA"
            return "OK"

        out["estado_final"] = out.apply(_final_state, axis=1)

        # columnas finales
        cols = [
            "estado_final",
            "tarifa_status",
            "guia",
            "source_pdf",
            "fecha_factura",
            "fecha_envio",
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
        safe_show_df(df5[["guia","region_key","kg_factura","matrix_name_usada","disd_esperado","tarifa_status"]], label="paso5_tarifa")

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
