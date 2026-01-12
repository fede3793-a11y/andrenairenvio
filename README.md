# Andreani App (Streamlit) — Matrices + Datasets + Auditoría de Facturas

App de gestión de datasets logísticos + gobernanza de matrices de tarifas + **auditoría de facturas Andreani (PDF vs Ventas)**.

## Qué hace (en criollo, pero enterprise)
- Importa y normaliza datasets (Ventas, Catálogo, CP Master).
- Gestiona matrices de tarifas con versionado y reglas de publicación (**solo PUBLISHED se usa para cálculos**).
- Audita facturas PDF: detecta **sobreprecio**, **peso inflado** y (opcional) diferencias en **SGD**.
- Exporta reportes a Excel.

## Estructura del repo
Archivos principales en la raíz:
- `app.py` (Streamlit)
- `config.yaml`
- `requirements.txt`
- Templates Excel (`template_*.xlsx`)  
  (ej.: `template_cp_master.xlsx`, `template_ventas.xlsx`, `template_matriz_andreani.xlsx`, etc.) :contentReference[oaicite:2]{index=2}

Datos persistidos típicamente en `data/` (pkl/jsonl) según lo que carga la app.

## Ejecutar
```bash
pip install -r requirements.txt
streamlit run app.py

streamlit run app.py --server.fileWatcherType none
