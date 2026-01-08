# Andreani App v1.50 — Modo simulación + visualización completa

## Modo simulación (lo que pediste)
- En cada import, por defecto **NO se guarda nada**.
- Se muestra preview normalizado + validación + sanity-check.
- Te deja exportar el preview normalizado (Excel/CSV).
- Recién guarda cuando apretás **Aplicar cambios** y marcás el checkbox de confirmación.

## Visualización
- Se puede ver el dataset actual completo (tabla), y descargarlo en CSV/Excel.
- Se puede ver el plan de cambios (impacto) antes de aplicar.

## Ejecutar
```bash
pip install -r requirements.txt
streamlit run app.py
```


## Matriz Andreani: RAW vs NORMALIZADA
- RAW: se registra con detalle por bultos.
- NORMALIZADA: se consolida por región + tramo (MAX) y se registra separada.


### Nota sobre `tier_id`
En matrices Andreani RAW, la app genera `tier_id` como índice técnico de la fila (para no perder duplicados). No es 'bultos' reales.


## Gobernanza de matrices
- Acciones por versión: Publicar/Despublicar, Editar vigencia+notas, Duplicar, Eliminar (solo DRAFT).
- Regla dura: SOLO PUBLISHED se usa para cálculos.


### Windows (recomendado)
```bash
streamlit run app.py --server.fileWatcherType none
```


## Auditor de facturas (PDF)
- Módulo: **Auditor Facturas**
- Requiere: Ventas + Catálogo + CP Master + Matriz Andreani RAW PUBLISHED vigente.
- Exporta reporte a Excel.


## Fix v1.34
- Auditor: la fecha por defecto ya no cae en 1970/1980; usa hoy si corresponde, sino el inicio de la última RAW publicada.


## Fix v1.34
- Auditor: corrige TypeError de comparación date vs datetime64 al elegir fecha por defecto.


## v1.34
- Matrices: Renombrar versión (registry + archivo) con audit trail.


## v1.34
- Fix: comparaciones de vigencia (date vs datetime) en pick_active_published.


## v1.34
- Auditor: selección automática de matriz RAW por guía usando fecha_envio (Ventas) y fallback fecha del PDF.
- Elimina la fecha de referencia manual.


## v1.34
- Auditor: parser PDF más robusto para capturar Nro. de Envío con variantes.
- Auditor: expander de debug con líneas extraídas cuando falla.


## v1.34
- Fix crítico: regex de guía en parser PDF estaba sobre-escapado (no matcheaba 'Nro. de Envío').


## v1.34
- Fix: NameError cp_to_int (region_from_cp) → usa parse_cp_to_int.


## v1.34
- Ventas: fecha_envio deja de ser obligatoria. Si falta/está inválida, se ignora y el auditor usa la fecha del PDF.


## v1.34
- Auditor: extrae fecha de envío desde el PDF por guía (busca DD.MM.YYYY en líneas de 'Servicio de transporte', ventana ±8).


## v1.34
- Fix: parser PDF definía 'lines' y 'i' (enumerate) para extraer fecha por guía sin NameError.


## v1.34
- Auditor: fecha por guía más robusta (escanea bloque entre guías, no solo ventana fija).
- Auditor: extrae y muestra fecha de emisión de la factura (header 'Fecha:').
- Auditor: agrega 'fecha_source' para saber si la fecha vino de líneas de servicio.


## v1.34
- Auditor: parser PDF reescrito (fecha/bultos/kg/imp neto por guía más robusto, soporta DD.MM.YYYY).
- Auditor: resultados separados en 3 tabs (Factura vs Base vs Comparación).


## v1.34
- Fix: define PATAGONIA_PROVS para region_from_cp (Patagonia I/II).


## v1.34
- Fix definitivo: region_from_cp ya no depende de PATAGONIA_PROVS (usa fallback interno), evita NameError.


## v1.34
- Fix: region_from_cp ya no usa CAPITAL_BY_PROV ni variables globales frágiles. Usa fallback interno (capital provincial vs interior + Patagonia vs Interior).
- Cambio de esquema de versión a v1.34xx.


## v1.34
- Auditor: parser PDF por bloque entre guías (robusto a saltos de página). Fix para casos donde la guía cae en fin de hoja y servicios continúan en hoja siguiente.
- Comparación: opción 'solo con diferencias' + coloreo (rojo sobreprecio, naranja peso inflado con salto de banda, verde a favor).
