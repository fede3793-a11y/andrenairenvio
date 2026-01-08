Andreani | Gestión logística es una herramienta para:

Auditar facturas Andreani contra una matriz madre y detectar sobreprecios y/o peso inflado.

Mantener datasets maestros (CP Master, Catálogo, Ventas) que permiten mapear envíos y calcular esperados.

Gestionar versionado de matrices (RAW y NORMALIZADA) con control de publicación.

Conceptos clave

Matriz Andreani RAW (auditoría): se usa para calcular el costo esperado de la factura.

Matriz Andreani NORMALIZADA (emitir matrices): se usa como base operativa para construir matrices derivadas.

DRAFT vs PUBLISHED:

DRAFT = borrador (no afecta cálculos).

PUBLISHED = oficial (la app calcula SOLO con publicadas).

CP Master: tabla de códigos postales que incluye provincia, localidad y región.

Catálogo: tabla SKU → peso aforado (peso esperado interno).

Ventas: tabla de guía/venta/SKUs usada para vincular envíos a productos (cuando aplique).

Flujo de auditoría (alto nivel)

Se carga factura PDF Andreani.

La app extrae envíos por Nro. de Envío (guía) y obtiene:

fecha de envío (desde el PDF)

CP (desde el PDF o datasets, según configuración)

kg facturados, bultos, importes DISD/SGD

Se determina región vía CP Master.

Se elige la matriz RAW PUBLISHED vigente para esa fecha.

Se calcula el esperado y se compara contra la factura.

Métricas y deltas

delta_kg = kg_factura - kg_esperado

positivo: posible peso inflado

negativo: facturado menor que esperado

delta_disd = disd_factura - disd_esperado

delta_sgd = sgd_factura - sgd_esperado

La app marca alertas cuando los deltas superan tolerancias configuradas.

Persistencia

La app guarda datos en ./data/ (persistencia local).
Antes de operaciones sensibles se crean copias en ./data/backups/.