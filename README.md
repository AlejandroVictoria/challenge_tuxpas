````markdown
# CaféNorte Serverless Data Lake

Este repositorio contiene la demo técnica y propuesta técnica para la implementación de un Data Lake Serverless en AWS para **CaféNorte**. La arquitectura automatiza la ingesta, limpieza, validación y agregación de datos (Ventas POS, E-commerce, Inventario y Tipos de Cambio) preparándolos para su consumo.

El flujo se divide en capas (Raw, Silver, Gold) y está diseñado para ejecutarse nativamente en **AWS Glue (Python Shell)** orquestado por **AWS Step Functions**, permitiendo al mismo tiempo una ejecución y validación 100% local mediante motores análiticos desacoplados **Polars**, **DuckDB** o herramientas internas de aws como **Athena**.

## Estructura del Proyecto

```text
.
├── analytics/
│   └── queries.py                     # Script para validación analítica y consultas SQL sobre la capa Gold
├── data/
│   ├── datos/                         # Archivos fuente originales
│   └── data_mock_generated/           # Datos simulados y particionados (dt=YYYY-MM-DD)
├── ingest/
│   ├── raw_to_silver.py               # Pipeline ETL: Limpieza y normalización
│   └── silver_to_gold.py              # Pipeline ETL: Modelado de negocio, agregaciones y KPIs
├── lake_configuration/
│   └── cafenorte_datalek_v2_orch.yaml # Plantilla CloudFormation (IaC) de la infraestructura AWS
├── requerimentos/                     # Documentación de negocio y propuesta técnica
├── .gitignore
├── AI_LOG.md                          # Bitácora del uso de herramientas de IA durante el desarrollo
├── demo_ingest.py                     # Gestor de simulación de ingesta (S3 / Local)
├── README.md                          # Este archivo
└── requirements.txt                   # Dependencias del proyecto (Polars, Boto3, etc.)
```
````

## Requisitos Previos

1. **Python 3.9+** (Recomendado)
2. Credenciales de AWS configuradas localmente (`aws configure`) si se desea probar la integración con S3 y Step Functions.
3. Instalar las dependencias del proyecto:

```bash
pip install -r requirements.txt

```

## Despliegue de Infraestructura (IaC)

La infraestructura completa (S3, roles IAM, Glue, Step Functions) está escrita para CloudFormation. Para desplegarla en tu cuenta de AWS:

```bash
aws cloudformation deploy \
  --template-file lake_configuration/cafenorte_datalek_v2_orch.yaml \
  --stack-name cafenorte-lakehouse-stack \
  --capabilities CAPABILITY_NAMED_IAM

```

## Guía de Ejecución

El proyecto está diseñado para funcionar en un entorno dual (Local para desarrollo/pruebas y `dev` conectado a AWS).

### 1. Ingesta de Datos (`demo_ingest.py`)

Simula el proceso de extracción de los sistemas origen y deposita los datos en la capa Raw particionada por fecha.

- **Modo Local:** Actualiza únicamente la carpeta `data/raw/` local.

```bash
python demo_ingest.py --date 2026-11-01 --ENV local

```

- **Modo Dev (AWS):** Sube los datos a S3 y puede detonar la máquina de estados automáticamente.

NOTA: para este caso de uso, se usa el parámetro "previous" para migrar los datos originales (contenidos en carpeta data/datos). El uso de fechas como "2026-11-01" y "2026-11-01" son valores copiados de los datos originales con la condición de los campos donde tienen una fecha, se actualizó a las fechas mencionadas. Su propósito es demostrar la capacidad de ingesta frente a nuevas fechas.

```bash
python demo_ingest.py --date previous --ENV dev --trigger

```

### 2. Procesamiento ETL (`raw_to_silver.py` y `silver_to_gold.py`)

Estos scripts utilizan Polars para procesar los datos. Están diseñados para ser ejecutados por AWS Glue (pasando parámetros como `--LAKEHOUSE_BUCKET`), pero admiten rutas locales para pruebas de desarrollo.

**Prueba Local del Pipeline:**

```bash
# De Raw a Silver
python ingest/raw_to_silver.py --TARGET_DATE 2026-11-01 --LOCAL_RAW_PATH data/raw/ --LOCAL_SILVER_PATH data/silver/

# De Silver a Gold
python ingest/silver_to_gold.py --TARGET_DATE 2026-11-01 --LOCAL_SILVER_PATH data/silver/ --LOCAL_GOLD_PATH data/gold/

```

### 3. Consultas Analíticas (`queries.py`)

Permite ejecutar consultas SQL sobre los archivos Parquet de la capa Gold utilizando el motor de Polars. Útil para validar que las métricas de negocio cuadren sin necesidad de usar Amazon Athena.

- **Consultar datos procesados localmente:**

```bash
python analytics/queries.py --ENV local

```

- **Consultar datos directamente desde el bucket S3 (Capa Gold):**

```bash
python analytics/queries.py --ENV dev

```

## Respuestas a Preguntas de Negocio (Challenge)

A continuación se presentan las consultas SQL y los resultados obtenidos al ejecutar sobre la capa Gold.

### Supuestos e Interpretaciones (Resolución de Ambigüedad)

- **Q1 (Rotación de Inventario):** Se interpretó como el _Total de Unidades Vendidas_ por SKU en los últimos 6 meses (proxy de rotación operativa). Nota: Se detectaron 4,005 unidades asociadas a un SKU no identificado en los datos de origen (anomalía de calidad de datos en Raw).
- **Q2 (Quiebres de Stock > 3 días):** Se calculó como la suma de días (`is_stockout = 1`) mayores a 3 dentro del último trimestre, sin requerir que sean consecutivos.
- **Q3 (Crecimiento MoM):** Se utilizó la función de ventana `LAG()` para calcular la variación porcentual mes a mes particionada por canal de venta.
- **Q4 (Margen Negativo):** Se buscó cualquier registro con `margen_bruto_mxn < 0`. El resultado vacío es un hallazgo de negocio (no hay productos operando en pérdida).

---

### 1. Top 10 SKUs por rotación de inventario en los últimos 6 meses

```sql
SELECT sku_erp, SUM(cantidad) AS total_unidades_rotadas
FROM fct_ventas_unificadas
WHERE fecha >= '2025-09-28'
GROUP BY sku_erp
ORDER BY total_unidades_rotadas DESC
LIMIT 10;

```

**Resultado:**

| SKU (sku_erp)     | Unidades Rotadas |
| ----------------- | ---------------- |
| _(Sin SKU / N/A)_ | 4005             |
| ERP-PROV-MX-054-B | 939              |
| ERP-PROV-MX-057-C | 901              |
| ERP-PROV-MX-047-B | 899              |
| ERP-PROV-MX-038-A | 894              |
| ERP-PROV-MX-037-C | 891              |
| ERP-PROV-MX-059-B | 881              |
| ERP-PROV-MX-034-D | 876              |
| ERP-PROV-MX-003-D | 857              |
| ERP-PROV-MX-024-A | 854              |

---

### 2. Tiendas con quiebres de stock de más de 3 días en el último trimestre

```sql
SELECT tienda_id, COUNT(fecha) AS dias_con_quiebre
FROM agg_inventario_diario
WHERE is_stockout = true AND fecha >= '2025-12-28'
GROUP BY tienda_id
HAVING COUNT(fecha) > 3
ORDER BY dias_con_quiebre DESC;

```

**Resultado:** _(Mostrando el Top 5 de 40 tiendas identificadas en riesgo)_

| Tienda ID | Días con quiebre |
| --------- | ---------------- |
| T006      | 294              |
| T025      | 271              |
| T036      | 266              |
| T001      | 263              |
| T012      | 252              |

---

### 3. Crecimiento mes a mes (MoM) de ventas por canal en el último año

```sql
WITH VentasMensuales AS (
    SELECT canal, year_month, SUM(monto_mxn) AS venta_actual
    FROM fct_ventas_unificadas
    WHERE fecha >= '2025-03-28'
    GROUP BY canal, year_month
)
SELECT
    canal, year_month, ROUND(venta_actual, 2),
    ROUND(LAG(venta_actual) OVER (PARTITION BY canal ORDER BY year_month), 2) AS venta_mes_anterior,
    ROUND(((venta_actual - LAG(venta_actual) OVER (...)) / LAG(venta_actual) OVER (...)) * 100, 2) AS crecimiento_mom_pct
FROM VentasMensuales;

```

**Resultado:** _(Muestra representativa de los últimos meses)_

| Canal     | Mes     | Venta Actual (MXN) | Venta Mes Anterior | Crecimiento MoM (%) |
| --------- | ------- | ------------------ | ------------------ | ------------------- |
| ECOMMERCE | 2025-07 | $ 247,874.17       | $ 246,024.30       | +0.75 %             |
| ECOMMERCE | 2025-08 | $ 236,166.84       | $ 247,874.17       | -4.72 %             |
| POS       | 2026-02 | $ 1,576,721.70     | $ 1,713,200.00     | -7.97 %             |
| POS       | 2026-03 | $ 1,718,200.00     | $ 1,576,721.70     | +8.97 %             |

---

### 4. Productos con margen negativo y en qué tiendas ocurren

```sql
SELECT DISTINCT tienda_id, sku_erp, MIN(margen_bruto_mxn) AS peor_margen_registrado
FROM fct_ventas_unificadas
WHERE margen_bruto_mxn < 0
GROUP BY tienda_id, sku_erp
ORDER BY peor_margen_registrado ASC;

```

**Resultado:**
`0 registros retornados.`
_(Nota: La ejecución confirma que, dentro del historial procesado, no existen transacciones que hayan generado un margen bruto negativo para CaféNorte)._

```

```
