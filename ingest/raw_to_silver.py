import argparse
import json
import logging
import os
import sys
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import polars as pl


for stream_name in ("stdout", "stderr"):
    stream = getattr(sys, stream_name, None)
    reconfig = getattr(stream, "reconfigure", None)
    if callable(reconfig):
        reconfig(encoding="utf-8", line_buffering=True)

# Configurar el formato y forzar el stream hacia stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,  # Limpia cualquier handler previo que AWS Glue o librerías hayan atado
)

# Asegurar que el logger del módulo opere en INFO
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =============================================================================
# 1. LECTURA DE DATOS (Data Readers)
# =============================================================================
class BaseReader(ABC):
    @abstractmethod
    def read(self, uri: str):
        pass


class S3LocalReader(BaseReader):
    """
    Lector unificado compatible con local y S3 (delegando en Polars/fsspec/pyarrow).
    """
    def read_csv(self, uri: str) -> pl.DataFrame:
        logger.info(f"Leyendo CSV desde: {uri}")
        return pl.read_csv(uri, ignore_errors=False)

    def read_parquet(self, uri: str) -> pl.DataFrame:
        logger.info(f"Leyendo Parquet desde: {uri}")
        return pl.read_parquet(uri)

    def read_json_raw(self, uri: str) -> dict:
        """
        Lee el archivo JSON sin alterar para poder desacoplar sus distintas entidades.
        Soporta s3:// (usando s3fs si está en Glue) o rutas locales.
        """
        logger.info(f"Leyendo JSON raw desde: {uri}")
        if uri.startswith("s3://"):
            import s3fs
            fs = s3fs.S3FileSystem()
            with fs.open(uri, "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            with open(uri, "r", encoding="utf-8") as f:
                return json.load(f)

    def read(self, uri: str):
        ext = uri.split(".")[-1].lower()
        if ext == "csv":
            return self.read_csv(uri)
        elif ext in ["parquet", "pq"]:
            return self.read_parquet(uri)
        elif ext == "json":
            return self.read_json_raw(uri)
        else:
            raise ValueError(f"Extensión no soportada: {ext}")


# =============================================================================
# 2. PROCESAMIENTO Y NORMALIZACIÓN (Data Processors)
# =============================================================================
class DataProcessor(ABC):
    @abstractmethod
    def process(self, raw_input) -> Dict[str, pl.DataFrame]:
        """Retorna un diccionario de DataFrames {nombre_tabla: pl.DataFrame}"""
        pass


class POSSalesProcessor(DataProcessor):
    def process(self, raw_input: pl.DataFrame) -> Dict[str, pl.DataFrame]:
        logger.info("Transformando ventas POS (sales.csv)...")
        # Casteo de timestamp, extracción de partición por fecha y trim de cadenas
        df = raw_input.with_columns([
            pl.col("venta_id").cast(pl.String).str.strip_chars(),
            pl.col("fecha_hora").str.to_datetime().alias("fecha_hora"),
            pl.col("fecha_hora").str.slice(0, 10).alias("dt"),
            pl.col("tienda_id").cast(pl.String).str.strip_chars(),
            pl.col("sku").cast(pl.String).str.strip_chars().alias("sku_pos"),
            pl.col("cantidad").cast(pl.Int64),
            pl.col("monto").cast(pl.Float64),
            pl.col("moneda").cast(pl.String).str.to_uppercase().str.strip_chars(),
            pl.col("tipo_comprobante").cast(pl.String).str.strip_chars(),
        ]).drop("sku")

        return {"fct_pos_sales": df}


class EcommerceOrdersProcessor(DataProcessor):

  def process(self, raw_input: pl.DataFrame) -> Dict[str, pl.DataFrame]:
    logger.info("Transformando ecommerce (ecommerce_orders.parquet)...")

    # Si la fecha viene como string con hora (ej: "2024-03-01 10:20:00") o ISO,
    # normalizamos primero a los primeros 10 caracteres (YYYY-MM-DD)
    date_str_expr = pl.col("fecha").cast(pl.String).str.slice(0, 10)

    # Parser robusto: intenta YYYY-MM-DD, si viene con '/' prueba alternativas
    df = raw_input.with_columns([
        pl.col("order_id").cast(pl.String).str.strip_chars(),
        date_str_expr.alias("dt"),
        # strptime tolerante con fallback
        pl.coalesce([
            date_str_expr.str.to_date("%Y-%m-%d", strict=False),
            date_str_expr.str.to_date("%d/%m/%Y", strict=False),
            date_str_expr.str.to_date("%Y/%m/%d", strict=False),
        ]).alias("fecha"),
        pl.col("product_handle").cast(pl.String).str.strip_chars(),
        pl.col("cantidad").cast(pl.Int64),
        pl.col("amount").cast(pl.Float64),
        pl.col("currency").cast(pl.String).str.to_uppercase().str.strip_chars(),
        pl.col("customer_name").cast(pl.String).str.strip_chars(),
        pl.col("customer_email")
        .cast(pl.String)
        .str.to_lowercase()
        .str.strip_chars(),
        pl.col("customer_rfc")
        .cast(pl.String)
        .str.to_uppercase()
        .str.strip_chars(),
        pl.col("shipping_city").cast(pl.String).str.strip_chars(),
        pl.col("shipping_address").cast(pl.String).str.strip_chars(),
    ])

    return {"fct_ecommerce_orders": df}


class ExchangeRatesProcessor(DataProcessor):
    def process(self, raw_input: pl.DataFrame) -> Dict[str, pl.DataFrame]:
        logger.info("Transformando tasas de cambio (exchange_rates.csv)...")
        df = raw_input.with_columns([
            pl.col("fecha").str.to_date().alias("fecha"),
            pl.col("currency").cast(pl.String).str.to_uppercase().str.strip_chars(),
            pl.col("rate_to_mxn").cast(pl.Float64),
        ])
        return {"ref_exchange_rates": df}


class InventoryJSONProcessor(DataProcessor):
  """Desacopla el JSON monolítico en sus 4 entidades relacionales Silver."""

  def process(self, raw_input: dict) -> Dict[str, pl.DataFrame]:
    logger.info("Desacoplando y transformando entidades de inventory.json...")
    results = {}

    # 1. Dimensión Tiendas
    if "tiendas_info" in raw_input and raw_input["tiendas_info"]:
      df_tiendas = pl.DataFrame(
          raw_input["tiendas_info"],
          schema={
              "tienda_id": pl.String,
              "ciudad": pl.String,
              "region": pl.String,
              "timezone": pl.String,
          },
      ).with_columns([
          pl.col("tienda_id").str.strip_chars(),
          pl.col("ciudad").str.strip_chars(),
          pl.col("region").str.strip_chars(),
          pl.col("timezone").str.strip_chars(),
      ])
      results["dim_tiendas"] = df_tiendas

    # 2. Dimensión SKU Mappings (Se pasa schema explícito para evitar inferencia a pl.Null)
    if "sku_mappings" in raw_input and raw_input["sku_mappings"]:
      # Normalizamos los valores nulos antes de construir el DataFrame
      clean_mappings = [
          {
              "sku_pos": str(m.get("sku_pos") or "").strip(),
              "sku_erp": str(m.get("sku_erp") or "").strip(),
              "handle": str(m.get("handle") or "N/A").strip(),
          }
          for m in raw_input["sku_mappings"]
      ]
      df_sku = pl.DataFrame(
          clean_mappings,
          schema={
              "sku_pos": pl.String,
              "sku_erp": pl.String,
              "handle": pl.String,
          },
      )
      results["dim_sku_mappings"] = df_sku

    # 3. Dimensión Productos y Cost History
    if "catalogo" in raw_input and "productos" in raw_input["catalogo"]:
      productos_raw = raw_input["catalogo"]["productos"]

      prod_records = [
          {
              "sku_erp": str(p.get("sku_erp") or "").strip(),
              "nombre": str(p.get("nombre") or "").strip(),
              "categoria": str(p.get("categoria") or "").strip(),
          }
          for p in productos_raw
      ]
      results["dim_productos"] = pl.DataFrame(
          prod_records,
          schema={
              "sku_erp": pl.String,
              "nombre": pl.String,
              "categoria": pl.String,
          },
      )

      cost_records = []
      for p in productos_raw:
        sku_erp = str(p.get("sku_erp") or "").strip()
        for cost_entry in p.get("cost_history", []):
          f_vig = str(cost_entry.get("fecha_vigencia") or "")[:10]
          cost_records.append({
              "sku_erp": sku_erp,
              "fecha_vigencia": f_vig,
              "costo_mxn": float(cost_entry.get("costo_mxn") or 0.0),
              "proveedor": str(cost_entry.get("proveedor") or "").strip(),
          })

      df_costs = pl.DataFrame(
          cost_records,
          schema={
              "sku_erp": pl.String,
              "fecha_vigencia": pl.String,
              "costo_mxn": pl.Float64,
              "proveedor": pl.String,
          },
      ).with_columns([
          pl.col("fecha_vigencia").str.to_date("%Y-%m-%d", strict=False)
      ])
      results["dim_cost_history"] = df_costs

    # 4. Snapshots de Inventario (Hechos)
    if "snapshots" in raw_input and raw_input["snapshots"]:

        def safe_int(val, default=0):
            if val is None:
                return default
            val_str = str(val).strip()
            if val_str.upper() in ["N/A", "NULL", "NONE", ""]:
                return default
            try:
                return int(float(val_str))
            except (ValueError, TypeError):
                return default

        clean_snapshots = [
            {
                "fecha": str(s.get("fecha") or "")[:10],
                "tienda_id": str(s.get("tienda_id") or "").strip(),
                "sku_erp": str(s.get("sku_erp") or "").strip(),
                "cantidad_en_stock": safe_int(s.get("cantidad_en_stock")),
            }
            for s in raw_input["snapshots"]
        ]

        df_snap = pl.DataFrame(
            clean_snapshots,
            schema={
                "fecha": pl.String,
                "tienda_id": pl.String,
                "sku_erp": pl.String,
                "cantidad_en_stock": pl.Int64,
            },
        ).with_columns([
            pl.col("fecha").alias("dt"),
            pl.col("fecha").str.to_date("%Y-%m-%d", strict=False),
        ])
        results["fct_inventory_snapshots"] = df_snap

    return results


# =============================================================================
# 3. GENERACIÓN Y VALIDACIÓN DE ESQUEMAS (Schema Enforcer)
# =============================================================================
class SchemaEnforcer:
    """
    Define los contratos canónicos de tipos esperados para la capa Silver.
    """
    SCHEMAS = {
        "dim_tiendas": {
            "tienda_id": pl.String,
            "ciudad": pl.String,
            "region": pl.String,
            "timezone": pl.String,
        },
        "dim_sku_mappings": {
            "sku_pos": pl.String,
            "sku_erp": pl.String,
            "handle": pl.String,
        },
        "dim_productos": {
            "sku_erp": pl.String,
            "nombre": pl.String,
            "categoria": pl.String,
        },
        "dim_cost_history": {
            "sku_erp": pl.String,
            "fecha_vigencia": pl.Date,
            "costo_mxn": pl.Float64,
            "proveedor": pl.String,
        },
        "fct_inventory_snapshots": {
            "fecha": pl.Date,
            "dt": pl.String,
            "tienda_id": pl.String,
            "sku_erp": pl.String,
            "cantidad_en_stock": pl.Int64,
        },
        "fct_pos_sales": {
            "venta_id": pl.String,
            "fecha_hora": pl.Datetime,
            "dt": pl.String,
            "tienda_id": pl.String,
            "sku_pos": pl.String,
            "cantidad": pl.Int64,
            "monto": pl.Float64,
            "moneda": pl.String,
            "tipo_comprobante": pl.String,
        },
        "fct_ecommerce_orders": {
            "order_id": pl.String,
            "fecha": pl.Date,
            "dt": pl.String,
            "product_handle": pl.String,
            "cantidad": pl.Int64,
            "amount": pl.Float64,
            "currency": pl.String,
            "customer_name": pl.String,
            "customer_email": pl.String,
            "customer_rfc": pl.String,
            "shipping_city": pl.String,
            "shipping_address": pl.String,
        },
        "ref_exchange_rates": {
            "fecha": pl.Date,
            "currency": pl.String,
            "rate_to_mxn": pl.Float64,
        },
    }

    @classmethod
    def validate_and_cast(cls, table_name: str, df: pl.DataFrame) -> pl.DataFrame:
        expected = cls.SCHEMAS.get(table_name)
        if not expected:
            logger.warning(f"No hay contrato de esquema explícito para {table_name}. Se omite validación estricta.")
            return df

        missing_cols = set(expected.keys()) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Faltan columnas requeridas en {table_name}: {missing_cols}")

        cast_expressions = [pl.col(c).cast(t) for c, t in expected.items()]
        logger.info(f"Esquema verificado exitosamente para la tabla: {table_name}")
        return df.select(cast_expressions)


# =============================================================================
# 4. PERSISTENCIA EN SILVER (Data Writer)
# =============================================================================
class ParquetDataWriter:
    """
    Escribe los DataFrames procesados en Parquet con compresión Snappy.
    Maneja el particionamiento Hive construyendo rutas explícitas para
    evitar el colapso a 'dt=...' en AWS Glue / S3.
    """
    PARTITION_COLUMNS = {
        "fct_pos_sales": ["dt"],
        "fct_ecommerce_orders": ["dt"],
        "fct_inventory_snapshots": ["dt"],
    }

    def __init__(self, base_output_uri: str):
        self.base_output_uri = base_output_uri.rstrip("/")
        self.is_s3 = self.base_output_uri.startswith("s3://")

    def write(self, table_name: str, df: pl.DataFrame):
        output_path = f"{self.base_output_uri}/{table_name}"
        logger.info(f"Guardando {table_name} ({df.height:,} filas) en: {output_path}")

        partition_cols = self.PARTITION_COLUMNS.get(table_name)

        if partition_cols:
            part_col = partition_cols[0]

            # 1. Extraer los valores unicos reales de la columna de particion
            unique_partitions = (
                df.select(pl.col(part_col).cast(pl.String))
                .unique()
                .to_series()
                .drop_nulls()
                .to_list()
            )

            logger.info(f"Escribiendo {len(unique_partitions)} particiones para {table_name}...")

            # 2. Escribir cada particion con ruta explicita
            for part_val in unique_partitions:
                # Blindaje contra cadenas vacias, espacios o elipsis
                part_val_clean = str(part_val).strip()
                if not part_val_clean or part_val_clean == "...":
                    continue

                # Filtrar el bloque de datos y remover la columna de particion del archivo fisico
                part_df = df.filter(pl.col(part_col) == part_val).drop(part_col)
                target_file = f"{output_path}/{part_col}={part_val_clean}/data.parquet"

                if self.is_s3:
                    # Polars escribe directo a S3 resolviendo credenciales del entorno
                    part_df.write_parquet(target_file, compression="snappy")
                else:
                    os.makedirs(os.path.dirname(target_file), exist_ok=True)
                    part_df.write_parquet(target_file, compression="snappy")
        else:
            # Tablas no particionadas (dim_*, ref_*)
            target_file = f"{output_path}/data.parquet"
            if not self.is_s3:
                os.makedirs(os.path.dirname(target_file), exist_ok=True)
            df.write_parquet(target_file, compression="snappy")

        logger.info(f"[OK] Tabla {table_name} persistida exitosamente.")
        
# =============================================================================
# 5. ORQUESTADOR DEL PIPELINE (Pipeline Orchestrator)
# =============================================================================
class RawToSilverPipeline:
    """
    Coordina la ejecución de extremo a extremo: lectura -> transformación -> tipado -> escritura.
    """
    def __init__(self, raw_base_uri: str, silver_base_uri: str, target_date: str):
        self.raw_base = raw_base_uri.rstrip("/")
        self.silver_base = silver_base_uri.rstrip("/")
        self.target_date = target_date
        self.reader = S3LocalReader()
        print(f"[DEBUG] Raw base URI: {self.raw_base}, Silver base URI: {self.silver_base}")
        self.writer = ParquetDataWriter(self.silver_base)

    def run(self):
        logger.info("Iniciando Pipeline Raw -> Silver...")
        execution_tasks = [
            {
                "name": "POS Sales",
                "uri": f"{self.raw_base}/pos/dt={self.target_date}/sales.csv",
                "processor": POSSalesProcessor(),
            },
            {
                "name": "Ecommerce Orders",
                "uri": f"{self.raw_base}/shopify/dt={self.target_date}/ecommerce_orders.parquet",
                "processor": EcommerceOrdersProcessor(),
            },
            {
                "name": "Exchange Rates",
                "uri": f"{self.raw_base}/exchange_rates/exchange_rates.csv",
                "processor": ExchangeRatesProcessor(),
            },
            {
                "name": "Inventory Monolith",
                "uri": f"{self.raw_base}/inventory/dt={self.target_date}/inventory.json",
                "processor": InventoryJSONProcessor(),
            },
        ]

        errors = []
        for task in execution_tasks:
            task_name = task["name"]
            logger.info(f"--- Procesando artefacto: {task_name} ---")
            try:
                raw_data = self.reader.read(task["uri"])
                tables = task["processor"].process(raw_data)

                for table_name, df in tables.items():
                    validated_df = SchemaEnforcer.validate_and_cast(table_name, df)
                    self.writer.write(table_name, validated_df)

                logger.info(f"Artefacto {task_name} completado con éxito.")
            except Exception as e:
                logger.error(f"Error procesando {task_name}: {str(e)}", exc_info=True)
                errors.append((task_name, str(e)))

        if errors:
            logger.error(f"El pipeline finalizó con {len(errors)} error(es): {errors}")
            sys.exit(1)
        else:
            logger.info("Pipeline Raw -> Silver finalizado exitosamente sin fallos.")


# =============================================================================
# PUNTO DE ENTRADA (CLI & AWS GLUE JOB ENTRYPOINT)
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="Raw to Silver ETL Pipeline")
    parser.add_argument("--LAKEHOUSE_BUCKET", type=str, default=None, help="Nombre del bucket S3")
    parser.add_argument("--RAW_PREFIX", type=str, default="raw/", help="Prefijo origen Raw")
    parser.add_argument("--SILVER_PREFIX", type=str, default="silver/", help="Prefijo destino Silver")
    parser.add_argument("--LOCAL_RAW_PATH", type=str, default=None, help="Ruta local Raw (opcional)")
    parser.add_argument("--LOCAL_SILVER_PATH", type=str, default=None, help="Ruta local Silver (opcional)")
    parser.add_argument("--TARGET_DATE", type=str, required=True, help="Fecha objetivo (YYYY-MM-DD) para particionar los datos")

    # Captura argumentos estándar de AWS Glue (los cuales vienen precedidos por --)
    args, unknown = parser.parse_known_args()
    
    # Extrae la fecha objetivo para particionar los datos
    target_date = args.TARGET_DATE
    
    # Si se pasan rutas locales, corre en modo local para pruebas inmediatas
    if args.LOCAL_RAW_PATH and args.LOCAL_SILVER_PATH:
        raw_uri = args.LOCAL_RAW_PATH
        silver_uri = args.LOCAL_SILVER_PATH
    elif args.LAKEHOUSE_BUCKET:
        raw_uri = f"s3://{args.LAKEHOUSE_BUCKET}/{args.RAW_PREFIX.strip('/')}"
        silver_uri = f"s3://{args.LAKEHOUSE_BUCKET}/{args.SILVER_PREFIX.strip('/')}"
    else:
        # Fallback para entorno de desarrollo en este repo
        raw_uri = "data"
        silver_uri = "data/silver"

    print(f"[DEBUG] Raw URI: {raw_uri}, Silver URI: {silver_uri}")
    pipeline = RawToSilverPipeline(raw_base_uri=raw_uri, silver_base_uri=silver_uri, target_date=target_date)
    pipeline.run()


if __name__ == "__main__":
    main()