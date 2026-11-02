import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path
from typing import Dict, Optional
import polars as pl

# =============================================================================
# LOGGING ESTRUCTURADO
# =============================================================================
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
# 1. LAKE CONNECTIVITY MANAGER
# =============================================================================
class LakeConnectivityManager:
    """
    Verifica precondiciones de conectividad, existencia de rutas
    y metadatos en AWS Glue / Lake Formation o filesystem local.
    """
    REQUIRED_SILVER_TABLES = [
        "dim_tiendas",
        "dim_sku_mappings",
        "dim_productos",
        "dim_cost_history",
        "fct_inventory_snapshots",
        "fct_pos_sales",
        "fct_ecommerce_orders",
        "ref_exchange_rates",
    ]

    def __init__(self, silver_base_uri: str, gold_base_uri: str, glue_db: Optional[str] = None):
        self.silver_base = silver_base_uri.rstrip("/")
        self.gold_base = gold_base_uri.rstrip("/")
        self.glue_db = glue_db
        self.is_s3 = self.silver_base.startswith("s3://")

    def validate_preconditions(self) -> bool:
        logger.info("Iniciando Pre-flight Check de conectividad e infraestructura...")
        
        # 1. Validación de almacenamiento
        if self.is_s3:
            import boto3
            from botocore.exceptions import ClientError
            
            s3 = boto3.client("s3")
            bucket = self.silver_base.replace("s3://", "").split("/")[0]
            prefix = "/".join(self.silver_base.replace("s3://", "").split("/")[1:])
            
            try:
                logger.info(f"Validando acceso a bucket S3: {bucket} en prefijo: {prefix}/")
                s3.head_bucket(Bucket=bucket)
                logger.info("✔ Conectividad con S3 exitosa.")
            except ClientError as e:
                logger.error(f"Fallo de conexión o permisos con S3 ({bucket}): {e}")
                return False

            # Validación de Glue Catalog
            if self.glue_db:
                glue = boto3.client("glue")
                try:
                    logger.info(f"Validando catálogo Glue para base de datos: {self.glue_db}")
                    glue.get_database(Name=self.glue_db)
                    logger.info(f"✔ Catálogo Glue disponible: {self.glue_db}")
                except ClientError as e:
                    logger.warning(f"Aviso en Glue Catalog: {e}. Se continuará validando a nivel S3.")
        else:
            logger.info(f"Validando existencia de directorios Silver en entorno local: {self.silver_base}")
            base_path = Path(self.silver_base)
            if not base_path.exists():
                logger.error(f"El directorio base Silver no existe: {base_path}")
                return False

            missing = []
            for table in self.REQUIRED_SILVER_TABLES:
                if not (base_path / table).exists():
                    missing.append(table)
            
            if missing:
                logger.error(f"Faltan tablas requeridas en Silver: {missing}")
                return False
            logger.info("✔ Todas las tablas requeridas de Silver están presentes físicamente en local.")

        return True


# =============================================================================
# 2. SILVER DATA READER
# =============================================================================
class SilverDataReader:
    """
    Abstrae la lectura y escaneo de tablas Parquet desde S3 o almacenamiento local.
    """
    def __init__(self, silver_base_uri: str, target_date: str):
        self.silver_base = silver_base_uri.rstrip("/")
        self.target_date = target_date

    def read_table(self, table_name: str) -> pl.DataFrame:
        print(f"[DEBUG] Leyendo tabla Silver: {table_name} desde {self.silver_base} para fecha objetivo: {self.target_date}")

        target_path = f"{self.silver_base}/{table_name}"
        logger.info(f"Cargando tabla Silver: {table_name}")
        
        # Lectura transparente usando wildcard recursivo para soportar carpetas particionadas o archivos directos
        try:
            if target_path.startswith("s3://"):
                df = pl.read_parquet(f"{target_path}/dt={self.target_date}/*.parquet")    # <- Deuda técnica: tablas dim no son indexadas por fecha, pero fct si.
            else:
                p = Path(target_path)
                parquet_files = list(p.rglob("*.parquet"))
                if not parquet_files:
                    raise FileNotFoundError(f"No se encontraron archivos .parquet en {target_path}")
                df = pl.read_parquet(target_path)
            
            logger.info(f"  └─ {table_name}: {df.height:,} filas cargadas.")
            return df
        except Exception as e:
            logger.error(f"Error al leer la tabla {table_name}: {e}")
            raise


# =============================================================================
# 3. SILVER DATA VALIDATOR
# =============================================================================
class SilverDataValidator:
    """
    Verifica contratos mínimos de integridad referencial y nulidad antes de transformar.
    """
    @staticmethod
    def validate(tables: Dict[str, pl.DataFrame]) -> bool:
        logger.info("Validando integridad de datos en capa Silver...")
        try:
            # Validar que mappings tenga llaves no vacías
            df_mappings = tables["dim_sku_mappings"]
            null_erp = df_mappings.filter(pl.col("sku_erp").is_null()).height
            if null_erp > 0:
                logger.warning(f"Se encontraron {null_erp} registros con sku_erp nulo en dim_sku_mappings.")

            # Validar tasas de cambio
            df_fx = tables["ref_exchange_rates"]
            invalid_fx = df_fx.filter((pl.col("rate_to_mxn").is_null()) | (pl.col("rate_to_mxn") <= 0)).height
            if invalid_fx > 0:
                raise ValueError(f"Existen {invalid_fx} tasas de cambio con valores nulos o <= 0.")

            # Validar unicidad de snapshots para evitar duplicados en stock
            df_snap = tables["fct_inventory_snapshots"]
            total_snaps = df_snap.height
            unique_snaps = df_snap.select(["fecha", "tienda_id", "sku_erp"]).unique().height
            if total_snaps != unique_snaps:
                logger.warning(f"Se detectaron snapshots duplicados ({total_snaps - unique_snaps} registros redundantes).")

            logger.info("Validación de contratos de datos Silver completada.")
            return True
        except Exception as e:
            logger.error(f"Fallo en validación de datos Silver: {e}")
            return False


# =============================================================================
# 4. TRANSFORMADORES A GOLD (Lógica de Negocio y Métricas)
# =============================================================================
class UnifiedSalesTransformer:
    """
    Unifica POS y Shopify, resuelve mapeos a sku_erp, convierte divisas a MXN
    y cruza costos históricos (SCD Tipo 2) para calcular el margen bruto.
    """
    @staticmethod
    def transform(
        df_pos: pl.DataFrame,
        df_ecom: pl.DataFrame,
        df_mappings: pl.DataFrame,
        df_fx: pl.DataFrame,
        df_costs: pl.DataFrame,
    ) -> pl.DataFrame:
        logger.info("Transformando y unificando ventas (POS + Shopify) hacia fct_ventas_unificadas...")

        # 1. Homologar Ventas POS
        pos_unified = (
            df_pos.join(df_mappings, on="sku_pos", how="left")
            .with_columns([
                pl.col("fecha_hora").dt.date().alias("fecha"),
                pl.lit("POS").alias("canal"),
                pl.col("sku_erp").fill_null(pl.col("sku_pos")),  # Fallback si no hay mapeo
                pl.col("monto").alias("monto_original"),
                pl.col("moneda").alias("moneda_original"),
            ])
            .select([
                pl.col("venta_id").alias("transaccion_id"),
                "fecha",
                "canal",
                "tienda_id",
                "sku_erp",
                "cantidad",
                "monto_original",
                "moneda_original",
            ])
        )

        # 2. Homologar Ventas Ecommerce
        # Cruce por product_handle hacia handle en dim_sku_mappings
        ecom_unified = (
            df_ecom.join(df_mappings, left_on="product_handle", right_on="handle", how="left")
            .with_columns([
                pl.lit("ECOMMERCE").alias("canal"),
                pl.lit("TIENDA_ONLINE").alias("tienda_id"),
                pl.col("sku_erp").fill_null(pl.col("product_handle")),
                pl.col("amount").alias("monto_original"),
                pl.col("currency").alias("moneda_original"),
            ])
            .select([
                pl.col("order_id").alias("transaccion_id"),
                "fecha",
                "canal",
                "tienda_id",
                "sku_erp",
                "cantidad",
                "monto_original",
                "moneda_original",
            ])
        )

        # 3. Concatenar transacciones de ambos canales
        ventas_totales = pl.concat([pos_unified, ecom_unified])

        # 4. Conversión de divisas a MXN
        ventas_con_fx = (
            ventas_totales.join(
                df_fx.select(["fecha", "currency", "rate_to_mxn"]),
                left_on=["fecha", "moneda_original"],
                right_on=["fecha", "currency"],
                how="left",
            )
            .with_columns([
                pl.when(pl.col("moneda_original") == "MXN")
                .then(1.0)
                .otherwise(pl.col("rate_to_mxn").fill_null(1.0))
                .alias("factor_fx")
            ])
            .with_columns([
                (pl.col("monto_original") * pl.col("factor_fx")).round(2).alias("monto_mxn")
            ])
        )

        # 5. Cruce As-Of con dim_cost_history (SCD Tipo 2) para obtener costo vigente
        # Ordenamos ambos datasets por fecha para habilitar join_asof
        ventas_sorted = ventas_con_fx.sort(["sku_erp", "fecha"])
        costs_sorted = df_costs.sort(["sku_erp", "fecha_vigencia"])

        ventas_con_costo = ventas_sorted.join_asof(
            costs_sorted,
            left_on="fecha",
            right_on="fecha_vigencia",
            by="sku_erp",
            strategy="backward",  # Selecciona el costo con fecha_vigencia <= fecha_venta
            check_sortedness = False # Se desativa la verificación porque ya se ordenó previamente
        ).with_columns([
            pl.col("costo_mxn").fill_null(0.0).alias("costo_unitario_mxn"),
            (pl.col("cantidad") * pl.col("costo_mxn").fill_null(0.0)).round(2).alias("costo_total_mxn"),
        ]).with_columns([
            (pl.col("monto_mxn") - pl.col("costo_total_mxn")).round(2).alias("margen_bruto_mxn"),
            pl.col("fecha").dt.strftime("%Y-%m").alias("year_month"),
        ])

        return ventas_con_costo.select([
            "transaccion_id",
            "fecha",
            "year_month",
            "canal",
            "tienda_id",
            "sku_erp",
            "cantidad",
            "monto_original",
            "moneda_original",
            "monto_mxn",
            "costo_unitario_mxn",
            "costo_total_mxn",
            "margen_bruto_mxn",
        ])


class InventoryPerformanceTransformer:
    """
    Cruza snapshots de inventario con ventas diarias por tienda y SKU para
    habilitar análisis de rotación y detección de quiebres de stock.
    """
    @staticmethod
    def transform(
        df_snapshots: pl.DataFrame,
        df_ventas: pl.DataFrame,
        df_productos: pl.DataFrame,
    ) -> pl.DataFrame:
        logger.info("Transformando inventario y ventas hacia agg_inventario_diario...")

        # Agrupar ventas diarias físicas por tienda y SKU (el inventario físico es en tiendas)
        ventas_dia = (
            df_ventas.filter(pl.col("canal") == "POS")
            .group_by(["fecha", "tienda_id", "sku_erp"])
            .agg([
                pl.col("cantidad").sum().alias("unidades_vendidas_dia"),
                pl.col("monto_mxn").sum().alias("ingreso_ventas_dia"),
            ])
        )

        # Unir snapshot diario con ventas diarias
        agg_inv = (
            df_snapshots.join(
                ventas_dia,
                on=["fecha", "tienda_id", "sku_erp"],
                how="left",
            )
            .with_columns([
                pl.col("unidades_vendidas_dia").fill_null(0),
                pl.col("ingreso_ventas_dia").fill_null(0.0),
                pl.when(pl.col("cantidad_en_stock") == 0).then(1).otherwise(0).alias("is_stockout"),
                pl.col("fecha").dt.strftime("%Y-%m").alias("year_month"),
            ])
            .join(
                df_productos.select(["sku_erp", "nombre", "categoria"]),
                on="sku_erp",
                how="left",
            )
        )

        return agg_inv.select([
            "fecha",
            "year_month",
            "tienda_id",
            "sku_erp",
            pl.col("nombre").alias("producto_nombre"),
            pl.col("categoria").alias("producto_categoria"),
            "cantidad_en_stock",
            "unidades_vendidas_dia",
            "ingreso_ventas_dia",
            "is_stockout",
        ])


# =============================================================================
# 5. GOLD TABLE MANAGER (Escritura y Metadatos)
# =============================================================================
class GoldTableManager:
    """
    Gestiona la persistencia en formato Parquet particionado con compresión Snappy
    tanto en local como en AWS S3.
    """
    PARTITIONS = {
        "fct_ventas_unificadas": ["year_month"],
        "agg_inventario_diario": ["year_month"],
    }

    def __init__(self, gold_base_uri: str):
        self.gold_base = gold_base_uri.rstrip("/")
        self.is_s3 = self.gold_base.startswith("s3://")

    def write_table(self, table_name: str, df: pl.DataFrame):
        output_path = f"{self.gold_base}/{table_name}"
        logger.info(f"Escribiendo tabla Gold '{table_name}' ({df.height:,} filas) en: {output_path}")

        partition_cols = self.PARTITIONS.get(table_name)

        if partition_cols:
            part_col = partition_cols[0]

            # 1. Obtener los valores únicos de partición (ej. '2024-10', '2024-11')
            unique_partitions = (
                df.select(pl.col(part_col).cast(pl.String))
                .unique()
                .to_series()
                .drop_nulls()
                .to_list()
            )

            logger.info(f"Escribiendo {len(unique_partitions)} particiones para tabla Gold '{table_name}'...")

            # 2. Persistir cada partición con ruta explícita
            for part_val in unique_partitions:
                part_val_clean = str(part_val).strip()
                if not part_val_clean or part_val_clean == "...":
                    continue

                # Filtrar el bloque de la partición y retirar la columna del cuerpo del Parquet
                part_df = df.filter(pl.col(part_col) == part_val).drop(part_col)
                target_file = f"{output_path}/{part_col}={part_val_clean}/data.parquet"

                if self.is_s3:
                    part_df.write_parquet(target_file, compression="snappy")
                else:
                    os.makedirs(os.path.dirname(target_file), exist_ok=True)
                    part_df.write_parquet(target_file, compression="snappy")
        else:
            # Tablas agregadas o dimensiones en Gold sin particionamiento
            target_file = f"{output_path}/data.parquet"
            if not self.is_s3:
                os.makedirs(os.path.dirname(target_file), exist_ok=True)
            df.write_parquet(target_file, compression="snappy")

        logger.info(f"[OK] Tabla {table_name} guardada exitosamente en Gold.")


# =============================================================================
# 6. PIPELINE ORCHESTRATOR (Harness)
# =============================================================================
class SilverToGoldPipeline:
    """
    Orquestador principal del ciclo de vida del pipeline Silver -> Gold.
    """
    def __init__(self, silver_base_uri: str, gold_base_uri: str, target_date: str, glue_db: Optional[str] = None):
        self.silver_base = silver_base_uri
        self.gold_base = gold_base_uri
        self.glue_db = glue_db
        self.target_date = target_date

        self.connectivity = LakeConnectivityManager(silver_base_uri, gold_base_uri, glue_db)
        self.reader = SilverDataReader(silver_base_uri, self.target_date)
        self.validator = SilverDataValidator()
        self.writer = GoldTableManager(gold_base_uri)

    def run(self):
        logger.info("================================================================================")
        logger.info(" INICIANDO PIPELINE SILVER -> GOLD (CafeNorte Lakehouse) ")
        logger.info("================================================================================")

        # 1. Pre-flight check
        if not self.connectivity.validate_preconditions():
            logger.error("Abortando ejecución: fallaron las precondiciones de conectividad.")
            sys.exit(1)

        # 2. Extracción de tablas Silver
        try:
            tables = {
                "dim_tiendas": self.reader.read_table("dim_tiendas"),
                "dim_sku_mappings": self.reader.read_table("dim_sku_mappings"),
                "dim_productos": self.reader.read_table("dim_productos"),
                "dim_cost_history": self.reader.read_table("dim_cost_history"),
                "fct_inventory_snapshots": self.reader.read_table("fct_inventory_snapshots"),
                "fct_pos_sales": self.reader.read_table("fct_pos_sales"),
                "fct_ecommerce_orders": self.reader.read_table("fct_ecommerce_orders"),
                "ref_exchange_rates": self.reader.read_table("ref_exchange_rates"),
            }
        except Exception as e:
            logger.error(f"Error crítico en la extracción de tablas Silver: {e}", exc_info=True)
            sys.exit(1)

        # 3. Validación de contratos
        if not self.validator.validate(tables):
            logger.error("Abortando ejecución: inconsistencias en los datos Silver.")
            sys.exit(1)

        # 4. Transformaciones analíticas
        try:
            # Transformación 1: Ventas Unificadas
            df_ventas_unificadas = UnifiedSalesTransformer.transform(
                df_pos=tables["fct_pos_sales"],
                df_ecom=tables["fct_ecommerce_orders"],
                df_mappings=tables["dim_sku_mappings"],
                df_fx=tables["ref_exchange_rates"],
                df_costs=tables["dim_cost_history"],
            )

            # Transformación 2: Rendimiento de Inventario
            df_agg_inventario = InventoryPerformanceTransformer.transform(
                df_snapshots=tables["fct_inventory_snapshots"],
                df_ventas=df_ventas_unificadas,
                df_productos=tables["dim_productos"],
            )
        except Exception as e:
            logger.error(f"Error durante las transformaciones de negocio: {e}", exc_info=True)
            sys.exit(1)

        # 5. Carga y Persistencia en Gold
        try:
            self.writer.write_table("fct_ventas_unificadas", df_ventas_unificadas)
            self.writer.write_table("agg_inventario_diario", df_agg_inventario)
        except Exception as e:
            logger.error(f"Error al escribir en la capa Gold: {e}", exc_info=True)
            sys.exit(1)

        logger.info("================================================================================")
        logger.info(" PIPELINE SILVER -> GOLD FINALIZADO CON ÉXITO ")
        logger.info("================================================================================")


# =============================================================================
# ENTRYPOINT (CLI & AWS GLUE JOB)
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="Silver to Gold Lakehouse ETL")
    parser.add_argument("--LAKEHOUSE_BUCKET", type=str, default=None, help="Nombre del bucket S3")
    parser.add_argument("--SILVER_PREFIX", type=str, default="silver/", help="Prefijo origen Silver")
    parser.add_argument("--GOLD_PREFIX", type=str, default="gold/", help="Prefijo destino Gold")
    parser.add_argument("--GLUE_DATABASE", type=str, default="cafenorte_lakehouse_db", help="Base de datos Glue")
    parser.add_argument("--LOCAL_SILVER_PATH", type=str, default=None, help="Ruta local Silver")
    parser.add_argument("--LOCAL_GOLD_PATH", type=str, default=None, help="Ruta local Gold")
    parser.add_argument("--TARGET_DATE", type=str, required=True, help="Fecha objetivo (YYYY-MM-DD) para particionar los datos")

    args, unknown = parser.parse_known_args()

    # Extrae la fecha objetivo para particionar los datos
    target_date = args.TARGET_DATE

    # Selección dinámica de entorno local vs AWS S3
    if args.LOCAL_SILVER_PATH and args.LOCAL_GOLD_PATH:
        silver_uri = args.LOCAL_SILVER_PATH
        gold_uri = args.LOCAL_GOLD_PATH
        glue_db = None
    elif args.LAKEHOUSE_BUCKET:
        silver_uri = f"s3://{args.LAKEHOUSE_BUCKET}/{args.SILVER_PREFIX.strip('/')}"
        gold_uri = f"s3://{args.LAKEHOUSE_BUCKET}/{args.GOLD_PREFIX.strip('/')}"
        glue_db = args.GLUE_DATABASE
    else:
        # Fallback por defecto en local
        silver_uri = "data/silver"
        gold_uri = "data/gold"
        glue_db = None

    pipeline = SilverToGoldPipeline(
        silver_base_uri=silver_uri,
        gold_base_uri=gold_uri,
        glue_db=glue_db,
        target_date=target_date
    )
    pipeline.run()


if __name__ == "__main__":
    main()