import argparse
from pathlib import Path
import shutil
import boto3

# --------------------------------------------------------------------------
# CONFIGURACIÓN
# --------------------------------------------------------------------------
BUCKET_NAME = "cafenorte-lakehouse-dev-496969872817"
STATE_MACHINE_ARN = (
    "arn:aws:states:us-east-1:496969872817:stateMachine:"
    "cafenorte-lakehouse-orchestrator-dev"
)

# Rutas locales
BASE_DATA_DIR = Path("data")
RAW_ORIGINAL_DIR = Path("data")
MOCK_DIR = Path("data/data_mock_generated")
LOCAL_RAW_TARGET_DIR = Path("data/raw")

FILE_ROUTING = {
    "sales.csv": "raw/pos",
    "ecommerce_orders.parquet": "raw/shopify",
    "inventory.json": "raw/inventory",
    "exchange_rates.csv": "raw/exchange_rates",
}


def sync_batch(target_date: str, env: str = "dev", trigger_execution: bool = False):
    # 1. Determinar directorio de origen y partición
    if target_date in ("previous", "initial", "historical"):
        source_dir = RAW_ORIGINAL_DIR
        partition_label = target_date
    else:
        source_dir = MOCK_DIR / f"dt={target_date}"
        partition_label = target_date

    print(f"[DEBUG] Target date: {target_date}")
    print(f"[DEBUG] Entorno configurado (--ENV): {env}")
    print(f"[DEBUG] Origen: {source_dir} -> Etiqueta partición: {partition_label}")

    if not source_dir.exists():
        raise FileNotFoundError(f"No existe el directorio de origen: {source_dir}")

    s3_client = boto3.client("s3") if env == "dev" else None

    # 2. Replicar siempre en local (data/raw/...) manteniendo la misma estructura
    print(f"\n[1/3] Actualizando carpeta local '{LOCAL_RAW_TARGET_DIR}'...")
    for file_name, prefix in FILE_ROUTING.items():
        # Evitar buscar dentro de la propia carpeta destino data/raw/
        matches = [
            f for f in source_dir.rglob(file_name)
            if not str(f.resolve()).startswith(str(LOCAL_RAW_TARGET_DIR.resolve()))
        ]
        if not matches:
            print(f"  [WARN] Archivo {file_name} no encontrado en {source_dir}")
            continue

        local_src_file = matches[0]

        if file_name == "exchange_rates.csv":
            dest_rel_path = Path(prefix) / file_name
        else:
            dest_rel_path = Path(prefix) / f"dt={partition_label}" / file_name

        # Mapea prefix 'raw/...' relativo a BASE_DATA_DIR ('data/raw/...')
        final_dest_file = BASE_DATA_DIR / dest_rel_path
        final_dest_file.parent.mkdir(parents=True, exist_ok=True)

        if local_src_file.resolve() != final_dest_file.resolve():
            shutil.copy2(local_src_file, final_dest_file)
            print(f"  [LOCAL COPY] -> {final_dest_file}")

    # 3. Acciones en AWS si ENV es 'dev'
    if env == "dev":
        print(f"\n[2/3] Subiendo lote '{partition_label}' a S3 (Bucket: {BUCKET_NAME})...")
        for file_name, s3_prefix in FILE_ROUTING.items():
            matches = [
                f for f in source_dir.rglob(file_name)
                if not str(f.resolve()).startswith(str(LOCAL_RAW_TARGET_DIR.resolve()))
            ]
            if not matches:
                continue

            local_file = matches[0]

            if file_name == "exchange_rates.csv":
                s3_key = f"{s3_prefix}/{file_name}"
            else:
                s3_key = f"{s3_prefix}/dt={partition_label}/{file_name}"

            file_size_mb = local_file.stat().st_size / (1024 * 1024)
            print(f"  Subiendo: {local_file.name} ({file_size_mb:.2f} MB) -> s3://{BUCKET_NAME}/{s3_key}")
            s3_client.upload_file(str(local_file), BUCKET_NAME, s3_key)

        print("[OK] S3 actualizado exitosamente.")

        # 4. Orquestación opcional en Step Functions
        if trigger_execution:
            print(f"\n[3/3] Disparando Step Function para partición: {partition_label}...")
            sfn_client = boto3.client("stepfunctions")
            input_payload = f'{{"target_date": "{partition_label}", "retry_count": 0}}'

            response = sfn_client.start_execution(
                stateMachineArn=STATE_MACHINE_ARN, input=input_payload
            )
            print(f"[OK] Ejecución iniciada. ARN: {response['executionArn']}")
        else:
            print("\n[INFO] Parámetro --trigger no especificado; omite disparo de Step Functions.")
    else:
        print("\n[INFO] Modo local activo: datos copiados en disco sin subida a AWS.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gestor de ingesta y sincronización de datos Raw (Local y AWS S3)."
    )
    parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="Fecha (YYYY-MM-DD) o etiqueta de carga base (previous / initial)",
    )
    parser.add_argument(
        "--ENV",
        type=str,
        choices=["local", "dev"],
        default="dev",
        help="Entorno de destino: 'local' (solo actualiza data/raw/) o 'dev' (actualiza local y S3)",
    )
    parser.add_argument(
        "--trigger",
        action="store_true",
        help="Dispara automáticamente la Step Function tras subir los datos (solo aplica con --ENV dev)",
    )

    args = parser.parse_args()
    sync_batch(args.date, env=args.ENV, trigger_execution=args.trigger)