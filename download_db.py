import os
import pathlib
import boto3

def must_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val

def main() -> None:
    # Dónde guardar el fichero en el contenedor
    db_path = pathlib.Path(os.getenv("DUCKDB_PATH", "/app/data/bicimad.duckdb"))
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Si ya existe y pesa > 0, no descargamos de nuevo
    if db_path.exists() and db_path.stat().st_size > 0:
        print(f"[download_db] DB already exists at {db_path} ({db_path.stat().st_size} bytes)")
        return

    endpoint = must_env("S3_ENDPOINT")
    access_key = must_env("S3_ACCESS_KEY_ID")
    secret_key = must_env("S3_SECRET_ACCESS_KEY")
    bucket = must_env("S3_BUCKET")
    key = must_env("S3_OBJECT_KEY")

    print(f"[download_db] Downloading s3://{bucket}/{key} -> {db_path}")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.getenv("S3_REGION", "auto"),
    )

    s3.download_file(bucket, key, str(db_path))

    if not db_path.exists() or db_path.stat().st_size == 0:
        raise RuntimeError("[download_db] Download finished but file is missing/empty")

    print(f"[download_db] Download completed: {db_path} ({db_path.stat().st_size} bytes)")

if __name__ == "__main__":
    main()
