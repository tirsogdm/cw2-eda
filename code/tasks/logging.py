import io
from minio import Minio
from minio.error import S3Error
from datetime import datetime, timezone
from config import MINIO_HOST, MINIO_PORT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_BUCKET_NAME

def _get_minio_client() -> Minio:
    return Minio(
        f"{MINIO_HOST}:{MINIO_PORT}",
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False
    )

def append_batch_log(run_id: str, message: str) -> None:
    try:
        client = _get_minio_client()
        key = f"logs/{run_id}.txt"
        try:
            response = client.get_object(MINIO_BUCKET_NAME, key)
            existing = response.read().decode('utf-8')
            response.close()
            response.release_conn()
        except:
            existing = ""
        
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        new_content = f"{timestamp} — {message}\n" + existing
        data = new_content.encode('utf-8')
        client.put_object(MINIO_BUCKET_NAME, key, io.BytesIO(data), len(data))
    except Exception:
        pass