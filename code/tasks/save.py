import io
import json
import numpy as np
from typing import Optional
from minio import Minio
from minio.error import S3Error
from config import (
    MINIO_HOST,
    MINIO_PORT,
    MINIO_ROOT_USER,
    MINIO_ROOT_PASSWORD,
    MINIO_BUCKET_NAME,
)

# MinIO client singleton
_client = None

def _get_client() -> Minio:
    global _client
    if _client is None:
        _client = Minio(
            f"{MINIO_HOST}:{MINIO_PORT}",
            access_key=MINIO_ROOT_USER,
            secret_key=MINIO_ROOT_PASSWORD,
            secure=False
        )
    return _client


def save_embedding(paper_id: str, embedding: np.ndarray) -> bool:
    """
    Save a paper embedding to MinIO.

    Args:
        paper_id: arXiv paper ID
        embedding: numpy array of shape (EMBEDDING_DIM,)

    Returns:
        True if successful, False otherwise
    """
    try:
        client = _get_client()

        # Serialise embedding to bytes
        buffer = io.BytesIO()
        np.save(buffer, embedding)
        buffer.seek(0)
        size = buffer.getbuffer().nbytes

        # Object key based on paper ID
        object_key = f"embeddings/{paper_id.replace('/', '_')}.npy"

        client.put_object(
            MINIO_BUCKET_NAME,
            object_key,
            buffer,
            size,
            content_type="application/octet-stream"
        )

        return True

    except S3Error as e:
        print(f"MinIO upload failed for {paper_id}: {e}")
        return False

    except Exception as e:
        print(f"Unexpected error saving embedding for {paper_id}: {e}")
        return False