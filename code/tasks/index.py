import io
import json
import numpy as np
import faiss
from typing import Optional
from minio import Minio
from minio.error import S3Error
from config import (
    MINIO_HOST,
    MINIO_PORT,
    MINIO_ROOT_USER,
    MINIO_ROOT_PASSWORD,
    MINIO_BUCKET_NAME,
    EMBEDDING_DIM,
    FAISS_INDEX_PATH,
    METADATA_PATH,
)

def _get_client() -> Minio:
    return Minio(
        f"{MINIO_HOST}:{MINIO_PORT}",
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False
    )


def build_index() -> bool:
    """
    Download all embeddings from MinIO, build a FAISS index,
    and save it to disk along with a metadata mapping.

    Returns:
        True if successful, False otherwise
    """
    try:
        client = _get_client()

        # List all embedding objects in bucket
        objects = list(client.list_objects(MINIO_BUCKET_NAME, prefix="embeddings/"))

        if not objects:
            print("No embeddings found in MinIO")
            return False

        print(f"Found {len(objects)} embeddings, building index...")

        embeddings = []
        paper_ids = []

        for obj in objects:
            try:
                response = client.get_object(MINIO_BUCKET_NAME, obj.object_name)
                data = response.read()
                response.close()
                response.release_conn()

                # Deserialise numpy array
                buffer = io.BytesIO(data)
                embedding = np.load(buffer)

                # Extract paper ID from object key
                paper_id = obj.object_name \
                    .replace("embeddings/", "") \
                    .replace(".npy", "") \
                    .replace("_", "/", 1)

                embeddings.append(embedding)
                paper_ids.append(paper_id)

            except Exception as e:
                print(f"Failed to load embedding {obj.object_name}: {e}")
                continue

        if not embeddings:
            print("No embeddings could be loaded")
            return False

        # Stack into 2D array (N, EMBEDDING_DIM)
        embeddings_matrix = np.stack(embeddings).astype(np.float32)

        # Build FAISS index
        index = faiss.IndexFlatIP(EMBEDDING_DIM)
        index.add(embeddings_matrix)

        # Save index to disk
        faiss.write_index(index, FAISS_INDEX_PATH)
        print(f"FAISS index saved to {FAISS_INDEX_PATH}")

        # Save paper ID mapping to disk
        # Maps index position -> paper ID
        with open(METADATA_PATH, "w") as f:
            json.dump(paper_ids, f)
        print(f"Metadata saved to {METADATA_PATH}")

        return True

    except S3Error as e:
        print(f"MinIO error during index build: {e}")
        return False

    except Exception as e:
        print(f"Unexpected error during index build: {e}")
        return False