import os

# Paths
DATA_DIR = os.getenv("DATA_DIR", "/opt/literature-sem-search/data")
METADATA_PATH = os.getenv("METADATA_PATH", f"{DATA_DIR}/metadata.json")

# arXiv
ARXIV_EXPORT_URL = "https://export.arxiv.org/pdf"
PDF_DOWNLOAD_TIMEOUT = 30
RATE_LIMIT_SLEEP = 1

# GROBID
GROBID_HOST = os.getenv("GROBID_HOST", "localhost")
GROBID_PORT = int(os.getenv("GROBID_PORT", "8070"))

# MinIO
MINIO_HOST = os.getenv("MINIO_HOST", "controller-node")
MINIO_PORT = int(os.getenv("MINIO_PORT", "9000"))
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "lit-sem-search-bucket")

# Embedding model
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
EMBEDDING_DIM = 384
CHUNK_SIZE = 256
CHUNK_OVERLAP = 32

# FAISS
FAISS_INDEX_PATH = os.getenv("FAISS_INDEX_PATH", f"{DATA_DIR}/faiss.index")
