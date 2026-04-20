import json
from pathlib import Path
from prefect import flow, get_run_logger
from prefect_dask import DaskTaskRunner
from minio import Minio
from minio.error import S3Error
import faiss

from tasks.process import process_paper
from tasks.index import build_index
from config import (
    DASK_SCHEDULER_URL,
    MINIO_HOST,
    MINIO_PORT,
    MINIO_ROOT_USER,
    MINIO_ROOT_PASSWORD,
    MINIO_BUCKET_NAME,
    FAISS_INDEX_PATH
)

def _get_minio_client() -> Minio:
    return Minio(
        f"{MINIO_HOST}:{MINIO_PORT}",
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False
    )

def _stream_paper_ids(paper_ids_key: str, max_papers: int) -> list:
    """
    Stream arXiv metadata from MinIO line by line, extracting paper IDs up to max_papers.
    """
    try:
        client = _get_minio_client()
        response = client.get_object(MINIO_BUCKET_NAME, paper_ids_key)
        
        paper_ids = []
        for line in response:
            if len(paper_ids) >= max_papers:
                break
            line = line.decode('utf-8').strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
                if isinstance(parsed, list):
                    # Plain JSON array format ["id1", "id2", ...]
                    paper_ids.extend(parsed[:max_papers - len(paper_ids)])
                    break
                elif isinstance(parsed, dict):
                    # JSONL format {"id": "...", ...}
                    paper_ids.append(parsed['id'])
            except json.JSONDecodeError:
                continue
        
        response.close()
        response.release_conn()
        return paper_ids

    except S3Error as e:
        return []

@flow(
    name="indexing-flow",
    task_runner=DaskTaskRunner(address=DASK_SCHEDULER_URL)
)
def indexing_flow(paper_ids_key: str = "papers/arxiv-metadata.json", max_papers: int = 20000):
    """
    Main indexing flow. Downloads arXiv metadata from MinIO,
    extracts paper IDs, distributes processing across Dask workers,
    then builds FAISS index.

    Args:
        paper_ids_key: MinIO object key for arXiv metadata JSONL
        max_papers: maximum number of papers to index
    """
    logger = get_run_logger()

    # Stream load paper IDs
    logger.info(f"Stream loading metadata from MinIO: {paper_ids_key}")
    paper_ids = _stream_paper_ids(paper_ids_key, max_papers)
    if not paper_ids:
        logger.error(f"Failed to load metadata from MinIO: {paper_ids_key}")
        return None
    logger.info(f"Loaded {len(paper_ids)} paper IDs (max: {max_papers})")

    # Submit all papers to Dask workers in parallel
    futures = [process_paper.submit(pid) for pid in paper_ids]
    logger.info(f"Submitted {len(futures)} tasks to Dask cluster")

    # Wait for all tasks to complete
    results = [f.result() for f in futures]

    successful = sum(1 for r in results if r is True)
    failed = len(results) - successful
    logger.info(f"Processing complete: {successful} succeeded, {failed} failed")

    # --------------------------------------------------------------------------
    # Build FAISS index on host
    logger.info("Building FAISS index...")
    success = build_index()

    if success:
        index = faiss.read_index(FAISS_INDEX_PATH)
        logger.info(f"Index built successfully — {index.ntotal} papers indexed")
    else:
        logger.error("Index build failed")

    return {"successful": successful, "failed": failed}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper-ids-key", default="papers/arxiv-metadata.json")
    parser.add_argument("--max-papers", type=int, default=50000)
    args = parser.parse_args()

    indexing_flow(
        paper_ids_key=args.paper_ids_key,
        max_papers=args.max_papers
    )