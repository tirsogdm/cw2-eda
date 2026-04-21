import json
from pathlib import Path
from prefect import flow, get_run_logger
from prefect.runtime import flow_run as flow_run_ctx
from prefect_dask import DaskTaskRunner
import faiss

import io
from minio import Minio
from minio.error import S3Error
from datetime import datetime, timezone

from tasks.process import process_paper
from tasks.index import build_index
from tasks.logging import append_batch_log
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
    run_id = flow_run_ctx.id

    # Stream load paper IDs
    logger.info(f"Stream loading metadata from MinIO: {paper_ids_key}")
    paper_ids = _stream_paper_ids(paper_ids_key, max_papers)
    if not paper_ids:
        logger.error(f"Failed to load metadata from MinIO: {paper_ids_key}")
        return None
    logger.info(f"Loaded {len(paper_ids)} paper IDs (max: {max_papers})")

    # Distribute in batches
    results = []
    batch_size = 5000
    batch_count = 1
    total_successful = 0
    total_failed = 0
    total_batches = (len(paper_ids) + batch_size - 1) // batch_size
    
    for i in range(0, len(paper_ids), batch_size):
        batch = paper_ids[i:i+batch_size]
        logger.info(f"[batch] {batch_count}/{total_batches} starting: submitting {len(batch)} tasks")
        append_batch_log(run_id, f"[batch] {batch_count}/{total_batches} starting...")
        append_batch_log(run_id, f"[batch] {batch_count}/{total_batches} submitting {len(batch)} tasks to cluster...")

        batch_futures = [process_paper.submit(pid) for pid in batch]
        
        batch_results = [f.result() for f in batch_futures]
        successful = sum(1 for r in batch_results if r is True)
        failed = len(batch_results) - successful
        
        total_successful += successful
        total_failed += failed
        results.extend(batch_results)

        logger.info(
            f"[batch] {batch_count}/{total_batches} complete — "
            f"batch: {successful} succeeded, {failed} failed | "
            f"running total: {total_successful} succeeded, {total_failed} failed"
        )

        append_batch_log(run_id, f"[batch] {batch_count}/{total_batches} complete.")
        append_batch_log(run_id, f"[batch] {batch_count}/{total_batches} {successful} succeeded, {failed} failed")
        append_batch_log(run_id, f"[batch] {batch_count}/{total_batches} running total: {total_successful} succeeded, {total_failed} failed")

        batch_count += 1

    # --------------------------------------------------------------------------
    # Build FAISS index on host
    logger.info("Building FAISS index...")
    success = build_index()

    if success:
        index = faiss.read_index(FAISS_INDEX_PATH)
        logger.info(f"Index built successfully — {index.ntotal} papers indexed")
    else:
        logger.error("Index build failed")

    return {"successful": total_successful, "failed": total_failed}


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