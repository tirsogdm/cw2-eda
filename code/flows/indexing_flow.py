import json
from pathlib import Path
from typing import Optional
from prefect import flow, task, get_run_logger
from prefect_dask import DaskTaskRunner
import tempfile

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tasks.fetch import fetch_pdf
from tasks.extract import extract_text
from tasks.embed import generate_embedding
from tasks.save import save_embedding
from tasks.index import build_index
from config import DASK_SCHEDULER_URL


@task(retries=2, retry_delay_seconds=30)
def process_paper(paper_id: str) -> Optional[bool]:
    """
    Full per-paper pipeline: fetch -> extract -> embed -> save.
    Runs on a Dask worker.

    Args:
        paper_id: arXiv paper ID

    Returns:
        True if successful, None if any stage failed
    """
    logger = get_run_logger()

    with tempfile.TemporaryDirectory() as tmpdir:
        # Stage 1: Fetch
        pdf_path = fetch_pdf(paper_id, tmpdir)
        if pdf_path is None:
            logger.warning(f"Fetch failed for {paper_id}, skipping")
            return None

        # Stage 2: Extract
        text = extract_text(pdf_path)
        if text is None:
            logger.warning(f"Extraction failed for {paper_id}, skipping")
            return None

    # Stage 3: Embed
    embedding = generate_embedding(text)
    if embedding is None:
        logger.warning(f"Embedding failed for {paper_id}, skipping")
        return None

    # Stage 4: Save
    success = save_embedding(paper_id, embedding)
    if not success:
        logger.warning(f"Save failed for {paper_id}, skipping")
        return None

    logger.info(f"Successfully processed {paper_id}")
    return True


@flow(
    name="indexing-flow",
    task_runner=DaskTaskRunner(address=DASK_SCHEDULER_URL)
)
def indexing_flow(paper_ids_file: str):
    """
    Main indexing flow. Reads paper IDs from file, distributes
    processing across Dask workers, then builds FAISS index.

    Args:
        paper_ids_file: path to JSON file containing list of paper IDs
    """
    logger = get_run_logger()

    # Load paper IDs
    with open(paper_ids_file, "r") as f:
        paper_ids = json.load(f)

    logger.info(f"Loaded {len(paper_ids)} paper IDs from {paper_ids_file}")

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
        logger.info("Index built successfully")
    else:
        logger.error("Index build failed")

    return {"successful": successful, "failed": failed}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper-ids-file", required=True, help="Path to JSON file of paper IDs")
    args = parser.parse_args()

    indexing_flow(args.paper_ids_file)