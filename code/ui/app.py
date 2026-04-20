import io
import json
import time
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
from minio import Minio
from minio.error import S3Error
from prefect import get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowFilter
from prefect.client.schemas.sorting import FlowRunSort
import asyncio

from config import (
    MINIO_HOST,
    MINIO_PORT,
    MINIO_ROOT_USER,
    MINIO_ROOT_PASSWORD,
    MINIO_BUCKET_NAME,
    FAISS_INDEX_PATH,
    METADATA_PATH,
    PREFECT_API_URL,
    PREFECT_UI_BASE_URL
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_minio_client() -> Minio:
    return Minio(
        f"{MINIO_HOST}:{MINIO_PORT}",
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False
    )


def get_index_info() -> dict:
    """Get current FAISS index status."""
    index_path = Path(FAISS_INDEX_PATH)
    if not index_path.exists():
        return {"exists": False}

    import faiss
    index = faiss.read_index(str(index_path))
    size_mb = index_path.stat().st_size / (1024 * 1024)

    return {
        "exists": True,
        "papers": index.ntotal,
        "size_mb": round(size_mb, 2),
    }


def trigger_deployment(deployment_name: str, parameters: dict) -> str:
    """Trigger a Prefect deployment and return the flow run ID."""
    async def _run():
        async with get_client() as client:
            deployment = await client.read_deployment_by_name(deployment_name)
            flow_run = await client.create_flow_run_from_deployment(
                deployment.id,
                parameters=parameters
            )
            return str(flow_run.id)
    return asyncio.run(_run())


def get_flow_run_status(flow_run_id: str) -> dict:
    """Get current status of a flow run."""
    async def _run():
        async with get_client() as client:
            flow_run = await client.read_flow_run(flow_run_id)
            return {
                "state": flow_run.state.name if flow_run.state else "Unknown",
                "type": flow_run.state.type.value if flow_run.state else "unknown",
            }
    return asyncio.run(_run())


def get_recent_flow_runs(flow_name: str, limit: int = 5) -> list:
    async def _run():
        async with get_client() as client:
            flows = await client.read_flows(
                flow_filter=FlowFilter(name={"like_": f"%{flow_name}%"})
            )
            if not flows:
                return []
            
            runs = await client.read_flow_runs(
                flow_run_filter=FlowRunFilter(
                    flow_id={"any_": [f.id for f in flows]}
                ),
                sort=FlowRunSort.START_TIME_DESC,
                limit=limit
            )
            return [
                {
                    "id": str(r.id),
                    "name": r.name,
                    "state": r.state.name if r.state else "Unknown",
                    "created": str(r.created)[:19] if r.created else "",
                }
                for r in runs
            ]
    return asyncio.run(_run())


def upload_to_minio(file_bytes: bytes, object_key: str) -> bool:
    """Upload file bytes to MinIO."""
    try:
        client = get_minio_client()
        client.put_object(
            MINIO_BUCKET_NAME,
            object_key,
            io.BytesIO(file_bytes),
            len(file_bytes),
            content_type="application/json"
        )
        return True
    except S3Error:
        return False


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Literature Semantic Search",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# Tab navigation
tab1, tab2 = st.tabs(["Index Management", "Semantic search"])

# ---------------------------------------------------------------------------
# Tab 1: Index Management
# ---------------------------------------------------------------------------
with tab1:
    st.title("Index Management")
    st.markdown("Build and manage the semantic search index across the distributed cluster.")

    # Current index status
    st.subheader("Current Index Status")
    info = get_index_info()

    if info["exists"]:
        c1, c2, c3 = st.columns(3)
        c1.metric("Status", "Ready")
        c2.metric("Papers Indexed", f"{info['papers']:,}")
        c3.metric("Index Size", f"{info['size_mb']} MB")
    else:
        st.warning("No index found. Run the indexing flow to build one.")

    st.divider()

    # Trigger indexing
    st.subheader("Run Indexing Flow")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Dataset Source**")
        source = st.radio(
            "Choose input",
            ["Use default dataset", "Upload custom paper IDs"],
            label_visibility="collapsed"
        )

    with col2:
        max_papers = st.number_input(
            "Max papers to index",
            min_value=10,
            max_value=200000,
            value=50000,
            step=1000
        )

    paper_ids_key = "papers/arxiv-metadata.json"

    if source == "Upload custom paper IDs":
        uploaded_file = st.file_uploader(
            "Upload paper_ids.json",
            type=["json"],
            help="JSON file containing a list of arXiv paper IDs"
        )
        if uploaded_file:
            file_bytes = uploaded_file.read()
            object_key = f"papers/custom_{uploaded_file.name}"
            if st.button("Upload to MinIO"):
                if upload_to_minio(file_bytes, object_key):
                    st.success(f"Uploaded to MinIO: {object_key}")
                    paper_ids_key = object_key
                    st.session_state["custom_key"] = object_key
                else:
                    st.error("Upload failed")

        if "custom_key" in st.session_state:
            paper_ids_key = st.session_state["custom_key"]
            st.info(f"Using: {paper_ids_key}")

    if st.button("Launch Indexing Flow", type="primary"):
        with st.spinner("Triggering indexing flow..."):
            try:
                run_id = trigger_deployment(
                    "indexing-flow/indexing-flow",
                    parameters={
                        "paper_ids_key": paper_ids_key,
                        "max_papers": max_papers
                    }
                )
                st.session_state["indexing_run_id"] = run_id
                st.success(f"Flow run started: `{run_id}`")
            except Exception as e:
                st.error(f"Failed to trigger flow: {e}")

    st.divider()

    # Live progress
    st.subheader("Flow Run Progress")

    auto_refresh = st.checkbox("Auto-refresh", value=True)
    refresh_interval = st.slider("Refresh interval (seconds)", 5, 60, 15)

    if "indexing_run_id" in st.session_state:
        run_id = st.session_state["indexing_run_id"]
        st.caption(f"Monitoring run: `{run_id}`")

        status = get_flow_run_status(run_id)
        state = status["state"]
        state_type = status["type"]

        if state_type == "COMPLETED":
            st.success(f"Flow completed successfully")
        elif state_type == "FAILED":
            st.error(f"Flow failed")
        elif state_type in ("RUNNING", "PENDING"):
            st.info(f"Flow is {state}...")
        else:
            st.write(f"State: {state}")

    st.subheader("Recent Indexing Runs")
    runs = get_recent_flow_runs("indexing-flow")
    if runs:
        st.dataframe(pd.DataFrame(runs), use_container_width=True, hide_index=True)
    else:
        st.info("No recent runs found.")

    st.divider()

    # Download index
    st.subheader("Download Index")
    if info["exists"]:
        index_bytes = Path(FAISS_INDEX_PATH).read_bytes()
        st.download_button(
            label="⬇ Download FAISS Index",
            data=index_bytes,
            file_name="faiss.index",
            mime="application/octet-stream"
        )
        metadata_bytes = Path(METADATA_PATH).read_bytes()
        st.download_button(
            label="⬇ Download Metadata (paper IDs)",
            data=metadata_bytes,
            file_name="metadata.json",
            mime="application/json"
        )
    else:
        st.info("Build an index first to enable downloads.")

    if auto_refresh and "indexing_run_id" in st.session_state:
        status = get_flow_run_status(st.session_state["indexing_run_id"])
        if status["type"] in ("RUNNING", "PENDING"):
            time.sleep(refresh_interval)
            st.rerun()


# ---------------------------------------------------------------------------
# Tab 2: Search
# ---------------------------------------------------------------------------
with tab2:
    st.title("Semantic Search")
    st.markdown("Search the indexed papers using natural language queries.")

    info = get_index_info()
    if not info["exists"]:
        st.warning("No index found. Please build an index first in Index Management.")
        st.stop()

    st.info(f"Index ready — {info['papers']:,} papers indexed")

    query = st.text_input(
        "Research question",
        placeholder="e.g. distributed computing frameworks for machine learning"
    )
    top_k = st.slider("Number of results", min_value=1, max_value=50, value=10)

    if st.button("Search", type="primary") and query:
        with st.spinner("Searching..."):
            try:
                run_id = trigger_deployment(
                    "query-flow/query-flow",
                    parameters={
                        "query": query,
                        "top_k": top_k
                    }
                )
                st.session_state["query_run_id"] = run_id
                st.session_state["query_results"] = None

                # Poll until complete
                for _ in range(30):
                    time.sleep(2)
                    status = get_flow_run_status(run_id)
                    if status["type"] == "COMPLETED":
                        break
                    elif status["type"] == "FAILED":
                        st.error("Query flow failed")
                        st.stop()

                st.success("Search complete!")

            except Exception as e:
                st.error(f"Search failed: {e}")

    # Display results
    if "query_run_id" in st.session_state:
        st.subheader("Results")
        st.info("Results are logged in the Prefect UI — check the flow run logs for ranked paper IDs and scores.")

        run_id = st.session_state["query_run_id"]
        st.markdown(f"[View in Prefect UI]({PREFECT_UI_BASE_URL}/runs/flow-run/{run_id})")