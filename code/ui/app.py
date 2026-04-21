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

def get_all_results() -> list:
    """Get all query results from MinIO"""
    try:
        client = get_minio_client()
        objects = list(client.list_objects(MINIO_BUCKET_NAME, prefix="results/"))
        all_results = []
        for obj in objects:
            response = client.get_object(MINIO_BUCKET_NAME, obj.object_name)
            data = json.loads(response.read().decode('utf-8'))
            response.close()
            response.release_conn()
            data["_last_modified"] = obj.last_modified
            all_results.append(data)
        return sorted(all_results, key=lambda x: x["_last_modified"], reverse=True)
    except S3Error:
        return []

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
    # Initialise and recover indexing run ID
    if "indexing_run_id" not in st.session_state:
        st.session_state["indexing_run_id"] = ""

    # Backup: check recent runs if session state is empty
    runs = get_recent_flow_runs("indexing-flow", limit=15)
    if not st.session_state["indexing_run_id"]:
        active = next((r for r in runs if r["state"] in ("Running", "Pending")), None)
        if active:
            st.session_state["indexing_run_id"] = active["id"]
    
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
        
        if st.button("Delete Index"):
            st.session_state["confirm_delete"] = True
   
        if st.session_state.get("confirm_delete"):
            st.error("Are you sure? This will delete the FAISS index and all embeddings.")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Yes, delete", type="primary"):
                    with st.spinner("Deleting index and embeddings..."):
                        Path(FAISS_INDEX_PATH).unlink(missing_ok=True)
                        Path(METADATA_PATH).unlink(missing_ok=True)
                        client = get_minio_client()
                        objects = list(client.list_objects(MINIO_BUCKET_NAME, prefix="embeddings/"))
                        for obj in objects:
                            client.remove_object(MINIO_BUCKET_NAME, obj.object_name)
                    st.session_state["confirm_delete"] = False
                    st.success(f"Deleted index and {len(objects)} embeddings")
                    st.rerun()
            with col2:
                if st.button("Cancel"):
                    st.session_state["confirm_delete"] = False
                    st.rerun()
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
            max_value=500000,
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

    if st.button("Launch Indexing Flow", type="primary", disabled=(info["exists"] or st.session_state["indexing_run_id"] != "")):
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
        
    if info["exists"]:
        st.warning("Delete existing index before launching a new indexing run.")
    elif st.session_state["indexing_run_id"]:
        st.warning("An indexing run is currently in progress. See progress section below.")

    st.divider()

    # Flow run live progress
    st.subheader("Flow Run Progress")
    auto_refresh = st.checkbox("Auto-refresh", value=True)

    if st.session_state["indexing_run_id"]:
        run_id = st.session_state["indexing_run_id"]
        # Prefect UI link
        st.link_button("View run in Prefect UI", f"{PREFECT_UI_BASE_URL}/runs/flow-run/{run_id}")

        st.caption(f"Monitoring run: `{run_id}`")
        status = get_flow_run_status(run_id)
        state = status["state"]
        state_type = status["type"]

        if state_type == "COMPLETED":
            st.success(f"Flow completed successfully")
            st.session_state["indexing_run_id"] = ""
        elif state_type == "FAILED":
            st.error(f"Flow failed")
            st.session_state["indexing_run_id"] = ""
        else:
            st.info(f"Flow is {state}...")

    st.subheader("Recent Indexing Runs")
    if runs:
        df = pd.DataFrame(runs)
        df["prefect_url"] = df["id"].apply(lambda x: f"{PREFECT_UI_BASE_URL}/runs/flow-run/{x}")
        st.dataframe(
            df[["name", "state", "created", "prefect_url"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "prefect_url": st.column_config.LinkColumn("Prefect UI")
            }
        )
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

    # Auto-refresh
    if auto_refresh and st.session_state["indexing_run_id"]:
        if state_type in ("RUNNING", "PENDING", "SCHEDULED"):
            time.sleep(2)
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
    else:
        st.info(f"Index ready — {info['papers']:,} papers indexed")

        if "query_input" not in st.session_state:
            st.session_state["query_input"] = ""

        query = st.text_input(
            "Research question",
            placeholder="e.g. distributed computing frameworks for machine learning",
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
                    # Prefect UI link
                    st.link_button("View run in Prefect UI", f"{PREFECT_UI_BASE_URL}/runs/flow-run/{run_id}")
                
                    st.session_state["query_run_id"] = run_id

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

                st.rerun()

        all_results = get_all_results()

        # Display results
        if "query_run_id" in st.session_state:
            run_id = st.session_state["query_run_id"]

            # Current run_id results payload
            current = next((r for r in all_results if r["run_id"] == run_id), None)
            if current:
                results = current["results"]
                query_text = current["query"]
                st.subheader(f"Results")
                st.caption(f"Query: *{query_text}*")
            
                df = pd.DataFrame(results)
                df["score"] = df["score"].round(4)
                st.dataframe(
                    df[["rank", "paper_id", "score", "url"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "url": st.column_config.LinkColumn("arXiv Link")
                    }
                )
                
                # Download results
                csv = df.to_csv(index=False)
                st.download_button(
                    label="⬇ Download Results CSV",
                    data=csv,
                    file_name=f"results_{run_id[:8]}.csv",
                    mime="text/csv"
                )
            else:
                st.info("Results not yet available — try refreshing.")

        if all_results:
            st.divider()
            st.subheader("Search History")
            for entry in all_results:
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.markdown(f"[*{entry['query']}*]({PREFECT_UI_BASE_URL}/runs/flow-run/{entry['run_id']})")
                with col2:
                    if st.button("Load", key=entry["run_id"]):
                        st.session_state["query_run_id"] = entry["run_id"]
                        st.rerun()