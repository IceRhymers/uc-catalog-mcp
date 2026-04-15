"""App-side text embedding via Databricks Foundation Models REST API.

Uses the Databricks SDK (same credential chain as the rest of the app).
No Spark required — this runs in the app venv.
"""

from __future__ import annotations

from databricks.sdk import WorkspaceClient

EMBEDDING_ENDPOINT = "databricks-bge-large-en"


def embed_text(text: str, ws: WorkspaceClient | None = None) -> list[float]:
    """Embed a single text string using the Databricks BGE-large endpoint.

    Returns a 1024-dimensional float vector. Uses WorkspaceClient for auth —
    same credential chain as the rest of the app, no extra env vars needed.

    Args:
        text: Text to embed.
        ws: Optional WorkspaceClient; creates one if not provided (injectable for tests).

    Returns:
        1024-dimensional embedding vector.
    """
    client = ws or WorkspaceClient()
    response = client.serving_endpoints.query(
        name=EMBEDDING_ENDPOINT,
        input=[text],
    )
    return response.data[0].embedding
