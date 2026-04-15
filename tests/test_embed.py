"""Unit tests for app/embed.py."""

from unittest.mock import MagicMock


def _mock_embedding_response(embedding):
    """Build a mock response matching the Foundation Model embedding API shape."""
    mock_ws = MagicMock()
    entry = MagicMock()
    entry.embedding = embedding
    mock_ws.serving_endpoints.query.return_value.data = [entry]
    return mock_ws


def test_embed_text_calls_serving_endpoint():
    from app.embed import embed_text, EMBEDDING_ENDPOINT

    mock_ws = _mock_embedding_response([0.1, 0.2, 0.3])

    result = embed_text("hello world", ws=mock_ws)

    mock_ws.serving_endpoints.query.assert_called_once_with(
        name=EMBEDDING_ENDPOINT,
        input=["hello world"],
    )
    assert result == [0.1, 0.2, 0.3]


def test_embed_text_returns_list_of_floats():
    from app.embed import embed_text

    mock_ws = _mock_embedding_response([0.1] * 1024)

    result = embed_text("test", ws=mock_ws)

    assert isinstance(result, list)
    assert len(result) == 1024
    assert all(isinstance(v, float) for v in result)


def test_embed_text_accepts_external_ws_client():
    from app.embed import embed_text

    mock_ws = _mock_embedding_response([0.5])

    embed_text("text", ws=mock_ws)

    # The provided ws client was used
    assert mock_ws.serving_endpoints.query.called
