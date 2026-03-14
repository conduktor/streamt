"""Confluent Cloud Flink SQL AI/ML compatibility tests.

Verifies that streamt correctly parses, validates, and compiles Confluent-specific
AI/ML inference functions: ML_PREDICT, AI_COMPLETE, AI_EMBEDDING, VECTOR_SEARCH_AGG,
KEY_SEARCH_AGG, anomaly detection, ML preprocessing, and end-to-end RAG pipelines.

Reference: https://docs.confluent.io/cloud/current/flink/reference/functions/model-inference-functions.html
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml

from streamt.core.models import StreamtProject
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator


def _parse(tmpdir: str, config: dict) -> StreamtProject:
    p = Path(tmpdir)
    (p / "stream_project.yml").write_text(yaml.dump(config))
    return ProjectParser(p).parse()


def _compile(project: StreamtProject):
    from streamt.compiler.compiler import Compiler

    return Compiler(project).compile(dry_run=True)


def _no_fatal_errors(result) -> bool:
    return result.is_valid


BASE = {
    "project": {"name": "test", "version": "1.0.0"},
    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
    "sources": [
        {"name": "events", "topic": "events.v1"},
        {"name": "products", "topic": "products.v1"},
        {"name": "orders", "topic": "orders.v1"},
        {"name": "text_input", "topic": "text.v1"},
    ],
}

# ML_PREDICT is stateful — requires a Flink cluster in runtime config
BASE_FLINK = {
    **BASE,
    "runtime": {
        "kafka": {"bootstrap_servers": "localhost:9092"},
        "flink": {
            "default": "local",
            "clusters": {
                "local": {
                    "rest_url": "http://localhost:8081",
                    "sql_gateway_url": "http://localhost:8083",
                }
            },
        },
    },
}


# ---------------------------------------------------------------------------
# ML_PREDICT — registered in flink_dialect
# ---------------------------------------------------------------------------


class TestMLPredictCompat:
    """ML_PREDICT is registered in flink_dialect — must compile and validate."""

    def test_ml_predict_basic_validates(self):
        """ML_PREDICT with model name and column validates without errors."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE_FLINK,
                "models": [
                    {
                        "name": "scored",
                        "sql": (
                            "SELECT order_id, "
                            "ML_PREDICT(`fraud_model`, features) AS prediction "
                            'FROM {{ source("orders") }}'
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_ml_predict_with_async_config_validates(self):
        """ML_PREDICT with async configuration map validates."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE_FLINK,
                "models": [
                    {
                        "name": "scored",
                        "sql": (
                            "SELECT order_id, "
                            "ML_PREDICT(`fraud_model`, features, "
                            "  MAP['async_enabled', true, 'max_parallelism', 10]) AS prediction "
                            'FROM {{ source("orders") }}'
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_ml_predict_compiles_to_artifact(self):
        """Model using ML_PREDICT produces a Flink job artifact."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE_FLINK,
                "models": [
                    {
                        "name": "scored",
                        "sql": (
                            "SELECT order_id, "
                            "ML_PREDICT(`fraud_model`, features) AS prediction "
                            'FROM {{ source("orders") }}'
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            manifest = _compile(project)
            all_names = [
                a.get("name") if isinstance(a, dict) else a.name
                for artifacts in manifest.artifacts.values()
                for a in artifacts
            ]
            assert "scored" in all_names


# ---------------------------------------------------------------------------
# AI_COMPLETE — LLM text completion
# ---------------------------------------------------------------------------


class TestAICompleteCompat:
    """AI_COMPLETE generates LLM completions. Stateless — no Flink cluster needed."""

    def test_ai_complete_basic_validates(self):
        """AI_COMPLETE with string prompt validates without error."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "enriched_events",
                        "sql": (
                            "SELECT event_id, "
                            "AI_COMPLETE(CONCAT('Classify this event: ', event_text)) AS classification "
                            'FROM {{ source("events") }}'
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_ai_complete_compiles(self):
        """Model using AI_COMPLETE produces a compilation artifact."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "enriched_events",
                        "sql": (
                            "SELECT event_id, "
                            "AI_COMPLETE(description) AS summary "
                            'FROM {{ source("events") }}'
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            manifest = _compile(project)
            all_names = [
                a.get("name") if isinstance(a, dict) else a.name
                for artifacts in manifest.artifacts.values()
                for a in artifacts
            ]
            assert "enriched_events" in all_names

    def test_ai_complete_infers_string_return_type(self):
        """AI_COMPLETE output column type resolves to STRING."""
        from streamt.compiler.type_inference import TypeInferenceMixin

        class _H(TypeInferenceMixin):
            def __init__(self):
                self._udf_types = {}

        h = _H()
        cols = h._extract_select_columns_with_types(
            "SELECT AI_COMPLETE(prompt_text) AS summary FROM t",
            schema_context={"prompt_text": "STRING"},
        )
        assert dict(cols).get("summary") == "STRING"


# ---------------------------------------------------------------------------
# AI_EMBEDDING — vector embeddings via LATERAL TABLE
# ---------------------------------------------------------------------------


class TestAIEmbeddingCompat:
    """AI_EMBEDDING produces ARRAY<FLOAT> via LATERAL TABLE pattern."""

    def test_ai_embedding_lateral_table_validates(self):
        """AI_EMBEDDING used in LATERAL TABLE validates without error."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "embeddings",
                        "sql": (
                            "SELECT t.event_id, emb.embedding "
                            'FROM {{ source("text_input") }} t, '
                            "LATERAL TABLE(AI_EMBEDDING('bedrock_embed', t.content)) AS emb(embedding)"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_ai_embedding_compiles(self):
        """Model using AI_EMBEDDING produces an artifact."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "embeddings",
                        "sql": (
                            "SELECT event_id, "
                            "AI_EMBEDDING('openai_ada', content) AS embedding "
                            'FROM {{ source("text_input") }}'
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            manifest = _compile(project)
            all_names = [
                a.get("name") if isinstance(a, dict) else a.name
                for artifacts in manifest.artifacts.values()
                for a in artifacts
            ]
            assert "embeddings" in all_names


# ---------------------------------------------------------------------------
# VECTOR_SEARCH_AGG — semantic similarity search
# ---------------------------------------------------------------------------


class TestVectorSearchAggCompat:
    """VECTOR_SEARCH_AGG performs semantic similarity search via LATERAL TABLE."""

    def test_vector_search_agg_validates(self):
        """VECTOR_SEARCH_AGG in LATERAL TABLE validates without error."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "similar_products",
                        "sql": (
                            "SELECT e.event_id, p.product_id, p.name "
                            'FROM {{ source("events") }} e, '
                            "LATERAL TABLE("
                            "  VECTOR_SEARCH_AGG(products_catalog, DESCRIPTOR(embedding), e.query_vec, 5)"
                            ") AS p(product_id, name, score)"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_vector_search_agg_with_options_validates(self):
        """VECTOR_SEARCH_AGG with options map validates."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "rag_context",
                        "sql": (
                            "SELECT e.event_id, doc.content "
                            'FROM {{ source("events") }} e, '
                            "LATERAL TABLE("
                            "  VECTOR_SEARCH_AGG(knowledge_base, DESCRIPTOR(vec), e.embedding, 3, "
                            "    MAP['async_enabled', true, 'max_parallelism', 5])"
                            ") AS doc(doc_id, content, score)"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors


# ---------------------------------------------------------------------------
# KEY_SEARCH_AGG — exact key lookup
# ---------------------------------------------------------------------------


class TestKeySearchAggCompat:
    """KEY_SEARCH_AGG performs exact key lookup against an external table."""

    def test_key_search_agg_validates(self):
        """KEY_SEARCH_AGG in LATERAL TABLE validates without error."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "enriched_orders",
                        "sql": (
                            "SELECT o.order_id, o.amount, p.price, p.category "
                            'FROM {{ source("orders") }} o, '
                            "LATERAL TABLE("
                            "  KEY_SEARCH_AGG(products_db, DESCRIPTOR(product_id), o.product_id)"
                            ") AS p(product_id, price, category)"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors


# ---------------------------------------------------------------------------
# ML_DETECT_ANOMALIES, ML_DETECT_ANOMALIES_ROBUST, ML_FORECAST
# ---------------------------------------------------------------------------


class TestMLAnomalyDetectionCompat:
    """ML anomaly detection and forecasting functions."""

    def test_ml_detect_anomalies_validates(self):
        """ML_DETECT_ANOMALIES on a numeric time-series validates without error."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "latency_anomalies",
                        "sql": (
                            "SELECT event_id, ts, result.is_anomaly, result.forecast_value "
                            "FROM ("
                            "  SELECT event_id, ts, "
                            "    ML_DETECT_ANOMALIES(latency_ms, ts, "
                            "      JSON_OBJECT('p' VALUE 1, 'q' VALUE 1, 'd' VALUE 1, "
                            "                  'minTrainingSize' VALUE 10)) AS result "
                            '  FROM {{ source("events") }}'
                            ")"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_ml_detect_anomalies_robust_validates(self):
        """ML_DETECT_ANOMALIES_ROBUST with MAD-based detection validates."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "robust_anomalies",
                        "sql": (
                            "SELECT event_id, anomaly.is_anomaly, anomaly.lower_bound "
                            "FROM ("
                            "  SELECT event_id, "
                            "    ML_DETECT_ANOMALIES_ROBUST(latency_ms, ts, "
                            "      JSON_OBJECT('majorityRule' VALUE false)) AS anomaly "
                            '  FROM {{ source("events") }}'
                            ")"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_ml_forecast_validates(self):
        """ML_FORECAST for ARIMA-based trend forecasting validates without error."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "sales_forecast",
                        "sql": (
                            "SELECT event_id, ts, forecast.forecast_value "
                            "FROM ("
                            "  SELECT event_id, ts, "
                            "    ML_FORECAST(amount, ts, "
                            "      JSON_OBJECT('horizon' VALUE 5, 'minTrainingSize' VALUE 128)) AS forecast "
                            '  FROM {{ source("orders") }}'
                            ")"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors


# ---------------------------------------------------------------------------
# ML preprocessing: bucketize, encode, text split
# ---------------------------------------------------------------------------


class TestMLPreprocessingCompat:
    """ML preprocessing functions: bucketize, label/one-hot encode, text splitter."""

    def test_ml_bucketize_validates(self):
        """ML_BUCKETIZE divides numeric values into discrete buckets."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "bucketed",
                        "sql": (
                            "SELECT event_id, "
                            "ML_BUCKETIZE(amount, ARRAY[0.0, 100.0, 500.0, 1000.0]) AS amount_bucket "
                            'FROM {{ source("orders") }}'
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_ml_label_encoder_validates(self):
        """ML_LABEL_ENCODER converts categories to numeric labels."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "encoded",
                        "sql": (
                            "SELECT event_id, "
                            "ML_LABEL_ENCODER(category, ARRAY['PENDING', 'ACTIVE', 'CLOSED']) AS category_id "
                            'FROM {{ source("events") }}'
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_ml_one_hot_encoder_validates(self):
        """ML_ONE_HOT_ENCODER converts categories to binary vectors."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "one_hot",
                        "sql": (
                            "SELECT event_id, "
                            "ML_ONE_HOT_ENCODER(status, ARRAY['pending', 'active', 'closed']) AS status_vec "
                            'FROM {{ source("events") }}'
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_ml_character_text_splitter_validates(self):
        """ML_CHARACTER_TEXT_SPLITTER chunks text for RAG pipelines."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "chunked_text",
                        "sql": (
                            "SELECT event_id, chunk "
                            'FROM {{ source("text_input") }}, '
                            "LATERAL TABLE(ML_CHARACTER_TEXT_SPLITTER(content, 500, 50, '\\n', false)) "
                            "AS t(chunk)"
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors


# ---------------------------------------------------------------------------
# Full RAG pipeline: AI_EMBEDDING + VECTOR_SEARCH_AGG + AI_COMPLETE
# ---------------------------------------------------------------------------


class TestRAGPipelineCompat:
    """End-to-end RAG pipeline: embed → vector search → LLM completion."""

    def test_rag_embedding_step_validates(self):
        """Step 1: generate embeddings from input text."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "query_embeddings",
                        "sql": (
                            "SELECT event_id, question, "
                            "AI_EMBEDDING('openai_ada', question) AS query_vec "
                            'FROM {{ source("events") }}'
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_rag_vector_search_step_validates(self):
        """Step 2: find similar docs via vector search on embeddings."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "query_embeddings",
                        "sql": (
                            "SELECT event_id, question, "
                            "AI_EMBEDDING('openai_ada', question) AS query_vec "
                            'FROM {{ source("events") }}'
                        ),
                    },
                    {
                        "name": "retrieved_context",
                        "sql": (
                            "SELECT q.event_id, q.question, doc.content AS context "
                            'FROM {{ ref("query_embeddings") }} AS q, '
                            "LATERAL TABLE("
                            "  VECTOR_SEARCH_AGG(knowledge_base, DESCRIPTOR(embedding), q.query_vec, 3)"
                            ") AS doc(doc_id, content, score)"
                        ),
                    },
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors

    def test_rag_completion_step_validates(self):
        """Step 3: generate answer from retrieved context + original question."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "rag_answers",
                        "sql": (
                            "SELECT event_id, question, context, "
                            "AI_COMPLETE(CONCAT("
                            "  'Context: ', context, "
                            "  '\\n\\nQuestion: ', question, "
                            "  '\\n\\nAnswer:')"
                            ") AS answer "
                            'FROM {{ source("events") }}'
                        ),
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert _no_fatal_errors(result), result.errors
