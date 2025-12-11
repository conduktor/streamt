"""Project Evolution test scenarios.

Tests how projects change and evolve over time:
- Adding new models to existing pipelines
- Refactoring: splitting, merging, renaming models
- Schema evolution and versioning
- Migration patterns
- Deprecation workflows
"""

import tempfile
from pathlib import Path

import yaml

from streamt.core.dag import DAGBuilder
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator

class TestAddingNewModels:
    """Test adding new models to existing pipelines."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_phase1_initial_mvp(self):
        """
        SCENARIO: Phase 1 - Initial MVP launch

        Story: A startup launches with a minimal viable product -
        just basic event ingestion and a simple derived model.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "user-analytics",
                    "version": "0.1.0",
                    "description": "User analytics platform - MVP",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {
                        "name": "page_views",
                        "description": "Raw page view events",
                        "topic": "events.pageviews.v1",
                    },
                ],
                "models": [
                    {
                        "name": "page_views_cleaned",
                        "description": "Cleaned page views",

                        "sql": """
                            SELECT
                                user_id,
                                page_url,
                                referrer,
                                user_agent,
                                event_time
                            FROM {{ source("page_views") }}
                            WHERE user_id IS NOT NULL
                        """,
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Simple DAG
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            assert len(dag.nodes) == 2  # 1 source + 1 model

    def test_phase2_add_aggregations(self):
        """
        SCENARIO: Phase 2 - Add aggregations for dashboards

        Story: Product team requests metrics dashboards.
        We add aggregation models without touching existing pipeline.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "user-analytics",
                    "version": "0.2.0",  # Bumped version
                    "description": "User analytics platform - with aggregations",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {
                        "name": "page_views",
                        "description": "Raw page view events",
                        "topic": "events.pageviews.v1",
                    },
                ],
                "models": [
                    # Existing model - unchanged
                    {
                        "name": "page_views_cleaned",
                        "description": "Cleaned page views",

                        "sql": """
                            SELECT
                                user_id,
                                page_url,
                                referrer,
                                user_agent,
                                event_time
                            FROM {{ source("page_views") }}
                            WHERE user_id IS NOT NULL
                        """,
                    },
                    # NEW: Session aggregation
                    {
                        "name": "user_sessions",
                        "description": "User sessions from page views",

                        "sql": """
                            SELECT
                                user_id,
                                SESSION_START(event_time, INTERVAL '30' MINUTE) as session_start,
                                SESSION_END(event_time, INTERVAL '30' MINUTE) as session_end,
                                COUNT(*) as page_count,
                                FIRST_VALUE(page_url) as landing_page,
                                LAST_VALUE(page_url) as exit_page
                            FROM {{ ref("page_views_cleaned") }}
                            GROUP BY
                                user_id,
                                SESSION(event_time, INTERVAL '30' MINUTE)
                        """,
                    },
                    # NEW: Hourly metrics
                    {
                        "name": "hourly_metrics",
                        "description": "Hourly aggregated metrics",

                        "sql": """
                            SELECT
                                TUMBLE_END(event_time, INTERVAL '1' HOUR) as hour,
                                COUNT(DISTINCT user_id) as unique_users,
                                COUNT(*) as total_page_views,
                                COUNT(*) / COUNT(DISTINCT user_id) as pages_per_user
                            FROM {{ ref("page_views_cleaned") }}
                            GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR)
                        """,
                    },
                ],
                "exposures": [
                    {
                        "name": "metrics_dashboard",
                        "type": "dashboard",
                        "description": "Product metrics dashboard",
                        "depends_on": [
                            {"ref": "hourly_metrics"},
                            {"ref": "user_sessions"},
                        ],
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # 1 source + 3 models (exposures count separately)
            model_and_source_count = len(
                [n for n in dag.nodes.keys() if dag.nodes[n].type.value in ("source", "model")]
            )
            assert model_and_source_count == 4

            # New models depend on existing model
            assert "page_views_cleaned" in dag.get_upstream("user_sessions")
            assert "page_views_cleaned" in dag.get_upstream("hourly_metrics")

    def test_phase3_add_new_source_stream(self):
        """
        SCENARIO: Phase 3 - Add new data source for enrichment

        Story: Marketing team wants campaign attribution.
        We add a new source and join it with existing data.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "user-analytics",
                    "version": "0.3.0",
                    "description": "User analytics platform - with attribution",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {
                        "name": "page_views",
                        "description": "Raw page view events",
                        "topic": "events.pageviews.v1",
                    },
                    # NEW SOURCE
                    {
                        "name": "utm_campaigns",
                        "description": "UTM campaign parameters",
                        "topic": "marketing.campaigns.v1",
                    },
                ],
                "models": [
                    {
                        "name": "page_views_cleaned",
                        "description": "Cleaned page views",

                        "sql": "SELECT * FROM {{ source('page_views') }} WHERE user_id IS NOT NULL",
                    },
                    # MODIFIED: Now includes utm join
                    {
                        "name": "page_views_attributed",
                        "description": "Page views with campaign attribution",

                        "sql": """
                            SELECT
                                pv.*,
                                u.campaign_name,
                                u.campaign_source,
                                u.campaign_medium
                            FROM {{ ref("page_views_cleaned") }} pv
                            LEFT JOIN {{ source("utm_campaigns") }} u
                                ON pv.user_id = u.user_id
                                AND pv.event_time >= u.start_time
                                AND pv.event_time <= u.end_time
                        """,
                    },
                    {
                        "name": "user_sessions",
                        "description": "User sessions from page views",

                        "sql": """
                            SELECT
                                user_id,
                                campaign_name,
                                SESSION_START(event_time, INTERVAL '30' MINUTE) as session_start,
                                COUNT(*) as page_count
                            FROM {{ ref("page_views_attributed") }}
                            GROUP BY
                                user_id,
                                campaign_name,
                                SESSION(event_time, INTERVAL '30' MINUTE)
                        """,
                    },
                    # NEW: Campaign metrics
                    {
                        "name": "campaign_metrics",
                        "description": "Metrics per campaign",

                        "sql": """
                            SELECT
                                campaign_name,
                                TUMBLE_END(session_start, INTERVAL '1' DAY) as day,
                                COUNT(DISTINCT user_id) as unique_users,
                                COUNT(*) as sessions,
                                SUM(page_count) as total_page_views
                            FROM {{ ref("user_sessions") }}
                            GROUP BY
                                campaign_name,
                                TUMBLE(session_start, INTERVAL '1' DAY)
                        """,
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # page_views_attributed joins two streams
            upstream = dag.get_upstream("page_views_attributed")
            assert "page_views_cleaned" in upstream
            assert "utm_campaigns" in upstream

class TestRefactoringModels:
    """Test refactoring patterns - splitting, merging, renaming."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_split_large_model(self):
        """
        SCENARIO: Split a large model into smaller, focused models

        Story: A monolithic model handling too many responsibilities
        is split into focused transformation stages.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # AFTER: Split into focused models
            config = {
                "project": {"name": "order-processing", "version": "2.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "raw_orders", "topic": "orders.raw.v1"},
                ],
                "models": [
                    # Stage 1: Parsing and validation
                    {
                        "name": "orders_parsed",
                        "description": "Parsed order JSON with schema validation",

                        "sql": """
                            SELECT
                                order_id,
                                CAST(customer_id AS BIGINT) as customer_id,
                                CAST(JSON_VALUE(items, '$') AS VARCHAR) as items_json,
                                CAST(total_amount AS DECIMAL(10,2)) as total_amount,
                                order_status,
                                event_time
                            FROM {{ source("raw_orders") }}
                            WHERE order_id IS NOT NULL
                        """,
                    },
                    # Stage 2: Business logic enrichment
                    {
                        "name": "orders_enriched",
                        "description": "Orders with business classifications",

                        "sql": """
                            SELECT
                                *,
                                CASE
                                    WHEN total_amount > 1000 THEN 'HIGH_VALUE'
                                    WHEN total_amount > 100 THEN 'MEDIUM_VALUE'
                                    ELSE 'LOW_VALUE'
                                END as order_tier,
                                CASE
                                    WHEN order_status = 'CANCELLED' THEN true
                                    ELSE false
                                END as is_cancelled
                            FROM {{ ref("orders_parsed") }}
                        """,
                    },
                    # Stage 3: Metrics calculation
                    {
                        "name": "order_metrics",
                        "description": "Aggregated order metrics",

                        "sql": """
                            SELECT
                                order_tier,
                                TUMBLE_END(event_time, INTERVAL '1' HOUR) as hour,
                                COUNT(*) as order_count,
                                SUM(total_amount) as revenue,
                                AVG(total_amount) as avg_order_value
                            FROM {{ ref("orders_enriched") }}
                            WHERE NOT is_cancelled
                            GROUP BY
                                order_tier,
                                TUMBLE(event_time, INTERVAL '1' HOUR)
                        """,
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # Verify clean pipeline: source -> parsed -> enriched -> metrics
            assert "raw_orders" in dag.get_upstream("orders_parsed")
            assert "orders_parsed" in dag.get_upstream("orders_enriched")
            assert "orders_enriched" in dag.get_upstream("order_metrics")

    def test_merge_redundant_models(self):
        """
        SCENARIO: Merge redundant models that were split too aggressively

        Story: Previous refactoring created too many small models
        with single-line transformations. Consolidate for clarity.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # AFTER: Merged into logical units
            config = {
                "project": {"name": "log-processing", "version": "2.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "raw_logs", "topic": "logs.raw.v1"},
                ],
                "models": [
                    # Consolidated model that does all parsing and enrichment
                    {
                        "name": "logs_processed",
                        "description": "Fully processed and enriched logs",

                        "sql": """
                            SELECT
                                log_id,
                                UPPER(TRIM(log_level)) as log_level,
                                REGEXP_EXTRACT(message, '^\\[([^\\]]+)\\]', 1) as component,
                                REGEXP_REPLACE(message, '^\\[[^\\]]+\\]\\s*', '') as clean_message,
                                COALESCE(user_id, 'SYSTEM') as user_id,
                                CASE UPPER(TRIM(log_level))
                                    WHEN 'ERROR' THEN 1
                                    WHEN 'WARN' THEN 2
                                    WHEN 'INFO' THEN 3
                                    ELSE 4
                                END as severity_order,
                                event_time
                            FROM {{ source("raw_logs") }}
                            WHERE message IS NOT NULL
                        """,
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Only 1 source + 1 model
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()
            assert len(dag.nodes) == 2

    def test_rename_model_with_alias(self):
        """
        SCENARIO: Rename model while maintaining backward compatibility

        Story: Team decides "user_events_enriched" should be renamed to
        "enriched_user_events" for consistency. An alias maintains
        backward compatibility during transition.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "user-events", "version": "2.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "raw_events", "topic": "events.raw.v1"},
                ],
                "models": [
                    {
                        "name": "enriched_user_events",  # New canonical name
                        "description": "Enriched user events (formerly user_events_enriched)",

                        "topic": {
                            "name": "events.enriched.v1",  # Explicit topic name
                        },
                        "sql": """
                            SELECT * FROM {{ source("raw_events") }}
                        """,
                    },
                    # Alias model for backward compatibility (use topic for now, virtual_topic requires gateway)
                    {
                        "name": "user_events_enriched",
                        "description": "DEPRECATED: Use enriched_user_events instead",
                        "sql": """
                            SELECT * FROM {{ ref("enriched_user_events") }}
                        """,
                        "tags": ["deprecated"],
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Verify alias points to new model
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()
            assert "enriched_user_events" in dag.get_upstream("user_events_enriched")

class TestSchemaEvolution:
    """Test schema evolution and versioning patterns."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_backward_compatible_change(self):
        """
        SCENARIO: Add optional field - backward compatible

        Story: Add a new optional field to a model. Existing consumers
        continue to work, new consumers can use the new field.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "user-profiles", "version": "1.1.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "profile_updates", "topic": "profiles.updates.v1"},
                ],
                "models": [
                    {
                        "name": "user_profiles",
                        "description": "Current user profile state",

                        "schema": {
                            "compatibility": "BACKWARD",
                        },
                        "sql": """
                            SELECT
                                user_id,
                                email,
                                full_name,
                                created_at,
                                -- NEW: Optional field with default
                                COALESCE(phone_number, '') as phone_number,
                                -- NEW: Optional field with default
                                COALESCE(preferences, '{}') as preferences,
                                updated_at
                            FROM {{ source("profile_updates") }}
                        """,
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

    def test_breaking_change_with_new_version(self):
        """
        SCENARIO: Breaking schema change requires new topic version

        Story: Rename a field or change its type. Create a new v2 topic
        while maintaining v1 for existing consumers.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "user-profiles", "version": "2.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "profile_updates", "topic": "profiles.updates.v1"},
                ],
                "models": [
                    # V1: Maintained for backward compatibility
                    {
                        "name": "user_profiles_v1",
                        "description": "DEPRECATED: User profiles v1 format",

                        "topic": {"name": "profiles.current.v1"},
                        "tags": ["deprecated", "v1"],
                        "sql": """
                            SELECT
                                user_id,
                                email,
                                full_name as name,
                                created_at,
                                updated_at
                            FROM {{ source("profile_updates") }}
                        """,
                    },
                    # V2: New schema with breaking changes
                    {
                        "name": "user_profiles_v2",
                        "description": "User profiles v2 with improved schema",

                        "topic": {"name": "profiles.current.v2"},
                        "tags": ["current", "v2"],
                        "sql": """
                            SELECT
                                user_id,
                                email,
                                -- Breaking change: split name into components
                                SPLIT(full_name, ' ')[1] as first_name,
                                SPLIT(full_name, ' ')[2] as last_name,
                                -- Breaking change: rename field
                                created_at as registered_at,
                                updated_at,
                                -- New required field
                                COALESCE(status, 'ACTIVE') as account_status
                            FROM {{ source("profile_updates") }}
                        """,
                    },
                ],
                "exposures": [
                    {
                        "name": "legacy_consumers",
                        "type": "application",
                        "role": "consumer",
                        "description": "Legacy apps still on v1",
                        "consumes": [{"ref": "user_profiles_v1"}],
                    },
                    {
                        "name": "new_consumers",
                        "type": "application",
                        "role": "consumer",
                        "description": "New apps using v2",
                        "consumes": [{"ref": "user_profiles_v2"}],
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Both v1 and v2 should be independent outputs
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            assert "profile_updates" in dag.get_upstream("user_profiles_v1")
            assert "profile_updates" in dag.get_upstream("user_profiles_v2")

class TestMigrationPatterns:
    """Test migration and cutover patterns."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_parallel_run_migration(self):
        """
        SCENARIO: Parallel run for safe migration

        Story: Migrating from legacy Kafka topics to new streamt-managed
        topics. Run both in parallel to verify correctness before cutover.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "migration-parallel", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {
                        "name": "events_source",
                        "description": "Raw events",
                        "topic": "events.raw.v1",
                    },
                    # Legacy topic to compare against
                    {
                        "name": "legacy_processed",
                        "description": "Legacy processed events (existing system)",
                        "topic": "legacy.events.processed",
                    },
                ],
                "models": [
                    # New processing pipeline
                    {
                        "name": "events_processed_new",
                        "description": "New event processing logic",

                        "topic": {"name": "streamt.events.processed.v1"},
                        "sql": """
                            SELECT
                                event_id,
                                user_id,
                                event_type,
                                COALESCE(metadata, '{}') as metadata,
                                event_time
                            FROM {{ source("events_source") }}
                        """,
                    },
                    # Comparison model for validation
                    {
                        "name": "migration_comparison",
                        "description": "Compare new vs legacy output",

                        "tags": ["migration", "validation"],
                        "sql": """
                            SELECT
                                COALESCE(n.event_id, l.event_id) as event_id,
                                CASE
                                    WHEN n.event_id IS NULL THEN 'MISSING_IN_NEW'
                                    WHEN l.event_id IS NULL THEN 'MISSING_IN_LEGACY'
                                    WHEN n.user_id != l.user_id THEN 'USER_MISMATCH'
                                    WHEN n.event_type != l.event_type THEN 'TYPE_MISMATCH'
                                    ELSE 'MATCH'
                                END as comparison_result,
                                n.event_time as new_time,
                                l.event_time as legacy_time
                            FROM {{ ref("events_processed_new") }} n
                            FULL OUTER JOIN {{ source("legacy_processed") }} l
                                ON n.event_id = l.event_id
                                AND n.event_time BETWEEN l.event_time - INTERVAL '1' MINUTE
                                    AND l.event_time + INTERVAL '1' MINUTE
                        """,
                    },
                    # Migration metrics
                    {
                        "name": "migration_metrics",
                        "description": "Migration comparison metrics",

                        "sql": """
                            SELECT
                                TUMBLE_END(new_time, INTERVAL '5' MINUTE) as window_end,
                                comparison_result,
                                COUNT(*) as count
                            FROM {{ ref("migration_comparison") }}
                            GROUP BY
                                TUMBLE(new_time, INTERVAL '5' MINUTE),
                                comparison_result
                        """,
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

    def test_gradual_rollout(self):
        """
        SCENARIO: Gradual rollout with feature flags

        Story: Roll out new processing logic gradually using percentage-based
        traffic splitting before full cutover.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "gradual-rollout", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "orders", "topic": "orders.v1"},
                    {"name": "rollout_config", "topic": "config.rollout.v1"},
                ],
                "models": [
                    {
                        "name": "orders_routed",
                        "description": "Route orders to old or new processing",

                        "sql": """
                            SELECT
                                o.*,
                                CASE
                                    WHEN MOD(HASH(o.order_id), 100) < c.new_logic_percent
                                    THEN 'NEW'
                                    ELSE 'OLD'
                                END as processing_path
                            FROM {{ source("orders") }} o
                            CROSS JOIN {{ source("rollout_config") }} c
                        """,
                    },
                    {
                        "name": "orders_old_logic",
                        "description": "Orders processed with old logic",

                        "sql": """
                            SELECT
                                order_id,
                                total_amount,
                                -- Old calculation
                                total_amount * 0.1 as tax_amount
                            FROM {{ ref("orders_routed") }}
                            WHERE processing_path = 'OLD'
                        """,
                    },
                    {
                        "name": "orders_new_logic",
                        "description": "Orders processed with new logic",

                        "sql": """
                            SELECT
                                order_id,
                                total_amount,
                                -- New calculation with different rates
                                total_amount * 0.08 as tax_amount
                            FROM {{ ref("orders_routed") }}
                            WHERE processing_path = 'NEW'
                        """,
                    },
                    {
                        "name": "orders_unified",
                        "description": "Unified output from both paths",

                        "sql": """
                            SELECT * FROM {{ ref("orders_old_logic") }}
                            UNION ALL
                            SELECT * FROM {{ ref("orders_new_logic") }}
                        """,
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

class TestDeprecationWorkflow:
    """Test model and source deprecation workflows."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_deprecation_with_sunset_date(self):
        """
        SCENARIO: Deprecate model with sunset date

        Story: A model is being deprecated. Mark it with deprecation notice
        and sunset date, while providing migration path to replacement.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "deprecation-example", "version": "1.0.0"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "local",
                        "clusters": {
                            "local": {"type": "rest", "rest_url": "http://localhost:8082"}
                        },
                    },
                },
                "sources": [
                    {"name": "events", "topic": "events.v1"},
                ],
                "models": [
                    # Deprecated model
                    {
                        "name": "events_summary_legacy",
                        "description": """
                            DEPRECATED as of 2024-01-15.
                            Sunset date: 2024-04-15.
                            Migration: Use events_summary_v2 instead.
                            Reason: Performance improvements in v2.
                        """,

                        "tags": ["deprecated", "sunset:2024-04-15"],
                        "sql": """
                            SELECT
                                event_type,
                                COUNT(*) as event_count
                            FROM {{ source("events") }}
                            GROUP BY event_type
                        """,
                    },
                    # Replacement model
                    {
                        "name": "events_summary_v2",
                        "description": "Event summary with improved performance",

                        "tags": ["current"],
                        "sql": """
                            SELECT
                                event_type,
                                TUMBLE_END(event_time, INTERVAL '1' MINUTE) as minute,
                                COUNT(*) as event_count,
                                COUNT(DISTINCT user_id) as unique_users
                            FROM {{ source("events") }}
                            GROUP BY
                                event_type,
                                TUMBLE(event_time, INTERVAL '1' MINUTE)
                        """,
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Verify both models exist
            model_names = [m.name for m in project.models]
            assert "events_summary_legacy" in model_names
            assert "events_summary_v2" in model_names

            # Verify deprecated model has tags
            deprecated_model = next(m for m in project.models if m.name == "events_summary_legacy")
            assert "deprecated" in deprecated_model.tags
