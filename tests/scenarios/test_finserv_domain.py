"""Financial Services domain test scenarios.

Simulates real-world finserv use cases:
- Real-time trading and market data
- Risk management and exposure
- Fraud detection
- Regulatory compliance and reporting
"""

import tempfile
from pathlib import Path

import yaml

from streamt.compiler import Compiler
from streamt.core.dag import DAGBuilder
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator


class TestTradingPipeline:
    """Test trading and market data scenarios."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_market_data_aggregation(self):
        """
        SCENARIO: Real-time market data aggregation for trading desk

        Story: An investment bank needs to aggregate market data from
        multiple exchanges, calculate VWAP (Volume Weighted Average Price),
        detect price anomalies, and publish consolidated quotes for traders.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "market-data-platform",
                    "version": "3.0.0",
                    "description": "Real-time market data aggregation",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "prod",
                        "clusters": {"prod": {"type": "rest", "rest_url": "http://localhost:8082"}},
                    },
                },
                "governance": {
                    "require_descriptions": True,
                    "require_owners": True,
                    "topic_rules": {
                        "naming_pattern": "^trading\\..*\\.v\\d+$",
                        "min_partitions": 6,
                    },
                },
                "sources": [
                    {
                        "name": "nyse_quotes",
                        "description": "NYSE market quotes",
                        "topic": "trading.nyse.quotes.v1",
                        "freshness": {"max_lag_seconds": 1, "warn_after_seconds": 1},
                        "owner": "market-data-team",
                    },
                    {
                        "name": "nasdaq_quotes",
                        "description": "NASDAQ market quotes",
                        "topic": "trading.nasdaq.quotes.v1",
                        "freshness": {"max_lag_seconds": 1, "warn_after_seconds": 1},
                        "owner": "market-data-team",
                    },
                    {
                        "name": "cboe_quotes",
                        "description": "CBOE options quotes",
                        "topic": "trading.cboe.quotes.v1",
                        "owner": "market-data-team",
                    },
                    {
                        "name": "reference_data",
                        "description": "Security reference data",
                        "topic": "trading.reference.securities.v1",
                        "owner": "reference-data-team",
                    },
                ],
                "models": [
                    {
                        "name": "consolidated_quotes",
                        "description": "Consolidated quotes from all exchanges",

                        "owner": "market-data-team",
                        "advanced": {
                            "topic": {"partitions": 24, "config": {"retention.ms": "3600000"}},
                            "flink": {"parallelism": 12},
                        },
                        "sql": """
                            SELECT
                                symbol,
                                'NYSE' as exchange,
                                bid_price,
                                ask_price,
                                bid_size,
                                ask_size,
                                event_time
                            FROM {{ source("nyse_quotes") }}
                            UNION ALL
                            SELECT
                                symbol,
                                'NASDAQ' as exchange,
                                bid_price,
                                ask_price,
                                bid_size,
                                ask_size,
                                event_time
                            FROM {{ source("nasdaq_quotes") }}
                        """,
                    },
                    {
                        "name": "nbbo",
                        "description": "National Best Bid and Offer",

                        "owner": "market-data-team",
                        "advanced": {
                            "topic": {"partitions": 12},
                        },
                        "sql": """
                            SELECT
                                symbol,
                                MAX(bid_price) as best_bid,
                                MIN(ask_price) as best_ask,
                                SUM(bid_size) as total_bid_size,
                                SUM(ask_size) as total_ask_size,
                                MAX(event_time) as last_updated
                            FROM {{ ref("consolidated_quotes") }}
                            GROUP BY symbol
                            HAVING MAX(bid_price) > 0 AND MIN(ask_price) > 0
                        """,
                    },
                    {
                        "name": "vwap",
                        "description": "Volume Weighted Average Price per symbol",

                        "owner": "quant-team",
                        "advanced": {
                            "topic": {"partitions": 12},
                        },
                        "sql": """
                            SELECT
                                symbol,
                                TUMBLE_END(event_time, INTERVAL '1' MINUTE) as window_end,
                                SUM(trade_price * trade_volume) / SUM(trade_volume) as vwap,
                                SUM(trade_volume) as total_volume,
                                COUNT(*) as trade_count
                            FROM {{ source("nyse_quotes") }}
                            WHERE trade_price IS NOT NULL
                            GROUP BY
                                symbol,
                                TUMBLE(event_time, INTERVAL '1' MINUTE)
                        """,
                    },
                    {
                        "name": "price_anomalies",
                        "description": "Detected price anomalies",

                        "owner": "surveillance-team",
                        "advanced": {
                            "topic": {"partitions": 6, "config": {"retention.ms": "86400000"}},
                        },
                        "sql": """
                            SELECT
                                n.symbol,
                                n.best_bid,
                                n.best_ask,
                                r.prev_close,
                                ABS(n.best_ask - r.prev_close) / r.prev_close * 100 as pct_change,
                                CASE
                                    WHEN ABS(n.best_ask - r.prev_close) / r.prev_close > 0.10 THEN 'CIRCUIT_BREAKER'
                                    WHEN n.best_ask < n.best_bid THEN 'CROSSED_MARKET'
                                    WHEN (n.best_ask - n.best_bid) / n.best_bid > 0.05 THEN 'WIDE_SPREAD'
                                    ELSE 'SUSPICIOUS'
                                END as anomaly_type,
                                n.last_updated
                            FROM {{ ref("nbbo") }} n
                            JOIN {{ source("reference_data") }} r ON n.symbol = r.symbol
                            WHERE ABS(n.best_ask - r.prev_close) / r.prev_close > 0.05
                               OR n.best_ask < n.best_bid
                               OR (n.best_ask - n.best_bid) / n.best_bid > 0.03
                        """,
                    },
                    {
                        "name": "market_summary",
                        "description": "End of day market summary",

                        "owner": "reporting-team",
                        "advanced": {
                            "topic": {"partitions": 6},
                        },
                        "sql": """
                            SELECT
                                symbol,
                                TUMBLE_END(window_end, INTERVAL '1' DAY) as trading_day,
                                FIRST_VALUE(vwap) as open_vwap,
                                LAST_VALUE(vwap) as close_vwap,
                                MAX(vwap) as high_vwap,
                                MIN(vwap) as low_vwap,
                                SUM(total_volume) as daily_volume
                            FROM {{ ref("vwap") }}
                            GROUP BY
                                symbol,
                                TUMBLE(window_end, INTERVAL '1' DAY)
                        """,
                    },
                ],
                "tests": [
                    {
                        "name": "quote_latency",
                        "model": "consolidated_quotes",
                        "type": "continuous",
                        "assertions": [
                            {"max_lag": {"column": "event_time", "max_seconds": 1}},
                        ],
                    },
                    {
                        "name": "nbbo_validity",
                        "model": "nbbo",
                        "type": "schema",
                        "assertions": [
                            {"not_null": {"columns": ["symbol", "best_bid", "best_ask"]}},
                            {
                                "expression": {
                                    "sql": "best_bid <= best_ask",
                                    "description": "Bid must not exceed ask",
                                }
                            },
                        ],
                    },
                ],
                "exposures": [
                    {
                        "name": "trading_terminals",
                        "type": "application",
                        "role": "consumer",
                        "description": "Trading terminals for desk traders",
                        "consumes": [{"ref": "nbbo"}, {"ref": "vwap"}],
                        "sla": {"max_end_to_end_latency_ms": 100},
                    },
                    {
                        "name": "surveillance_system",
                        "type": "application",
                        "role": "consumer",
                        "description": "Market surveillance and compliance",
                        "consumes": [{"ref": "price_anomalies"}],
                    },
                    {
                        "name": "risk_dashboard",
                        "type": "dashboard",
                        "description": "Real-time risk monitoring",
                        "depends_on": [{"ref": "nbbo"}, {"ref": "market_summary"}],
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid, f"Errors: {[e.message for e in result.errors]}"

            # Verify DAG
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # Check multi-source union
            upstream = dag.get_upstream("consolidated_quotes")
            assert "nyse_quotes" in upstream
            assert "nasdaq_quotes" in upstream

class TestRiskManagement:
    """Test risk management scenarios."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_position_risk_monitoring(self):
        """
        SCENARIO: Real-time position and risk monitoring

        Story: A hedge fund needs to monitor positions across multiple
        portfolios, calculate real-time P&L, track exposure limits,
        and alert when risk thresholds are breached.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "risk-platform",
                    "version": "2.0.0",
                    "description": "Real-time risk monitoring",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "prod",
                        "clusters": {"prod": {"type": "rest", "rest_url": "http://localhost:8082"}},
                    },
                },
                "sources": [
                    {
                        "name": "trades",
                        "description": "Executed trades",
                        "topic": "risk.trades.v1",
                    },
                    {
                        "name": "market_prices",
                        "description": "Current market prices",
                        "topic": "risk.prices.v1",
                        "freshness": {"max_lag_seconds": 5},
                    },
                    {
                        "name": "risk_limits",
                        "description": "Portfolio risk limits",
                        "topic": "risk.limits.v1",
                    },
                    {
                        "name": "fx_rates",
                        "description": "FX exchange rates",
                        "topic": "risk.fx.v1",
                    },
                ],
                "models": [
                    {
                        "name": "positions",
                        "description": "Current positions per portfolio/symbol",

                        "topic": {"partitions": 12},
                        "sql": """
                            SELECT
                                portfolio_id,
                                symbol,
                                SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) as net_quantity,
                                SUM(CASE WHEN side = 'BUY' THEN quantity * price ELSE -quantity * price END) as cost_basis,
                                MAX(trade_time) as last_trade_time
                            FROM {{ source("trades") }}
                            GROUP BY portfolio_id, symbol
                        """,
                    },
                    {
                        "name": "position_valuations",
                        "description": "Current position valuations",

                        "topic": {"partitions": 12},
                        "sql": """
                            SELECT
                                p.portfolio_id,
                                p.symbol,
                                p.net_quantity,
                                p.cost_basis,
                                m.current_price,
                                p.net_quantity * m.current_price as market_value,
                                (p.net_quantity * m.current_price) - p.cost_basis as unrealized_pnl,
                                ((p.net_quantity * m.current_price) - p.cost_basis) / ABS(p.cost_basis) * 100 as pnl_pct,
                                m.event_time as priced_at
                            FROM {{ ref("positions") }} p
                            JOIN {{ source("market_prices") }} m ON p.symbol = m.symbol
                        """,
                    },
                    {
                        "name": "portfolio_exposure",
                        "description": "Portfolio level exposure",

                        "topic": {"partitions": 6},
                        "sql": """
                            SELECT
                                v.portfolio_id,
                                SUM(v.market_value) as total_market_value,
                                SUM(v.unrealized_pnl) as total_unrealized_pnl,
                                SUM(CASE WHEN v.net_quantity > 0 THEN v.market_value ELSE 0 END) as long_exposure,
                                SUM(CASE WHEN v.net_quantity < 0 THEN ABS(v.market_value) ELSE 0 END) as short_exposure,
                                COUNT(DISTINCT v.symbol) as position_count,
                                MAX(v.priced_at) as last_updated
                            FROM {{ ref("position_valuations") }} v
                            GROUP BY v.portfolio_id
                        """,
                    },
                    {
                        "name": "exposure_usd",
                        "description": "Portfolio exposure in USD",

                        "sql": """
                            SELECT
                                e.portfolio_id,
                                e.total_market_value * COALESCE(f.rate_to_usd, 1.0) as market_value_usd,
                                e.long_exposure * COALESCE(f.rate_to_usd, 1.0) as long_exposure_usd,
                                e.short_exposure * COALESCE(f.rate_to_usd, 1.0) as short_exposure_usd,
                                e.total_unrealized_pnl * COALESCE(f.rate_to_usd, 1.0) as unrealized_pnl_usd,
                                e.last_updated
                            FROM {{ ref("portfolio_exposure") }} e
                            LEFT JOIN {{ source("fx_rates") }} f ON e.portfolio_id = f.portfolio_id
                        """,
                    },
                    {
                        "name": "limit_breaches",
                        "description": "Risk limit breaches",

                        "topic": {"partitions": 3},
                        "sql": """
                            SELECT
                                e.portfolio_id,
                                e.market_value_usd,
                                e.long_exposure_usd,
                                e.short_exposure_usd,
                                l.max_market_value,
                                l.max_long_exposure,
                                l.max_short_exposure,
                                CASE
                                    WHEN e.market_value_usd > l.max_market_value THEN 'TOTAL_EXPOSURE'
                                    WHEN e.long_exposure_usd > l.max_long_exposure THEN 'LONG_EXPOSURE'
                                    WHEN e.short_exposure_usd > l.max_short_exposure THEN 'SHORT_EXPOSURE'
                                END as breach_type,
                                CASE
                                    WHEN e.market_value_usd > l.max_market_value * 1.2 THEN 'CRITICAL'
                                    WHEN e.market_value_usd > l.max_market_value * 1.1 THEN 'HIGH'
                                    ELSE 'WARNING'
                                END as severity,
                                e.last_updated as detected_at
                            FROM {{ ref("exposure_usd") }} e
                            JOIN {{ source("risk_limits") }} l ON e.portfolio_id = l.portfolio_id
                            WHERE e.market_value_usd > l.max_market_value
                               OR e.long_exposure_usd > l.max_long_exposure
                               OR e.short_exposure_usd > l.max_short_exposure
                        """,
                    },
                    {
                        "name": "pnl_attribution",
                        "description": "P&L attribution by portfolio and symbol",

                        "sql": """
                            SELECT
                                portfolio_id,
                                symbol,
                                unrealized_pnl,
                                pnl_pct,
                                CASE
                                    WHEN pnl_pct > 10 THEN 'LARGE_GAIN'
                                    WHEN pnl_pct > 0 THEN 'GAIN'
                                    WHEN pnl_pct > -10 THEN 'LOSS'
                                    ELSE 'LARGE_LOSS'
                                END as pnl_category,
                                priced_at
                            FROM {{ ref("position_valuations") }}
                        """,
                    },
                ],
                "exposures": [
                    {
                        "name": "risk_system",
                        "type": "application",
                        "role": "consumer",
                        "description": "Core risk management system",
                        "consumes": [
                            {"ref": "limit_breaches"},
                            {"ref": "portfolio_exposure"},
                        ],
                        "sla": {"max_end_to_end_latency_ms": 5000},
                    },
                    {
                        "name": "portfolio_managers",
                        "type": "dashboard",
                        "description": "Portfolio manager dashboard",
                        "depends_on": [
                            {"ref": "position_valuations"},
                            {"ref": "pnl_attribution"},
                        ],
                    },
                    {
                        "name": "compliance_reporting",
                        "type": "application",
                        "role": "consumer",
                        "description": "Regulatory compliance reporting",
                        "consumes": [{"ref": "exposure_usd"}],
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Verify risk calculation chain
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            assert "position_valuations" in dag.get_upstream("portfolio_exposure")
            assert "portfolio_exposure" in dag.get_upstream("exposure_usd")
            assert "exposure_usd" in dag.get_upstream("limit_breaches")

class TestFraudDetection:
    """Test fraud detection scenarios."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_transaction_fraud_detection(self):
        """
        SCENARIO: Real-time transaction fraud detection

        Story: A payment processor needs to detect fraudulent transactions
        in real-time. The system analyzes transaction patterns, velocity,
        geographic anomalies, and device fingerprints to calculate fraud
        scores and block suspicious transactions.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "fraud-detection",
                    "version": "4.0.0",
                    "description": "Real-time fraud detection platform",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "prod",
                        "clusters": {"prod": {"type": "rest", "rest_url": "http://localhost:8082"}},
                    },
                },
                "governance": {
                    "require_descriptions": True,
                    "model_rules": {
                        "require_tests": ["fraud_score", "blocked_transactions"],
                    },
                },
                "sources": [
                    {
                        "name": "transactions",
                        "description": "Incoming payment transactions",
                        "topic": "fraud.transactions.v1",
                        "freshness": {"max_lag_seconds": 2},
                    },
                    {
                        "name": "customer_profiles",
                        "description": "Customer profile and history",
                        "topic": "fraud.customers.v1",
                    },
                    {
                        "name": "device_fingerprints",
                        "description": "Device fingerprint data",
                        "topic": "fraud.devices.v1",
                    },
                    {
                        "name": "blocked_merchants",
                        "description": "Blocklisted merchants",
                        "topic": "fraud.blocked_merchants.v1",
                    },
                    {
                        "name": "known_fraud_patterns",
                        "description": "Known fraud patterns from ML",
                        "topic": "fraud.patterns.v1",
                    },
                ],
                "models": [
                    {
                        "name": "transaction_enriched",
                        "description": "Transactions enriched with customer data",

                        "topic": {"partitions": 24},
                        "flink": {"parallelism": 12},
                        "sql": """
                            SELECT
                                t.transaction_id,
                                t.customer_id,
                                t.merchant_id,
                                t.amount,
                                t.currency,
                                t.country,
                                t.channel,
                                t.device_id,
                                c.account_age_days,
                                c.total_transactions,
                                c.avg_transaction_amount,
                                c.countries_used,
                                c.is_verified,
                                t.event_time
                            FROM {{ source("transactions") }} t
                            JOIN {{ source("customer_profiles") }} c ON t.customer_id = c.customer_id
                        """,
                    },
                    {
                        "name": "velocity_metrics",
                        "description": "Transaction velocity per customer",

                        "topic": {"partitions": 12},
                        "sql": """
                            SELECT
                                customer_id,
                                HOP_END(event_time, INTERVAL '1' MINUTE, INTERVAL '1' HOUR) as window_end,
                                COUNT(*) as tx_count_1h,
                                SUM(amount) as total_amount_1h,
                                COUNT(DISTINCT merchant_id) as unique_merchants_1h,
                                COUNT(DISTINCT country) as unique_countries_1h,
                                MAX(amount) as max_single_tx_1h
                            FROM {{ ref("transaction_enriched") }}
                            GROUP BY
                                customer_id,
                                HOP(event_time, INTERVAL '1' MINUTE, INTERVAL '1' HOUR)
                        """,
                    },
                    {
                        "name": "device_anomalies",
                        "description": "Device-based anomaly signals",

                        "topic": {"partitions": 6},
                        "sql": """
                            SELECT
                                t.transaction_id,
                                t.customer_id,
                                t.device_id,
                                d.device_type,
                                d.os_version,
                                d.first_seen,
                                d.associated_customers,
                                CASE
                                    WHEN d.associated_customers > 3 THEN 50
                                    WHEN d.first_seen > NOW() - INTERVAL '1' DAY THEN 30
                                    WHEN d.device_type = 'EMULATOR' THEN 80
                                    ELSE 0
                                END as device_risk_score,
                                t.event_time
                            FROM {{ ref("transaction_enriched") }} t
                            JOIN {{ source("device_fingerprints") }} d ON t.device_id = d.device_id
                        """,
                    },
                    {
                        "name": "fraud_score",
                        "description": "Calculated fraud risk score",

                        "topic": {"partitions": 12},
                        "sql": """
                            SELECT
                                t.transaction_id,
                                t.customer_id,
                                t.merchant_id,
                                t.amount,
                                t.country,
                                -- Calculate fraud score components
                                CASE WHEN t.amount > t.avg_transaction_amount * 5 THEN 30 ELSE 0 END +
                                CASE WHEN t.account_age_days < 7 THEN 20 ELSE 0 END +
                                CASE WHEN v.unique_countries_1h > 2 THEN 40 ELSE 0 END +
                                CASE WHEN v.tx_count_1h > 20 THEN 25 ELSE 0 END +
                                COALESCE(d.device_risk_score, 0) +
                                CASE WHEN b.merchant_id IS NOT NULL THEN 100 ELSE 0 END
                                as fraud_score,
                                t.event_time
                            FROM {{ ref("transaction_enriched") }} t
                            LEFT JOIN {{ ref("velocity_metrics") }} v
                                ON t.customer_id = v.customer_id
                            LEFT JOIN {{ ref("device_anomalies") }} d
                                ON t.transaction_id = d.transaction_id
                            LEFT JOIN {{ source("blocked_merchants") }} b
                                ON t.merchant_id = b.merchant_id
                        """,
                    },
                    {
                        "name": "blocked_transactions",
                        "description": "Transactions blocked due to fraud",

                        "topic": {"partitions": 6},
                        "sql": """
                            SELECT
                                transaction_id,
                                customer_id,
                                merchant_id,
                                amount,
                                fraud_score,
                                CASE
                                    WHEN fraud_score >= 80 THEN 'HARD_BLOCK'
                                    WHEN fraud_score >= 60 THEN 'SOFT_BLOCK'
                                    ELSE 'REVIEW'
                                END as block_type,
                                'Fraud score: ' || CAST(fraud_score AS VARCHAR) as block_reason,
                                event_time as blocked_at
                            FROM {{ ref("fraud_score") }}
                            WHERE fraud_score >= 50
                        """,
                    },
                    {
                        "name": "fraud_metrics",
                        "description": "Aggregated fraud metrics",

                        "sql": """
                            SELECT
                                TUMBLE_END(blocked_at, INTERVAL '5' MINUTE) as window_end,
                                block_type,
                                COUNT(*) as block_count,
                                SUM(amount) as blocked_amount,
                                AVG(fraud_score) as avg_fraud_score
                            FROM {{ ref("blocked_transactions") }}
                            GROUP BY
                                TUMBLE(blocked_at, INTERVAL '5' MINUTE),
                                block_type
                        """,
                    },
                ],
                "tests": [
                    {
                        "name": "fraud_score_valid",
                        "model": "fraud_score",
                        "type": "schema",
                        "assertions": [
                            {"not_null": {"columns": ["transaction_id", "fraud_score"]}},
                            {"accepted_values": {"column": "fraud_score", "values": "0-200"}},
                        ],
                    },
                    {
                        "name": "blocked_tx_latency",
                        "model": "blocked_transactions",
                        "type": "continuous",
                        "assertions": [
                            {"max_lag": {"column": "blocked_at", "max_seconds": 5}},
                        ],
                    },
                ],
                "exposures": [
                    {
                        "name": "payment_gateway",
                        "type": "application",
                        "role": "consumer",
                        "description": "Payment gateway for transaction decisions",
                        "consumes": [{"ref": "fraud_score"}],
                        "sla": {"max_end_to_end_latency_ms": 500},
                    },
                    {
                        "name": "fraud_ops_dashboard",
                        "type": "dashboard",
                        "description": "Fraud operations dashboard",
                        "depends_on": [
                            {"ref": "blocked_transactions"},
                            {"ref": "fraud_metrics"},
                        ],
                    },
                    {
                        "name": "ml_feedback_loop",
                        "type": "ml_training",
                        "description": "ML model retraining pipeline",
                        "depends_on": [
                            {"ref": "blocked_transactions"},
                            {"source": "known_fraud_patterns"},
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

            # Verify fraud detection pipeline
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # Check that fraud_score depends on multiple signals
            upstream = dag.get_upstream("fraud_score")
            assert "transaction_enriched" in upstream
            assert "velocity_metrics" in upstream
            assert "device_anomalies" in upstream

class TestRegulatoryCompliance:
    """Test regulatory compliance scenarios."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_transaction_reporting(self):
        """
        SCENARIO: Regulatory transaction reporting (AML/BSA)

        Story: A bank must report suspicious activities and large
        transactions to regulators. The system monitors transactions
        for AML red flags and generates required reports.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "compliance-reporting",
                    "version": "1.0.0",
                    "description": "AML/BSA compliance reporting",
                },
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "flink": {
                        "default": "prod",
                        "clusters": {"prod": {"type": "rest", "rest_url": "http://localhost:8082"}},
                    },
                },
                "governance": {
                    "require_descriptions": True,
                    "require_owners": True,
                },
                "sources": [
                    {
                        "name": "transactions",
                        "description": "All bank transactions",
                        "topic": "compliance.transactions.v1",
                        "owner": "core-banking",
                    },
                    {
                        "name": "customer_kyc",
                        "description": "Customer KYC information",
                        "topic": "compliance.kyc.v1",
                        "owner": "kyc-team",
                    },
                    {
                        "name": "watchlists",
                        "description": "Sanctions and PEP watchlists",
                        "topic": "compliance.watchlists.v1",
                        "owner": "compliance-team",
                    },
                ],
                "models": [
                    {
                        "name": "ctr_candidates",
                        "description": "Currency Transaction Report candidates (>$10k)",

                        "topic": {"partitions": 6},
                        "owner": "compliance-team",
                        "sql": """
                            SELECT
                                transaction_id,
                                customer_id,
                                amount,
                                currency,
                                transaction_type,
                                event_time,
                                'CTR' as report_type
                            FROM {{ source("transactions") }}
                            WHERE currency = 'USD'
                              AND amount >= 10000
                        """,
                    },
                    {
                        "name": "structuring_detection",
                        "description": "Potential structuring activity detection",

                        "topic": {"partitions": 6},
                        "owner": "compliance-team",
                        "sql": """
                            SELECT
                                customer_id,
                                TUMBLE_END(event_time, INTERVAL '1' DAY) as detection_day,
                                COUNT(*) as tx_count,
                                SUM(amount) as total_amount,
                                AVG(amount) as avg_amount,
                                MAX(amount) as max_amount,
                                CASE
                                    WHEN COUNT(*) >= 3
                                     AND SUM(amount) > 10000
                                     AND MAX(amount) < 10000
                                    THEN true
                                    ELSE false
                                END as is_potential_structuring
                            FROM {{ source("transactions") }}
                            WHERE currency = 'USD'
                              AND amount BETWEEN 3000 AND 9999
                            GROUP BY
                                customer_id,
                                TUMBLE(event_time, INTERVAL '1' DAY)
                        """,
                    },
                    {
                        "name": "watchlist_matches",
                        "description": "Matches against sanctions watchlists",

                        "topic": {"partitions": 3},
                        "owner": "compliance-team",
                        "sql": """
                            SELECT
                                t.transaction_id,
                                t.customer_id,
                                k.full_name,
                                w.watchlist_name,
                                w.match_type,
                                w.match_score,
                                t.event_time
                            FROM {{ source("transactions") }} t
                            JOIN {{ source("customer_kyc") }} k ON t.customer_id = k.customer_id
                            JOIN {{ source("watchlists") }} w ON k.full_name LIKE '%' || w.entity_name || '%'
                            WHERE w.match_score >= 0.8
                        """,
                    },
                    {
                        "name": "sar_candidates",
                        "description": "Suspicious Activity Report candidates",

                        "topic": {"partitions": 3},
                        "owner": "compliance-team",
                        "sql": """
                            SELECT
                                s.customer_id,
                                s.detection_day,
                                s.tx_count,
                                s.total_amount,
                                'STRUCTURING' as sar_type,
                                'Multiple transactions under reporting threshold' as description
                            FROM {{ ref("structuring_detection") }} s
                            WHERE s.is_potential_structuring = true
                            UNION ALL
                            SELECT
                                w.customer_id,
                                CAST(w.event_time AS DATE) as detection_day,
                                1 as tx_count,
                                0 as total_amount,
                                'WATCHLIST_MATCH' as sar_type,
                                'Matched against ' || w.watchlist_name as description
                            FROM {{ ref("watchlist_matches") }} w
                        """,
                    },
                ],
                "exposures": [
                    {
                        "name": "compliance_system",
                        "type": "application",
                        "role": "consumer",
                        "description": "Compliance case management",
                        "consumes": [
                            {"ref": "sar_candidates"},
                            {"ref": "ctr_candidates"},
                        ],
                    },
                    {
                        "name": "regulatory_filing",
                        "type": "application",
                        "role": "consumer",
                        "description": "FinCEN filing system",
                        "consumes": [{"ref": "ctr_candidates"}],
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

            # Compile and verify
            compiler = Compiler(project)
            manifest = compiler.compile()

            # Check that compliance topics are created
            topic_artifacts = manifest.artifacts.get("topics", [])
            topic_names = [t["name"] for t in topic_artifacts]
            assert any("ctr" in t.lower() for t in topic_names)
            assert any("sar" in t.lower() for t in topic_names)
