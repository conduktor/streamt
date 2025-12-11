"""IoT and Telemetry domain test scenarios.

Simulates real-world IoT use cases:
- Industrial sensor monitoring
- Fleet management/vehicle tracking
- Smart building management
- Environmental monitoring
"""

import tempfile
from pathlib import Path

import yaml

from streamt.core.dag import DAGBuilder
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator

class TestIndustrialSensorMonitoring:
    """Test industrial IoT sensor monitoring scenarios."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_predictive_maintenance_pipeline(self):
        """
        SCENARIO: Predictive maintenance for industrial equipment

        Story: A manufacturing plant has hundreds of machines with sensors
        reporting temperature, vibration, and power consumption. The system
        needs to detect anomalies, predict failures, and generate maintenance
        alerts before equipment fails.

        Pipeline:
        sensor_readings -> readings_cleaned -> readings_aggregated ->
            -> anomaly_detection -> maintenance_alerts
            -> equipment_health_scores
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {
                    "name": "predictive-maintenance",
                    "version": "2.1.0",
                    "description": "Industrial predictive maintenance system",
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
                        "name": "sensor_readings_raw",
                        "description": "Raw sensor data from industrial equipment",
                        "topic": "iot.sensors.raw.v1",
                        "freshness": {"max_lag_seconds": 30, "warn_after_seconds": 10},
                    },
                    {
                        "name": "equipment_metadata",
                        "description": "Equipment master data and thresholds",
                        "topic": "iot.equipment.metadata.v1",
                    },
                    {
                        "name": "maintenance_history",
                        "description": "Historical maintenance records",
                        "topic": "iot.maintenance.history.v1",
                    },
                ],
                "models": [
                    {
                        "name": "sensor_readings_cleaned",
                        "description": "Cleaned and normalized sensor readings",

                        "advanced": {
                            "topic": {"partitions": 24},
                            "flink": {"parallelism": 8},
                        },
                        "sql": """
                            SELECT
                                equipment_id,
                                sensor_type,
                                CASE
                                    WHEN sensor_value < -1000 OR sensor_value > 10000 THEN NULL
                                    ELSE sensor_value
                                END as sensor_value,
                                CASE
                                    WHEN sensor_type = 'temperature' THEN 'celsius'
                                    WHEN sensor_type = 'vibration' THEN 'mm/s'
                                    WHEN sensor_type = 'power' THEN 'kw'
                                    ELSE 'unknown'
                                END as unit,
                                event_time,
                                processing_time
                            FROM {{ source("sensor_readings_raw") }}
                            WHERE sensor_value IS NOT NULL
                        """,
                    },
                    {
                        "name": "sensor_readings_1min",
                        "description": "1-minute aggregated sensor readings",

                        "advanced": {
                            "topic": {"partitions": 12},
                        },
                        "sql": """
                            SELECT
                                equipment_id,
                                sensor_type,
                                TUMBLE_END(event_time, INTERVAL '1' MINUTE) as window_end,
                                AVG(sensor_value) as avg_value,
                                MIN(sensor_value) as min_value,
                                MAX(sensor_value) as max_value,
                                STDDEV(sensor_value) as stddev_value,
                                COUNT(*) as reading_count
                            FROM {{ ref("sensor_readings_cleaned") }}
                            GROUP BY
                                equipment_id,
                                sensor_type,
                                TUMBLE(event_time, INTERVAL '1' MINUTE)
                        """,
                    },
                    {
                        "name": "sensor_readings_5min",
                        "description": "5-minute aggregated sensor readings",

                        "advanced": {
                            "topic": {"partitions": 6},
                        },
                        "sql": """
                            SELECT
                                equipment_id,
                                sensor_type,
                                TUMBLE_END(window_end, INTERVAL '5' MINUTE) as window_end,
                                AVG(avg_value) as avg_value,
                                MIN(min_value) as min_value,
                                MAX(max_value) as max_value,
                                AVG(stddev_value) as avg_stddev,
                                SUM(reading_count) as total_readings
                            FROM {{ ref("sensor_readings_1min") }}
                            GROUP BY
                                equipment_id,
                                sensor_type,
                                TUMBLE(window_end, INTERVAL '5' MINUTE)
                        """,
                    },
                    {
                        "name": "anomaly_detection",
                        "description": "Detected anomalies based on thresholds",

                        "advanced": {
                            "topic": {"partitions": 6},
                        },
                        "sql": """
                            SELECT
                                s.equipment_id,
                                s.sensor_type,
                                s.avg_value,
                                e.threshold_min,
                                e.threshold_max,
                                CASE
                                    WHEN s.avg_value < e.threshold_min THEN 'LOW'
                                    WHEN s.avg_value > e.threshold_max THEN 'HIGH'
                                    WHEN s.avg_stddev > e.stddev_threshold THEN 'UNSTABLE'
                                    ELSE 'NORMAL'
                                END as anomaly_type,
                                s.window_end as detected_at
                            FROM {{ ref("sensor_readings_5min") }} s
                            JOIN {{ source("equipment_metadata") }} e
                                ON s.equipment_id = e.equipment_id
                                AND s.sensor_type = e.sensor_type
                            WHERE s.avg_value < e.threshold_min
                               OR s.avg_value > e.threshold_max
                               OR s.avg_stddev > e.stddev_threshold
                        """,
                    },
                    {
                        "name": "equipment_health_score",
                        "description": "Real-time equipment health score (0-100)",

                        "advanced": {
                            "topic": {"partitions": 6},
                        },
                        "sql": """
                            SELECT
                                equipment_id,
                                TUMBLE_END(detected_at, INTERVAL '15' MINUTE) as window_end,
                                100 - (COUNT(CASE WHEN anomaly_type != 'NORMAL' THEN 1 END) * 10) as health_score,
                                COLLECT(DISTINCT anomaly_type) as anomaly_types
                            FROM {{ ref("anomaly_detection") }}
                            GROUP BY
                                equipment_id,
                                TUMBLE(detected_at, INTERVAL '15' MINUTE)
                        """,
                    },
                    {
                        "name": "maintenance_alerts",
                        "description": "Maintenance alerts for equipment needing attention",

                        "advanced": {
                            "topic": {"partitions": 3},
                        },
                        "sql": """
                            SELECT
                                equipment_id,
                                health_score,
                                anomaly_types,
                                CASE
                                    WHEN health_score < 50 THEN 'CRITICAL'
                                    WHEN health_score < 70 THEN 'WARNING'
                                    ELSE 'INFO'
                                END as severity,
                                window_end as alert_time
                            FROM {{ ref("equipment_health_score") }}
                            WHERE health_score < 80
                        """,
                    },
                ],
                "tests": [
                    {
                        "name": "sensor_freshness",
                        "model": "sensor_readings_cleaned",
                        "type": "continuous",
                        "assertions": [
                            {"max_lag": {"column": "event_time", "max_seconds": 60}},
                            {"throughput": {"min_per_second": 1000}},
                        ],
                    },
                    {
                        "name": "anomaly_detection_schema",
                        "model": "anomaly_detection",
                        "type": "schema",
                        "assertions": [
                            {"not_null": {"columns": ["equipment_id", "anomaly_type"]}},
                            {
                                "accepted_values": {
                                    "column": "anomaly_type",
                                    "values": ["LOW", "HIGH", "UNSTABLE", "NORMAL"],
                                }
                            },
                        ],
                    },
                ],
                "exposures": [
                    {
                        "name": "maintenance_scheduler",
                        "type": "application",
                        "role": "consumer",
                        "description": "Automatic maintenance scheduling system",
                        "consumes": [{"ref": "maintenance_alerts"}],
                    },
                    {
                        "name": "plant_monitoring_dashboard",
                        "type": "dashboard",
                        "description": "Real-time plant monitoring dashboard",
                        "depends_on": [
                            {"ref": "sensor_readings_1min"},
                            {"ref": "equipment_health_score"},
                            {"ref": "anomaly_detection"},
                        ],
                    },
                    {
                        "name": "ml_training_pipeline",
                        "type": "ml_training",
                        "description": "ML model training for failure prediction",
                        "depends_on": [
                            {"ref": "sensor_readings_5min"},
                            {"source": "maintenance_history"},
                        ],
                        "schedule": "0 2 * * *",  # Daily at 2 AM
                    },
                ],
            }

            project_path = self._create_project(tmpdir, config)
            parser = ProjectParser(project_path)
            project = parser.parse()

            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid, f"Errors: {[e.message for e in result.errors]}"

            # Verify multi-level aggregation chain
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            # Verify cascading dependencies
            assert "sensor_readings_cleaned" in dag.get_upstream("sensor_readings_1min")
            assert "sensor_readings_1min" in dag.get_upstream("sensor_readings_5min")
            assert "sensor_readings_5min" in dag.get_upstream("anomaly_detection")

            # Verify exposures are connected
            assert "plant_monitoring_dashboard" in dag.get_downstream("equipment_health_score")

class TestFleetManagement:
    """Test vehicle fleet management scenarios."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_vehicle_tracking_pipeline(self):
        """
        SCENARIO: Real-time fleet tracking and route optimization

        Story: A logistics company tracks thousands of delivery vehicles.
        GPS data is processed to calculate ETAs, detect route deviations,
        and optimize delivery schedules.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "fleet-tracking", "version": "1.0.0"},
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
                        "name": "gps_events",
                        "description": "Raw GPS coordinates from vehicles",
                        "topic": "fleet.gps.v1",
                        "freshness": {"max_lag_seconds": 10},
                    },
                    {
                        "name": "delivery_assignments",
                        "description": "Vehicle to delivery assignments",
                        "topic": "fleet.assignments.v1",
                    },
                    {
                        "name": "geofences",
                        "description": "Geofence definitions",
                        "topic": "fleet.geofences.v1",
                    },
                ],
                "models": [
                    {
                        "name": "vehicle_positions",
                        "description": "Latest vehicle positions with speed",

                        "topic": {"partitions": 12},
                        "sql": """
                            SELECT
                                vehicle_id,
                                latitude,
                                longitude,
                                speed_kmh,
                                heading,
                                event_time,
                                LAG(latitude) OVER (PARTITION BY vehicle_id ORDER BY event_time) as prev_lat,
                                LAG(longitude) OVER (PARTITION BY vehicle_id ORDER BY event_time) as prev_lon
                            FROM {{ source("gps_events") }}
                        """,
                    },
                    {
                        "name": "vehicle_geofence_events",
                        "description": "Geofence entry/exit events",

                        "topic": {"partitions": 6},
                        "sql": """
                            SELECT
                                v.vehicle_id,
                                g.geofence_id,
                                g.geofence_name,
                                CASE
                                    WHEN ST_CONTAINS(g.boundary, ST_POINT(v.longitude, v.latitude))
                                    THEN 'ENTER'
                                    ELSE 'EXIT'
                                END as event_type,
                                v.event_time
                            FROM {{ ref("vehicle_positions") }} v
                            CROSS JOIN {{ source("geofences") }} g
                        """,
                    },
                    {
                        "name": "delivery_eta",
                        "description": "Real-time delivery ETA calculations",

                        "topic": {"partitions": 6},
                        "sql": """
                            SELECT
                                v.vehicle_id,
                                d.delivery_id,
                                d.destination_lat,
                                d.destination_lon,
                                v.latitude as current_lat,
                                v.longitude as current_lon,
                                v.speed_kmh,
                                -- Simplified ETA calculation
                                (SQRT(POWER(d.destination_lat - v.latitude, 2) +
                                      POWER(d.destination_lon - v.longitude, 2)) * 111)
                                    / GREATEST(v.speed_kmh, 1) * 60 as eta_minutes,
                                v.event_time as calculated_at
                            FROM {{ ref("vehicle_positions") }} v
                            JOIN {{ source("delivery_assignments") }} d
                                ON v.vehicle_id = d.vehicle_id
                            WHERE d.status = 'IN_PROGRESS'
                        """,
                    },
                    {
                        "name": "late_deliveries",
                        "description": "Deliveries at risk of being late",

                        "topic": {"partitions": 3},
                        "sql": """
                            SELECT
                                vehicle_id,
                                delivery_id,
                                eta_minutes,
                                d.promised_time,
                                TIMESTAMPDIFF(MINUTE, calculated_at, d.promised_time) as minutes_until_deadline,
                                CASE
                                    WHEN eta_minutes > TIMESTAMPDIFF(MINUTE, calculated_at, d.promised_time) + 10
                                    THEN 'CRITICAL'
                                    WHEN eta_minutes > TIMESTAMPDIFF(MINUTE, calculated_at, d.promised_time)
                                    THEN 'AT_RISK'
                                    ELSE 'ON_TIME'
                                END as risk_status
                            FROM {{ ref("delivery_eta") }} e
                            JOIN {{ source("delivery_assignments") }} d ON e.delivery_id = d.delivery_id
                            WHERE eta_minutes > TIMESTAMPDIFF(MINUTE, calculated_at, d.promised_time) - 5
                        """,
                    },
                    {
                        "name": "fleet_metrics",
                        "description": "Aggregated fleet performance metrics",

                        "sql": """
                            SELECT
                                TUMBLE_END(event_time, INTERVAL '5' MINUTE) as window_end,
                                COUNT(DISTINCT vehicle_id) as active_vehicles,
                                AVG(speed_kmh) as avg_speed,
                                COUNT(*) as total_pings
                            FROM {{ ref("vehicle_positions") }}
                            GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE)
                        """,
                    },
                ],
                "exposures": [
                    {
                        "name": "dispatch_system",
                        "type": "application",
                        "role": "consumer",
                        "description": "Real-time dispatch and routing system",
                        "consumes": [
                            {"ref": "vehicle_positions"},
                            {"ref": "late_deliveries"},
                        ],
                    },
                    {
                        "name": "customer_notifications",
                        "type": "application",
                        "role": "consumer",
                        "description": "Customer ETA notification service",
                        "consumes": [{"ref": "delivery_eta"}],
                    },
                    {
                        "name": "fleet_ops_dashboard",
                        "type": "dashboard",
                        "description": "Fleet operations dashboard",
                        "depends_on": [
                            {"ref": "fleet_metrics"},
                            {"ref": "vehicle_geofence_events"},
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

class TestSmartBuilding:
    """Test smart building IoT scenarios."""

    def _create_project(self, tmpdir: str, config: dict) -> Path:
        project_path = Path(tmpdir)
        with open(project_path / "stream_project.yml", "w") as f:
            yaml.dump(config, f)
        return project_path

    def test_hvac_optimization_pipeline(self):
        """
        SCENARIO: Smart building HVAC optimization

        Story: A commercial building uses IoT sensors to optimize
        heating/cooling. Sensors report temperature, humidity, occupancy,
        and CO2 levels. System generates HVAC commands based on comfort
        and energy efficiency goals.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "project": {"name": "smart-building", "version": "1.0.0"},
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
                    {"name": "temperature_sensors", "topic": "building.temp.v1"},
                    {"name": "humidity_sensors", "topic": "building.humidity.v1"},
                    {"name": "occupancy_sensors", "topic": "building.occupancy.v1"},
                    {"name": "co2_sensors", "topic": "building.co2.v1"},
                    {"name": "energy_prices", "topic": "energy.prices.v1"},
                ],
                "models": [
                    {
                        "name": "zone_environment",
                        "description": "Combined environment metrics per zone",

                        "topic": {"partitions": 6},
                        "sql": """
                            SELECT
                                t.zone_id,
                                t.temperature,
                                h.humidity,
                                o.occupant_count,
                                c.co2_level,
                                t.event_time
                            FROM {{ source("temperature_sensors") }} t
                            JOIN {{ source("humidity_sensors") }} h
                                ON t.zone_id = h.zone_id
                                AND t.event_time BETWEEN h.event_time - INTERVAL '30' SECOND
                                    AND h.event_time + INTERVAL '30' SECOND
                            JOIN {{ source("occupancy_sensors") }} o
                                ON t.zone_id = o.zone_id
                            JOIN {{ source("co2_sensors") }} c
                                ON t.zone_id = c.zone_id
                        """,
                    },
                    {
                        "name": "comfort_score",
                        "description": "Calculated comfort score per zone",

                        "sql": """
                            SELECT
                                zone_id,
                                temperature,
                                humidity,
                                co2_level,
                                occupant_count,
                                -- Comfort score: 100 = perfect
                                100 -
                                    ABS(temperature - 22) * 5 -  -- Ideal temp 22C
                                    ABS(humidity - 45) * 0.5 -   -- Ideal humidity 45%
                                    CASE WHEN co2_level > 800 THEN (co2_level - 800) * 0.1 ELSE 0 END
                                as comfort_score,
                                event_time
                            FROM {{ ref("zone_environment") }}
                        """,
                    },
                    {
                        "name": "hvac_commands",
                        "description": "HVAC control commands",

                        "topic": {"partitions": 3},
                        "sql": """
                            SELECT
                                z.zone_id,
                                CASE
                                    WHEN z.temperature > 24 AND z.occupant_count > 0 THEN 'COOL'
                                    WHEN z.temperature < 20 AND z.occupant_count > 0 THEN 'HEAT'
                                    WHEN z.occupant_count = 0 THEN 'ECO_MODE'
                                    ELSE 'MAINTAIN'
                                END as command,
                                CASE
                                    WHEN e.price_tier = 'PEAK' THEN 'LOW'
                                    ELSE 'NORMAL'
                                END as intensity,
                                z.event_time as commanded_at
                            FROM {{ ref("zone_environment") }} z
                            CROSS JOIN {{ source("energy_prices") }} e
                            WHERE z.comfort_score < 80 OR z.occupant_count = 0
                        """,
                    },
                    {
                        "name": "energy_consumption",
                        "description": "Aggregated energy consumption per zone",

                        "sql": """
                            SELECT
                                zone_id,
                                TUMBLE_END(commanded_at, INTERVAL '1' HOUR) as hour,
                                COUNT(CASE WHEN command IN ('COOL', 'HEAT') THEN 1 END) as hvac_cycles,
                                SUM(CASE
                                    WHEN command = 'COOL' THEN 3.5
                                    WHEN command = 'HEAT' THEN 4.0
                                    WHEN command = 'ECO_MODE' THEN 0.5
                                    ELSE 1.0
                                END) as estimated_kwh
                            FROM {{ ref("hvac_commands") }}
                            GROUP BY zone_id, TUMBLE(commanded_at, INTERVAL '1' HOUR)
                        """,
                    },
                ],
                "exposures": [
                    {
                        "name": "hvac_controller",
                        "type": "application",
                        "role": "consumer",
                        "description": "Building HVAC control system",
                        "consumes": [{"ref": "hvac_commands"}],
                        "sla": {"max_end_to_end_latency_ms": 5000},
                    },
                    {
                        "name": "building_dashboard",
                        "type": "dashboard",
                        "description": "Building management dashboard",
                        "depends_on": [
                            {"ref": "comfort_score"},
                            {"ref": "energy_consumption"},
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

            # Verify multi-source joins
            dag_builder = DAGBuilder(project)
            dag = dag_builder.build()

            upstream = dag.get_upstream("zone_environment")
            assert "temperature_sensors" in upstream
            assert "humidity_sensors" in upstream
            assert "occupancy_sensors" in upstream
            assert "co2_sensors" in upstream
