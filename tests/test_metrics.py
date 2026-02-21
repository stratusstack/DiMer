"""Tests for metrics collection and analysis."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest

from dimer.metrics.collector import (
    ConnectionAttempt,
    MetricsCollector,
    QueryMetrics,
)


class TestMetricsCollector:
    """Tests for MetricsCollector."""

    def test_collector_initialization(self):
        """Test metrics collector initialization."""
        collector = MetricsCollector(max_records=100, cleanup_interval=3600)
        assert collector._max_records == 100
        assert collector._cleanup_interval == 3600
        assert len(collector._connection_attempts) == 0
        collector.stop()

    def test_record_connection_attempt(self, metrics_collector):
        """Test recording connection attempts."""
        metrics_collector.record_connection_attempt(
            source_type="snowflake",
            method="arrow",
            success=True,
            duration=1.5,
            connection_id="conn1",
        )

        assert len(metrics_collector._connection_attempts) == 1
        attempt = metrics_collector._connection_attempts[0]
        assert attempt.source_type == "snowflake"
        assert attempt.method == "arrow"
        assert attempt.success is True
        assert attempt.duration == 1.5
        assert attempt.connection_id == "conn1"

    def test_record_query_execution(self, metrics_collector):
        """Test recording query executions."""
        metrics_collector.record_query_execution(
            connection_id="conn1",
            query="SELECT * FROM table1",
            execution_time=0.5,
            rows_returned=100,
            bytes_transferred=5000,
            connection_method="arrow",
        )

        assert len(metrics_collector._query_metrics) == 1
        metric = metrics_collector._query_metrics[0]
        assert metric.connection_id == "conn1"
        assert metric.execution_time == 0.5
        assert metric.rows_returned == 100
        assert metric.bytes_transferred == 5000

    def test_get_connection_statistics_all(self, metrics_collector):
        """Test getting connection statistics for all attempts."""
        # Record multiple attempts
        metrics_collector.record_connection_attempt("snowflake", "arrow", True, 1.0)
        metrics_collector.record_connection_attempt("snowflake", "arrow", True, 2.0)
        metrics_collector.record_connection_attempt(
            "snowflake", "arrow", False, 0.5, "Connection failed"
        )
        metrics_collector.record_connection_attempt("postgresql", "asyncpg", True, 1.5)

        stats = metrics_collector.get_connection_statistics()

        # Should have stats for both source+method combinations
        assert "snowflake_arrow" in stats
        assert "postgresql_asyncpg" in stats

        # Check snowflake stats
        sf_stats = stats["snowflake_arrow"]
        assert sf_stats.total_attempts == 3
        assert sf_stats.successful_attempts == 2
        assert sf_stats.failed_attempts == 1
        assert sf_stats.success_rate == 2 / 3
        assert sf_stats.avg_duration == 1.17  # (1.0 + 2.0 + 0.5) / 3

        # Check postgresql stats
        pg_stats = stats["postgresql_asyncpg"]
        assert pg_stats.total_attempts == 1
        assert pg_stats.successful_attempts == 1
        assert pg_stats.success_rate == 1.0

    def test_get_connection_statistics_filtered(self, metrics_collector):
        """Test getting filtered connection statistics."""
        # Record attempts for different sources
        metrics_collector.record_connection_attempt("snowflake", "arrow", True, 1.0)
        metrics_collector.record_connection_attempt("postgresql", "asyncpg", True, 1.5)

        # Filter by source type
        stats = metrics_collector.get_connection_statistics(source_type="snowflake")
        assert len(stats) == 1
        assert "snowflake_arrow" in stats

        # Filter by method
        stats = metrics_collector.get_connection_statistics(method="arrow")
        assert len(stats) == 1
        assert "snowflake_arrow" in stats

    def test_get_connection_statistics_time_window(self, metrics_collector):
        """Test getting statistics with time window filter."""
        # Record old attempt
        old_attempt = ConnectionAttempt(
            source_type="snowflake",
            method="arrow",
            success=True,
            duration=1.0,
            timestamp=datetime.now() - timedelta(hours=2),
        )
        metrics_collector._connection_attempts.append(old_attempt)

        # Record recent attempt
        metrics_collector.record_connection_attempt("snowflake", "arrow", True, 2.0)

        # Get stats for last hour only
        stats = metrics_collector.get_connection_statistics(
            time_window=timedelta(hours=1)
        )

        # Should only include recent attempt
        sf_stats = stats["snowflake_arrow"]
        assert sf_stats.total_attempts == 1
        assert sf_stats.avg_duration == 2.0

    def test_get_query_statistics(self, metrics_collector):
        """Test getting query statistics."""
        # Record query executions
        metrics_collector.record_query_execution("conn1", "SELECT 1", 0.1, 1, 100)
        metrics_collector.record_query_execution("conn1", "SELECT 2", 0.2, 2, 200)
        metrics_collector.record_query_execution(
            "conn1", "SELECT 3", 0.3, 3, 300, success=False
        )

        stats = metrics_collector.get_query_statistics()

        assert stats["total_queries"] == 3
        assert stats["successful_queries"] == 2
        assert stats["failed_queries"] == 1
        assert stats["success_rate"] == 2 / 3
        assert stats["avg_execution_time"] == 0.15  # (0.1 + 0.2) / 2
        assert stats["total_rows"] == 3  # 1 + 2
        assert stats["total_bytes"] == 300  # 100 + 200

    def test_get_method_comparison(self, metrics_collector):
        """Test comparing methods for a source."""
        # Record attempts with different methods
        metrics_collector.record_connection_attempt("snowflake", "arrow", True, 1.0)
        metrics_collector.record_connection_attempt("snowflake", "arrow", True, 1.2)
        metrics_collector.record_connection_attempt("snowflake", "native", True, 2.0)
        metrics_collector.record_connection_attempt(
            "snowflake", "native", False, 0.5, "Auth error"
        )

        comparison = metrics_collector.get_method_comparison("snowflake")

        assert "arrow" in comparison
        assert "native" in comparison

        # Check arrow method
        arrow_stats = comparison["arrow"]
        assert arrow_stats["usage_frequency"] == 2
        assert arrow_stats["performance_stats"].success_rate == 1.0
        assert arrow_stats["performance_stats"].avg_duration == 1.1

        # Check native method
        native_stats = comparison["native"]
        assert native_stats["usage_frequency"] == 2
        assert native_stats["performance_stats"].success_rate == 0.5
        assert native_stats["error_types"]["Auth error"] == 1

    def test_get_top_errors(self, metrics_collector):
        """Test getting top errors."""
        # Record failed attempts with different errors
        metrics_collector.record_connection_attempt(
            "snowflake", "arrow", False, 1.0, "Auth failed"
        )
        metrics_collector.record_connection_attempt(
            "snowflake", "arrow", False, 1.0, "Auth failed"
        )
        metrics_collector.record_connection_attempt(
            "snowflake", "native", False, 1.0, "Network timeout"
        )
        metrics_collector.record_connection_attempt(
            "postgresql", "asyncpg", False, 1.0, "Auth failed"
        )

        # Get top errors for all sources
        errors = metrics_collector.get_top_errors()
        assert len(errors) >= 2
        assert errors[0] == ("Auth failed", 3)  # Most frequent
        assert errors[1] == ("Network timeout", 1)

        # Get top errors for specific source
        sf_errors = metrics_collector.get_top_errors(source_type="snowflake")
        assert len(sf_errors) == 2
        assert sf_errors[0] == ("Auth failed", 2)

    def test_max_records_limit(self, metrics_collector):
        """Test that records are trimmed when exceeding max limit."""
        metrics_collector._max_records = 5

        # Add more records than the limit
        for i in range(10):
            metrics_collector.record_connection_attempt(
                f"source{i}", "method", True, 1.0
            )

        # Should only keep the last 5 records
        assert len(metrics_collector._connection_attempts) == 5

        # Should be the most recent records
        source_types = [
            attempt.source_type for attempt in metrics_collector._connection_attempts
        ]
        assert source_types == ["source5", "source6", "source7", "source8", "source9"]

    def test_export_metrics(self, metrics_collector):
        """Test exporting metrics data."""
        # Add some test data
        metrics_collector.record_connection_attempt(
            "snowflake", "arrow", True, 1.0, connection_id="conn1"
        )
        metrics_collector.record_query_execution("conn1", "SELECT 1", 0.5, 10, 1000)

        # Export as dictionary
        exported = metrics_collector.export_metrics(format="dict")

        assert "connection_attempts" in exported
        assert "query_metrics" in exported
        assert len(exported["connection_attempts"]) == 1
        assert len(exported["query_metrics"]) == 1

        # Check connection attempt data
        conn_attempt = exported["connection_attempts"][0]
        assert conn_attempt["source_type"] == "snowflake"
        assert conn_attempt["method"] == "arrow"
        assert conn_attempt["success"] is True

        # Check query metric data
        query_metric = exported["query_metrics"][0]
        assert query_metric["connection_id"] == "conn1"
        assert query_metric["execution_time"] == 0.5
        assert query_metric["rows_returned"] == 10

    def test_export_unsupported_format(self, metrics_collector):
        """Test exporting with unsupported format."""
        with pytest.raises(ValueError, match="Unsupported export format"):
            metrics_collector.export_metrics(format="xml")

    def test_clear_metrics(self, metrics_collector):
        """Test clearing all metrics."""
        # Add some data
        metrics_collector.record_connection_attempt("snowflake", "arrow", True, 1.0)
        metrics_collector.record_query_execution("conn1", "SELECT 1", 0.5, 10, 1000)

        assert len(metrics_collector._connection_attempts) > 0
        assert len(metrics_collector._query_metrics) > 0

        # Clear metrics
        metrics_collector.clear_metrics()

        assert len(metrics_collector._connection_attempts) == 0
        assert len(metrics_collector._query_metrics) == 0

    @patch("time.sleep")
    def test_cleanup_old_records(self, mock_sleep, metrics_collector):
        """Test background cleanup of old records."""
        # Add old records
        old_timestamp = datetime.now() - timedelta(hours=25)
        old_attempt = ConnectionAttempt(
            source_type="snowflake",
            method="arrow",
            success=True,
            duration=1.0,
            timestamp=old_timestamp,
        )
        old_metric = QueryMetrics(
            connection_id="conn1",
            query_hash="abc123",
            execution_time=0.5,
            rows_returned=10,
            bytes_transferred=1000,
            success=True,
            timestamp=old_timestamp,
        )

        metrics_collector._connection_attempts.append(old_attempt)
        metrics_collector._query_metrics.append(old_metric)

        # Add recent records
        metrics_collector.record_connection_attempt("snowflake", "arrow", True, 1.0)
        metrics_collector.record_query_execution("conn1", "SELECT 1", 0.5, 10, 1000)

        # Manually run cleanup
        with patch.object(metrics_collector, "_cleanup_running", True):
            # Mock the loop to run only once
            mock_sleep.side_effect = [None, Exception("Break loop")]

            try:
                metrics_collector._cleanup_old_records()
            except Exception:
                pass  # Expected to break the loop

        # Old records should be removed, recent ones kept
        assert len(metrics_collector._connection_attempts) == 1
        assert len(metrics_collector._query_metrics) == 1

        # Remaining records should be recent
        assert metrics_collector._connection_attempts[0].timestamp > old_timestamp
        assert metrics_collector._query_metrics[0].timestamp > old_timestamp

    def test_percentile_calculations(self, metrics_collector):
        """Test percentile calculations in performance stats."""
        # Add attempts with known durations for percentile testing
        durations = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        for duration in durations:
            metrics_collector.record_connection_attempt(
                "test", "method", True, duration
            )

        stats = metrics_collector.get_connection_statistics()
        test_stats = stats["test_method"]

        # Check percentile calculations
        assert test_stats.min_duration == 1.0
        assert test_stats.max_duration == 10.0
        assert test_stats.median_duration == 5.5  # median of 1-10
        assert test_stats.avg_duration == 5.5
        # 95th percentile of 10 items should be the 9th item (index 8) = 9.0
        assert test_stats.p95_duration == 9.0
