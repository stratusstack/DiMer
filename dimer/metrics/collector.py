"""Performance metrics collection and analysis."""

import statistics
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class ConnectionAttempt:
    """Record of a connection attempt."""

    source_type: str
    method: str
    success: bool
    duration: float
    error_message: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    connection_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryMetrics:
    """Metrics for a single query execution."""

    connection_id: str
    query_hash: str
    execution_time: float
    rows_returned: int
    bytes_transferred: int
    success: bool
    error_message: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    connection_method: Optional[str] = None


@dataclass
class PerformanceStats:
    """Aggregated performance statistics."""

    total_attempts: int
    successful_attempts: int
    failed_attempts: int
    success_rate: float
    avg_duration: float
    min_duration: float
    max_duration: float
    median_duration: float
    p95_duration: float
    total_queries: int = 0
    avg_query_time: float = 0.0
    total_bytes: int = 0


class MetricsCollector:
    """Collect and analyze connection and query performance metrics."""

    def __init__(self, max_records: int = 10000, cleanup_interval: int = 3600):
        """
        Initialize metrics collector.

        Args:
            max_records: Maximum number of records to keep in memory
            cleanup_interval: Interval in seconds to clean up old records
        """
        self._connection_attempts: List[ConnectionAttempt] = []
        self._query_metrics: List[QueryMetrics] = []
        self._max_records = max_records
        self._cleanup_interval = cleanup_interval
        self._lock = threading.RLock()

        # Start cleanup thread
        self._cleanup_running = True
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_old_records, daemon=True
        )
        self._cleanup_thread.start()

        logger.info("Metrics collector initialized", max_records=max_records)

    def record_connection_attempt(
        self,
        source_type: str,
        method: str,
        success: bool,
        duration: float,
        error_message: Optional[str] = None,
        connection_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Record a connection attempt.

        Args:
            source_type: Type of data source
            method: Connection method used
            success: Whether connection succeeded
            duration: Time taken for connection attempt
            error_message: Error message if failed
            connection_id: Optional connection identifier
            metadata: Additional metadata
        """
        with self._lock:
            attempt = ConnectionAttempt(
                source_type=source_type,
                method=method,
                success=success,
                duration=duration,
                error_message=error_message,
                connection_id=connection_id,
                metadata=metadata or {},
            )

            self._connection_attempts.append(attempt)

            # Trim if exceeding max records
            if len(self._connection_attempts) > self._max_records:
                self._connection_attempts = self._connection_attempts[
                    -self._max_records :
                ]

        logger.debug(
            "Connection attempt recorded",
            source_type=source_type,
            method=method,
            success=success,
            duration=duration,
        )

    def record_query_execution(
        self,
        connection_id: str,
        query: str,
        execution_time: float,
        rows_returned: int,
        bytes_transferred: int = 0,
        success: bool = True,
        error_message: Optional[str] = None,
        connection_method: Optional[str] = None,
    ) -> None:
        """
        Record a query execution.

        Args:
            connection_id: Connection identifier
            query: SQL query executed
            execution_time: Time taken for query execution
            rows_returned: Number of rows returned
            bytes_transferred: Bytes transferred (if available)
            success: Whether query succeeded
            error_message: Error message if failed
            connection_method: Connection method used
        """
        with self._lock:
            # Create a hash of the query for grouping similar queries
            query_hash = str(hash(query.strip().lower()))[:12]

            metric = QueryMetrics(
                connection_id=connection_id,
                query_hash=query_hash,
                execution_time=execution_time,
                rows_returned=rows_returned,
                bytes_transferred=bytes_transferred,
                success=success,
                error_message=error_message,
                connection_method=connection_method,
            )

            self._query_metrics.append(metric)

            # Trim if exceeding max records
            if len(self._query_metrics) > self._max_records:
                self._query_metrics = self._query_metrics[-self._max_records :]

        logger.debug(
            "Query execution recorded",
            connection_id=connection_id,
            execution_time=execution_time,
            rows_returned=rows_returned,
            success=success,
        )

    def get_connection_statistics(
        self,
        source_type: Optional[str] = None,
        method: Optional[str] = None,
        time_window: Optional[timedelta] = None,
    ) -> Dict[str, PerformanceStats]:
        """
        Get connection performance statistics.

        Args:
            source_type: Filter by source type
            method: Filter by connection method
            time_window: Only include records within this time window

        Returns:
            Dictionary of performance statistics grouped by source_type+method
        """
        with self._lock:
            # Filter attempts based on criteria
            filtered_attempts = self._filter_connection_attempts(
                source_type=source_type, method=method, time_window=time_window
            )

            # Group by source_type + method
            grouped_stats = defaultdict(list)
            for attempt in filtered_attempts:
                key = f"{attempt.source_type}_{attempt.method}"
                grouped_stats[key].append(attempt)

            # Calculate statistics for each group
            result = {}
            for key, attempts in grouped_stats.items():
                result[key] = self._calculate_performance_stats(attempts)

            return result

    def get_query_statistics(
        self,
        connection_id: Optional[str] = None,
        connection_method: Optional[str] = None,
        time_window: Optional[timedelta] = None,
    ) -> Dict[str, Any]:
        """
        Get query performance statistics.

        Args:
            connection_id: Filter by connection ID
            connection_method: Filter by connection method
            time_window: Only include records within this time window

        Returns:
            Dictionary containing query statistics
        """
        with self._lock:
            # Filter metrics based on criteria
            filtered_metrics = self._filter_query_metrics(
                connection_id=connection_id,
                connection_method=connection_method,
                time_window=time_window,
            )

            if not filtered_metrics:
                return {
                    "total_queries": 0,
                    "successful_queries": 0,
                    "failed_queries": 0,
                    "success_rate": 0.0,
                    "avg_execution_time": 0.0,
                    "total_rows": 0,
                    "total_bytes": 0,
                    "queries_per_minute": 0.0,
                }

            # Calculate statistics
            successful = [m for m in filtered_metrics if m.success]
            failed = [m for m in filtered_metrics if not m.success]

            execution_times = [m.execution_time for m in successful]
            total_rows = sum(m.rows_returned for m in successful)
            total_bytes = sum(m.bytes_transferred for m in successful)

            # Calculate queries per minute
            if len(filtered_metrics) > 1:
                time_span = (
                    filtered_metrics[-1].timestamp - filtered_metrics[0].timestamp
                ).total_seconds()
                queries_per_minute = (
                    len(filtered_metrics) / (time_span / 60) if time_span > 0 else 0
                )
            else:
                queries_per_minute = 0

            return {
                "total_queries": len(filtered_metrics),
                "successful_queries": len(successful),
                "failed_queries": len(failed),
                "success_rate": len(successful) / len(filtered_metrics),
                "avg_execution_time": (
                    statistics.mean(execution_times) if execution_times else 0
                ),
                "min_execution_time": min(execution_times) if execution_times else 0,
                "max_execution_time": max(execution_times) if execution_times else 0,
                "median_execution_time": (
                    statistics.median(execution_times) if execution_times else 0
                ),
                "total_rows": total_rows,
                "total_bytes": total_bytes,
                "queries_per_minute": queries_per_minute,
                "avg_rows_per_query": total_rows / len(successful) if successful else 0,
            }

    def get_method_comparison(
        self, source_type: str, time_window: Optional[timedelta] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Compare performance across different connection methods for a source.

        Args:
            source_type: The data source type to analyze
            time_window: Only include records within this time window

        Returns:
            Dictionary with method names as keys and performance metrics as values
        """
        with self._lock:
            filtered_attempts = self._filter_connection_attempts(
                source_type=source_type, time_window=time_window
            )

            # Group by method
            method_groups = defaultdict(list)
            for attempt in filtered_attempts:
                method_groups[attempt.method].append(attempt)

            # Calculate statistics for each method
            result = {}
            for method, attempts in method_groups.items():
                stats = self._calculate_performance_stats(attempts)

                # Add method-specific metrics
                result[method] = {
                    "performance_stats": stats,
                    "usage_frequency": len(attempts),
                    "last_used": (
                        max(attempt.timestamp for attempt in attempts)
                        if attempts
                        else None
                    ),
                    "error_types": self._get_error_frequency(
                        [a for a in attempts if not a.success]
                    ),
                }

            return result

    def get_top_errors(
        self,
        source_type: Optional[str] = None,
        limit: int = 10,
        time_window: Optional[timedelta] = None,
    ) -> List[Tuple[str, int]]:
        """
        Get the most frequent connection errors.

        Args:
            source_type: Filter by source type
            limit: Maximum number of errors to return
            time_window: Only include records within this time window

        Returns:
            List of tuples (error_message, count) sorted by frequency
        """
        with self._lock:
            filtered_attempts = self._filter_connection_attempts(
                source_type=source_type, time_window=time_window
            )

            failed_attempts = [
                a for a in filtered_attempts if not a.success and a.error_message
            ]
            error_counts = defaultdict(int)

            for attempt in failed_attempts:
                # Normalize error message
                error_msg = attempt.error_message.strip()[
                    :100
                ]  # Truncate long messages
                error_counts[error_msg] += 1

            # Sort by frequency and return top errors
            return sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[
                :limit
            ]

    def _filter_connection_attempts(
        self,
        source_type: Optional[str] = None,
        method: Optional[str] = None,
        time_window: Optional[timedelta] = None,
    ) -> List[ConnectionAttempt]:
        """Filter connection attempts based on criteria."""
        cutoff_time = datetime.now() - time_window if time_window else None

        return [
            attempt
            for attempt in self._connection_attempts
            if (source_type is None or attempt.source_type == source_type)
            and (method is None or attempt.method == method)
            and (cutoff_time is None or attempt.timestamp >= cutoff_time)
        ]

    def _filter_query_metrics(
        self,
        connection_id: Optional[str] = None,
        connection_method: Optional[str] = None,
        time_window: Optional[timedelta] = None,
    ) -> List[QueryMetrics]:
        """Filter query metrics based on criteria."""
        cutoff_time = datetime.now() - time_window if time_window else None

        return [
            metric
            for metric in self._query_metrics
            if (connection_id is None or metric.connection_id == connection_id)
            and (
                connection_method is None
                or metric.connection_method == connection_method
            )
            and (cutoff_time is None or metric.timestamp >= cutoff_time)
        ]

    def _calculate_performance_stats(
        self, attempts: List[ConnectionAttempt]
    ) -> PerformanceStats:
        """Calculate performance statistics for a list of connection attempts."""
        if not attempts:
            return PerformanceStats(
                total_attempts=0,
                successful_attempts=0,
                failed_attempts=0,
                success_rate=0.0,
                avg_duration=0.0,
                min_duration=0.0,
                max_duration=0.0,
                median_duration=0.0,
                p95_duration=0.0,
            )

        successful = [a for a in attempts if a.success]
        durations = [a.duration for a in attempts]

        # Calculate percentiles
        sorted_durations = sorted(durations)
        p95_index = int(0.95 * (len(sorted_durations) - 1))
        p95_duration = sorted_durations[p95_index] if sorted_durations else 0

        return PerformanceStats(
            total_attempts=len(attempts),
            successful_attempts=len(successful),
            failed_attempts=len(attempts) - len(successful),
            success_rate=len(successful) / len(attempts),
            avg_duration=statistics.mean(durations),
            min_duration=min(durations),
            max_duration=max(durations),
            median_duration=statistics.median(durations),
            p95_duration=p95_duration,
        )

    def _get_error_frequency(
        self, failed_attempts: List[ConnectionAttempt]
    ) -> Dict[str, int]:
        """Get frequency of different error types."""
        error_counts = defaultdict(int)
        for attempt in failed_attempts:
            if attempt.error_message:
                # Extract error type (first part of error message)
                error_type = attempt.error_message.split(":")[0].strip()[:50]
                error_counts[error_type] += 1
        return dict(error_counts)

    def _cleanup_old_records(self) -> None:
        """Background thread to clean up old metrics records."""
        while self._cleanup_running:
            try:
                cutoff_time = datetime.now() - timedelta(
                    hours=24
                )  # Keep 24 hours of data

                with self._lock:
                    # Clean up connection attempts
                    original_count = len(self._connection_attempts)
                    self._connection_attempts = [
                        attempt
                        for attempt in self._connection_attempts
                        if attempt.timestamp >= cutoff_time
                    ]

                    # Clean up query metrics
                    original_query_count = len(self._query_metrics)
                    self._query_metrics = [
                        metric
                        for metric in self._query_metrics
                        if metric.timestamp >= cutoff_time
                    ]

                    cleaned_attempts = original_count - len(self._connection_attempts)
                    cleaned_queries = original_query_count - len(self._query_metrics)

                    if cleaned_attempts > 0 or cleaned_queries > 0:
                        logger.info(
                            "Cleaned up old metrics records",
                            cleaned_attempts=cleaned_attempts,
                            cleaned_queries=cleaned_queries,
                        )

                # Sleep for cleanup interval
                time.sleep(self._cleanup_interval)

            except Exception as e:
                logger.error("Error in metrics cleanup thread", error=str(e))
                time.sleep(60)  # Wait before retrying

    def _to_dict(self) -> Dict[str, Any]:
        """Convert metrics to a dictionary representation."""
        return {
            "connection_attempts": [
                {
                    "source_type": a.source_type,
                    "method": a.method,
                    "success": a.success,
                    "duration": a.duration,
                    "error_message": a.error_message,
                    "timestamp": a.timestamp.isoformat(),
                    "connection_id": a.connection_id,
                    "metadata": a.metadata,
                }
                for a in self._connection_attempts
            ],
            "query_metrics": [
                {
                    "connection_id": m.connection_id,
                    "query_hash": m.query_hash,
                    "execution_time": m.execution_time,
                    "rows_returned": m.rows_returned,
                    "bytes_transferred": m.bytes_transferred,
                    "success": m.success,
                    "error_message": m.error_message,
                    "timestamp": m.timestamp.isoformat(),
                    "connection_method": m.connection_method,
                }
                for m in self._query_metrics
            ],
        }

    def export_metrics(self, format: str = "dict") -> Any:
        """
        Export all metrics data.

        Args:
            format: Export format ('dict', 'json', 'csv')

        Returns:
            Exported metrics data
        """
        import csv
        import io
        import json

        with self._lock:
            if format == "dict":
                return self._to_dict()
            elif format == "json":
                return json.dumps(self._to_dict(), default=str)
            elif format == "csv":
                data = self._to_dict()
                output = io.StringIO()

                # Write connection attempts
                if data["connection_attempts"]:
                    writer = csv.DictWriter(
                        output, fieldnames=data["connection_attempts"][0].keys()
                    )
                    writer.writeheader()
                    writer.writerows(data["connection_attempts"])

                output.write("\n")

                # Write query metrics
                if data["query_metrics"]:
                    writer = csv.DictWriter(
                        output, fieldnames=data["query_metrics"][0].keys()
                    )
                    writer.writeheader()
                    writer.writerows(data["query_metrics"])

                return output.getvalue()
            else:
                raise ValueError(f"Unsupported export format: {format}")

    def clear_metrics(self) -> None:
        """Clear all collected metrics."""
        with self._lock:
            self._connection_attempts.clear()
            self._query_metrics.clear()

        logger.info("All metrics cleared")

    def stop(self) -> None:
        """Stop the metrics collector and cleanup thread."""
        self._cleanup_running = False
        if self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=1.0)

        logger.info("Metrics collector stopped")

    def __del__(self):
        """Destructor to ensure cleanup."""
        try:
            self.stop()
        except Exception:
            pass


# Global metrics collector instance
_global_collector: Optional[MetricsCollector] = None
_collector_lock = threading.Lock()


def get_metrics_collector() -> MetricsCollector:
    """
    Get the global metrics collector instance.

    Returns:
        Global MetricsCollector instance
    """
    global _global_collector

    if _global_collector is None:
        with _collector_lock:
            if _global_collector is None:
                _global_collector = MetricsCollector()

    return _global_collector


def set_metrics_collector(collector: MetricsCollector) -> None:
    """
    Set a custom metrics collector as the global instance.

    Args:
        collector: MetricsCollector instance to use globally
    """
    global _global_collector

    with _collector_lock:
        if _global_collector is not None:
            _global_collector.stop()
        _global_collector = collector
