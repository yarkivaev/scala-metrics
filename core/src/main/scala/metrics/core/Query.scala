package metrics.core

import java.time.{Instant, LocalDate}

/**
 * Query ADT - Extensible query language for data sources
 *
 * This allows different data sources to handle queries they support
 * and pass through queries they don't understand.
 */
sealed trait Query:
  def metricName: String

object Query:
  /**
   * Simple query by metric name
   * Returns the current/latest value
   */
  case class ByName(metricName: String) extends Query

  /**
   * Query for time range data
   * Returns time series data points
   */
  case class WithTimeRange(
    metricName: String,
    from: Instant,
    to: Instant,
  ) extends Query

  /**
   * Query with filters/constraints
   * Example: workshop = "Casting", shift = 1
   */
  case class WithFilters(
    metricName: String,
    filters: Map[String, Any],
  ) extends Query

  /**
   * Aggregated query
   * Example: sum of downtime, grouped by workshop
   */
  case class Aggregated(
    metricName: String,
    aggregation: AggregationType,
    groupBy: Option[String] = None,
  ) extends Query

  /**
   * Custom extensible query
   * For future query types we haven't thought of yet
   */
  case class Custom(
    metricName: String,
    params: Map[String, Any],
  ) extends Query

  /**
   * Helper constructors
   */
  def byName(name: String): Query.ByName = ByName(name)

  def withFilter(name: String, filters: (String, Any)*): Query.WithFilters =
    WithFilters(name, filters.toMap)

  /**
   * Query with date range filter
   * Convenience helper for filtering data by date range
   */
  def withDateRange(name: String, startDate: LocalDate, endDate: LocalDate): Query.WithFilters =
    WithFilters(name, Map("startDate" -> startDate, "endDate" -> endDate))

  /**
   * Query with additional filters and date range
   */
  def withFiltersAndDateRange(
    name: String,
    startDate: LocalDate,
    endDate: LocalDate,
    filters: Map[String, Any],
  ): Query.WithFilters =
    WithFilters(name, filters ++ Map("startDate" -> startDate, "endDate" -> endDate))
