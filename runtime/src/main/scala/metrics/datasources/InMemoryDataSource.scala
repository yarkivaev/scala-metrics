package metrics.datasources

import java.time.Instant

import fs2.Stream

import metrics.algebra.DataSource
import metrics.core.Query

/**
 * InMemoryDataSource - simple Map-backed data source
 *
 * Useful for:
 * - Testing (provide mock data)
 * - Configuration constants (margins, thresholds, etc.)
 * - Cached/precomputed values
 *
 * Stores either single values or time series
 */
class InMemoryDataSource[F[_], A](
  singleValues: Map[String, A],
  timeSeriesData: Map[String, List[(Instant, A)]],
) extends DataSource[F, A]:

  def get(query: Query, timeRange: metrics.core.TimeRange): Stream[F, (Instant, A)] =
    val rawStream = query match
      case Query.ByName(metricName) =>
        singleValues.get(metricName) match
          case Some(value) =>
            Stream.emit((timeRange.from, value))
          case None        =>
            timeSeriesData.get(metricName) match
              case Some(series) => Stream.emits(series)
              case None         => Stream.empty

      case Query.WithTimeRange(metricName, from, to) =>
        // Use the more restrictive of query timeRange and parameter timeRange
        val effectiveFrom = if from.isAfter(timeRange.from) then from else timeRange.from
        val effectiveTo   = if to.isBefore(timeRange.to) then to else timeRange.to
        get(Query.ByName(metricName), metrics.core.TimeRange(effectiveFrom, effectiveTo))

      case Query.WithFilters(metricName, filters) =>
        get(Query.ByName(metricName), timeRange)

      case Query.Custom(metricName, params) =>
        params.get("table") match
          case Some(table) =>
            params.get("column") match
              case Some(column) =>
                get(Query.ByName(s"$table.$column"), timeRange)
              case None         => Stream.empty
          case None        =>
            get(Query.ByName(metricName), timeRange)

      case _ =>
        Stream.empty

    // Filter all results by timeRange
    rawStream.filter { case (instant, _) => timeRange.contains(instant) }

  override def supports(query: Query): Boolean =
    query match
      case Query.ByName(_)              => true
      case Query.WithTimeRange(_, _, _) => true
      case Query.WithFilters(_, _)      => true
      case Query.Custom(_, _)           => true
      case _                            => false

object InMemoryDataSource:
  /**
   * Create from simple value map (single values only)
   */
  def fromValues[F[_], A](values: Map[String, A]): InMemoryDataSource[F, A] =
    new InMemoryDataSource(values, Map.empty)

  /**
   * Create from time series map
   */
  def fromTimeSeries[F[_], A](series: Map[String, List[(Instant, A)]]): InMemoryDataSource[F, A] =
    new InMemoryDataSource(Map.empty, series)

  /**
   * Create with both single values and time series
   */
  def apply[F[_], A](
    singleValues: Map[String, A] = Map.empty[String, A],
    timeSeries: Map[String, List[(Instant, A)]] = Map.empty[String, List[(Instant, A)]],
  ): InMemoryDataSource[F, A] =
    new InMemoryDataSource(singleValues, timeSeries)

  /**
   * Builder for fluent API
   */
  class Builder[F[_], A]:
    private var singleValues = Map.empty[String, A]
    private var timeSeries   = Map.empty[String, List[(Instant, A)]]

    def addValue(name: String, value: A): Builder[F, A] =
      singleValues = singleValues + (name -> value)
      this

    def addTimeSeries(name: String, points: List[(Instant, A)]): Builder[F, A] =
      timeSeries = timeSeries + (name -> points)
      this

    def build: InMemoryDataSource[F, A] =
      new InMemoryDataSource(singleValues, timeSeries)

  def builder[F[_], A]: Builder[F, A] = new Builder[F, A]
