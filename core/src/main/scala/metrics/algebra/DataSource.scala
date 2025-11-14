package metrics.algebra

import java.time.Instant

import fs2.Stream

import metrics.core.Query

/**
 * DataSource - Tagless Final interface for data fetching
 *
 * Returns a stream of timestamped values:
 * - Single value: Stream with 1 element
 * - Time series: Stream with N elements
 * - Empty/not found: Stream.empty
 * - Infinite stream: Lazy, can represent endless data
 * - Side effects: Stream can represent async operations (future: load to DB)
 *
 * Generic over value type A (Double, Int, String, etc.)
 */
trait DataSource[F[_], A]:
  /**
   * Fetch data for the given query within a time range
   *
   * Implementations should filter data by timeRange at the source for efficiency.
   * Only return data points where timeRange.contains(instant) is true.
   *
   * @param query The query to execute
   * @param timeRange The time range to filter data
   * @return Stream of timestamped values within the time range
   */
  def get(query: Query, timeRange: metrics.core.TimeRange): Stream[F, (Instant, A)]

  /**
   * Optional: Check if this source supports a query without executing it
   * Useful for debugging and query routing optimization
   */
  def supports(query: Query): Boolean = true

object DataSource:
  /**
   * Chain of Responsibility implementation
   *
   * Tries primary source first, falls back to secondary if primary returns empty stream
   */
  class ChainedDataSource[F[_], A](
    primary: DataSource[F, A],
    fallback: DataSource[F, A],
  ) extends DataSource[F, A]:

    def get(query: Query, timeRange: metrics.core.TimeRange): Stream[F, (Instant, A)] =
      primary
        .get(query, timeRange)
        .flatMap(result => Stream.emit(result))
        .handleErrorWith(_ => fallback.get(query, timeRange))
        .ifEmpty {
          fallback.get(query, timeRange)
        }

    override def supports(query: Query): Boolean =
      primary.supports(query) || fallback.supports(query)

  /**
   * Extension methods for composing data sources
   */
  extension [F[_], A](source: DataSource[F, A])
    /**
     * Chain this source with a fallback
     * Usage: csvSource.orElse(apiSource).orElse(configSource)
     */
    def orElse(fallback: DataSource[F, A]): DataSource[F, A] =
      new ChainedDataSource(source, fallback)
