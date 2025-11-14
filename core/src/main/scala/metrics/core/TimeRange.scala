package metrics.core

import java.time.{Duration, Instant}

/**
 * Represents a time range for metric computation.
 *
 * TimeRange is used to filter time-series data to a specific period.
 * It's passed as an explicit parameter through the computation pipeline,
 * ensuring metrics can be reused for different time periods.
 *
 * @param from Start of the time range (inclusive)
 * @param to End of the time range (inclusive)
 */
case class TimeRange(from: Instant, to: Instant):
  require(
    !from.isAfter(to),
    s"TimeRange 'from' ($from) must not be after 'to' ($to)",
  )

  /**
   * Check if an instant falls within this time range (inclusive bounds).
   *
   * @param instant The instant to check
   * @return true if instant is within [from, to]
   */
  def contains(instant: Instant): Boolean =
    !instant.isBefore(from) && !instant.isAfter(to)

  /**
   * Calculate the duration of this time range.
   *
   * @return Duration between from and to
   */
  def duration: Duration =
    Duration.between(from, to)

  override def toString: String =
    s"TimeRange($from to $to, duration: ${duration.toDays} days)"

object TimeRange:
  /**
   * Create a TimeRange from string dates in ISO-8601 format.
   *
   * @param fromStr Start date string (e.g., "2025-01-01T00:00:00Z")
   * @param toStr End date string (e.g., "2025-01-31T23:59:59Z")
   * @return TimeRange
   */
  def parse(fromStr: String, toStr: String): TimeRange =
    TimeRange(Instant.parse(fromStr), Instant.parse(toStr))

  /**
   * Create a TimeRange for the last N days from now.
   *
   * @param days Number of days
   * @return TimeRange from (now - days) to now
   */
  def lastDays(days: Int): TimeRange =
    val now  = Instant.now()
    val from = now.minus(Duration.ofDays(days.toLong))
    TimeRange(from, now)

  /**
   * Create a TimeRange for the last N hours from now.
   *
   * @param hours Number of hours
   * @return TimeRange from (now - hours) to now
   */
  def lastHours(hours: Int): TimeRange =
    val now  = Instant.now()
    val from = now.minus(Duration.ofHours(hours.toLong))
    TimeRange(from, now)

  /**
   * Create an instant TimeRange (from == to) for a single timestamp.
   *
   * @param instant The instant
   * @return TimeRange with from == to == instant
   */
  def instant(instant: Instant): TimeRange =
    TimeRange(instant, instant)

  /**
   * Create an instant TimeRange for the current time.
   *
   * @return TimeRange with from == to == now
   */
  def now(): TimeRange =
    val n = Instant.now()
    TimeRange(n, n)
