package metrics.core

import java.time.{Duration, Instant}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TimeRangeSpec extends AnyFlatSpec with Matchers:

  val jan1  = Instant.parse("2025-01-01T00:00:00Z")
  val jan15 = Instant.parse("2025-01-15T12:00:00Z")
  val jan31 = Instant.parse("2025-01-31T23:59:59Z")
  val feb1  = Instant.parse("2025-02-01T00:00:00Z")
  val feb28 = Instant.parse("2025-02-28T23:59:59Z")

  "TimeRange" should "create a valid time range" in {
    val range = TimeRange(jan1, jan31)
    range.from should be(jan1)
    range.to should be(jan31)
  }

  it should "reject invalid time range where from > to" in {
    assertThrows[IllegalArgumentException] {
      TimeRange(jan31, jan1)
    }
  }

  it should "allow instant range where from == to" in {
    val range = TimeRange(jan1, jan1)
    range.from should be(jan1)
    range.to should be(jan1)
  }

  "TimeRange.contains" should "return true for instant within range" in {
    val range = TimeRange(jan1, jan31)
    range.contains(jan15) should be(true)
  }

  it should "return true for instant at start boundary" in {
    val range = TimeRange(jan1, jan31)
    range.contains(jan1) should be(true)
  }

  it should "return true for instant at end boundary" in {
    val range = TimeRange(jan1, jan31)
    range.contains(jan31) should be(true)
  }

  it should "return false for instant before range" in {
    val range = TimeRange(jan15, jan31)
    range.contains(jan1) should be(false)
  }

  it should "return false for instant after range" in {
    val range = TimeRange(jan1, jan15)
    range.contains(jan31) should be(false)
  }

  "TimeRange.duration" should "calculate correct duration" in {
    val range = TimeRange(jan1, jan31)
    range.duration should be(Duration.between(jan1, jan31))
  }

  it should "return zero duration for instant range" in {
    val range = TimeRange(jan1, jan1)
    range.duration should be(Duration.ZERO)
  }

  it should "calculate duration for 30-day range" in {
    val range = TimeRange(jan1, jan31)
    range.duration.toDays should be(30)
  }

  "TimeRange.parse" should "parse ISO-8601 format strings" in {
    val range = TimeRange.parse("2025-01-01T00:00:00Z", "2025-01-31T23:59:59Z")
    range.from should be(jan1)
    range.to should be(jan31)
  }

  it should "throw exception for invalid date format" in {
    assertThrows[Exception] {
      TimeRange.parse("2025-01-01", "2025-01-31")
    }
  }

  "TimeRange.lastDays" should "create range for last N days from now" in {
    val range            = TimeRange.lastDays(7)
    val expectedDuration = Duration.ofDays(7)

    math.abs(range.duration.toMillis - expectedDuration.toMillis) should be < 1000L
  }

  it should "create valid range with from before to" in {
    val range = TimeRange.lastDays(30)
    range.from.isBefore(range.to) should be(true)
  }

  "TimeRange.lastHours" should "create range for last N hours from now" in {
    val range            = TimeRange.lastHours(24)
    val expectedDuration = Duration.ofHours(24)

    math.abs(range.duration.toMillis - expectedDuration.toMillis) should be < 1000L
  }

  it should "create valid range with from before to" in {
    val range = TimeRange.lastHours(12)
    range.from.isBefore(range.to) should be(true)
  }

  "TimeRange.toString" should "include from, to, and duration" in {
    val range = TimeRange(jan1, jan31)
    val str   = range.toString

    str should include("TimeRange")
    str should include("30 days")
  }
