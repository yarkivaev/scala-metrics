package metrics.datasources

import java.time.Instant

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import metrics.core.{Query, TimeRange}

class InMemoryDataSourceSpec extends AnyFlatSpec with Matchers:

  // Default wide time range for tests (includes past and future)
  def wideTimeRange = TimeRange(
    from = Instant.now().minusSeconds(365 * 24 * 3600),
    to = Instant.now().plusSeconds(365 * 24 * 3600),
  )

  "InMemoryDataSource.fromValues" should "create data source from simple values" in {
    val dataSource = InMemoryDataSource.fromValues[IO, Double](
      Map(
        "temperature" -> 25.5,
        "humidity"    -> 60.0,
      ),
    )

    val tempStream = dataSource.get(Query.ByName("temperature"), wideTimeRange)
    val tempResult = tempStream.compile.toList.unsafeRunSync()
    tempResult should have length 1
    tempResult.head._2 should be(25.5)

    val humidityStream = dataSource.get(Query.ByName("humidity"), wideTimeRange)
    val humidityResult = humidityStream.compile.toList.unsafeRunSync()
    humidityResult should have length 1
    humidityResult.head._2 should be(60.0)
  }

  "InMemoryDataSource.fromTimeSeries" should "create data source from time series" in {
    val now        = Instant.now()
    val dataSource = InMemoryDataSource.fromTimeSeries[IO, Double](
      Map(
        "temps" -> List((now, 20.0), (now.plusSeconds(60), 25.0)),
      ),
    )

    val stream = dataSource.get(Query.ByName("temps"), wideTimeRange)
    val result = stream.compile.toList.unsafeRunSync()
    result should have length 2
    result.head._2 should be(20.0)
    result(1)._2 should be(25.0)
  }

  "InMemoryDataSource" should "return empty stream for non-existent metrics" in {
    val dataSource = InMemoryDataSource.fromValues[IO, Double](Map("existing" -> 10.0))

    val stream = dataSource.get(Query.ByName("nonExistent"), wideTimeRange)
    val result = stream.compile.toList.unsafeRunSync()
    result should be(empty)
  }

  it should "handle ByName queries" in {
    val dataSource = InMemoryDataSource.fromValues[IO, Double](Map("metric1" -> 100.0))

    val stream = dataSource.get(Query.ByName("metric1"), wideTimeRange)
    val result = stream.compile.toList.unsafeRunSync()
    result should have length 1
    result.head._2 should be(100.0)
  }

  it should "handle WithFilters queries (ignoring filters)" in {
    val dataSource = InMemoryDataSource.fromValues[IO, Double](Map("metric1" -> 100.0))

    val stream = dataSource.get(Query.WithFilters("metric1", Map("filter" -> "value")), wideTimeRange)
    val result = stream.compile.toList.unsafeRunSync()
    result should have length 1
    result.head._2 should be(100.0)
  }

  it should "handle Custom queries" in {
    val dataSource = InMemoryDataSource.fromValues[IO, Double](Map("metric1" -> 50.0))

    val stream = dataSource.get(Query.Custom("metric1", Map("param" -> "value")), wideTimeRange)
    val result = stream.compile.toList.unsafeRunSync()
    result should have length 1
    result.head._2 should be(50.0)
  }

  it should "handle WithTimeRange queries" in {
    val now        = Instant.now()
    val dataSource = InMemoryDataSource.fromValues[IO, Double](Map("metric1" -> 10.0))

    val stream =
      dataSource.get(Query.WithTimeRange("metric1", now.minusSeconds(60), now.plusSeconds(60)), wideTimeRange)
    val result = stream.compile.toList.unsafeRunSync()
    result should have length 1
    result.head._2 should be(10.0)
  }

  "InMemoryDataSource.supports" should "return true for ByName queries" in {
    val dataSource = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    dataSource.supports(Query.ByName("any")) should be(true)
  }

  it should "return true for WithFilters queries" in {
    val dataSource = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    dataSource.supports(Query.WithFilters("any", Map.empty)) should be(true)
  }

  it should "return true for Custom queries" in {
    val dataSource = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    dataSource.supports(Query.Custom("any", Map.empty)) should be(true)
  }

  it should "return true for WithTimeRange queries" in {
    val now        = Instant.now()
    val dataSource = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    dataSource.supports(Query.WithTimeRange("any", now, now)) should be(true)
  }

  it should "return false for Aggregated queries" in {
    val dataSource = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    dataSource.supports(Query.Aggregated("any", metrics.core.AggregationType.Sum, None)) should be(false)
  }

  "InMemoryDataSource" should "handle empty data source" in {
    val dataSource = InMemoryDataSource.fromValues[IO, Double](Map.empty)

    val stream = dataSource.get(Query.ByName("anything"), wideTimeRange)
    val result = stream.compile.toList.unsafeRunSync()
    result should be(empty)
  }

  it should "handle large number of metrics" in {
    val largeMap   = (1 to 1000).map(i => s"metric$i" -> i.toDouble).toMap
    val dataSource = InMemoryDataSource.fromValues[IO, Double](largeMap)

    val stream500 = dataSource.get(Query.ByName("metric500"), wideTimeRange)
    val result500 = stream500.compile.toList.unsafeRunSync()
    result500 should have length 1
    result500.head._2 should be(500.0)

    val stream1000 = dataSource.get(Query.ByName("metric1000"), wideTimeRange)
    val result1000 = stream1000.compile.toList.unsafeRunSync()
    result1000 should have length 1
    result1000.head._2 should be(1000.0)
  }

  it should "combine single values and time series" in {
    val now        = Instant.now()
    val dataSource = new InMemoryDataSource[IO, Double](
      singleValues = Map("single" -> 42.0),
      timeSeriesData = Map("series" -> List((now, 10.0), (now.plusSeconds(60), 20.0))),
    )

    val singleStream = dataSource.get(Query.ByName("single"), wideTimeRange)
    val singleResult = singleStream.compile.toList.unsafeRunSync()
    singleResult should have length 1
    singleResult.head._2 should be(42.0)

    val seriesStream = dataSource.get(Query.ByName("series"), wideTimeRange)
    val seriesResult = seriesStream.compile.toList.unsafeRunSync()
    seriesResult should have length 2
    seriesResult.head._2 should be(10.0)
    seriesResult(1)._2 should be(20.0)
  }
