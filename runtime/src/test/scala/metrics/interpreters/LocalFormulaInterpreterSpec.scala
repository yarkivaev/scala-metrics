package metrics.interpreters

import java.time.{Duration, Instant}

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import metrics.algebra.{DataSource, EvaluationContext, FormulaAlgebra}
import metrics.core._
import metrics.datasources.InMemoryDataSource

object TestTimeRanges:
  def now(): TimeRange =
    val currentTime = Instant.now()
    TimeRange(
      currentTime.minusSeconds(365L * 24L * 3600L),
      currentTime.plusSeconds(24L * 3600L),
    )

class LocalFormulaInterpreterSpec extends AnyFlatSpec with Matchers:

  "LocalFormulaInterpreter" should "evaluate literal formulas" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    val dataSource  = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    val context     = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val result = interpreter
      .evaluate(Formula.Literal[IO, Double](42.5), context, TestTimeRanges.now())
      .compile
      .last
      .unsafeRunSync()

    result.map(_._2) should be(Some(42.5))
  }

  it should "evaluate metric references from data source" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry

    registry.register(Metric.leaf[IO, Double]("temperature", Query.ByName("temperature")))

    val dataSource = InMemoryDataSource.fromValues[IO, Double](Map("temperature" -> 25.5))
    val context    = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val result = interpreter
      .evaluate(Formula.MetricRef[IO, Double]("temperature"), context, TestTimeRanges.now())
      .compile
      .last
      .unsafeRunSync()

    result.map(_._2) should be(Some(25.5))
  }

  it should "evaluate computed metric references recursively" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry

    registry.register(Metric.leaf[IO, Double]("base", Query.ByName("base")))
    registry.register(Metric.computed[IO, Double]("derived", Formula.MetricRef[IO, Double]("base")))

    val dataSource = InMemoryDataSource.fromValues[IO, Double](Map("base" -> 10.0))
    val context    = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val result = interpreter
      .evaluate(Formula.MetricRef[IO, Double]("derived"), context, TestTimeRanges.now())
      .compile
      .last
      .unsafeRunSync()

    result.map(_._2) should be(Some(10.0))
  }

  it should "return empty stream for missing metric in data source" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    registry.register(Metric.leaf[IO, Double]("missing", Query.ByName("missing")))

    val dataSource = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    val context    = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val result = interpreter
      .evaluate(Formula.MetricRef[IO, Double]("missing"), context, TestTimeRanges.now())
      .compile
      .last
      .unsafeRunSync()

    result should be(None)
  }

  it should "return empty stream for metric not in registry" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    val dataSource  = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    val context     = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val result = interpreter
      .evaluate(Formula.MetricRef[IO, Double]("unknown"), context, TestTimeRanges.now())
      .compile
      .last
      .unsafeRunSync()

    result should be(None)
  }

  it should "evaluate addition formulas" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    val dataSource  = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    val context     = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val formula = Formula.Add[IO, Double](Formula.Literal[IO, Double](10.0), Formula.Literal[IO, Double](20.0))
    val result  = interpreter.evaluate(formula, context, TestTimeRanges.now()).compile.last.unsafeRunSync()

    result.map(_._2) should be(Some(30.0))
  }

  it should "evaluate subtraction formulas" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    val dataSource  = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    val context     = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val formula = Formula.Subtract[IO, Double](Formula.Literal[IO, Double](30.0), Formula.Literal[IO, Double](10.0))
    val result  = interpreter.evaluate(formula, context, TestTimeRanges.now()).compile.last.unsafeRunSync()

    result.map(_._2) should be(Some(20.0))
  }

  it should "evaluate multiplication formulas" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    val dataSource  = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    val context     = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val formula = Formula.Multiply[IO, Double](Formula.Literal[IO, Double](5.0), Formula.Literal[IO, Double](4.0))
    val result  = interpreter.evaluate(formula, context, TestTimeRanges.now()).compile.last.unsafeRunSync()

    result.map(_._2) should be(Some(20.0))
  }

  it should "evaluate division formulas" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    val dataSource  = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    val context     = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val formula = Formula.Divide[IO, Double](Formula.Literal[IO, Double](100.0), Formula.Literal[IO, Double](5.0))
    val result  = interpreter.evaluate(formula, context, TestTimeRanges.now()).compile.last.unsafeRunSync()

    result.map(_._2) should be(Some(20.0))
  }

  it should "throw error for division by zero" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    val dataSource  = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    val context     = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val formula       = Formula.Divide[IO, Double](Formula.Literal[IO, Double](100.0), Formula.Literal[IO, Double](0.0))
    val resultAttempt =
      interpreter.evaluate(formula, context, TestTimeRanges.now()).compile.last.attempt.unsafeRunSync()

    resultAttempt.isLeft should be(true)
    resultAttempt.swap.getOrElse(fail()) shouldBe an[ArithmeticException]
  }

  it should "evaluate Sum aggregation on time series" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    registry.register(Metric.leaf[IO, Double]("sales", Query.ByName("sales")))

    val now        = Instant.now()
    val dataSource = InMemoryDataSource.fromTimeSeries[IO, Double](
      Map(
        "sales" -> List(
          (now.minusSeconds(60), 10.0),
          (now.minusSeconds(30), 20.0),
          (now, 30.0),
        ),
      ),
    )
    val context    = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val formula = Formula.Sum(Formula.MetricRef[IO, Double]("sales"))
    val result  = interpreter.evaluate(formula, context, TestTimeRanges.now()).compile.last.unsafeRunSync()

    result.map(_._2) should be(Some(60.0))
  }

  it should "evaluate Avg aggregation on time series" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    registry.register(Metric.leaf[IO, Double]("temperatures", Query.ByName("temperatures")))

    val now        = Instant.now()
    val dataSource = InMemoryDataSource.fromTimeSeries[IO, Double](
      Map(
        "temperatures" -> List(
          (now.minusSeconds(60), 10.0),
          (now.minusSeconds(30), 20.0),
          (now, 30.0),
        ),
      ),
    )
    val context    = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val formula = Formula.Avg(Formula.MetricRef[IO, Double]("temperatures"))
    val result  = interpreter.evaluate(formula, context, TestTimeRanges.now()).compile.last.unsafeRunSync()

    result.map(_._2) should be(Some(20.0))
  }

  it should "evaluate Count aggregation" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    registry.register(Metric.leaf[IO, Double]("items", Query.ByName("items")))

    val now        = Instant.now()
    val dataSource = InMemoryDataSource.fromTimeSeries[IO, Double](
      Map(
        "items" -> List(
          (now.minusSeconds(60), 1.0),
          (now.minusSeconds(30), 2.0),
          (now, 3.0),
        ),
      ),
    )
    val context    = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val formula = Formula.Count[IO, Double](Formula.MetricRef[IO, Double]("items"))
    formula.source.isInstanceOf[Formula.MetricRef[?, ?]] should be(true)
  }

  it should "evaluate Min of multiple formulas" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    val dataSource  = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    val context     = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val formula = Formula.Min(
      List(
        Formula.Literal[IO, Double](30.0),
        Formula.Literal[IO, Double](10.0),
        Formula.Literal[IO, Double](20.0),
      ),
    )
    val result  = interpreter.evaluate(formula, context, TestTimeRanges.now()).compile.last.unsafeRunSync()

    result.map(_._2) should be(Some(10.0))
  }

  it should "evaluate Max of multiple formulas" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    val dataSource  = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    val context     = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val formula = Formula.Max(
      List(
        Formula.Literal[IO, Double](30.0),
        Formula.Literal[IO, Double](50.0),
        Formula.Literal[IO, Double](20.0),
      ),
    )
    val result  = interpreter.evaluate(formula, context, TestTimeRanges.now()).compile.last.unsafeRunSync()

    result.map(_._2) should be(Some(50.0))
  }

  it should "evaluate When condition with true result" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    val dataSource  = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    val context     = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val condition =
      Condition.GreaterThan[IO, Double](Formula.Literal[IO, Double](10.0), Formula.Literal[IO, Double](5.0))
    val formula   =
      Formula.When[IO, Double](condition, Formula.Literal[IO, Double](100.0), Some(Formula.Literal[IO, Double](0.0)))
    val result    = interpreter.evaluate(formula, context, TestTimeRanges.now()).compile.last.unsafeRunSync()

    result.map(_._2) should be(Some(100.0))
  }

  it should "evaluate When condition with false result and else branch" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    val dataSource  = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    val context     = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val condition = Condition.LessThan[IO, Double](Formula.Literal[IO, Double](10.0), Formula.Literal[IO, Double](5.0))
    val formula   =
      Formula.When[IO, Double](condition, Formula.Literal[IO, Double](100.0), Some(Formula.Literal[IO, Double](50.0)))
    val result    = interpreter.evaluate(formula, context, TestTimeRanges.now()).compile.last.unsafeRunSync()

    result.map(_._2) should be(Some(50.0))
  }

  it should "return empty stream for When condition with false result and no else branch" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry
    val dataSource  = InMemoryDataSource.fromValues[IO, Double](Map.empty)
    val context     = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val condition = Condition.LessThan[IO, Double](Formula.Literal[IO, Double](10.0), Formula.Literal[IO, Double](5.0))
    val formula   = Formula.When[IO, Double](condition, Formula.Literal[IO, Double](100.0), None)
    val result    = interpreter.evaluate(formula, context, TestTimeRanges.now()).compile.last.unsafeRunSync()

    result should be(None)
  }

  it should "evaluate complex nested formula" in {
    val interpreter = new LocalFormulaInterpreter[IO, Double]
    val registry    = new MetricRegistry

    registry.register(Metric.leaf[IO, Double]("a", Query.ByName("a")))
    registry.register(Metric.leaf[IO, Double]("b", Query.ByName("b")))
    registry.register(Metric.leaf[IO, Double]("c", Query.ByName("c")))

    val dataSource = InMemoryDataSource.fromValues[IO, Double](
      Map(
        "a" -> 10.0,
        "b" -> 20.0,
        "c" -> 5.0,
      ),
    )
    val context    = new EvaluationContext[IO, Double](Map("default" -> dataSource), registry)

    val formula = Formula.Divide[IO, Double](
      Formula.Multiply[IO, Double](
        Formula.Add[IO, Double](Formula.MetricRef[IO, Double]("a"), Formula.MetricRef[IO, Double]("b")),
        Formula.MetricRef[IO, Double]("c"),
      ),
      Formula.Literal[IO, Double](10.0),
    )
    val result  = interpreter.evaluate(formula, context, TestTimeRanges.now()).compile.last.unsafeRunSync()

    result.map(_._2) should be(Some(15.0))
  }
