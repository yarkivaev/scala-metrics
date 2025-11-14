package metrics.dsl

import java.time.Duration

import scala.language.implicitConversions

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import metrics.core._
import metrics.dsl.FormulaDSL.{_, given}

class FormulaDSLSpec extends AnyFlatSpec with Matchers:
  type Id[A] = A

  "FormulaDSL implicit conversions" should "convert Double to Formula.Literal" in {
    val formula: Formula[Id, Double] = 42.5
    formula should be(Formula.Literal[Id, Double](42.5))
  }

  it should "convert Int to Formula.Literal" in {
    val formula: Formula[Id, Double] = 42
    formula should be(Formula.Literal[Id, Double](42.0))
  }

  it should "convert Metric to Formula.MetricRef" in {
    val metric                       = Metric.leaf[Id, Double]("testMetric", Query.ByName("testMetric"))
    val formula: Formula[Id, Double] = metric
    formula should be(Formula.MetricRef[Id, Double]("testMetric"))
  }

  "Formula operators" should "support addition with +" in {
    val left   = Formula.Literal[Id, Double](10.0)
    val right  = Formula.Literal[Id, Double](20.0)
    val result = left + right

    result.isInstanceOf[Formula.Add[?, ?]] should be(true)
    result.asInstanceOf[Formula.Add[Id, Double]].left should be(left)
    result.asInstanceOf[Formula.Add[Id, Double]].right should be(right)
  }

  it should "support addition with Metric on right side" in {
    val left   = Formula.Literal[Id, Double](10.0)
    val right  = Metric.leaf[Id, Double]("metric1", Query.ByName("metric1"))
    val result = left + right

    result.isInstanceOf[Formula.Add[?, ?]] should be(true)
    result.asInstanceOf[Formula.Add[Id, Double]].right should be(Formula.MetricRef[Id, Double]("metric1"))
  }

  it should "support subtraction with -" in {
    val left   = Formula.Literal[Id, Double](30.0)
    val right  = Formula.Literal[Id, Double](10.0)
    val result = left - right

    result.isInstanceOf[Formula.Subtract[?, ?]] should be(true)
    result.asInstanceOf[Formula.Subtract[Id, Double]].left should be(left)
    result.asInstanceOf[Formula.Subtract[Id, Double]].right should be(right)
  }

  it should "support subtraction with Metric on right side" in {
    val left   = Formula.Literal[Id, Double](30.0)
    val right  = Metric.leaf[Id, Double]("metric1", Query.ByName("metric1"))
    val result = left - right

    result.isInstanceOf[Formula.Subtract[?, ?]] should be(true)
    result.asInstanceOf[Formula.Subtract[Id, Double]].right should be(Formula.MetricRef[Id, Double]("metric1"))
  }

  it should "support multiplication with *" in {
    val left   = Formula.Literal[Id, Double](5.0)
    val right  = Formula.Literal[Id, Double](4.0)
    val result = left * right

    result.isInstanceOf[Formula.Multiply[?, ?]] should be(true)
    result.asInstanceOf[Formula.Multiply[Id, Double]].left should be(left)
    result.asInstanceOf[Formula.Multiply[Id, Double]].right should be(right)
  }

  it should "support multiplication with Metric on right side" in {
    val left   = Formula.Literal[Id, Double](5.0)
    val right  = Metric.leaf[Id, Double]("metric1", Query.ByName("metric1"))
    val result = left * right

    result.isInstanceOf[Formula.Multiply[?, ?]] should be(true)
    result.asInstanceOf[Formula.Multiply[Id, Double]].right should be(Formula.MetricRef[Id, Double]("metric1"))
  }

  it should "support division with /" in {
    val left   = Formula.Literal[Id, Double](100.0)
    val right  = Formula.Literal[Id, Double](5.0)
    val result = left / right

    result.isInstanceOf[Formula.Divide[?, ?]] should be(true)
    result.asInstanceOf[Formula.Divide[Id, Double]].left should be(left)
    result.asInstanceOf[Formula.Divide[Id, Double]].right should be(right)
  }

  it should "support division with Metric on right side" in {
    val left   = Formula.Literal[Id, Double](100.0)
    val right  = Metric.leaf[Id, Double]("metric1", Query.ByName("metric1"))
    val result = left / right

    result.isInstanceOf[Formula.Divide[?, ?]] should be(true)
    result.asInstanceOf[Formula.Divide[Id, Double]].right should be(Formula.MetricRef[Id, Double]("metric1"))
  }

  "Metric operators" should "support addition with +" in {
    val left   = Metric.leaf[Id, Double]("metric1", Query.ByName("metric1"))
    val right  = Metric.leaf[Id, Double]("metric2", Query.ByName("metric2"))
    val result = left + right

    result.isInstanceOf[Formula.Add[?, ?]] should be(true)
    result.asInstanceOf[Formula.Add[Id, Double]].left should be(Formula.MetricRef[Id, Double]("metric1"))
    result.asInstanceOf[Formula.Add[Id, Double]].right should be(Formula.MetricRef[Id, Double]("metric2"))
  }

  it should "support addition with Formula on right side" in {
    val left   = Metric.leaf[Id, Double]("metric1", Query.ByName("metric1"))
    val right  = Formula.Literal[Id, Double](42.0)
    val result = left + right

    result.isInstanceOf[Formula.Add[?, ?]] should be(true)
    result.asInstanceOf[Formula.Add[Id, Double]].left should be(Formula.MetricRef[Id, Double]("metric1"))
    result.asInstanceOf[Formula.Add[Id, Double]].right should be(right)
  }

  it should "support subtraction with -" in {
    val left   = Metric.leaf[Id, Double]("metric1", Query.ByName("metric1"))
    val right  = Metric.leaf[Id, Double]("metric2", Query.ByName("metric2"))
    val result = left - right

    result.isInstanceOf[Formula.Subtract[?, ?]] should be(true)
    result.asInstanceOf[Formula.Subtract[Id, Double]].left should be(Formula.MetricRef[Id, Double]("metric1"))
    result.asInstanceOf[Formula.Subtract[Id, Double]].right should be(Formula.MetricRef[Id, Double]("metric2"))
  }

  it should "support multiplication with *" in {
    val left   = Metric.leaf[Id, Double]("metric1", Query.ByName("metric1"))
    val right  = Metric.leaf[Id, Double]("metric2", Query.ByName("metric2"))
    val result = left * right

    result.isInstanceOf[Formula.Multiply[?, ?]] should be(true)
    result.asInstanceOf[Formula.Multiply[Id, Double]].left should be(Formula.MetricRef[Id, Double]("metric1"))
    result.asInstanceOf[Formula.Multiply[Id, Double]].right should be(Formula.MetricRef[Id, Double]("metric2"))
  }

  it should "support division with /" in {
    val left   = Metric.leaf[Id, Double]("metric1", Query.ByName("metric1"))
    val right  = Metric.leaf[Id, Double]("metric2", Query.ByName("metric2"))
    val result = left / right

    result.isInstanceOf[Formula.Divide[?, ?]] should be(true)
    result.asInstanceOf[Formula.Divide[Id, Double]].left should be(Formula.MetricRef[Id, Double]("metric1"))
    result.asInstanceOf[Formula.Divide[Id, Double]].right should be(Formula.MetricRef[Id, Double]("metric2"))
  }

  "Comparison operators" should "support > operator" in {
    val left      = Formula.Literal[Id, Double](10.0)
    val right     = Formula.Literal[Id, Double](5.0)
    val condition = left > right

    condition.isInstanceOf[Condition.GreaterThan[?, ?]] should be(true)
    condition.asInstanceOf[Condition.GreaterThan[Id, Double]].left should be(left)
    condition.asInstanceOf[Condition.GreaterThan[Id, Double]].right should be(right)
  }

  it should "support > with Metric on right side" in {
    val left      = Formula.Literal[Id, Double](10.0)
    val right     = Metric.leaf[Id, Double]("metric1", Query.ByName("metric1"))
    val condition = left > right

    condition.isInstanceOf[Condition.GreaterThan[?, ?]] should be(true)
    condition.asInstanceOf[Condition.GreaterThan[Id, Double]].right should be(Formula.MetricRef[Id, Double]("metric1"))
  }

  it should "support < operator" in {
    val left      = Formula.Literal[Id, Double](5.0)
    val right     = Formula.Literal[Id, Double](10.0)
    val condition = left < right

    condition.isInstanceOf[Condition.LessThan[?, ?]] should be(true)
    condition.asInstanceOf[Condition.LessThan[Id, Double]].left should be(left)
    condition.asInstanceOf[Condition.LessThan[Id, Double]].right should be(right)
  }

  it should "support === operator" in {
    val left      = Formula.Literal[Id, Double](42.0)
    val right     = Formula.Literal[Id, Double](42.0)
    val condition = left === right

    condition.isInstanceOf[Condition.Equals[?, ?]] should be(true)
    condition.asInstanceOf[Condition.Equals[Id, Double]].left should be(left)
    condition.asInstanceOf[Condition.Equals[Id, Double]].right should be(right)
  }

  "Condition operators" should "support && operator" in {
    val cond1  = Formula.Literal[Id, Double](10.0) > Formula.Literal[Id, Double](5.0)
    val cond2  = Formula.Literal[Id, Double](10.0) < Formula.Literal[Id, Double](20.0)
    val result = cond1 && cond2

    result.isInstanceOf[Condition.And[?, ?]] should be(true)
    val andCondition = result.asInstanceOf[Condition.And[Id, Double]]
    andCondition.conditions should have length 2
    andCondition.conditions should contain(cond1)
    andCondition.conditions should contain(cond2)
  }

  it should "support || operator" in {
    val cond1  = Formula.Literal[Id, Double](10.0) > Formula.Literal[Id, Double](5.0)
    val cond2  = Formula.Literal[Id, Double](5.0) > Formula.Literal[Id, Double](10.0)
    val result = cond1 || cond2

    result.isInstanceOf[Condition.Or[?, ?]] should be(true)
    val orCondition = result.asInstanceOf[Condition.Or[Id, Double]]
    orCondition.conditions should have length 2
    orCondition.conditions should contain(cond1)
    orCondition.conditions should contain(cond2)
  }

  it should "support unary ! operator" in {
    val cond   = Formula.Literal[Id, Double](10.0) > Formula.Literal[Id, Double](5.0)
    val result = !cond

    result.isInstanceOf[Condition.Not[?, ?]] should be(true)
    result.asInstanceOf[Condition.Not[Id, Double]].condition should be(cond)
  }

  "Aggregation functions" should "create Sum formula" in {
    val source = Formula.MetricRef[Id, Double]("sales")
    val result = sum(source)

    result.isInstanceOf[Formula.Sum[?, ?]] should be(true)
    result.asInstanceOf[Formula.Sum[Id, Double]].source should be(source)
  }

  it should "create Avg formula" in {
    val source = Formula.MetricRef[Id, Double]("temperatures")
    val result = avg(source)

    result.isInstanceOf[Formula.Avg[?, ?]] should be(true)
    result.asInstanceOf[Formula.Avg[Id, Double]].source should be(source)
  }

  it should "create Min formula with multiple sources" in {
    val source1 = Formula.Literal[Id, Double](10.0)
    val source2 = Formula.Literal[Id, Double](20.0)
    val source3 = Formula.Literal[Id, Double](5.0)
    val result  = min(source1, source2, source3)

    result.isInstanceOf[Formula.Min[?, ?]] should be(true)
    val minFormula = result.asInstanceOf[Formula.Min[Id, Double]]
    minFormula.sources should have length 3
  }

  it should "create Max formula with multiple sources" in {
    val source1 = Formula.Literal[Id, Double](10.0)
    val source2 = Formula.Literal[Id, Double](20.0)
    val result  = max(source1, source2)

    result.isInstanceOf[Formula.Max[?, ?]] should be(true)
    val maxFormula = result.asInstanceOf[Formula.Max[Id, Double]]
    maxFormula.sources should have length 2
  }

  it should "create Count formula" in {
    val source = Formula.MetricRef[Id, Double]("items")
    val result = count(source)

    result.isInstanceOf[Formula.Count[?, ?]] should be(true)
    result.asInstanceOf[Formula.Count[Id, Double]].source should be(source)
  }

  "Conditional expressions" should "create When formula with otherwise" in {
    val condition = Formula.Literal[Id, Double](10.0) > Formula.Literal[Id, Double](5.0)
    val thenValue = Formula.Literal[Id, Double](100.0)
    val elseValue = Formula.Literal[Id, Double](0.0)

    val result = when(condition)(thenValue).otherwise(elseValue)

    result.isInstanceOf[Formula.When[?, ?]] should be(true)
    val whenFormula = result.asInstanceOf[Formula.When[Id, Double]]
    whenFormula.condition should be(condition)
    whenFormula.thenFormula should be(thenValue)
    whenFormula.elseFormula should be(Some(elseValue))
  }

  it should "create When formula without otherwise" in {
    val condition = Formula.Literal[Id, Double](10.0) > Formula.Literal[Id, Double](5.0)
    val thenValue = Formula.Literal[Id, Double](100.0)

    val result = when(condition)(thenValue).toFormula

    result.isInstanceOf[Formula.When[?, ?]] should be(true)
    val whenFormula = result.asInstanceOf[Formula.When[Id, Double]]
    whenFormula.condition should be(condition)
    whenFormula.thenFormula should be(thenValue)
    whenFormula.elseFormula should be(None)
  }

  "Time window DSL" should "create TimeWindow formula" in {
    val source   = Formula.MetricRef[Id, Double]("sensor_readings")
    val duration = Duration.ofHours(24)
    val result   = source.over(duration).aggregating(AggregationType.Avg)

    result.isInstanceOf[Formula.TimeWindow[?, ?]] should be(true)
    val timeWindow = result.asInstanceOf[Formula.TimeWindow[Id, Double]]
    timeWindow.source should be(source)
    timeWindow.duration should be(duration)
    timeWindow.aggregation should be(AggregationType.Avg)
  }

  "Duration helpers" should "create hours duration" in {
    val duration = 24.hours
    duration should be(Duration.ofHours(24))
  }

  it should "create days duration" in {
    val duration = 7.days
    duration should be(Duration.ofDays(7))
  }

  it should "create minutes duration" in {
    val duration = 30.minutes
    duration should be(Duration.ofMinutes(30))
  }

  it should "create seconds duration" in {
    val duration = 60.seconds
    duration should be(Duration.ofSeconds(60))
  }

  "Literal helpers" should "create literal from Double" in {
    val formula = lit(42.5)
    formula should be(Formula.Literal[Id, Double](42.5))
  }

  it should "create literal from Int" in {
    val formula = lit(42)
    formula should be(Formula.Literal[Id, Double](42.0))
  }

  "Complex DSL expressions" should "support natural syntax for formulas" in {
    val metric1 = Formula.MetricRef[Id, Double]("metric1")
    val metric2 = Formula.MetricRef[Id, Double]("metric2")
    val metric3 = Formula.MetricRef[Id, Double]("metric3")

    val result = (metric1 + metric2) * metric3 / 100

    result.isInstanceOf[Formula.Divide[?, ?]] should be(true)
    val divide = result.asInstanceOf[Formula.Divide[Id, Double]]
    divide.left.isInstanceOf[Formula.Multiply[?, ?]] should be(true)
    divide.right should be(Formula.Literal[Id, Double](100.0))

    val multiply = divide.left.asInstanceOf[Formula.Multiply[Id, Double]]
    multiply.left.isInstanceOf[Formula.Add[?, ?]] should be(true)
  }

  it should "support natural syntax for conditionals" in {
    val metric1 = Formula.MetricRef[Id, Double]("metric1")
    val metric2 = Formula.MetricRef[Id, Double]("metric2")

    val condition = (metric1 > 100) && (metric2 < 50)
    val result    = when(condition)(lit(1.0)).otherwise(lit(0.0))

    result.isInstanceOf[Formula.When[?, ?]] should be(true)
    result.asInstanceOf[Formula.When[Id, Double]].condition.isInstanceOf[Condition.And[?, ?]] should be(true)
  }

  it should "support time-based aggregations" in {
    val source = Formula.MetricRef[Id, Double]("temperature")

    val timeWindow = source.over(24.hours).aggregating(AggregationType.Sum)
    val result     = avg(timeWindow)

    result.isInstanceOf[Formula.Avg[?, ?]] should be(true)
    result.asInstanceOf[Formula.Avg[Id, Double]].source.isInstanceOf[Formula.TimeWindow[?, ?]] should be(true)
  }
