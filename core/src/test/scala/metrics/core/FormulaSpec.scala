package metrics.core

import java.time.Duration

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FormulaSpec extends AnyFlatSpec with Matchers:
  type Id[A] = A

  "Formula.Literal" should "create a literal formula with a value" in {
    val formula = Formula.Literal[Id, Double](42.5)
    formula.value should be(42.5)
  }

  "Formula.MetricRef" should "create a reference to a metric by name" in {
    val formula = Formula.MetricRef[Id, Double]("myMetric")
    formula.metricName should be("myMetric")
  }

  "Formula.Add" should "create an addition formula" in {
    val left    = Formula.Literal[Id, Double](10.0)
    val right   = Formula.Literal[Id, Double](20.0)
    val formula = Formula.Add[Id, Double](left, right)
    formula.left should be(left)
    formula.right should be(right)
  }

  "Formula.Subtract" should "create a subtraction formula" in {
    val left    = Formula.Literal[Id, Double](30.0)
    val right   = Formula.Literal[Id, Double](10.0)
    val formula = Formula.Subtract[Id, Double](left, right)
    formula.left should be(left)
    formula.right should be(right)
  }

  "Formula.Multiply" should "create a multiplication formula" in {
    val left    = Formula.Literal[Id, Double](5.0)
    val right   = Formula.Literal[Id, Double](4.0)
    val formula = Formula.Multiply[Id, Double](left, right)
    formula.left should be(left)
    formula.right should be(right)
  }

  "Formula.Divide" should "create a division formula" in {
    val left    = Formula.Literal[Id, Double](100.0)
    val right   = Formula.Literal[Id, Double](5.0)
    val formula = Formula.Divide[Id, Double](left, right)
    formula.left should be(left)
    formula.right should be(right)
  }

  "Formula.Sum" should "create a sum aggregation formula" in {
    val source  = Formula.MetricRef[Id, Double]("sales")
    val formula = Formula.Sum[Id, Double](source)
    formula.source should be(source)
  }

  "Formula.Avg" should "create an average aggregation formula" in {
    val source  = Formula.MetricRef[Id, Double]("temperatures")
    val formula = Formula.Avg[Id, Double](source)
    formula.source should be(source)
  }

  "Formula.Min" should "create a min aggregation formula with multiple sources" in {
    val sources =
      List(Formula.Literal[Id, Double](10.0), Formula.Literal[Id, Double](20.0), Formula.Literal[Id, Double](5.0))
    val formula = Formula.Min[Id, Double](sources)
    formula.sources should be(sources)
    formula.sources.size should be(3)
  }

  "Formula.Max" should "create a max aggregation formula with multiple sources" in {
    val sources =
      List(Formula.Literal[Id, Double](10.0), Formula.Literal[Id, Double](20.0), Formula.Literal[Id, Double](5.0))
    val formula = Formula.Max[Id, Double](sources)
    formula.sources should be(sources)
    formula.sources.size should be(3)
  }

  "Formula.Count" should "create a count aggregation formula" in {
    val source  = Formula.MetricRef[Id, Double]("items")
    val formula = Formula.Count[Id, Double](source)
    formula.source should be(source)
  }

  "Formula.When" should "create a conditional formula with else branch" in {
    val condition   =
      Condition.GreaterThan[Id, Double](Formula.Literal[Id, Double](10.0), Formula.Literal[Id, Double](5.0))
    val thenFormula = Formula.Literal[Id, Double](100.0)
    val elseFormula = Formula.Literal[Id, Double](0.0)
    val formula     = Formula.When[Id, Double](condition, thenFormula, Some(elseFormula))

    formula.condition should be(condition)
    formula.thenFormula should be(thenFormula)
    formula.elseFormula should be(Some(elseFormula))
  }

  it should "create a conditional formula without else branch" in {
    val condition = Condition.LessThan[Id, Double](Formula.Literal[Id, Double](5.0), Formula.Literal[Id, Double](10.0))
    val thenFormula = Formula.Literal[Id, Double](100.0)
    val formula     = Formula.When[Id, Double](condition, thenFormula, None)

    formula.condition should be(condition)
    formula.thenFormula should be(thenFormula)
    formula.elseFormula should be(None)
  }

  "Formula.TimeWindow" should "create a time window formula" in {
    val source      = Formula.MetricRef[Id, Double]("sensor_readings")
    val duration    = Duration.ofHours(24)
    val aggregation = AggregationType.Avg
    val formula     = Formula.TimeWindow[Id, Double](source, duration, aggregation)

    formula.source should be(source)
    formula.duration should be(duration)
    formula.aggregation should be(aggregation)
  }

  "Nested formulas" should "support complex expressions" in {
    val add      = Formula.Add[Id, Double](Formula.Literal[Id, Double](10.0), Formula.Literal[Id, Double](20.0))
    val multiply = Formula.Multiply[Id, Double](add, Formula.Literal[Id, Double](5.0))

    multiply.isInstanceOf[Formula.Multiply[?, ?]] should be(true)
    multiply.left.isInstanceOf[Formula.Add[?, ?]] should be(true)
    multiply.right.isInstanceOf[Formula.Literal[?, ?]] should be(true)
  }

  it should "support deeply nested expressions" in {
    val a = Formula.MetricRef[Id, Double]("a")
    val b = Formula.MetricRef[Id, Double]("b")
    val c = Formula.MetricRef[Id, Double]("c")
    val d = Formula.MetricRef[Id, Double]("d")
    val e = Formula.MetricRef[Id, Double]("e")

    val add      = Formula.Add[Id, Double](a, b)
    val subtract = Formula.Subtract[Id, Double](c, d)
    val divide   = Formula.Divide[Id, Double](add, subtract)
    val multiply = Formula.Multiply[Id, Double](divide, e)

    multiply.isInstanceOf[Formula.Multiply[?, ?]] should be(true)
    multiply.left.isInstanceOf[Formula.Divide[?, ?]] should be(true)
    val divideLeft = multiply.left.asInstanceOf[Formula.Divide[Id, Double]]
    divideLeft.left.isInstanceOf[Formula.Add[?, ?]] should be(true)
    divideLeft.right.isInstanceOf[Formula.Subtract[?, ?]] should be(true)
  }
