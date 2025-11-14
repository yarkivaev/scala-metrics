package metrics.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MetricSpec extends AnyFlatSpec with Matchers:
  type Id[A] = A

  "Metric.leaf" should "create a metric with QueryRef formula" in {
    val metric = Metric.leaf[Id, Double]("leafMetric", Query.ByName("leafMetric"))
    metric.name should be("leafMetric")
    metric.formula.isInstanceOf[Formula.QueryRef[?, ?]] should be(true)
  }

  "Metric.computed" should "create a computed metric with given formula" in {
    val formula = Formula.Add[Id, Double](Formula.Literal[Id, Double](10.0), Formula.Literal[Id, Double](20.0))
    val metric  = Metric.computed[Id, Double]("computedMetric", formula)

    metric.name should be("computedMetric")
    metric.formula should be(formula)
  }

  "Metric.extractDependencies" should "extract dependencies from MetricRef" in {
    val formula = Formula.MetricRef[Id, Double]("otherMetric")
    val deps    = Metric.extractDependencies(formula)

    deps should contain("otherMetric")
    deps should have size 1
  }

  it should "extract dependencies from Add formula" in {
    val formula = Formula.Add[Id, Double](
      Formula.MetricRef[Id, Double]("metric1"),
      Formula.MetricRef[Id, Double]("metric2"),
    )
    val deps    = Metric.extractDependencies(formula)

    deps should contain("metric1")
    deps should contain("metric2")
    deps should have size 2
  }

  it should "extract dependencies from Subtract formula" in {
    val formula = Formula.Subtract[Id, Double](
      Formula.MetricRef[Id, Double]("metric1"),
      Formula.MetricRef[Id, Double]("metric2"),
    )
    val deps    = Metric.extractDependencies(formula)

    deps should contain("metric1")
    deps should contain("metric2")
    deps should have size 2
  }

  it should "extract dependencies from Multiply formula" in {
    val formula = Formula.Multiply[Id, Double](
      Formula.MetricRef[Id, Double]("metric1"),
      Formula.MetricRef[Id, Double]("metric2"),
    )
    val deps    = Metric.extractDependencies(formula)

    deps should contain("metric1")
    deps should contain("metric2")
    deps should have size 2
  }

  it should "extract dependencies from Divide formula" in {
    val formula = Formula.Divide[Id, Double](
      Formula.MetricRef[Id, Double]("metric1"),
      Formula.MetricRef[Id, Double]("metric2"),
    )
    val deps    = Metric.extractDependencies(formula)

    deps should contain("metric1")
    deps should contain("metric2")
    deps should have size 2
  }

  it should "extract dependencies from Sum formula" in {
    val formula = Formula.Sum[Id, Double](Formula.MetricRef[Id, Double]("metric1"))
    val deps    = Metric.extractDependencies(formula)

    deps should contain("metric1")
    deps should have size 1
  }

  it should "extract dependencies from Avg formula" in {
    val formula = Formula.Avg[Id, Double](Formula.MetricRef[Id, Double]("metric1"))
    val deps    = Metric.extractDependencies(formula)

    deps should contain("metric1")
    deps should have size 1
  }

  it should "extract dependencies from Min formula" in {
    val formula = Formula.Min(
      List(
        Formula.MetricRef[Id, Double]("metric1"),
        Formula.MetricRef[Id, Double]("metric2"),
        Formula.MetricRef[Id, Double]("metric3"),
      ),
    )
    val deps    = Metric.extractDependencies(formula)

    deps should contain("metric1")
    deps should contain("metric2")
    deps should contain("metric3")
    deps should have size 3
  }

  it should "extract dependencies from Max formula" in {
    val formula = Formula.Max(
      List(
        Formula.MetricRef[Id, Double]("metric1"),
        Formula.MetricRef[Id, Double]("metric2"),
      ),
    )
    val deps    = Metric.extractDependencies(formula)

    deps should contain("metric1")
    deps should contain("metric2")
    deps should have size 2
  }

  it should "extract dependencies from Count formula" in {
    val formula = Formula.Count[Id, Double](Formula.MetricRef[Id, Double]("metric1"))
    val deps    = Metric.extractDependencies(formula)

    deps should contain("metric1")
    deps should have size 1
  }

  it should "extract dependencies from When formula" in {
    val formula = Formula.When(
      Condition.GreaterThan[Id, Double](Formula.Literal[Id, Double](10.0), Formula.Literal[Id, Double](5.0)),
      Formula.MetricRef[Id, Double]("thenMetric"),
      Some(Formula.MetricRef[Id, Double]("elseMetric")),
    )
    val deps    = Metric.extractDependencies(formula)

    deps should contain("thenMetric")
    deps should contain("elseMetric")
    deps should have size 2
  }

  it should "extract dependencies from TimeWindow formula" in {
    val formula = Formula.TimeWindow(
      Formula.MetricRef[Id, Double]("metric1"),
      java.time.Duration.ofHours(1),
      AggregationType.Avg,
    )
    val deps    = Metric.extractDependencies(formula)

    deps should contain("metric1")
    deps should have size 1
  }

  it should "return empty set for Literal formula" in {
    val formula = Formula.Literal[Id, Double](42.0)
    val deps    = Metric.extractDependencies(formula)

    deps should be(empty)
  }

  it should "return empty set for QueryRef formula" in {
    val formula = Formula.QueryRef[Id, Double](Query.ByName("test"))
    val deps    = Metric.extractDependencies(formula)

    deps should be(empty)
  }

  it should "extract dependencies from nested formulas" in {
    val formula = Formula.Multiply[Id, Double](
      Formula.Add[Id, Double](
        Formula.MetricRef[Id, Double]("metric1"),
        Formula.MetricRef[Id, Double]("metric2"),
      ),
      Formula.MetricRef[Id, Double]("metric3"),
    )
    val deps    = Metric.extractDependencies(formula)

    deps should contain("metric1")
    deps should contain("metric2")
    deps should contain("metric3")
    deps should have size 3
  }

  it should "extract unique dependencies from complex formulas" in {
    val formula = Formula.Divide[Id, Double](
      Formula.Add[Id, Double](
        Formula.MetricRef[Id, Double]("metric1"),
        Formula.MetricRef[Id, Double]("metric1"),
      ),
      Formula.MetricRef[Id, Double]("metric2"),
    )
    val deps    = Metric.extractDependencies(formula)

    deps should contain("metric1")
    deps should contain("metric2")
    deps should have size 2
  }
