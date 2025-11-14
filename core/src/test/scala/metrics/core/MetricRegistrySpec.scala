package metrics.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MetricRegistrySpec extends AnyFlatSpec with Matchers:
  type Id[A] = A

  "MetricRegistry.register" should "register a metric" in {
    val registry = new MetricRegistry
    val metric   = Metric.leaf[Id, Double]("testMetric", Query.ByName("testMetric"))

    registry.register(metric)
    registry.get("testMetric") should be(Some(metric))
  }

  "MetricRegistry.get" should "return None for non-existent metric" in {
    val registry = new MetricRegistry
    registry.get("nonExistent") should be(None)
  }

  it should "return the registered metric" in {
    val registry = new MetricRegistry
    val metric   = Metric.leaf[Id, Double]("testMetric", Query.ByName("testMetric"))
    registry.register(metric)

    registry.get("testMetric") should be(Some(metric))
  }

  "MetricRegistry.all" should "return all registered metrics" in {
    val registry = new MetricRegistry
    val metric1  = Metric.leaf[Id, Double]("metric1", Query.ByName("metric1"))
    val metric2  = Metric.leaf[Id, Double]("metric2", Query.ByName("metric2"))
    val metric3  = Metric.leaf[Id, Double]("metric3", Query.ByName("metric3"))

    registry.register(metric1)
    registry.register(metric2)
    registry.register(metric3)

    val all = registry.all
    all should have length 3
    all should contain(metric1)
    all should contain(metric2)
    all should contain(metric3)
  }

  it should "return empty list when no metrics are registered" in {
    val registry = new MetricRegistry
    registry.all should be(empty)
  }

  "MetricRegistry.inDependencyOrder" should "return metrics in correct order for simple dependency chain" in {
    val registry = new MetricRegistry

    val metric1 = Metric.leaf[Id, Double]("metric1", Query.ByName("metric1"))

    val metric2 = Metric.computed[Id, Double]("metric2", Formula.MetricRef[Id, Double]("metric1"))

    val metric3 = Metric.computed[Id, Double]("metric3", Formula.MetricRef[Id, Double]("metric2"))

    registry.register(metric3)
    registry.register(metric2)
    registry.register(metric1)

    val ordered = registry.inDependencyOrder
    ordered should have length 3

    val metric1Index = ordered.indexWhere(_.name == "metric1")
    val metric2Index = ordered.indexWhere(_.name == "metric2")
    val metric3Index = ordered.indexWhere(_.name == "metric3")

    metric1Index should be < metric2Index
    metric2Index should be < metric3Index
  }

  it should "handle metrics with multiple dependencies" in {
    val registry = new MetricRegistry

    val a = Metric.leaf[Id, Double]("a", Query.ByName("a"))
    val b = Metric.leaf[Id, Double]("b", Query.ByName("b"))

    val c = Metric.computed[Id, Double](
      "c",
      Formula.Add[Id, Double](
        Formula.MetricRef[Id, Double]("a"),
        Formula.MetricRef[Id, Double]("b"),
      ),
    )

    registry.register(c)
    registry.register(b)
    registry.register(a)

    val ordered = registry.inDependencyOrder
    ordered should have length 3

    val aIndex = ordered.indexWhere(_.name == "a")
    val bIndex = ordered.indexWhere(_.name == "b")
    val cIndex = ordered.indexWhere(_.name == "c")

    aIndex should be < cIndex
    bIndex should be < cIndex
  }

  it should "handle complex dependency graphs" in {
    val registry = new MetricRegistry

    val a = Metric.leaf[Id, Double]("a", Query.ByName("a"))
    val b = Metric.leaf[Id, Double]("b", Query.ByName("b"))
    val c = Metric.computed[Id, Double](
      "c",
      Formula.Add[Id, Double](
        Formula.MetricRef[Id, Double]("a"),
        Formula.MetricRef[Id, Double]("b"),
      ),
    )
    val d = Metric.computed[Id, Double]("d", Formula.MetricRef[Id, Double]("c"))

    registry.register(d)
    registry.register(c)
    registry.register(b)
    registry.register(a)

    val ordered = registry.inDependencyOrder
    ordered should have length 4

    val aIndex = ordered.indexWhere(_.name == "a")
    val bIndex = ordered.indexWhere(_.name == "b")
    val cIndex = ordered.indexWhere(_.name == "c")
    val dIndex = ordered.indexWhere(_.name == "d")

    aIndex should be < cIndex
    bIndex should be < cIndex
    cIndex should be < dIndex
  }

  it should "detect circular dependencies" in {
    val registry = new MetricRegistry

    val a = Metric.computed[Id, Double]("a", Formula.MetricRef[Id, Double]("b"))
    val b = Metric.computed[Id, Double]("b", Formula.MetricRef[Id, Double]("a"))

    registry.register(a)
    registry.register(b)

    an[IllegalStateException] should be thrownBy {
      registry.inDependencyOrder
    }
  }

  it should "detect self-referencing circular dependencies" in {
    val registry = new MetricRegistry

    val a = Metric.computed[Id, Double]("a", Formula.MetricRef[Id, Double]("a"))
    registry.register(a)

    an[IllegalStateException] should be thrownBy {
      registry.inDependencyOrder
    }
  }

  it should "handle diamond dependency pattern" in {
    val registry = new MetricRegistry

    val a = Metric.leaf[Id, Double]("a", Query.ByName("a"))
    val b = Metric.computed[Id, Double]("b", Formula.MetricRef[Id, Double]("a"))
    val c = Metric.computed[Id, Double]("c", Formula.MetricRef[Id, Double]("a"))
    val d = Metric.computed[Id, Double](
      "d",
      Formula.Add[Id, Double](
        Formula.MetricRef[Id, Double]("b"),
        Formula.MetricRef[Id, Double]("c"),
      ),
    )

    registry.register(d)
    registry.register(c)
    registry.register(b)
    registry.register(a)

    val ordered = registry.inDependencyOrder
    ordered should have length 4

    val aIndex = ordered.indexWhere(_.name == "a")
    val bIndex = ordered.indexWhere(_.name == "b")
    val cIndex = ordered.indexWhere(_.name == "c")
    val dIndex = ordered.indexWhere(_.name == "d")

    aIndex should be < bIndex
    aIndex should be < cIndex
    bIndex should be < dIndex
    cIndex should be < dIndex
  }

  it should "handle independent metrics" in {
    val registry = new MetricRegistry

    val a = Metric.leaf[Id, Double]("a", Query.ByName("a"))
    val b = Metric.leaf[Id, Double]("b", Query.ByName("b"))
    val c = Metric.leaf[Id, Double]("c", Query.ByName("c"))

    registry.register(a)
    registry.register(b)
    registry.register(c)

    val ordered = registry.inDependencyOrder
    ordered should have length 3
    ordered should contain(a)
    ordered should contain(b)
    ordered should contain(c)
  }

  "MetricRegistry.leafMetrics" should "return only leaf metrics (QueryRef formulas)" in {
    val registry = new MetricRegistry

    val leaf1    = Metric.leaf[Id, Double]("leaf1", Query.ByName("leaf1"))
    val leaf2    = Metric.leaf[Id, Double]("leaf2", Query.ByName("leaf2"))
    val computed = Metric.computed[Id, Double](
      "computed",
      Formula.Add[Id, Double](Formula.MetricRef[Id, Double]("leaf1"), Formula.MetricRef[Id, Double]("leaf2")),
    )

    registry.register(leaf1)
    registry.register(leaf2)
    registry.register(computed)

    val result = registry.leafMetrics
    result should have length 2
    result.map(_.name) should contain("leaf1")
    result.map(_.name) should contain("leaf2")
    result.map(_.name) should not contain "computed"
  }

  it should "return empty list when no leaf metrics exist" in {
    val registry = new MetricRegistry

    val computed = Metric.computed[Id, Double](
      "computed",
      Formula.Literal[Id, Double](42.0),
    )
    registry.register(computed)

    registry.leafMetrics should be(empty)
  }

  "MetricRegistry.computedMetrics" should "return only computed metrics (non-QueryRef formulas)" in {
    val registry = new MetricRegistry

    val leaf1     = Metric.leaf[Id, Double]("leaf1", Query.ByName("leaf1"))
    val leaf2     = Metric.leaf[Id, Double]("leaf2", Query.ByName("leaf2"))
    val computed1 = Metric.computed[Id, Double](
      "computed1",
      Formula.Add[Id, Double](Formula.MetricRef[Id, Double]("leaf1"), Formula.MetricRef[Id, Double]("leaf2")),
    )
    val computed2 = Metric.computed[Id, Double](
      "computed2",
      Formula.Multiply[Id, Double](Formula.MetricRef[Id, Double]("computed1"), Formula.Literal[Id, Double](2.0)),
    )

    registry.register(leaf1)
    registry.register(leaf2)
    registry.register(computed1)
    registry.register(computed2)

    val result = registry.computedMetrics
    result should have length 2
    result.map(_.name) should contain("computed1")
    result.map(_.name) should contain("computed2")
    result.map(_.name) should not contain "leaf1"
    result.map(_.name) should not contain "leaf2"
  }

  it should "return empty list when no computed metrics exist" in {
    val registry = new MetricRegistry

    val leaf = Metric.leaf[Id, Double]("leaf", Query.ByName("leaf"))
    registry.register(leaf)

    registry.computedMetrics should be(empty)
  }
