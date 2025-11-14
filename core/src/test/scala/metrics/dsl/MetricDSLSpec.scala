package metrics.dsl

import scala.language.implicitConversions

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import metrics.core._
import metrics.dsl.FormulaDSL.{_, given}
import metrics.dsl.MetricDSL._

class MetricDSLSpec extends AnyFlatSpec with Matchers:
  type Id[A] = A

  "metric() function" should "create a MetricBuilder with name and formula" in {
    val formula = Formula.Literal[Id, Double](42.0)
    val builder = metric("testMetric", formula)
    builder.name should be("testMetric")
    builder.formula should be(formula)
  }

  "MetricBuilder.describedAs" should "set description" in {
    val formula = Formula.Literal[Id, Double](42.0)
    val builder = metric("testMetric", formula).describedAs("Test description")
    builder.description should be("Test description")
  }

  it should "preserve other properties" in {
    val formula = Formula.Literal[Id, Double](42.0)
    val builder = metric("testMetric", formula)
      .withUnit("kg")
      .describedAs("Test description")

    builder.description should be("Test description")
    builder.unit should be(Some("kg"))
  }

  "MetricBuilder.calculatedBy" should "replace formula" in {
    val formula1 = Formula.Literal[Id, Double](42.0)
    val formula2 = Formula.Literal[Id, Double](100.0)
    val builder  = metric("testMetric", formula1).calculatedBy(formula2)

    builder.formula should be(formula2)
  }

  it should "preserve other properties" in {
    val formula1 = Formula.Literal[Id, Double](42.0)
    val formula2 = Formula.Literal[Id, Double](100.0)
    val builder  = metric("testMetric", formula1)
      .describedAs("Test")
      .calculatedBy(formula2)

    builder.description should be("Test")
    builder.formula should be(formula2)
  }

  "MetricBuilder.withUnit" should "set unit" in {
    val formula = Formula.Literal[Id, Double](42.0)
    val builder = metric("testMetric", formula).withUnit("kg")
    builder.unit should be(Some("kg"))
  }

  it should "overwrite previous unit" in {
    val formula = Formula.Literal[Id, Double](42.0)
    val builder = metric("testMetric", formula)
      .withUnit("kg")
      .withUnit("tons")

    builder.unit should be(Some("tons"))
  }

  "MetricBuilder.build" should "create a Metric" in {
    val formula     = Formula.Literal[Id, Double](42.0)
    val builtMetric = metric("testMetric", formula)
      .describedAs("Test description")
      .withUnit("kg")
      .build

    builtMetric.name should be("testMetric")
    builtMetric.description should be("Test description")
    builtMetric.unit should be(Some("kg"))
  }

  it should "create a metric with the given formula" in {
    val formula     = Formula.Literal[Id, Double](42.0)
    val builtMetric = metric("testMetric", formula).build

    builtMetric.formula should be(formula)
  }

  "MetricBuilder fluent API" should "support method chaining" in {
    val formula = Formula.Literal[Id, Double](42.0)

    val builtMetric = metric("testMetric", formula)
      .describedAs("A comprehensive test metric")
      .withUnit("kg")
      .build

    builtMetric.name should be("testMetric")
    builtMetric.description should be("A comprehensive test metric")
    builtMetric.formula should be(formula)
    builtMetric.unit should be(Some("kg"))
  }

  "MetricRegistry extension methods" should "add MetricBuilder to registry" in {
    val registry = new MetricRegistry
    val formula  = Formula.Literal[Id, Double](42.0)
    val builder  = metric("testMetric", formula).describedAs("Test")

    registry += builder

    val registered = registry.get("testMetric")
    registered should be(defined)
    registered.get.name should be("testMetric")
    registered.get.description should be("Test")
  }

  it should "add Metric to registry" in {
    val registry    = new MetricRegistry
    val builtMetric = Metric.leaf[Id, Double]("testMetric", Query.ByName("testMetric"))

    registry += builtMetric

    val registered = registry.get("testMetric")
    registered should be(defined)
    registered.get.name should be("testMetric")
  }

  it should "support multiple registrations" in {
    val registry = new MetricRegistry
    val formula1 = Formula.Literal[Id, Double](1.0)
    val formula2 = Formula.Literal[Id, Double](2.0)

    registry += metric("metric1", formula1).build
    registry += metric("metric2", formula2).describedAs("Test")
    registry += Metric.leaf[Id, Double]("metric3", Query.ByName("metric3"))

    registry.all should have length 3
    registry.get("metric1") should be(defined)
    registry.get("metric2") should be(defined)
    registry.get("metric3") should be(defined)
  }

  "Real-world metric examples" should "create a simple leaf metric" in {
    val temperature = metric("temperature", Formula.QueryRef[Id, Double](Query.ByName("temperature")))
      .describedAs("Current temperature reading")
      .withUnit("°C")
      .build

    temperature.unit should be(Some("°C"))
  }

  it should "create a computed metric with dependencies" in {
    val calendarTime  = Formula.MetricRef[Id, Double]("CalendarTime")
    val totalDowntime = Formula.MetricRef[Id, Double]("TotalDowntime")

    val ktgFormula = (calendarTime - totalDowntime) / calendarTime * 100.0
    val ktg        = metric("КТГ", ktgFormula)
      .describedAs("Technical Readiness Coefficient")
      .withUnit("%")
      .build

    ktg.formula should be(ktgFormula)
    ktg.unit should be(Some("%"))
  }
