package metrics.dsl

import scala.reflect.ClassTag

import metrics.core._

/**
 * DSL for building metrics with natural syntax
 */
object MetricDSL:

  /**
   * Create a new metric builder - requires a formula
   * For leaf metrics, use QueryRef. For computed metrics, use formula expressions.
   * Captures ClassTag for runtime type validation.
   */
  def metric[F[_], A: ClassTag](name: String, formula: Formula[F, A]): MetricBuilder =
    MetricBuilder(name, formula, summon[ClassTag[A]])

  /**
   * Builder for creating metrics
   * Uses existential types for Formula to allow building metrics with different effect types
   * Captures ClassTag for runtime type validation
   */
  case class MetricBuilder(
    name: String,
    formula: Formula[?, ?],
    valueType: ClassTag[?],
    unit: Option[String] = None,
    description: String = "",
  ):

    def describedAs(desc: String): MetricBuilder =
      copy(description = desc)

    def calculatedBy[F[_], A](f: Formula[F, A]): MetricBuilder =
      copy(formula = f)

    def withUnit(u: String): MetricBuilder =
      copy(unit = Some(u))

    def build: Metric =
      Metric(name, formula, valueType, unit, description)

  /**
   * Extension methods for easy metric registration
   */
  extension (registry: MetricRegistry)
    def +=(builder: MetricBuilder): Unit =
      registry.register(builder.build)

    def +=(metric: Metric): Unit =
      registry.register(metric)
