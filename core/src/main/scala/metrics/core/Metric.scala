package metrics.core

import java.time.Instant

import scala.reflect.ClassTag

import cats.effect.kernel.Sync
import fs2.Stream

import metrics.algebra.{EvaluationContext, FormulaAlgebra}

/**
 * Error types for type-safe metric computation
 */
sealed trait ComputeError
object ComputeError:
  case class TypeError(expected: String, actual: String) extends ComputeError
  case class MetricNotFound(name: String)                extends ComputeError

/**
 * Represents a metric with minimal metadata and runtime type information
 *
 * All metrics now have formulas:
 * - Leaf metrics: formula = QueryRef(...) (data comes from datasource via query)
 * - Computed metrics: formula = Add/Multiply/etc. (calculated from other formulas)
 *
 * Uses existential types (wildcards) for Formula to allow storing
 * formulas of different effect types and value types in the same registry.
 *
 * Runtime type safety:
 * - valueType: ClassTag captures the value type A at metric creation time
 * - Enables runtime validation before casting from Formula[?, ?] to Formula[F, A]
 * - Use computeValidated for type-safe computation with error handling
 *
 * @param name The metric name
 * @param formula The formula AST with existential types
 * @param valueType Runtime type information for value type A
 * @param unit Optional unit of measurement
 * @param description Optional description
 */
case class Metric(
  name: String,
  formula: Formula[?, ?],
  valueType: ClassTag[?],
  unit: Option[String] = None,
  description: String = "",
):

  /**
   * Compute this metric with runtime type validation
   *
   * This is the type-safe method for evaluating metrics. It validates that the
   * requested type A matches the type stored in valueType ClassTag before casting.
   *
   * Type safety guarantees:
   * - Returns Left(TypeError) if requested type A doesn't match stored valueType
   * - Only performs cast after successful type validation
   * - Uses SafeCasts.castFormula for documented, validated casting
   *
   * @param interpreter The formula interpreter to use for evaluation
   * @param context The evaluation context (datasources, metric registry)
   * @param timeRange The time range to compute the metric for
   * @tparam F The effect type (IO, cats.effect.IO, etc.)
   * @tparam A The value type (Double, Int, etc.)
   * @return Either error or stream of timestamped values within the time range
   *
   * Example:
   * {{{
   *   val metric = Metric.computed[IO, Double]("temperature", formula)
   *   val interpreter = new LocalFormulaInterpreter[IO, Double]()
   *   val context = new EvaluationContext(dataSource, registry)
   *   val timeRange = TimeRange.lastDays(7)
   *
   *   metric.compute(interpreter, context, timeRange) match
   *     case Right(stream) =>
   *       stream.compile.toList.map { values =>
   *         println(s"Got ${values.size} temperature readings")
   *       }
   *     case Left(ComputeError.TypeError(expected, actual)) =>
   *       println(s"Type mismatch: expected $expected, got $actual")
   *     case Left(error) =>
   *       println(s"Computation error: $error")
   * }}}
   */
  def compute[F[_]: Sync, A: ClassTag](
    interpreter: FormulaAlgebra[F, A],
    context: EvaluationContext[F, A],
    timeRange: TimeRange,
  ): Either[ComputeError, Stream[F, (Instant, A)]] =
    SafeCasts.castFormula[F, A](formula, valueType) match
      case Right(typedFormula) =>
        Right(
          interpreter
            .evaluate(typedFormula, context, timeRange)
            .filter { case (instant, _) => timeRange.contains(instant) },
        )
      case Left(typeError)     =>
        Left(
          ComputeError.TypeError(
            expected = typeError.expected,
            actual = typeError.actual,
          ),
        )

object Metric:
  /**
   * Create a computed metric with name and formula
   *
   * Automatically captures the ClassTag for type A to enable runtime type validation.
   *
   * @param name The metric name
   * @param formula The formula AST
   * @param unit Optional unit of measurement
   * @param description Optional description
   * @tparam F Effect type
   * @tparam A Value type (ClassTag captured implicitly)
   * @return Metric with runtime type information
   */
  def computed[F[_], A: ClassTag](
    name: String,
    formula: Formula[F, A],
    unit: Option[String] = None,
    description: String = "",
  ): Metric =
    Metric(name, formula, summon[ClassTag[A]], unit, description)

  /**
   * Create a leaf metric (data from datasource via query)
   *
   * Automatically captures the ClassTag for type A to enable runtime type validation.
   *
   * @param name The metric name
   * @param query The query specification for the datasource
   * @param unit Optional unit of measurement
   * @param description Optional description
   * @tparam F Effect type
   * @tparam A Value type (ClassTag captured implicitly)
   * @return Metric with runtime type information
   */
  def leaf[F[_], A: ClassTag](
    name: String,
    query: Query,
    unit: Option[String] = None,
    description: String = "",
  ): Metric =
    Metric(name, Formula.QueryRef[F, A](query), summon[ClassTag[A]], unit, description)

  /**
   * Extract all metric dependencies from a formula
   */
  def extractDependencies[F[_], A](formula: Formula[F, A]): Set[String] =
    formula match
      case Formula.MetricRef(name)     => Set(name)
      case Formula.Add(l, r)           => extractDependencies(l) ++ extractDependencies(r)
      case Formula.Subtract(l, r)      => extractDependencies(l) ++ extractDependencies(r)
      case Formula.Multiply(l, r)      => extractDependencies(l) ++ extractDependencies(r)
      case Formula.Divide(l, r)        => extractDependencies(l) ++ extractDependencies(r)
      case Formula.Sum(s)              => extractDependencies(s)
      case Formula.Avg(s)              => extractDependencies(s)
      case Formula.Min(sources)        => sources.flatMap(extractDependencies).toSet
      case Formula.Max(sources)        => sources.flatMap(extractDependencies).toSet
      case Formula.Count(s)            => extractDependencies(s)
      case Formula.When(_, t, e)       =>
        extractDependencies(t) ++ e.map(extractDependencies).getOrElse(Set.empty)
      case Formula.TimeWindow(s, _, _) => extractDependencies(s)
      case _                           => Set.empty

/**
 * A simple registry for storing and retrieving metrics
 *
 * Uses LinkedHashMap to preserve insertion order, ensuring that when searching
 * for formulas, metrics registered first are matched first.
 */
class MetricRegistry:
  private var metrics: scala.collection.mutable.LinkedHashMap[String, Metric] =
    scala.collection.mutable.LinkedHashMap.empty

  /**
   * Register a metric
   */
  def register(metric: Metric): Unit =
    metrics(metric.name) = metric

  /**
   * Get a metric by name
   */
  def get(name: String): Option[Metric] =
    metrics.get(name)

  /**
   * Compute a metric by name with runtime type validation
   *
   * Type-safe method for computing metrics from the registry:
   * - Validates that the metric exists (returns Left(MetricNotFound) if not)
   * - Validates that the requested type A matches the metric's stored type
   * - Only performs unsafe cast after successful validation
   * - Uses SafeCasts.castFormula for documented casting
   *
   * @param name The metric name to look up
   * @param interpreter The formula interpreter to use for evaluation
   * @param context The evaluation context (datasources, metric registry)
   * @param timeRange The time range to compute the metric for
   * @tparam F The effect type (IO, cats.effect.IO, etc.)
   * @tparam A The value type (Double, Int, etc.) - must have ClassTag
   * @return Either error or stream of timestamped values
   *
   * Example:
   * {{{
   *   val registry = new MetricRegistry()
   *   registry.register(Metric.computed[IO, Double]("temperature", formula))
   *
   *   registry.compute[IO, Double](
   *     "temperature",
   *     interpreter,
   *     context,
   *     timeRange
   *   ) match
   *     case Right(stream) =>
   *       stream.compile.toList.map(values => println(s"Results: $values"))
   *     case Left(ComputeError.MetricNotFound(name)) =>
   *       println(s"Metric not found: $name")
   *     case Left(ComputeError.TypeError(expected, actual)) =>
   *       println(s"Type mismatch: expected $expected, got $actual")
   * }}}
   */
  def compute[F[_]: Sync, A: ClassTag](
    name: String,
    interpreter: FormulaAlgebra[F, A],
    context: EvaluationContext[F, A],
    timeRange: TimeRange,
  ): Either[ComputeError, Stream[F, (Instant, A)]] =
    metrics.get(name) match
      case Some(metric) =>
        metric.compute(interpreter, context, timeRange)
      case None         =>
        Left(ComputeError.MetricNotFound(name))

  /**
   * Get all registered metrics
   */
  def all: List[Metric] =
    metrics.values.toList

  /**
   * Get all leaf metrics (metrics with QueryRef formulas, data from datasource)
   */
  def leafMetrics: List[Metric] =
    metrics.values.filter(m => m.formula.isInstanceOf[Formula.QueryRef[?, ?]]).toList

  /**
   * Get all computed metrics (metrics with non-QueryRef formulas)
   */
  def computedMetrics: List[Metric] =
    metrics.values.filter(m => !m.formula.isInstanceOf[Formula.QueryRef[?, ?]]).toList

  /**
   * Return metrics in dependency order (topological sort)
   * Metrics with no dependencies come first, then metrics that depend on them, etc.
   * Throws exception if circular dependencies are detected
   */
  def inDependencyOrder: List[Metric] =
    val allMetrics = metrics.values.toList
    val visited    = scala.collection.mutable.Set[String]()
    val result     = scala.collection.mutable.ListBuffer[Metric]()
    val visiting   = scala.collection.mutable.Set[String]()

    def visit(metricName: String): Unit =
      if visiting.contains(metricName) then
        throw new IllegalStateException(s"Circular dependency detected involving metric: $metricName")

      if !visited.contains(metricName) then
        metrics.get(metricName) match
          case Some(metric) =>
            visiting.add(metricName)

            val dependencies =
              Metric.extractDependencies[Nothing, Double](metric.formula.asInstanceOf[Formula[Nothing, Double]])

            dependencies.foreach(visit)

            visiting.remove(metricName)
            visited.add(metricName)
            result.append(metric)
          case None         =>
            ()

    allMetrics.foreach(m => visit(m.name))

    result.toList
