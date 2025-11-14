package metrics.dsl

import java.time.Duration

import metrics.core._

/**
 * DSL for building formulas with natural syntax
 */
object FormulaDSL:

  /**
   * Implicit conversions for numeric literals
   * These are generic over effect type F and value type A for flexibility
   */
  given [F[_]]: Conversion[Double, Formula[F, Double]] = d => Formula.Literal[F, Double](d)
  given [F[_]]: Conversion[Int, Formula[F, Double]]    = i => Formula.Literal[F, Double](i.toDouble)

  /**
   * Implicit conversion from Metric to Formula
   * This allows using Metric objects directly in formulas while maintaining lazy evaluation.
   * The metric is converted to a MetricRef which stores only the name - evaluation happens later.
   */
  given [F[_], A]: Conversion[Metric, Formula[F, A]] = m => Formula.MetricRef[F, A](m.name)

  /**
   * Extension methods for Formula to support operators
   * These now also accept Metric on the right side for easier composition
   * Generic over effect type F and value type A
   */
  extension [F[_], A](left: Formula[F, A])
    def +(right: Metric | Formula[F, A]): Formula[F, A] =
      val rightFormula = right match
        case m: Metric        => Formula.MetricRef[F, A](m.name)
        case f: Formula[F, A] => f
      Formula.Add[F, A](left, rightFormula)

    def -(right: Metric | Formula[F, A]): Formula[F, A] =
      val rightFormula = right match
        case m: Metric        => Formula.MetricRef[F, A](m.name)
        case f: Formula[F, A] => f
      Formula.Subtract[F, A](left, rightFormula)

    def *(right: Metric | Formula[F, A]): Formula[F, A] =
      val rightFormula = right match
        case m: Metric        => Formula.MetricRef[F, A](m.name)
        case f: Formula[F, A] => f
      Formula.Multiply[F, A](left, rightFormula)

    def /(right: Metric | Formula[F, A]): Formula[F, A] =
      val rightFormula = right match
        case m: Metric        => Formula.MetricRef[F, A](m.name)
        case f: Formula[F, A] => f
      Formula.Divide[F, A](left, rightFormula)

    def >(right: Metric | Formula[F, A]): Condition[F, A] =
      val rightFormula = right match
        case m: Metric        => Formula.MetricRef[F, A](m.name)
        case f: Formula[F, A] => f
      Condition.GreaterThan[F, A](left, rightFormula)

    def <(right: Metric | Formula[F, A]): Condition[F, A] =
      val rightFormula = right match
        case m: Metric        => Formula.MetricRef[F, A](m.name)
        case f: Formula[F, A] => f
      Condition.LessThan[F, A](left, rightFormula)

    def ===(right: Metric | Formula[F, A]): Condition[F, A] =
      val rightFormula = right match
        case m: Metric        => Formula.MetricRef[F, A](m.name)
        case f: Formula[F, A] => f
      Condition.Equals[F, A](left, rightFormula)

  /**
   * Extension methods for Metric to support operators
   * These convert Metrics to Formulas and then apply the operation
   * Generic over effect type F and value type A
   */
  extension (left: Metric)
    def +[F[_], A](right: Metric | Formula[F, A]): Formula[F, A] =
      val leftFormula  = Formula.MetricRef[F, A](left.name)
      val rightFormula = right match
        case m: Metric        => Formula.MetricRef[F, A](m.name)
        case f: Formula[F, A] => f
      Formula.Add[F, A](leftFormula, rightFormula)

    def -[F[_], A](right: Metric | Formula[F, A]): Formula[F, A] =
      val leftFormula  = Formula.MetricRef[F, A](left.name)
      val rightFormula = right match
        case m: Metric        => Formula.MetricRef[F, A](m.name)
        case f: Formula[F, A] => f
      Formula.Subtract[F, A](leftFormula, rightFormula)

    def *[F[_], A](right: Metric | Formula[F, A]): Formula[F, A] =
      val leftFormula  = Formula.MetricRef[F, A](left.name)
      val rightFormula = right match
        case m: Metric        => Formula.MetricRef[F, A](m.name)
        case f: Formula[F, A] => f
      Formula.Multiply[F, A](leftFormula, rightFormula)

    def /[F[_], A](right: Metric | Formula[F, A]): Formula[F, A] =
      val leftFormula  = Formula.MetricRef[F, A](left.name)
      val rightFormula = right match
        case m: Metric        => Formula.MetricRef[F, A](m.name)
        case f: Formula[F, A] => f
      Formula.Divide[F, A](leftFormula, rightFormula)

  /**
   * Extension methods for Condition
   */
  extension [F[_], A](left: Condition[F, A])
    def &&(right: Condition[F, A]): Condition[F, A] = Condition.And[F, A](List(left, right))
    def ||(right: Condition[F, A]): Condition[F, A] = Condition.Or[F, A](List(left, right))
    def unary_! : Condition[F, A]                   = Condition.Not[F, A](left)

  /**
   * Aggregation functions
   */
  def sum[F[_], A](formula: Formula[F, A]): Formula[F, A]   = Formula.Sum[F, A](formula)
  def avg[F[_], A](formula: Formula[F, A]): Formula[F, A]   = Formula.Avg[F, A](formula)
  def min[F[_], A](formulas: Formula[F, A]*): Formula[F, A] = Formula.Min[F, A](formulas.toList)
  def max[F[_], A](formulas: Formula[F, A]*): Formula[F, A] = Formula.Max[F, A](formulas.toList)
  def count[F[_], A](formula: Formula[F, A]): Formula[F, A] = Formula.Count[F, A](formula)

  /**
   * Conditional expressions
   */
  def when[F[_], A](condition: Condition[F, A]): WhenBuilder[F, A] = WhenBuilder[F, A](condition)

  case class WhenBuilder[F[_], A](condition: Condition[F, A]):
    def apply(thenFormula: Formula[F, A]): ThenBuilder[F, A] =
      ThenBuilder[F, A](condition, thenFormula)

  case class ThenBuilder[F[_], A](condition: Condition[F, A], thenFormula: Formula[F, A]):
    def otherwise(elseFormula: Formula[F, A]): Formula[F, A] =
      Formula.When[F, A](condition, thenFormula, Some(elseFormula))

    def toFormula: Formula[F, A] =
      Formula.When[F, A](condition, thenFormula, None)

  /**
   * Time-based operations
   */
  extension [F[_], A](formula: Formula[F, A])
    def over(duration: Duration): TimeWindowBuilder[F, A] =
      TimeWindowBuilder[F, A](formula, duration)

  case class TimeWindowBuilder[F[_], A](formula: Formula[F, A], duration: Duration):
    def aggregating(aggregation: AggregationType): Formula[F, A] =
      Formula.TimeWindow[F, A](formula, duration, aggregation)

  /**
   * Duration helpers
   */
  extension (n: Int)
    def hours: Duration   = Duration.ofHours(n.toLong)
    def days: Duration    = Duration.ofDays(n.toLong)
    def minutes: Duration = Duration.ofMinutes(n.toLong)
    def seconds: Duration = Duration.ofSeconds(n.toLong)

  /**
   * Helper for creating literal values
   */
  def lit[F[_]](value: Double): Formula[F, Double] = Formula.Literal[F, Double](value)
  def lit[F[_]](value: Int): Formula[F, Double]    = Formula.Literal[F, Double](value.toDouble)

  /**
   * Helpers for creating configuration lookups
   */

  /**
   * Create a config lookup with a formula key
   * @param configName Name of the configuration source
   * @param key Formula that evaluates to the lookup key
   * @param defaultValue Optional default value if key not found
   */
  def configRef[F[_], A, K](
    configName: String,
    key: Formula[F, K],
    defaultValue: Option[A] = None,
  ): Formula[F, A] =
    Formula.ConfigRef[F, A, K](configName, key, defaultValue)

  /**
   * Create a config lookup with a literal key (most common case)
   * @param configName Name of the configuration source
   * @param key Literal key value to look up
   * @param defaultValue Optional default value if key not found
   */
  def configRef[F[_], A, K](
    configName: String,
    key: K,
    defaultValue: Option[A],
  ): Formula[F, A] =
    Formula.ConfigRef[F, A, K](configName, Formula.Literal[F, K](key), defaultValue)

  /**
   * Create a config lookup with a literal key and no default
   * @param configName Name of the configuration source
   * @param key Literal key value to look up
   */
  def configRefRequired[F[_], A, K](
    configName: String,
    key: K,
  ): Formula[F, A] =
    Formula.ConfigRef[F, A, K](configName, Formula.Literal[F, K](key), None)

  /**
   * Helpers for stream transformations with Property/Map/FlatMap
   */

  /**
   * Create a property reference to extract field from current item
   * @param propertyName Name of the field to extract
   */
  def property[F[_], A](propertyName: String): Formula[F, A] =
    Formula.Property[F, A](propertyName)

  /**
   * Create a tuple of two formulas (useful for composite config keys)
   * @param first First formula
   * @param second Second formula
   */
  def tuple2[F[_], A, B](first: Formula[F, A], second: Formula[F, B]): Formula[F, (A, B)] =
    Formula.Tuple2[F, A, B](first, second)

  /**
   * Extension methods for Formula to support map/flatMap
   */
  extension [F[_], A](formula: Formula[F, A])
    /**
     * Map over stream elements
     * @param transform Formula to apply to each element (can use property())
     */
    def map[B](transform: Formula[F, B]): Formula[F, B] =
      Formula.Map[F, A, B](formula, transform)

    /**
     * FlatMap over stream elements
     * @param inner Formula to evaluate for each element (can use property() and configRef())
     */
    def flatMap[B](inner: Formula[F, B]): Formula[F, B] =
      Formula.FlatMap[F, A, B](formula, inner)
