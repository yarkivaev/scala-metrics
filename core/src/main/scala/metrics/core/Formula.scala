package metrics.core

import java.time.{Duration, Instant}

import fs2.Stream

/**
 * Abstract Syntax Tree for formulas - now generic over effect type F and value type A
 *
 * Formula is a pure AST - no evaluation logic.
 * Evaluation is handled by FormulaAlgebra interpreters.
 *
 * Type parameters:
 * - F[_]: Effect type (IO, cats.effect.IO, etc.)
 * - A: Value type (Double, Int, String, etc.)
 */
sealed trait Formula[F[_], A]

object Formula:

  /**
   * Literal value embedded directly in the formula
   *
   * Represents a constant value of type A that is part of the formula AST.
   * Commonly used for constants in calculations (e.g., 100.0, 24, etc.)
   *
   * @param value The constant value to embed
   * @tparam F Effect type for evaluation
   * @tparam A Value type
   *
   * Example: Literal(100.0) represents the constant 100.0
   */
  case class Literal[F[_], A](value: A) extends Formula[F, A]

  /**
   * Reference to another metric by name
   *
   * Creates a dependency on another metric in the registry. The referenced metric
   * will be evaluated first, and its result stream will be used as input to this formula.
   * This enables composition of metrics and reuse of calculations.
   *
   * @param metricName The name of the metric to reference
   * @tparam F Effect type for evaluation
   * @tparam A Value type
   *
   * Example: MetricRef("ProductionVolume") references a metric named "ProductionVolume"
   */
  case class MetricRef[F[_], A](metricName: String) extends Formula[F, A]

  /**
   * Addition of two formulas
   *
   * Performs element-wise addition of two streams. Both formulas are evaluated
   * and their results are zipped together and added.
   *
   * @param left First operand formula
   * @param right Second operand formula
   * @tparam F Effect type for evaluation
   * @tparam A Value type (must support addition)
   *
   * Example: Add(MetricRef("A"), MetricRef("B")) computes A + B
   */
  case class Add[F[_], A](left: Formula[F, A], right: Formula[F, A]) extends Formula[F, A]

  /**
   * Subtraction of two formulas
   *
   * Performs element-wise subtraction of two streams. Both formulas are evaluated
   * and their results are zipped together, with right subtracted from left.
   *
   * @param left Minuend formula
   * @param right Subtrahend formula
   * @tparam F Effect type for evaluation
   * @tparam A Value type (must support subtraction)
   *
   * Example: Subtract(MetricRef("Total"), MetricRef("Waste")) computes Total - Waste
   */
  case class Subtract[F[_], A](left: Formula[F, A], right: Formula[F, A]) extends Formula[F, A]

  /**
   * Multiplication of two formulas
   *
   * Performs element-wise multiplication of two streams. Both formulas are evaluated
   * and their results are zipped together and multiplied.
   *
   * @param left First factor formula
   * @param right Second factor formula
   * @tparam F Effect type for evaluation
   * @tparam A Value type (must support multiplication)
   *
   * Example: Multiply(MetricRef("Price"), MetricRef("Quantity")) computes Price * Quantity
   */
  case class Multiply[F[_], A](left: Formula[F, A], right: Formula[F, A]) extends Formula[F, A]

  /**
   * Division of two formulas
   *
   * Performs element-wise division of two streams. Both formulas are evaluated
   * and their results are zipped together, with left divided by right.
   * Division by zero handling depends on the interpreter implementation.
   *
   * @param left Dividend formula
   * @param right Divisor formula
   * @tparam F Effect type for evaluation
   * @tparam A Value type (must support division)
   *
   * Example: Divide(MetricRef("Total"), MetricRef("Count")) computes Total / Count
   */
  case class Divide[F[_], A](left: Formula[F, A], right: Formula[F, A]) extends Formula[F, A]

  /**
   * Sum aggregation over a stream
   *
   * Aggregates all values from the source formula stream into a single sum value.
   * Evaluates the source formula and then sums all emitted values.
   *
   * @param source Formula producing the stream to sum
   * @tparam F Effect type for evaluation
   * @tparam A Value type (must support addition and have a zero value)
   *
   * Example: Sum(QueryRef("ProductionByDay")) computes total production across all days
   */
  case class Sum[F[_], A](source: Formula[F, A]) extends Formula[F, A]

  /**
   * Average aggregation over a stream
   *
   * Aggregates all values from the source formula stream into a single average value.
   * Evaluates the source formula, sums all values, and divides by the count.
   *
   * @param source Formula producing the stream to average
   * @tparam F Effect type for evaluation
   * @tparam A Value type (must support addition, division, and have a zero value)
   *
   * Example: Avg(QueryRef("DailyTemperature")) computes average temperature across all readings
   */
  case class Avg[F[_], A](source: Formula[F, A]) extends Formula[F, A]

  /**
   * Minimum value across multiple formulas
   *
   * Evaluates all source formulas and returns the minimum value among them.
   * All formulas are evaluated and their results are compared element-wise.
   *
   * @param sources List of formulas to compare
   * @tparam F Effect type for evaluation
   * @tparam A Value type (must support comparison)
   *
   * Example: Min(List(MetricRef("A"), MetricRef("B"), Literal(0.0))) computes min(A, B, 0.0)
   */
  case class Min[F[_], A](sources: List[Formula[F, A]]) extends Formula[F, A]

  /**
   * Maximum value across multiple formulas
   *
   * Evaluates all source formulas and returns the maximum value among them.
   * All formulas are evaluated and their results are compared element-wise.
   *
   * @param sources List of formulas to compare
   * @tparam F Effect type for evaluation
   * @tparam A Value type (must support comparison)
   *
   * Example: Max(List(MetricRef("TargetRate"), MetricRef("ActualRate"))) computes max(Target, Actual)
   */
  case class Max[F[_], A](sources: List[Formula[F, A]]) extends Formula[F, A]

  /**
   * Count aggregation over a stream
   *
   * Counts the number of elements emitted by the source formula stream.
   * Evaluates the source formula and returns the count as type A.
   *
   * @param source Formula producing the stream to count
   * @tparam F Effect type for evaluation
   * @tparam A Value type (must have Fractional instance to convert from Int)
   *
   * Example: Count(QueryRef("Defects")) counts the number of defect records
   * Note: Result is converted to type A using Fractional.fromInt
   */
  case class Count[F[_], A](source: Formula[F, A]) extends Formula[F, A]

  /**
   * Direct datasource query reference - serializable query specification
   * Example: QueryRef(Query.ByName("DowntimeHours"))
   * Example: QueryRef(Query.WithFilters("DowntimeHours", Map("equipment" -> "ЦМ1")))
   * Example: QueryRef(Query.Custom("downtimes", Map("column" -> "hours")))
   *
   * This is the primary way to reference data from datasources.
   * The query is resolved at evaluation time using the datasource from context.
   * Unlike StreamRef, this is serializable.
   */
  case class QueryRef[F[_], A](query: Query) extends Formula[F, A]

  /**
   * Configuration data lookup reference - static (non-time-series) data
   *
   * References static reference data from ConfigurationSource (e.g., lookup tables).
   * Unlike QueryRef which returns time-series streams, ConfigRef returns a single
   * static value based on the provided key.
   *
   * @param configName Name of the configuration source to query
   * @param key Formula that evaluates to the lookup key
   * @param defaultValue Optional default value if key not found
   * @tparam F Effect type for evaluation
   * @tparam A Value type returned by the lookup
   * @tparam K Key type used for the lookup
   *
   * Examples:
   * - ConfigRef("pipe_weights", Literal(700), Some(85.0)) looks up weight for diameter 700mm
   * - ConfigRef("efficiency", Tuple2(machine, diameter), None) looks up efficiency by (machine, diameter)
   *
   * The configName must match a key in the EvaluationContext's configSources map.
   * The key formula is evaluated first to determine what to look up.
   * If the key is not found and no defaultValue is provided, evaluation fails.
   */
  case class ConfigRef[F[_], A, K](
    configName: String,
    key: Formula[F, K],
    defaultValue: Option[A] = None,
  ) extends Formula[F, A]

  /**
   * Property - Extract field value from current item in stream context
   *
   * Used inside Map/FlatMap to access fields of typed query results.
   * Works with any case class using reflection at evaluation time.
   *
   * @param propertyName Name of the field to extract
   * @tparam F Effect type for evaluation
   * @tparam A Value type of the property
   *
   * Example: Property("diameter") extracts diameter field from ProductionPlan
   * Note: Only valid inside Map/FlatMap context, raises error otherwise
   */
  case class Property[F[_], A](propertyName: String) extends Formula[F, A]

  /**
   * Map - Transform each element in a stream using a formula
   *
   * Applies a transformation formula to each item in the source stream.
   * The transform formula can use Property to access fields from the current item.
   * This enables row-level transformations before aggregation.
   *
   * @param source Formula producing the stream of items
   * @param transform Formula to apply to each item (can use Property)
   * @tparam F Effect type for evaluation
   * @tparam A Type of items in source stream
   * @tparam B Type of items in result stream
   *
   * Example: Map(productionQuery, Property("weightTons") * Literal(1000))
   * Multiplies each weightTons value by 1000
   */
  case class Map[F[_], A, B](
    source: Formula[F, A],
    transform: Formula[F, B],
  ) extends Formula[F, B]

  /**
   * FlatMap - For each element, evaluate an inner formula
   *
   * For each item in the source stream, sets the item as context and evaluates
   * the inner formula. The inner formula can use Property to access source item
   * fields and ConfigRef to join with configuration data.
   * This is the key operation for implementing joins.
   *
   * @param source Formula producing the stream of items
   * @param inner Formula to evaluate for each item (can use Property + ConfigRef)
   * @tparam F Effect type for evaluation
   * @tparam A Type of items in source stream
   * @tparam B Type of values produced by inner formula
   *
   * Example: FlatMap(productionQuery,
   *            Property("weight") / ConfigRef("pipe_weights", Property("diameter")))
   * For each production record, divides weight by pipe weight looked up by diameter
   */
  case class FlatMap[F[_], A, B](
    source: Formula[F, A],
    inner: Formula[F, B],
  ) extends Formula[F, B]

  /**
   * Tuple2 - Combine two formulas into a tuple
   *
   * Evaluates both formulas and combines their results into a tuple.
   * Useful for creating composite keys for ConfigRef lookups.
   *
   * @param first First formula
   * @param second Second formula
   * @tparam F Effect type for evaluation
   * @tparam A Type of first formula result
   * @tparam B Type of second formula result
   *
   * Example: Tuple2(Literal(7), Property("diameter"))
   * Creates tuple (7, diameter) for composite config key
   */
  case class Tuple2[F[_], A, B](
    first: Formula[F, A],
    second: Formula[F, B],
  ) extends Formula[F, (A, B)]

  /**
   * Conditional expression (if-then-else)
   *
   * Evaluates the condition and returns values from thenFormula if true,
   * or from elseFormula if false (or filters out if elseFormula is None).
   * Enables conditional logic within formulas.
   *
   * @param condition The condition to evaluate
   * @param thenFormula Formula to evaluate when condition is true
   * @param elseFormula Optional formula to evaluate when condition is false
   * @tparam F Effect type for evaluation
   * @tparam A Value type
   *
   * Example: When(GreaterThan(MetricRef("Temp"), Literal(100)), Literal(1.0), Some(Literal(0.0)))
   * Returns 1.0 when temperature > 100, otherwise 0.0
   */
  case class When[F[_], A](
    condition: Condition[F, A],
    thenFormula: Formula[F, A],
    elseFormula: Option[Formula[F, A]] = None,
  ) extends Formula[F, A]

  /**
   * Time-based windowing aggregation
   *
   * Applies a sliding or tumbling time window over the source stream and aggregates
   * values within each window using the specified aggregation type.
   * Useful for computing metrics over fixed time periods.
   *
   * @param source Formula producing the time-series stream
   * @param duration Size of the time window
   * @param aggregation Type of aggregation to apply (Sum, Avg, Min, Max, Count)
   * @tparam F Effect type for evaluation
   * @tparam A Value type
   *
   * Example: TimeWindow(QueryRef("HourlyProduction"), Duration.ofDays(1), AggregationType.Sum)
   * Computes daily production totals from hourly data
   */
  case class TimeWindow[F[_], A](
    source: Formula[F, A],
    duration: Duration,
    aggregation: AggregationType,
  ) extends Formula[F, A]

/**
 * Conditions for conditional expressions
 *
 * Represents boolean conditions that can be used in When formulas.
 * Pure AST - evaluation handled by interpreters.
 * Conditions operate on formulas of type A and produce Boolean results.
 *
 * Note: Although Condition doesn't extend Formula, it always evaluates to Boolean.
 * The type parameter A represents the type of values being compared, not the result type.
 *
 * @tparam F Effect type for evaluation
 * @tparam A Value type of the formulas being compared (not the result type, which is always Boolean)
 */
sealed trait Condition[F[_], A]

object Condition:
  /**
   * Greater than comparison
   *
   * Evaluates to true when left formula produces values greater than right formula.
   * Performs element-wise comparison for streams.
   *
   * @param left Left-hand side formula
   * @param right Right-hand side formula
   * @tparam F Effect type for evaluation
   * @tparam A Value type (must support comparison)
   *
   * Example: GreaterThan(MetricRef("Temperature"), Literal(100.0)) checks if temperature > 100
   */
  case class GreaterThan[F[_], A](left: Formula[F, A], right: Formula[F, A]) extends Condition[F, A]

  /**
   * Less than comparison
   *
   * Evaluates to true when left formula produces values less than right formula.
   * Performs element-wise comparison for streams.
   *
   * @param left Left-hand side formula
   * @param right Right-hand side formula
   * @tparam F Effect type for evaluation
   * @tparam A Value type (must support comparison)
   *
   * Example: LessThan(MetricRef("DefectRate"), Literal(0.05)) checks if defect rate < 5%
   */
  case class LessThan[F[_], A](left: Formula[F, A], right: Formula[F, A]) extends Condition[F, A]

  /**
   * Equality comparison
   *
   * Evaluates to true when left formula produces values equal to right formula.
   * Performs element-wise comparison for streams.
   *
   * @param left Left-hand side formula
   * @param right Right-hand side formula
   * @tparam F Effect type for evaluation
   * @tparam A Value type (must support equality comparison)
   *
   * Example: Equals(MetricRef("Status"), Literal("Active")) checks if status equals "Active"
   */
  case class Equals[F[_], A](left: Formula[F, A], right: Formula[F, A]) extends Condition[F, A]

  /**
   * Logical AND of multiple conditions
   *
   * Evaluates to true only when all conditions in the list are true.
   * Short-circuits evaluation if any condition is false.
   *
   * @param conditions List of conditions to combine with AND
   * @tparam F Effect type for evaluation
   * @tparam A Value type
   *
   * Example: And(List(GreaterThan(temp, low), LessThan(temp, high)))
   * Checks if temperature is within range [low, high]
   */
  case class And[F[_], A](conditions: List[Condition[F, A]]) extends Condition[F, A]

  /**
   * Logical OR of multiple conditions
   *
   * Evaluates to true when at least one condition in the list is true.
   * Short-circuits evaluation if any condition is true.
   *
   * @param conditions List of conditions to combine with OR
   * @tparam F Effect type for evaluation
   * @tparam A Value type
   *
   * Example: Or(List(Equals(status, "Error"), Equals(status, "Warning")))
   * Checks if status is either Error or Warning
   */
  case class Or[F[_], A](conditions: List[Condition[F, A]]) extends Condition[F, A]

  /**
   * Logical NOT (negation)
   *
   * Evaluates to true when the wrapped condition is false, and vice versa.
   * Inverts the boolean result of the condition.
   *
   * @param condition The condition to negate
   * @tparam F Effect type for evaluation
   * @tparam A Value type
   *
   * Example: Not(Equals(status, "Inactive")) checks if status is not "Inactive"
   */
  case class Not[F[_], A](condition: Condition[F, A]) extends Condition[F, A]
