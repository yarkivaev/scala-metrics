package metrics.interpreters

import java.time.{Duration, Instant}

import scala.reflect.ClassTag

import cats.effect.kernel.Sync
import cats.syntax.all._
import fs2.Stream

import metrics.algebra.{EvaluationContext, FormulaAlgebra}
import metrics.core.{AggregationType, Condition, Formula, Query, SafeCasts, TimeRange}

/**
 * LocalFormulaInterpreter - evaluates formulas in-memory returning streams
 *
 * This interpreter:
 * - Evaluates formulas locally (not sent to remote system)
 * - Returns Stream[F, (Instant, A)] preserving time-series data
 * - Aggregation happens at call site, not in interpreter
 * - Generic over value type A (Double, Int, Long, etc.)
 * - Requires Fractional[A] for arithmetic operations (includes division)
 */
class LocalFormulaInterpreter[F[_]: Sync, A: Fractional: Ordering: ClassTag] extends FormulaAlgebra[F, A]:

  private val num = summon[Fractional[A]]
  import num.*

  /**
   * Helper method to evaluate formulas with arbitrary type parameters
   * Used for Map/FlatMap/Tuple2/ConfigRef where we need to evaluate formulas of different types
   * Note: Does not require ClassTag since we handle types dynamically
   */
  private def evaluateTyped[B](
    formula: Formula[F, B],
    context: EvaluationContext[F, A],
    timeRange: TimeRange,
  ): Stream[F, (Instant, B)] =
    formula match
      case Formula.Literal(value) =>
        Stream.emit((timeRange.from, value))

      case Formula.QueryRef(query) =>
        // Query typed datasource - the datasource must return Stream[F, (Instant, B)]
        // We need to cast the context to access the datasource with type B
        context.asInstanceOf[EvaluationContext[F, B]].query(query, timeRange)

      case Formula.Property(propertyName) =>
        context.currentItemContext match {
          case Some(item) =>
            val value = PropertyExtractor.extract[B](item, propertyName)
            Stream.emit((timeRange.from, value))
          case None       =>
            Stream.raiseError[F](
              new RuntimeException(
                s"Property '$propertyName' accessed outside Map/FlatMap context",
              ),
            )
        }

      case t2: (Formula.Tuple2[F, ?, ?] @unchecked) =>
        // Handle Tuple2 with type erasure
        Stream
          .eval(
            for {
              f <- evaluateTyped(t2.first.asInstanceOf[Formula[F, Any]], context, timeRange).compile.last
              s <- evaluateTyped(t2.second.asInstanceOf[Formula[F, Any]], context, timeRange).compile.last
            } yield (f, s),
          )
          .flatMap {
            case (Some((t1, v1)), Some((_, v2))) =>
              Stream.emit((t1, (v1, v2).asInstanceOf[B]))
            case _                               =>
              Stream.empty
          }

      case _ =>
        // For other formula types, we can't handle them here without the proper Fractional/Ordering instances
        Stream.raiseError[F](
          new RuntimeException(s"Cannot evaluate formula type in typed context: ${formula.getClass.getSimpleName}"),
        )

  def evaluate(
    formula: Formula[F, A],
    context: EvaluationContext[F, A],
    timeRange: TimeRange,
  ): Stream[F, (Instant, A)] =
    formula match
      case Formula.Literal(value) =>
        Stream.emit((timeRange.from, value))

      case Formula.MetricRef(metricName) =>
        context.getMetric(metricName) match
          case Some(metric) =>
            SafeCasts.castFormula[F, A](metric.formula, metric.valueType) match
              case Right(typedFormula) =>
                evaluate(typedFormula, context, timeRange)
              case Left(_)             =>
                Stream.empty
          case None         =>
            Stream.empty

      case Formula.Add(left, right) =>
        evalBinaryOpStream(left, right, plus, context, timeRange)

      case Formula.Subtract(left, right) =>
        evalBinaryOpStream(left, right, minus, context, timeRange)

      case Formula.Multiply(left, right) =>
        evalBinaryOpStream(left, right, times, context, timeRange)

      case Formula.Divide(left, right) =>
        evalBinaryOpStream(
          left,
          right,
          (l, r) =>
            if num.toDouble(r) == 0.0 then throw new ArithmeticException("Division by zero")
            else div(l, r),
          context,
          timeRange,
        )

      case Formula.Sum(source) =>
        evalAggregation(source, AggregationType.Sum, context, timeRange)

      case Formula.Avg(source) =>
        evalAggregation(source, AggregationType.Avg, context, timeRange)

      case Formula.Min(sources) =>
        evalMultiMin(sources, context, timeRange)

      case Formula.Max(sources) =>
        evalMultiMax(sources, context, timeRange)

      case Formula.Count(source) =>
        val sourceStream = evaluate(source, context, timeRange)
        val timestamp    = timeRange.from
        Stream.eval(
          sourceStream.compile.toList.map(values => (timestamp, num.fromInt(values.size))),
        )

      case Formula.QueryRef(query) =>
        context.query(query, timeRange)

      case Formula.ConfigRef(configName, keyFormula, defaultValue) =>
        // Evaluate the key formula to get the lookup key
        // Supports Literal keys and Property keys (when in Map/FlatMap context)
        keyFormula match
          case Formula.Literal(keyValue)                  =>
            Stream
              .eval(
                context.getConfig[Any, A](configName, keyValue),
              )
              .flatMap {
                case Some(value) =>
                  Stream.emit((timeRange.from, value))
                case None        =>
                  defaultValue match
                    case Some(default) =>
                      Stream.emit((timeRange.from, default))
                    case None          =>
                      Stream.raiseError[F](
                        new RuntimeException(
                          s"Config key '$keyValue' not found in '$configName' and no default value provided",
                        ),
                      )
              }
          case Formula.Property(_) | Formula.Tuple2(_, _) =>
            // Property/Tuple2 keys are evaluated in context (for Map/FlatMap joins)
            Stream
              .eval(
                evaluateTyped(keyFormula, context, timeRange).compile.last,
              )
              .flatMap {
                case Some((_, keyValue)) =>
                  Stream.eval(context.getConfig[Any, A](configName, keyValue)).flatMap {
                    case Some(value) =>
                      Stream.emit((timeRange.from, value))
                    case None        =>
                      defaultValue match
                        case Some(default) =>
                          Stream.emit((timeRange.from, default))
                        case None          =>
                          Stream.raiseError[F](
                            new RuntimeException(
                              s"Config key '$keyValue' not found in '$configName' and no default value provided",
                            ),
                          )
                  }
                case None                =>
                  Stream.raiseError[F](
                    new RuntimeException(
                      s"Key formula did not produce a value for config lookup",
                    ),
                  )
              }
          case _                                          =>
            Stream.raiseError[F](
              new RuntimeException(
                "ConfigRef only supports Literal, Property, and Tuple2 keys",
              ),
            )

      case Formula.Property(propertyName) =>
        context.currentItemContext match {
          case Some(item) =>
            val value = PropertyExtractor.extract[A](item, propertyName)
            Stream.emit((timeRange.from, value))
          case None       =>
            Stream.raiseError(
              new RuntimeException(
                s"Property '$propertyName' accessed outside Map/FlatMap context",
              ),
            )
        }

      case Formula.Map(source, transform) =>
        // Cast to handle different source type
        val sourceStream = evaluateTyped(source.asInstanceOf[Formula[F, Any]], context, timeRange)
        sourceStream.flatMap {
          case (timestamp, item) =>
            // Push context, evaluate, then pop - all within a single effect
            // IMPORTANT: evaluate() call must be delayed until effect runs
            Stream
              .eval(
                for {
                  _       <- Sync[F].delay(context.pushItemContext(item))
                  results <- evaluate(transform, context, timeRange).compile.toList
                  _       <- Sync[F].delay(context.popItemContext())
                } yield results,
              )
              .flatMap(results =>
                Stream.emits(results).map {
                  case (_, transformedValue) =>
                    (timestamp, transformedValue)
                },
              )
        }

      case Formula.FlatMap(source, inner) =>
        // Cast to handle different source type
        val sourceStream = evaluateTyped(source.asInstanceOf[Formula[F, Any]], context, timeRange)
        sourceStream.flatMap {
          case (_, item) =>
            // Push context, evaluate, then pop - all within a single effect
            // IMPORTANT: evaluate() call must be delayed until effect runs
            Stream
              .eval(
                for {
                  _       <- Sync[F].delay(context.pushItemContext(item))
                  results <- evaluate(inner, context, timeRange).compile.toList
                  _       <- Sync[F].delay(context.popItemContext())
                } yield results,
              )
              .flatMap(results => Stream.emits(results))
        }

      case Formula.Tuple2(first, second) =>
        // This case shouldn't occur in evaluate since Tuple2 produces (A, B) not A
        // It's handled in evaluateTyped instead
        Stream.raiseError[F](
          new RuntimeException(
            "Tuple2 should be evaluated using evaluateTyped, not evaluate",
          ),
        )

      case Formula.When(condition, thenFormula, elseFormula) =>
        Stream.eval(evalCondition(condition, context, timeRange)).flatMap { result =>
          if result then evaluate(thenFormula, context, timeRange)
          else
            elseFormula match
              case Some(f) => evaluate(f, context, timeRange)
              case None    => Stream.empty
        }

      case Formula.TimeWindow(source, duration, aggregation) =>
        evaluate(source, context, timeRange)

  /**
   * Evaluate binary operation on two formula streams
   * Aggregates both streams to single values, applies operation, emits result
   */
  private def evalBinaryOpStream(
    left: Formula[F, A],
    right: Formula[F, A],
    op: (A, A) => A,
    context: EvaluationContext[F, A],
    timeRange: TimeRange,
  ): Stream[F, (Instant, A)] =
    val leftStream  = evaluate(left, context, timeRange)
    val rightStream = evaluate(right, context, timeRange)

    val timestamp = timeRange.from
    Stream
      .eval(
        for {
          leftOpt  <- leftStream.compile.last
          rightOpt <- rightStream.compile.last
        } yield (leftOpt, rightOpt),
      )
      .flatMap {
        case (Some((_, l)), Some((_, r))) =>
          Stream.emit((timestamp, op(l, r)))
        case (Some((_, l)), None)         =>
          Stream.emit((timestamp, op(l, num.zero)))
        case (None, Some((_, r)))         =>
          Stream.emit((timestamp, op(num.zero, r)))
        case _                            =>
          Stream.empty
      }

  /**
   * Aggregate a stream to a single value (takes most recent value)
   */
  private def aggregateStream(stream: Stream[F, (Instant, A)]): F[Option[A]] =
    stream.compile.last.map(_.map(_._2))

  /**
   * Aggregate a stream with a specific aggregation type
   */
  private def aggregateStreamWith(
    stream: Stream[F, (Instant, A)],
    agg: AggregationType,
  ): F[Option[A]] =
    stream.compile.toList.map { values =>
      if values.isEmpty then None
      else Some(applyAggregation(values.map(_._2), agg))
    }

  /**
   * Apply aggregation function to a list of values
   */
  private def applyAggregation(values: List[A], agg: AggregationType): A =
    import num.mkNumericOps
    import num.mkOrderingOps

    agg match
      case AggregationType.Sum   =>
        values.foldLeft(num.zero)((acc, v) => num.plus(acc, v))
      case AggregationType.Avg   =>
        val sum = values.foldLeft(num.zero)((acc, v) => num.plus(acc, v))
        num.div(sum, num.fromInt(values.size))
      case AggregationType.Min   =>
        values.min
      case AggregationType.Max   =>
        values.max
      case AggregationType.Count =>
        num.fromInt(values.size)

  /**
   * Evaluate aggregation on a formula source
   * Returns a single-element stream with the aggregated value
   */
  private def evalAggregation(
    source: Formula[F, A],
    agg: AggregationType,
    context: EvaluationContext[F, A],
    timeRange: TimeRange,
  ): Stream[F, (Instant, A)] =
    val sourceStream = evaluate(source, context, timeRange)
    val timestamp    = timeRange.from
    Stream
      .eval(
        aggregateStreamWith(sourceStream, agg),
      )
      .flatMap {
        case Some(value) => Stream.emit((timestamp, value))
        case None        => Stream.empty
      }

  /**
   * Evaluate min of multiple formulas
   * Returns a single-element stream with the minimum value
   */
  private def evalMultiMin(
    sources: List[Formula[F, A]],
    context: EvaluationContext[F, A],
    timeRange: TimeRange,
  ): Stream[F, (Instant, A)] =
    Stream
      .eval(
        for {
          values    <- sources.traverse(f => evaluate(f, context, timeRange).compile.last)
          timestamp <- Sync[F].realTimeInstant
        } yield (values.flatten.map(_._2), timestamp),
      )
      .flatMap {
        case (values, ts) if values.nonEmpty =>
          Stream.emit((ts, values.min))
        case _                               =>
          Stream.raiseError[F](new RuntimeException("No values to compute min"))
      }

  /**
   * Evaluate max of multiple formulas
   * Returns a single-element stream with the maximum value
   */
  private def evalMultiMax(
    sources: List[Formula[F, A]],
    context: EvaluationContext[F, A],
    timeRange: TimeRange,
  ): Stream[F, (Instant, A)] =
    Stream
      .eval(
        for {
          values    <- sources.traverse(f => evaluate(f, context, timeRange).compile.last)
          timestamp <- Sync[F].realTimeInstant
        } yield (values.flatten.map(_._2), timestamp),
      )
      .flatMap {
        case (values, ts) if values.nonEmpty =>
          Stream.emit((ts, values.max))
        case _                               =>
          Stream.raiseError[F](new RuntimeException("No values to compute max"))
      }

  /**
   * Evaluate a condition to boolean
   */
  private def evalCondition(
    condition: Condition[F, A],
    context: EvaluationContext[F, A],
    timeRange: TimeRange,
  ): F[Boolean] =
    import num.mkOrderingOps

    condition match
      case Condition.GreaterThan(left, right) =>
        for
          leftOpt  <- evaluate(left, context, timeRange).compile.last
          rightOpt <- evaluate(right, context, timeRange).compile.last
        yield (leftOpt, rightOpt) match {
          case (Some((_, l)), Some((_, r))) => l > r
          case _                            => false
        }

      case Condition.LessThan(left, right) =>
        for
          leftOpt  <- evaluate(left, context, timeRange).compile.last
          rightOpt <- evaluate(right, context, timeRange).compile.last
        yield (leftOpt, rightOpt) match {
          case (Some((_, l)), Some((_, r))) => l < r
          case _                            => false
        }

      case Condition.Equals(left, right) =>
        for
          leftOpt  <- evaluate(left, context, timeRange).compile.last
          rightOpt <- evaluate(right, context, timeRange).compile.last
        yield (leftOpt, rightOpt) match {
          case (Some((_, l)), Some((_, r))) => l == r
          case _                            => false
        }

      case Condition.And(conditions) =>
        conditions.traverse(c => evalCondition(c, context, timeRange)).map(_.forall(identity))

      case Condition.Or(conditions) =>
        conditions.traverse(c => evalCondition(c, context, timeRange)).map(_.exists(identity))

      case Condition.Not(cond) =>
        evalCondition(cond, context, timeRange).map(!_)
