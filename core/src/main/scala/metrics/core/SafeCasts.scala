package metrics.core

import java.time.Instant

import scala.reflect.ClassTag

import fs2.Stream

/**
 * SafeCasts - Type casting utilities for the metrics system
 *
 * This object contains runtime type validation for casting formulas
 * from existential types to concrete types.
 *
 * Why we need type casts:
 * 1. Metrics are stored with existential types Formula[?, ?] in the registry
 * 2. JVM type erasure prevents runtime type distinction
 * 3. The compiler cannot prove type relationships across dynamic lookup
 *
 * All casts are validated at runtime using ClassTag before casting.
 */
object SafeCasts:

  /**
   * Type-safe formula cast with mandatory runtime validation
   *
   * WHY THIS CAST IS NECESSARY:
   * - Metrics are stored with existential types Formula[?, ?] in the registry
   * - At evaluation time, we need concrete types Formula[F, A]
   * - The type system cannot prove the relationship at compile time
   * - String-based lookup loses type information
   *
   * SAFETY GUARANTEES:
   * - Type validation is automatic and mandatory
   * - Returns Either to enforce error handling
   * - Impossible to forget validation at call sites
   * - Only casts after successful type match
   *
   * @tparam F Effect type
   * @tparam A Value type (ClassTag required for validation)
   * @param formula The formula with existential types
   * @param storedType The ClassTag stored with the metric
   * @return Either TypeError or validated formula
   *
   * Example:
   * {{{
   *   SafeCasts.castFormula[IO, Double](formula, metric.valueType) match
   *     case Right(typedFormula) => // Type validated, safe to use
   *     case Left(TypeError(expected, actual)) => // Type mismatch caught
   * }}}
   */
  def castFormula[F[_], A: ClassTag](
    formula: Formula[?, ?],
    storedType: ClassTag[?],
  ): Either[TypeError, Formula[F, A]] =
    val requestedType = summon[ClassTag[A]]

    if storedType.runtimeClass == requestedType.runtimeClass then Right(formula.asInstanceOf[Formula[F, A]])
    else
      Left(
        TypeError(
          expected = storedType.runtimeClass.getName,
          actual = requestedType.runtimeClass.getName,
        ),
      )

/**
 * Type error from SafeCasts validation
 *
 * Indicates that a cast was attempted with incompatible types.
 * Used by SafeCasts.castFormula when runtime type validation fails.
 *
 * @param expected The expected type (stored in metric)
 * @param actual The actual type (requested by caller)
 */
case class TypeError(expected: String, actual: String)
