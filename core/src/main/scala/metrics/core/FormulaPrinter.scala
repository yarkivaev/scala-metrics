package metrics.core

import java.time.LocalDate

/**
 * FormulaPrinter - converts Formula ADT to readable symbolic expressions
 *
 * Pure function - no side effects, no evaluation
 * Used for displaying formulas in human-readable format
 */
object FormulaPrinter:

  /**
   * Print a formula with metric name substitution
   *
   * Checks if the formula corresponds to a registered metric and uses the metric name instead
   *
   * @param formula The formula to print
   * @param registry Metric registry for looking up metric names
   * @param skipTopLevelMatch If true, skip checking if this formula matches a metric (for top-level)
   * @param parenthesizeOps Whether to add parentheses around binary operations
   * @return Symbolic string representation with metric names
   */
  def printWithRegistry[F[_], A](
    formula: Formula[F, A],
    registry: MetricRegistry,
    skipTopLevelMatch: Boolean = false,
    parenthesizeOps: Boolean = false,
  ): String =
    if !skipTopLevelMatch then
      formula match
        case Formula.Literal(_) =>
          registry.all.find(m => m.formula.asInstanceOf[AnyRef] eq formula.asInstanceOf[AnyRef]) match
            case Some(metric) =>
              return metric.name
            case None         =>
        case _                  =>
          registry.all.find(m => formulaEquals(m.formula, formula)) match
            case Some(metric) =>
              return metric.name
            case None         =>

    formula match
      case Formula.Literal(value) =>
        value.toString

      case Formula.MetricRef(metricName) =>
        metricName

      case Formula.Add(left, right) =>
        val leftStr  = printWithRegistry(left, registry, skipTopLevelMatch = false, parenthesizeOps = true)
        val rightStr = printWithRegistry(right, registry, skipTopLevelMatch = false, parenthesizeOps = true)
        if parenthesizeOps then s"($leftStr + $rightStr)"
        else s"$leftStr + $rightStr"

      case Formula.Subtract(left, right) =>
        val leftStr  = printWithRegistry(left, registry, skipTopLevelMatch = false, parenthesizeOps = true)
        val rightStr = printWithRegistry(right, registry, skipTopLevelMatch = false, parenthesizeOps = true)
        if parenthesizeOps then s"($leftStr - $rightStr)"
        else s"$leftStr - $rightStr"

      case Formula.Multiply(left, right) =>
        val leftStr  = printWithRegistry(left, registry, skipTopLevelMatch = false, parenthesizeOps = true)
        val rightStr = printWithRegistry(right, registry, skipTopLevelMatch = false, parenthesizeOps = true)
        s"$leftStr × $rightStr"

      case Formula.Divide(left, right) =>
        val leftStr  = printWithRegistry(left, registry, skipTopLevelMatch = false, parenthesizeOps = true)
        val rightStr = printWithRegistry(right, registry, skipTopLevelMatch = false, parenthesizeOps = true)
        s"$leftStr / $rightStr"

      case Formula.Sum(source) =>
        s"Sum(${printWithRegistry(source, registry, skipTopLevelMatch = false)})"

      case Formula.Avg(source) =>
        s"Avg(${printWithRegistry(source, registry, skipTopLevelMatch = false)})"

      case Formula.Min(sources) =>
        val sourcesStr = sources.map(printWithRegistry(_, registry, skipTopLevelMatch = false)).mkString(", ")
        s"Min($sourcesStr)"

      case Formula.Max(sources) =>
        val sourcesStr = sources.map(printWithRegistry(_, registry, skipTopLevelMatch = false)).mkString(", ")
        s"Max($sourcesStr)"

      case Formula.Count(source) =>
        s"Count(${printWithRegistry(source, registry, skipTopLevelMatch = false)})"

      case Formula.QueryRef(query) =>
        printQuery(query)

      case Formula.ConfigRef(configName, keyFormula, defaultValue) =>
        val keyStr     = printWithRegistry(keyFormula, registry, skipTopLevelMatch = false)
        val defaultStr = defaultValue.map(v => s", default=$v").getOrElse("")
        s"Config($configName[$keyStr]$defaultStr)"

      case Formula.Property(propertyName) =>
        s".$propertyName"

      case Formula.Map(source, transform) =>
        val sourceStr    = printWithRegistry(source, registry, skipTopLevelMatch = false)
        val transformStr = printWithRegistry(transform, registry, skipTopLevelMatch = false)
        s"$sourceStr.map($transformStr)"

      case Formula.FlatMap(source, inner) =>
        val sourceStr = printWithRegistry(source, registry, skipTopLevelMatch = false)
        val innerStr  = printWithRegistry(inner, registry, skipTopLevelMatch = false)
        s"$sourceStr.flatMap($innerStr)"

      case Formula.Tuple2(first, second) =>
        val firstStr  = printWithRegistry(first, registry, skipTopLevelMatch = false)
        val secondStr = printWithRegistry(second, registry, skipTopLevelMatch = false)
        s"($firstStr, $secondStr)"

      case Formula.When(condition, thenFormula, elseFormula) =>
        val thenStr = printWithRegistry(thenFormula, registry, skipTopLevelMatch = false)
        val elseStr =
          elseFormula.map(f => s" else ${printWithRegistry(f, registry, skipTopLevelMatch = false)}").getOrElse("")
        s"if ${printCondition(condition)} then $thenStr$elseStr"

      case Formula.TimeWindow(source, duration, aggregation) =>
        s"TimeWindow(${printWithRegistry(source, registry, skipTopLevelMatch = false)}, $duration, $aggregation)"

  /**
   * Print a formula as a symbolic expression
   *
   * @param formula The formula to print
   * @param parenthesizeOps Whether to add parentheses around binary operations (default: false)
   * @return Symbolic string representation
   */
  def print[F[_], A](formula: Formula[F, A], parenthesizeOps: Boolean = false): String =
    formula match
      case Formula.Literal(value) =>
        value.toString

      case Formula.MetricRef(metricName) =>
        metricName

      case Formula.Add(left, right) =>
        val leftStr  = print(left, parenthesizeOps = true)
        val rightStr = print(right, parenthesizeOps = true)
        if parenthesizeOps then s"($leftStr + $rightStr)"
        else s"$leftStr + $rightStr"

      case Formula.Subtract(left, right) =>
        val leftStr  = print(left, parenthesizeOps = true)
        val rightStr = print(right, parenthesizeOps = true)
        if parenthesizeOps then s"($leftStr - $rightStr)"
        else s"$leftStr - $rightStr"

      case Formula.Multiply(left, right) =>
        val leftStr  = print(left, parenthesizeOps = true)
        val rightStr = print(right, parenthesizeOps = true)
        s"$leftStr × $rightStr"

      case Formula.Divide(left, right) =>
        val leftStr  = print(left, parenthesizeOps = true)
        val rightStr = print(right, parenthesizeOps = true)
        s"$leftStr / $rightStr"

      case Formula.Sum(source) =>
        s"Sum(${print(source)})"

      case Formula.Avg(source) =>
        s"Avg(${print(source)})"

      case Formula.Min(sources) =>
        val sourcesStr = sources.map(print(_)).mkString(", ")
        s"Min($sourcesStr)"

      case Formula.Max(sources) =>
        val sourcesStr = sources.map(print(_)).mkString(", ")
        s"Max($sourcesStr)"

      case Formula.Count(source) =>
        s"Count(${print(source)})"

      case Formula.QueryRef(query) =>
        printQuery(query)

      case Formula.ConfigRef(configName, keyFormula, defaultValue) =>
        val keyStr     = print(keyFormula)
        val defaultStr = defaultValue.map(v => s", default=$v").getOrElse("")
        s"Config($configName[$keyStr]$defaultStr)"

      case Formula.Property(propertyName) =>
        s".$propertyName"

      case Formula.Map(source, transform) =>
        s"${print(source)}.map(${print(transform)})"

      case Formula.FlatMap(source, inner) =>
        s"${print(source)}.flatMap(${print(inner)})"

      case Formula.Tuple2(first, second) =>
        s"(${print(first)}, ${print(second)})"

      case Formula.When(condition, thenFormula, elseFormula) =>
        val thenStr = print(thenFormula)
        val elseStr = elseFormula.map(f => s" else ${print(f)}").getOrElse("")
        s"if ${printCondition(condition)} then $thenStr$elseStr"

      case Formula.TimeWindow(source, duration, aggregation) =>
        s"TimeWindow(${print(source)}, $duration, $aggregation)"

  /**
   * Print a Query in readable format
   */
  private def printQuery(query: Query): String =
    query match
      case Query.ByName(metricName) =>
        s"Query($metricName)"

      case Query.WithTimeRange(metricName, from, to) =>
        s"Query($metricName, from=$from, to=$to)"

      case Query.WithFilters(metricName, filters) =>
        val filtersStr = filters.map {
          case (k, v: LocalDate) => s"$k=${v.toString}"
          case (k, v)            => s"$k=$v"
        }.mkString(", ")
        s"Query($metricName, filters: $filtersStr)"

      case Query.Custom(metricName, params) =>
        val table       = params.get("table")
        val column      = params.get("column")
        val otherParams = params - "table" - "column"

        val tableCol = (table, column) match
          case (Some(t), Some(c)) => s"table=$t, column=$c"
          case (Some(t), None)    => s"table=$t"
          case (None, Some(c))    => s"column=$c"
          case _                  => ""

        val filtersStr = if otherParams.nonEmpty then
          val filters = otherParams.map {
            case (k, v: LocalDate) => s"$k=${v.toString}"
            case (k, v)            => s"$k=$v"
          }.mkString(", ")
          if tableCol.nonEmpty then s", $filters" else filters
        else ""

        s"Query($metricName, $tableCol$filtersStr)"

      case Query.Aggregated(metricName, aggregation, groupBy) =>
        val groupByStr = groupBy.map(g => s", groupBy=$g").getOrElse("")
        s"Query($metricName, agg=$aggregation$groupByStr)"

  /**
   * Print a Condition in readable format
   */
  private def printCondition[F[_], A](condition: Condition[F, A]): String =
    condition match
      case Condition.GreaterThan(left, right) =>
        s"${print(left)} > ${print(right)}"

      case Condition.LessThan(left, right) =>
        s"${print(left)} < ${print(right)}"

      case Condition.Equals(left, right) =>
        s"${print(left)} == ${print(right)}"

      case Condition.And(conditions) =>
        conditions.map(printCondition(_)).mkString("(", " && ", ")")

      case Condition.Or(conditions) =>
        conditions.map(printCondition(_)).mkString("(", " || ", ")")

      case Condition.Not(cond) =>
        s"!${printCondition(cond)}"

  /**
   * Print formula with metric name prefix
   * Example: "КИО = Working Time / Available Time × 100"
   */
  def printWithName[F[_], A](metricName: String, formula: Formula[F, A]): String =
    s"$metricName = ${print(formula)}"

  /**
   * Print a more compact query representation for inline display
   */
  def printQueryCompact(query: Query): String =
    query match
      case Query.ByName(metricName) =>
        metricName

      case Query.Custom(metricName, params) if params.contains("table") && params.contains("column") =>
        metricName

      case _ =>
        query.metricName

  /**
   * Check if two formulas are structurally equal
   * Used to match formulas against registered metrics
   */
  private def formulaEquals[F[_], A](f1: Any, f2: Any): Boolean =
    (f1, f2) match
      case (a, b) if a.asInstanceOf[AnyRef] eq b.asInstanceOf[AnyRef] => true
      case _                                                          => f1 == f2
