package metrics.algebra

import java.time.Instant

import cats.effect.kernel.Sync
import fs2.Stream

import metrics.core.{Formula, MetricRegistry, Query, TimeRange}

/**
 * FormulaAlgebra - Tagless Final interface for formula evaluation
 *
 * This separates:
 * - WHAT to compute (Formula - pure AST)
 * - WHERE to compute (Interpreter - local vs remote)
 * - HOW to get data (DataSource[F, A])
 *
 * Generic over result type A (Double, Int, String, etc.)
 *
 * Formula evaluation returns Stream[F, (Instant, A)] to preserve time-series data.
 * Aggregation happens at the call site, not in the interpreter.
 */
trait FormulaAlgebra[F[_], A]:
  /**
   * Evaluate a formula in the given context for a specific time range
   *
   * @param formula The formula AST to evaluate
   * @param context The evaluation context (data sources, registry)
   * @param timeRange The time range to evaluate the formula for
   * @return Stream of timestamped values within the time range
   */
  def evaluate(formula: Formula[F, A], context: EvaluationContext[F, A], timeRange: TimeRange): Stream[F, (Instant, A)]

/**
 * Evaluation Context - provides access to data and metrics
 *
 * This is the "environment" in which formulas are evaluated.
 * It's polymorphic in F and A to support different execution strategies and result types.
 *
 * Uses a registry of typed datasources to support queries returning different types
 * (e.g., Double, ProductionPlan, CalendarTimePlan).
 *
 * Maintains an item context stack for Map/FlatMap operations to provide access
 * to the current item being processed via Formula.Property.
 */
class EvaluationContext[F[_]: Sync, A](
  val dataSources: Map[String, Any],
  val registry: MetricRegistry,
  val configSources: Map[String, ConfigurationSource[F, _, _]] =
    Map.empty[String, ConfigurationSource[F, Nothing, Nothing]],
):
  private val itemContextStack: scala.collection.mutable.Stack[Any] = scala.collection.mutable.Stack()
  private val stackLock                                             = new Object()

  /**
   * Push an item onto the context stack (used by Map/FlatMap)
   * Thread-safe operation using synchronized block
   */
  def pushItemContext(item: Any): Unit = stackLock.synchronized {
    itemContextStack.push(item)
  }

  /**
   * Pop an item from the context stack (used by Map/FlatMap cleanup)
   * Thread-safe operation using synchronized block
   */
  def popItemContext(): Unit = stackLock.synchronized {
    if itemContextStack.nonEmpty then itemContextStack.pop()
  }

  /**
   * Get the current item context (used by Property access)
   * Thread-safe operation using synchronized block
   */
  def currentItemContext: Option[Any] = stackLock.synchronized {
    itemContextStack.headOption
  }

  /**
   * Get a metric by name from the registry
   */
  def getMetric(name: String): Option[metrics.core.Metric] =
    registry.get(name)

  /**
   * Query data from the appropriate typed datasource for a specific time range
   *
   * Looks up the datasource by query name/table and delegates to it.
   *
   * @param q The query to execute
   * @param timeRange The time range to filter results
   * @return Stream of timestamped values within the time range
   */
  def query(q: Query, timeRange: TimeRange): Stream[F, (Instant, A)] =
    val datasourceName = q match
      case Query.ByName(name)              => name
      case Query.WithFilters(name, _)      => name
      case Query.Custom(name, _)           => name
      case Query.WithTimeRange(name, _, _) => name
      case Query.Aggregated(name, _, _)    => name

    dataSources.get(datasourceName) match
      case Some(ds) =>
        // Cast to the appropriate datasource type
        // This is safe because the query name determines the expected type
        ds.asInstanceOf[DataSource[F, A]].get(q, timeRange)
      case None     =>
        // Fall back to "default" datasource if available
        dataSources.get("default") match
          case Some(ds) =>
            ds.asInstanceOf[DataSource[F, A]].get(q, timeRange)
          case None     =>
            Stream.empty

  /**
   * Helper: Query for a single value by metric name within time range
   *
   * @param metricName The name of the metric
   * @param timeRange The time range to filter results
   * @return Stream of timestamped values within the time range
   */
  def queryByName(metricName: String, timeRange: TimeRange): Stream[F, (Instant, A)] =
    query(Query.ByName(metricName), timeRange)

  /**
   * Helper: Query with filters for dimensional metrics within time range
   *
   * @param metricName The name of the metric
   * @param filters Additional filters for the query
   * @param timeRange The time range to filter results
   * @return Stream of timestamped values within the time range
   */
  def queryWithFilters(metricName: String, filters: Map[String, Any], timeRange: TimeRange): Stream[F, (Instant, A)] =
    query(Query.WithFilters(metricName, filters), timeRange)

  /**
   * Get a configuration value by name and key
   *
   * Looks up a static configuration value from the named ConfigurationSource.
   * Used by ConfigRef formula evaluation to retrieve reference data.
   *
   * @param configName The name of the configuration source
   * @param key The lookup key
   * @tparam K Key type
   * @tparam V Value type
   * @return Effect containing the optional value
   */
  def getConfig[K, V](configName: String, key: K): F[Option[V]] =
    configSources.get(configName) match
      case Some(source) =>
        // Type erasure means we need to cast, but this is safe because
        // ConfigRef[F, A, K] ensures type consistency at the call site
        source.asInstanceOf[ConfigurationSource[F, K, V]].get(key)
      case None         =>
        Sync[F].pure(None)
