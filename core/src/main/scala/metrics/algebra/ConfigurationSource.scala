package metrics.algebra

import cats.effect.Sync

/**
 * ConfigurationSource - Source of static reference/configuration data
 *
 * Unlike DataSource which provides time-series data, ConfigurationSource
 * provides static lookup tables that don't change over time.
 *
 * Examples:
 * - Pipe diameter → mean weight mapping
 * - (Machine, Diameter) → efficiency mapping
 * - Equipment codes → equipment names
 *
 * @tparam F Effect type
 * @tparam K Key type (can be tuple for composite keys)
 * @tparam V Value type (can be tuple for multiple values)
 */
trait ConfigurationSource[F[_], K, V]:
  /**
   * Get value by key
   */
  def get(key: K): F[Option[V]]

  /**
   * Get all entries as a Map
   */
  def getAll: F[Map[K, V]]

  /**
   * Check if key exists
   */
  def contains(key: K)(using F: Sync[F]): F[Boolean] =
    F.map(get(key))(_.isDefined)
