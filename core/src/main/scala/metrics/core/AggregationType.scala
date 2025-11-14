package metrics.core

/**
 * Aggregation types for time series and data aggregation
 */
enum AggregationType:
  case Sum
  case Avg
  case Min
  case Max
  case Count
