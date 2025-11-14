package metrics.datasources

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime}

import scala.util.control.Exception._
import scala.util.{Try, Using}

import com.github.tototoshi.csv.{CSVFormat, CSVReader, defaultCSVFormat}
import fs2.Stream

import metrics.algebra.DataSource
import metrics.core.Query

/**
 * CSV file data source - implements DataSource[F, A] trait
 *
 * Supports:
 * - ByName queries (returns column values as stream)
 * - Custom queries with table.column syntax
 * - Semicolon-delimited CSV files (configurable)
 */
class CSVDataSource[F[_], A](
  basePath: String,
  tableMapping: Map[String, String],
  delimiter: Char = ',',
  valueParser: String => Option[A],
) extends DataSource[F, A]:

  /**
   * Internal table data structure for CSV parsing
   */
  private case class TableData(
    columns: List[String],
    rows: List[Map[String, String]],
  )

  private val cache = scala.collection.mutable.Map.empty[String, TableData]

  def get(query: Query, timeRange: metrics.core.TimeRange): Stream[F, (Instant, A)] =
    query match
      case Query.ByName(metricName) =>
        findMetricInTables(metricName, timeRange)

      case Query.WithFilters(metricName, filters) =>
        findMetricInTablesWithFilters(metricName, filters, timeRange)

      case Query.Custom(metricName, params) =>
        params.get("table") match
          case Some(table: String) =>
            params.get("column") match
              case Some(column: String) =>
                val filters = params.filterNot { case (k, _) => k == "table" || k == "column" }.map {
                  case (k, v) => (k, v.toString)
                }
                loadColumnFromTableWithFilters(table, column, filters, timeRange)
              case _                    =>
                Stream.empty
          case _                   =>
            findMetricInTables(metricName, timeRange)

      case _ =>
        Stream.empty

  override def supports(query: Query): Boolean =
    query match
      case Query.ByName(_)         => true
      case Query.WithFilters(_, _) => true
      case Query.Custom(_, _)      => true
      case _                       => false

  /**
   * Load table data (with caching)
   */
  private def loadTable(tableName: String): Try[TableData] =
    cache.get(tableName) match
      case Some(data) => scala.util.Success(data)
      case None       =>
        tableMapping.get(tableName) match
          case Some(fileName) =>
            val filePath = if basePath.endsWith("/") then s"$basePath$fileName" else s"$basePath/$fileName"
            loadCSV(filePath, hasHeader = true).map { data =>
              cache(tableName) = data
              data
            }
          case None           =>
            scala.util.Failure(new IllegalArgumentException(s"Table '$tableName' not found in mapping"))

  /**
   * Find metric in any available table
   */
  private def findMetricInTables(metricName: String, timeRange: metrics.core.TimeRange): Stream[F, (Instant, A)] =
    tableMapping.keys.toList match
      case Nil    => Stream.empty
      case tables =>
        tables.foldLeft(Stream.empty: Stream[F, (Instant, A)]) { (acc, tableName) =>
          acc ++ loadColumnFromTable(tableName, metricName, timeRange)
        }

  /**
   * Find metric in any available table with filters applied
   */
  private def findMetricInTablesWithFilters(
    metricName: String,
    filters: Map[String, Any],
    timeRange: metrics.core.TimeRange,
  ): Stream[F, (Instant, A)] =
    val stringFilters = filters.map {
      case (k, v: LocalDate) => (k, v.toString)
      case (k, v)            => (k, v.toString)
    }

    tableMapping.keys.toList match
      case Nil    => Stream.empty
      case tables =>
        tables.foldLeft(Stream.empty: Stream[F, (Instant, A)]) { (acc, tableName) =>
          acc ++ loadColumnFromTableWithFilters(tableName, metricName, stringFilters, timeRange)
        }

  /**
   * Load a specific column from a table
   */
  private def loadColumnFromTable(
    tableName: String,
    columnName: String,
    timeRange: metrics.core.TimeRange,
  ): Stream[F, (Instant, A)] =
    loadColumnFromTableWithFilters(tableName, columnName, Map.empty, timeRange)

  /**
   * Load a specific column from a table with optional row filters
   */
  private def loadColumnFromTableWithFilters(
    tableName: String,
    columnName: String,
    filters: Map[String, String],
    timeRange: metrics.core.TimeRange,
  ): Stream[F, (Instant, A)] =
    loadTable(tableName) match
      case scala.util.Success(tableData) =>
        if tableData.columns.contains(columnName) then
          // Use timeRange for filtering
          val startDate      = LocalDate.ofInstant(timeRange.from, java.time.ZoneOffset.UTC)
          val endDate        = LocalDate.ofInstant(timeRange.to, java.time.ZoneOffset.UTC)
          val nonDateFilters = filters - "startDate" - "endDate"

          val filteredRows = tableData.rows.filter { row =>
            val passesNonDateFilters = nonDateFilters.isEmpty || nonDateFilters.forall {
              case (filterColumn, filterValue) =>
                row.get(filterColumn).contains(filterValue)
            }

            // Always filter by timeRange
            val singleDateMatch = row
              .get("date")
              .flatMap(parseDateFromCSV)
              .orElse(row.get("stop_date").flatMap(parseDateFromCSV))
              .orElse(row.get("start_date").flatMap(parseDateFromCSV))
              .map(rowDate => !rowDate.isBefore(startDate) && !rowDate.isAfter(endDate))

            val periodRangeMatch =
              if singleDateMatch.isEmpty then
                row.get("period").flatMap(parsePeriodRange).map {
                  case (periodStart, periodEnd) =>
                    !periodStart.isAfter(endDate) && !periodEnd.isBefore(startDate)
                }
              else None

            val passesDateFilter = singleDateMatch.orElse(periodRangeMatch).getOrElse(true)

            passesNonDateFilters && passesDateFilter
          }

          val valuesWithTimestamps = filteredRows.flatMap { row =>
            val cellValue = row.get(columnName)
            val parsed    = cellValue.flatMap(valueParser)

            val rowTimestamp = row
              .get("date")
              .flatMap(parseDateFromCSV)
              .orElse(row.get("stop_date").flatMap(parseDateFromCSV))
              .orElse(row.get("start_date").flatMap(parseDateFromCSV))
              .orElse {
                row.get("period").flatMap(parsePeriodRange).map(_._1)
              }
              .map(_.atStartOfDay(java.time.ZoneOffset.UTC).toInstant)
              .getOrElse {
                startDate.atStartOfDay(java.time.ZoneOffset.UTC).toInstant
              }

            parsed.map(v => (rowTimestamp, v))
          }

          Stream.emits(valuesWithTimestamps)
        else Stream.empty
      case scala.util.Failure(_)         =>
        Stream.empty

  /**
   * Parse end date from period range string
   * Format: "01.01.2025 - 31.01.2025" → "31.01.2025"
   */
  private def parsePeriodEndDate(periodStr: String): String =
    periodStr.split(" - ").lastOption.getOrElse(periodStr).trim

  /**
   * Parse start and end dates from period range string
   * Format: "01.08.2025 - 31.08.2025" → (LocalDate(2025-08-01), LocalDate(2025-08-31))
   */
  private def parsePeriodRange(periodStr: String): Option[(LocalDate, LocalDate)] =
    val parts = periodStr.split(" - ").map(_.trim)
    if parts.length == 2 then
      for
        start <- parseDateFromCSV(parts(0))
        end   <- parseDateFromCSV(parts(1))
      yield (start, end)
    else None

  /**
   * Parse LocalDate from string (format: dd.MM.yyyy)
   */
  private def parseLocalDate(dateStr: String): Option[LocalDate] =
    allCatch.opt(LocalDate.parse(dateStr))

  /**
   * Parse date from CSV (format: dd.MM.yyyy HH:mm:ss or dd.MM.yyyy)
   * Supports both single-digit and double-digit hours
   */
  private def parseDateFromCSV(dateStr: String): Option[LocalDate] =
    val patterns = List(
      "dd.MM.yyyy H:mm:ss",
      "dd.MM.yyyy HH:mm:ss",
      "dd.MM.yyyy",
      "yyyy-MM-dd HH:mm:ss",
      "yyyy-MM-dd H:mm:ss",
      "yyyy-MM-dd",
    )

    patterns.iterator.flatMap { pattern =>
      allCatch.opt {
        val formatter = DateTimeFormatter.ofPattern(pattern)
        if pattern.contains("mm:ss") then LocalDateTime.parse(dateStr.trim, formatter).toLocalDate
        else LocalDate.parse(dateStr.trim, formatter)
      }
    }.nextOption()

  /**
   * Load CSV data from a file
   */
  private def loadCSV(filePath: String, hasHeader: Boolean): Try[TableData] =
    implicit val csvFormat: CSVFormat = new CSVFormat {
      val delimiter: Char                        = CSVDataSource.this.delimiter
      val quoteChar: Char                        = '"'
      val escapeChar: Char                       = '"'
      val lineTerminator: String                 = "\r\n"
      val quoting: defaultCSVFormat.quoting.type = defaultCSVFormat.quoting
      val treatEmptyLineAsNil: Boolean           = false
    }

    Try {
      Using.resource(CSVReader.open(new File(filePath))(csvFormat)) { reader =>
        val allRows = reader.all()

        if allRows.isEmpty then TableData(List.empty, List.empty)
        else
          val (headers, dataRows) =
            if hasHeader then (allRows.head, allRows.tail)
            else ((0 until allRows.head.size).map(i => s"column_$i").toList, allRows)

          val rows = dataRows.map(row => headers.zip(row).toMap)

          TableData(headers, rows)
      }
    }

object CSVDataSource:
  /**
   * Create CSV data source for typed case class results
   *
   * Parses entire rows into typed case classes instead of individual column values.
   * The rowParser receives the full row as Map[String, String] and the timestamp.
   *
   * @param basePath Base directory path
   * @param tableMapping Map of table names to CSV file names
   * @param delimiter CSV delimiter character
   * @param rowParser Function to parse a row Map and date into a typed result
   */
  def forTyped[F[_], R](
    basePath: String,
    tableMapping: Map[String, String],
    delimiter: Char = ',',
    rowParser: (Map[String, String], LocalDate) => Option[R],
  ): DataSource[F, R] =
    new CSVDataSource[F, R](basePath, tableMapping, delimiter, _ => None) {
      // Override get to handle typed queries differently
      override def get(query: Query, timeRange: metrics.core.TimeRange): Stream[F, (Instant, R)] =
        query match {
          case Query.ByName(metricName)               =>
            // For typed queries, metric name corresponds to table name
            tableMapping.get(metricName) match {
              case Some(fileName) =>
                val filePath = if basePath.endsWith("/") then s"$basePath$fileName" else s"$basePath/$fileName"
                parseTypedRows(filePath, rowParser, timeRange, Map.empty)
              case None           =>
                Stream.empty
            }
          case Query.WithFilters(metricName, filters) =>
            // For typed queries with filters
            tableMapping.get(metricName) match {
              case Some(fileName) =>
                val filePath      = if basePath.endsWith("/") then s"$basePath$fileName" else s"$basePath/$fileName"
                val stringFilters = filters.map {
                  case (k, v: Int)       => (k, v.toString)
                  case (k, v: LocalDate) => (k, v.toString)
                  case (k, v)            => (k, v.toString)
                }
                parseTypedRows(filePath, rowParser, timeRange, stringFilters)
              case None           =>
                Stream.empty
            }
          case _                                      =>
            Stream.empty
        }
    }

  /**
   * Parse CSV file into typed rows with optional filters
   */
  private def parseTypedRows[F[_], R](
    filePath: String,
    rowParser: (Map[String, String], LocalDate) => Option[R],
    timeRange: metrics.core.TimeRange,
    filters: Map[String, String],
  ): Stream[F, (Instant, R)] = {
    implicit val csvFormat: CSVFormat = new CSVFormat {
      val delimiter: Char                        = ';'
      val quoteChar: Char                        = '"'
      val escapeChar: Char                       = '"'
      val lineTerminator: String                 = "\r\n"
      val quoting: defaultCSVFormat.quoting.type = defaultCSVFormat.quoting
      val treatEmptyLineAsNil: Boolean           = false
    }

    val dateFormatter1 = DateTimeFormatter.ofPattern("dd.MM.yyyy")
    val dateFormatter2 = DateTimeFormatter.ofPattern("yyyy-MM.yyyy")

    def parseDate(dateStr: String): Option[LocalDate] =
      allCatch
        .opt(LocalDate.parse(dateStr, dateFormatter1))
        .orElse(allCatch.opt(LocalDate.parse(dateStr, dateFormatter2)))

    val result = Try {
      Using.resource(CSVReader.open(new File(filePath))(csvFormat)) { reader =>
        val allRows = reader.all()
        if allRows.isEmpty then List.empty
        else
          val headers  = allRows.head
          val dataRows = allRows.tail

          dataRows.flatMap { row =>
            val rowMap = headers.zip(row).toMap

            // Apply filters
            val passesFilters = filters.isEmpty || filters.forall {
              case (filterColumn, filterValue) =>
                rowMap.get(filterColumn).contains(filterValue)
            }

            if passesFilters then
              for {
                dateStr <- rowMap.get("date")
                date    <- parseDate(dateStr)
                timestamp = date.atStartOfDay(java.time.ZoneOffset.UTC).toInstant
                if !timestamp.isBefore(timeRange.from) && !timestamp.isAfter(timeRange.to)
                parsed <- rowParser(rowMap, date)
              } yield (timestamp, parsed)
            else None
          }
      }
    }

    result match {
      case scala.util.Success(rows) => Stream.emits(rows)
      case scala.util.Failure(ex)   =>
        throw new RuntimeException(s"Error reading typed CSV: ${ex.getMessage}", ex)
    }
  }

  /**
   * Create CSV data source with table mapping for Double values
   */
  def forDouble[F[_]](
    basePath: String,
    tableMapping: Map[String, String],
    delimiter: Char = ',',
  ): CSVDataSource[F, Double] =
    new CSVDataSource(basePath, tableMapping, delimiter, parseDouble)

  /**
   * Create CSV data source with table mapping for String values
   */
  def forString[F[_]](
    basePath: String,
    tableMapping: Map[String, String],
    delimiter: Char = ',',
  ): CSVDataSource[F, String] =
    new CSVDataSource(basePath, tableMapping, delimiter, s => Some(s))

  /**
   * Create CSV data source with table mapping for Int values
   */
  def forInt[F[_]](
    basePath: String,
    tableMapping: Map[String, String],
    delimiter: Char = ',',
  ): CSVDataSource[F, Int] =
    new CSVDataSource(basePath, tableMapping, delimiter, parseInt)

  /**
   * Create from single CSV file (Double values)
   */
  def fromFile[F[_]](
    tableName: String,
    filePath: String,
    delimiter: Char = ',',
  ): CSVDataSource[F, Double] =
    val file = new File(filePath)
    new CSVDataSource(file.getParent, Map(tableName -> file.getName), delimiter, parseDouble)

  /**
   * Helper: parse string to Double
   * Handles space-separated numbers like "3 198.7260"
   */
  private def parseDouble(value: String): Option[Double] =
    Try(value.replaceAll("\\s", "").toDouble).toOption

  /**
   * Helper: parse string to Int
   */
  private def parseInt(value: String): Option[Int] =
    Try(value.trim.toInt).toOption
