package metrics.datasources

import java.io.File

import scala.util.control.Exception._
import scala.util.{Try, Using}

import cats.effect.Sync
import cats.syntax.all._
import com.github.tototoshi.csv.{CSVFormat, CSVReader, defaultCSVFormat}

import metrics.algebra.ConfigurationSource

/**
 * CSV-based ConfigurationSource implementation
 *
 * Reads static reference data from CSV files and provides efficient lookup.
 * Data is loaded once and cached in memory.
 */
class CSVConfigurationSource[F[_]: Sync, K, V](
  filePath: String,
  delimiter: Char,
  keyExtractor: Map[String, String] => Option[K],
  valueExtractor: Map[String, String] => Option[V],
) extends ConfigurationSource[F, K, V]:

  private lazy val data: Map[K, V] = loadData()

  def get(key: K): F[Option[V]] =
    Sync[F].pure(data.get(key))

  def getAll: F[Map[K, V]] =
    Sync[F].pure(data)

  private def loadData(): Map[K, V] =
    implicit val csvFormat: CSVFormat = new CSVFormat {
      val delimiter: Char                        = CSVConfigurationSource.this.delimiter
      val quoteChar: Char                        = '"'
      val escapeChar: Char                       = '"'
      val lineTerminator: String                 = "\r\n"
      val quoting: defaultCSVFormat.quoting.type = defaultCSVFormat.quoting
      val treatEmptyLineAsNil: Boolean           = false
    }

    val result = Try {
      Using.resource(CSVReader.open(new File(filePath))(csvFormat)) { reader =>
        val allRows = reader.all()
        if allRows.isEmpty then Map.empty[K, V]
        else
          val headers  = allRows.head
          val dataRows = allRows.tail

          dataRows.flatMap { row =>
            val rowMap = headers.zip(row).toMap
            for
              key   <- keyExtractor(rowMap)
              value <- valueExtractor(rowMap)
            yield (key, value)
          }.toMap
      }
    }

    result.getOrElse(Map.empty)

object CSVConfigurationSource:
  /**
   * Create a ConfigurationSource with a single key column and single value column
   *
   * @param basePath Base directory path
   * @param fileName CSV file name
   * @param keyColumn Name of the key column
   * @param valueColumn Name of the value column
   * @param delimiter CSV delimiter (default: ',')
   * @param keyParser Function to parse key from string
   * @param valueParser Function to parse value from string
   */
  def singleKey[F[_]: Sync, K, V](
    basePath: String,
    fileName: String,
    keyColumn: String,
    valueColumn: String,
    delimiter: Char = ',',
    keyParser: String => Option[K],
    valueParser: String => Option[V],
  ): CSVConfigurationSource[F, K, V] =
    val filePath = if basePath.endsWith("/") then s"$basePath$fileName" else s"$basePath/$fileName"

    val keyExtractor: Map[String, String] => Option[K] = rowMap => rowMap.get(keyColumn).flatMap(keyParser)

    val valueExtractor: Map[String, String] => Option[V] = rowMap => rowMap.get(valueColumn).flatMap(valueParser)

    new CSVConfigurationSource[F, K, V](filePath, delimiter, keyExtractor, valueExtractor)

  /**
   * Create a ConfigurationSource with composite key (2 columns) and single value column
   *
   * @param basePath Base directory path
   * @param fileName CSV file name
   * @param keyColumn1 Name of first key column
   * @param keyColumn2 Name of second key column
   * @param valueColumn Name of the value column
   * @param delimiter CSV delimiter (default: ',')
   * @param key1Parser Function to parse first key component from string
   * @param key2Parser Function to parse second key component from string
   * @param valueParser Function to parse value from string
   */
  def compositeKey[F[_]: Sync, K1, K2, V](
    basePath: String,
    fileName: String,
    keyColumn1: String,
    keyColumn2: String,
    valueColumn: String,
    delimiter: Char = ',',
    key1Parser: String => Option[K1],
    key2Parser: String => Option[K2],
    valueParser: String => Option[V],
  ): CSVConfigurationSource[F, (K1, K2), V] =
    val filePath = if basePath.endsWith("/") then s"$basePath$fileName" else s"$basePath/$fileName"

    val keyExtractor: Map[String, String] => Option[(K1, K2)] = rowMap =>
      for
        k1 <- rowMap.get(keyColumn1).flatMap(key1Parser)
        k2 <- rowMap.get(keyColumn2).flatMap(key2Parser)
      yield (k1, k2)

    val valueExtractor: Map[String, String] => Option[V] = rowMap => rowMap.get(valueColumn).flatMap(valueParser)

    new CSVConfigurationSource[F, (K1, K2), V](filePath, delimiter, keyExtractor, valueExtractor)

  /**
   * Helper parsers for common types
   */
  def parseInt(s: String): Option[Int] =
    allCatch.opt(s.trim.toInt)

  def parseDouble(s: String): Option[Double] =
    allCatch.opt(s.replaceAll("\\s", "").toDouble)

  def parseString(s: String): Option[String] =
    Some(s.trim).filter(_.nonEmpty)
