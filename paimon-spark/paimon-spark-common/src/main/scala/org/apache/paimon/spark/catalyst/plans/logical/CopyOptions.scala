/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.catalyst.plans.logical

sealed trait OnErrorMode

object OnErrorMode {
  case object AbortStatement extends OnErrorMode
  case object Continue extends OnErrorMode
  case object SkipFile extends OnErrorMode
}

sealed trait FileFormatType

object FileFormatType {
  case object CSV extends FileFormatType
  case object JSON extends FileFormatType
  case object PARQUET extends FileFormatType
  case class Unsupported(name: String) extends FileFormatType
}

case class CopyFileFormat(formatType: FileFormatType, options: Map[String, String]) {

  def toSparkReaderOptions: Map[String, String] = {
    val mapped = scala.collection.mutable.Map[String, String]("mode" -> "FAILFAST")
    formatType match {
      case FileFormatType.CSV =>
        options.foreach {
          case (k, v) =>
            k match {
              case "FIELD_DELIMITER" => mapped("sep") = v
              case "QUOTE" => mapped("quote") = v
              case "ESCAPE" => mapped("escape") = v
              case "COMPRESSION" => mapped("compression") = v
              case "SKIP_HEADER" =>
                mapped("header") = if (v == "1" || v.equalsIgnoreCase("TRUE")) "true" else "false"
              case _ =>
            }
        }
      case FileFormatType.JSON =>
        options.foreach {
          case (k, v) =>
            k match {
              case "MULTI_LINE" => mapped("multiLine") = v.toLowerCase
              case "COMPRESSION" => mapped("compression") = v
              case _ =>
            }
        }
      case FileFormatType.PARQUET =>
        mapped.remove("mode")
        options.foreach {
          case (k, v) =>
            k match {
              case "COMPRESSION" => mapped("compression") = v
              case _ =>
            }
        }
      case _ =>
    }
    mapped.toMap
  }

  def toSparkWriterOptions: Map[String, String] = {
    val mapped = scala.collection.mutable.Map[String, String]()
    formatType match {
      case FileFormatType.CSV =>
        options.foreach {
          case (k, v) =>
            k match {
              case "FIELD_DELIMITER" => mapped("sep") = v
              case "HEADER" => mapped("header") = v.toLowerCase
              case "QUOTE" => mapped("quote") = v
              case "ESCAPE" => mapped("escape") = v
              case "COMPRESSION" => mapped("compression") = v
              case _ =>
            }
        }
      case FileFormatType.JSON =>
        options.foreach {
          case (k, v) =>
            k match {
              case "COMPRESSION" => mapped("compression") = v
              case "DATE_FORMAT" => mapped("dateFormat") = v
              case "TIMESTAMP_FORMAT" => mapped("timestampFormat") = v
              case _ =>
            }
        }
      case FileFormatType.PARQUET =>
        options.foreach {
          case (k, v) =>
            k match {
              case "COMPRESSION" => mapped("compression") = v
              case _ =>
            }
        }
      case _ =>
    }
    mapped.toMap
  }

  def nullIfValues: Seq[String] = {
    options.get("NULL_IF") match {
      case Some(v) if v.nonEmpty =>
        v.split(CopyFileFormat.LIST_SEPARATOR, -1).toSeq
      case _ => Seq.empty
    }
  }

  def emptyFieldAsNull: Boolean = {
    options.get("EMPTY_FIELD_AS_NULL").exists(v => v == "TRUE" || v.equalsIgnoreCase("TRUE"))
  }

  def validateForImport(): Unit = {
    validateFormatType()
    if (options.contains("MODE")) {
      throw new IllegalArgumentException(
        "MODE cannot be specified in FILE_FORMAT options; it is reserved for ON_ERROR handling")
    }
    val validKeys = formatType match {
      case FileFormatType.JSON => CopyFileFormat.VALID_JSON_IMPORT_KEYS
      case FileFormatType.PARQUET => CopyFileFormat.VALID_PARQUET_IMPORT_KEYS
      case _ => CopyFileFormat.VALID_CSV_IMPORT_KEYS
    }
    val invalid = options.keys.filterNot(validKeys.contains)
    if (invalid.nonEmpty) {
      throw new IllegalArgumentException(
        s"Unsupported FILE_FORMAT options for import: ${invalid.mkString(", ")}")
    }
    if (formatType == FileFormatType.CSV) {
      options.get("SKIP_HEADER").foreach {
        v =>
          val intVal =
            try v.toInt
            catch { case _: NumberFormatException => -1 }
          if (intVal != 0 && intVal != 1) {
            throw new IllegalArgumentException(s"SKIP_HEADER supports only 0 or 1, got: $v")
          }
      }
    }
  }

  def validateForExport(): Unit = {
    validateFormatType()
    val validKeys = formatType match {
      case FileFormatType.JSON => CopyFileFormat.VALID_JSON_EXPORT_KEYS
      case FileFormatType.PARQUET => CopyFileFormat.VALID_PARQUET_EXPORT_KEYS
      case _ => CopyFileFormat.VALID_CSV_EXPORT_KEYS
    }
    val invalid = options.keys.filterNot(validKeys.contains)
    if (invalid.nonEmpty) {
      throw new IllegalArgumentException(
        s"Unsupported FILE_FORMAT options for export: ${invalid.mkString(", ")}")
    }
  }

  private def validateFormatType(): Unit = {
    formatType match {
      case FileFormatType.CSV =>
      case FileFormatType.JSON =>
      case FileFormatType.PARQUET =>
      case FileFormatType.Unsupported(name) =>
        throw new IllegalArgumentException(
          s"Unsupported file format type: $name. Supported types: CSV, JSON, PARQUET")
    }
  }
}

object CopyFileFormat {

  val VALID_CSV_IMPORT_KEYS: Set[String] = Set(
    "FIELD_DELIMITER",
    "SKIP_HEADER",
    "QUOTE",
    "ESCAPE",
    "NULL_IF",
    "EMPTY_FIELD_AS_NULL",
    "COMPRESSION"
  )

  val VALID_JSON_IMPORT_KEYS: Set[String] = Set(
    "MULTI_LINE",
    "COMPRESSION",
    "NULL_IF",
    "EMPTY_FIELD_AS_NULL"
  )

  val VALID_CSV_EXPORT_KEYS: Set[String] = Set(
    "FIELD_DELIMITER",
    "HEADER",
    "QUOTE",
    "ESCAPE",
    "COMPRESSION"
  )

  val VALID_JSON_EXPORT_KEYS: Set[String] = Set(
    "COMPRESSION",
    "DATE_FORMAT",
    "TIMESTAMP_FORMAT"
  )

  val VALID_PARQUET_IMPORT_KEYS: Set[String] = Set(
    "COMPRESSION"
  )

  val VALID_PARQUET_EXPORT_KEYS: Set[String] = Set(
    "COMPRESSION"
  )

  // Unit Separator (U+001F) used to encode multi-value lists in a single string
  val LIST_SEPARATOR: String = "\u001f"

  def parseFormatType(typeStr: String): FileFormatType = {
    typeStr.toUpperCase match {
      case "CSV" => FileFormatType.CSV
      case "JSON" => FileFormatType.JSON
      case "PARQUET" => FileFormatType.PARQUET
      case other => FileFormatType.Unsupported(other)
    }
  }
}
