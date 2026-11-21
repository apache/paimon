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

package org.apache.paimon.spark

import org.apache.paimon.CoreOptions
import org.apache.paimon.data.{GenericRow, InternalRow => PaimonInternalRow, Timestamp}
import org.apache.paimon.data.serializer.InternalRowSerializer
import org.apache.paimon.options.Options
import org.apache.paimon.partition.{PartitionPredicate, PartitionStatistics}
import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.table.{FileStoreTable, FormatTable, Table}
import org.apache.paimon.table.source.ScanMode
import org.apache.paimon.types.{DataTypeChecks, RowType}
import org.apache.paimon.utils.{DateTimeUtils, InternalRowPartitionComputer, TypeUtils}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{SupportsAtomicPartitionManagement, TableCatalog}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.nio.charset.StandardCharsets
import java.util.{Arrays, LinkedHashMap => JLinkedHashMap, Map => JMap, Objects}

import scala.collection.JavaConverters._

trait PaimonPartitionManagement extends SupportsAtomicPartitionManagement with Logging {

  val table: Table

  lazy val partitionRowType: RowType = TypeUtils.project(table.rowType, table.partitionKeys)

  override lazy val partitionSchema: StructType = SparkTypeUtils.fromPaimonRowType(partitionRowType)

  private def toPaimonPartitions(rows: Array[InternalRow]): Array[java.util.Map[String, String]] = {
    table match {
      case _: FileStoreTable =>
        val partitionKeys = table.partitionKeys().asScala.toSeq
        rows.map {
          r =>
            require(
              r.numFields <= partitionKeys.length,
              s"Partition values length ${r.numFields} exceeds partition keys " +
                s"${partitionKeys.mkString("[", ", ", "]")}."
            )
            toPaimonPartition(r, partitionKeys.take(r.numFields))
        }
      case _: FormatTable =>
        // Reached by the partition operations this trait still serves directly. Saying that only
        // a FileStoreTable has partitions would be wrong for a Format Table with catalog-managed
        // partitions, which has them and lists them here; a Format Table is still a Paimon table,
        // just not a native one.
        throw new UnsupportedOperationException(
          s"This partition operation is supported only for a native Paimon table; " +
            s"${table.name()} is a Format Table, which manages its partitions through " +
            s"ADD PARTITION, DROP PARTITION and MSCK REPAIR TABLE.")
      case _ =>
        throw new UnsupportedOperationException(
          s"This partition operation is supported only for a native Paimon table, " +
            s"which ${table.name()} is not.")
    }
  }

  protected def toPaimonPartition(
      row: InternalRow,
      partitionNames: Seq[String]): java.util.Map[String, String] = {
    val coreOptions = CoreOptions.fromMap(table.options())
    val partitionDefaultName = coreOptions.partitionDefaultName()
    val legacyPartitionName = coreOptions.legacyPartitionName
    val currentPartitionRowType = TypeUtils.project(table.rowType, partitionNames.asJava)
    val currentPartitionSchema = SparkTypeUtils.fromPaimonRowType(currentPartitionRowType)
    val rowConverter = CatalystTypeConverters.createToScalaConverter(
      CharVarcharUtils.replaceCharVarcharWithString(currentPartitionSchema))
    val rowDataPartitionComputer = new InternalRowPartitionComputer(
      partitionDefaultName,
      currentPartitionRowType,
      partitionNames.toArray,
      legacyPartitionName
    )

    rowDataPartitionComputer.generatePartValues(
      new SparkRow(currentPartitionRowType, rowConverter(row).asInstanceOf[Row]))
  }

  /**
   * The first partition column whose value in `spec` does not parse back to its value in `row`. A
   * spec holds each value as a string, and a blank one is written as the default partition name,
   * which parses back as NULL.
   */
  private def columnNotParsedBack(row: InternalRow, spec: JMap[String, String]): Option[String] = {
    val names = table.partitionKeys().asScala.toSeq.take(row.numFields)
    val specRowType = TypeUtils.project(table.rowType, names.asJava)
    val parsed = InternalRowPartitionComputer.convertSpecToInternal(
      spec,
      specRowType,
      CoreOptions.fromMap(table.options()).partitionDefaultName())
    names.zipWithIndex
      .find {
        case (name, index) =>
          val parsedValue =
            DataConverter.fromPaimon(parsed.get(name), specRowType.getField(name).`type`())
          !Objects.deepEquals(row.get(index, partitionSchema(name).dataType), parsedValue)
      }
      .map(_._1)
  }

  /** Whether the Spark value is a STRING or BINARY value equal to `text`. */
  private def isText(value: Any, text: String): Boolean = value match {
    case s: UTF8String => s.toString == text
    case b: Array[Byte] => new String(b, StandardCharsets.UTF_8) == text
    case _ => false
  }

  /** The first column of `row` whose value is the default partition name, which is not NULL. */
  private def columnHoldingDefaultPartitionName(row: InternalRow): Option[String] = {
    val defaultPartitionName = CoreOptions.fromMap(table.options()).partitionDefaultName()
    partitionSchema.fields.take(row.numFields).zipWithIndex.collectFirst {
      case (field, i) if isText(row.get(i, field.dataType), defaultPartitionName) => field.name
    }
  }

  /**
   * The Paimon values of the partition columns each row holds, the first ones of the table, with
   * timestamps at the column's precision.
   */
  private def toPaimonPartitionValues(rows: Seq[InternalRow]): Seq[Array[AnyRef]] = {
    val converters = rows
      .map(_.numFields)
      .distinct
      .map {
        numFields =>
          val names = table.partitionKeys().asScala.toSeq.take(numFields)
          val rowType = TypeUtils.project(table.rowType, names.asJava)
          val toScala = CatalystTypeConverters.createToScalaConverter(
            CharVarcharUtils.replaceCharVarcharWithString(
              SparkTypeUtils.fromPaimonRowType(rowType)))
          val getters = rowType.getFields.asScala.zipWithIndex.map {
            case (field, i) =>
              field.`type`() -> PaimonInternalRow.createFieldGetter(field.`type`(), i)
          }
          numFields -> {
            (row: InternalRow) =>
              val paimonRow = new SparkRow(rowType, toScala(row).asInstanceOf[Row])
              getters.map {
                case (dataType, getter) =>
                  getter.getFieldOrNull(paimonRow) match {
                    case timestamp: Timestamp =>
                      DateTimeUtils.truncate(timestamp, DataTypeChecks.getPrecision(dataType))
                    case other => other
                  }
              }.toArray
          }
      }
      .toMap
    rows.map(row => converters(row.numFields)(row))
  }

  /**
   * The partitions `values` address, compared by value: full values address their own partition, a
   * prefix every partition under it.
   */
  private def toPartitionPredicate(values: Seq[Array[AnyRef]]): PartitionPredicate = {
    if (values.forall(_.length == partitionRowType.getFieldCount)) {
      val serializer = new InternalRowSerializer(partitionRowType)
      PartitionPredicate.fromMultiple(
        partitionRowType,
        values.map(v => serializer.toBinaryRow(GenericRow.of(v: _*)).copy()).asJava)
    } else {
      val names = partitionRowType.getFieldNames
      val predicates = values.map {
        v =>
          val spec = new JLinkedHashMap[String, AnyRef]()
          v.indices.foreach(i => spec.put(names.get(i), v(i)))
          PartitionPredicate.createPartitionPredicate(partitionRowType, spec)
      }
      PartitionPredicate.fromPredicate(partitionRowType, PredicateBuilder.or(predicates.asJava))
    }
  }

  private def toPartitionPredicate(row: InternalRow): PartitionPredicate =
    toPartitionPredicate(toPaimonPartitionValues(Seq(row)))

  /** The specs of the partitions to drop by name; each must address its own partition. */
  private def toRemovedPaimonPartitions(rows: Array[InternalRow]): Array[JMap[String, String]] = {
    val specs = toPaimonPartitions(rows)
    rows.zip(specs).foreach {
      case (row, spec) =>
        columnNotParsedBack(row, spec).foreach {
          name =>
            val nullPartitionHint = if (columnHoldingDefaultPartitionName(row).contains(name)) {
              s" If you meant the NULL partition, use $name = NULL."
            } else {
              ""
            }
            throw new IllegalArgumentException(
              s"Partition spec $spec of table ${table.name()} does not name the partition to " +
                s"remove: the value of partition column $name does not parse back from it." +
                nullPartitionHint)
        }
    }
    specs
  }

  /**
   * Removes the data of the partitions the rows address. The commit matches them by value in the
   * snapshot it commits on, as it matches a partition spec.
   */
  private def truncatePartitionsByValue(
      fileStoreTable: FileStoreTable,
      rows: Array[InternalRow]): Unit = {
    if (rows.isEmpty) {
      return
    }
    rows.foreach {
      row =>
        columnHoldingDefaultPartitionName(row).foreach {
          name =>
            val reader = fileStoreTable.newSnapshotReader().withMode(ScanMode.ALL)
            if (reader.withPartitionFilter(toPartitionPredicate(row)).partitions().isEmpty) {
              throw new IllegalArgumentException(
                s"No partition of table ${table.name()} has the default partition name as the " +
                  s"value of partition column $name. If you meant the NULL partition, use " +
                  s"$name = NULL.")
            }
        }
    }
    val values = toPaimonPartitionValues(rows.toSeq)
    logInfo(
      s"Try to truncate the partitions of table ${table.name()} matching " +
        values.map(v => Arrays.deepToString(v)).mkString(", "))
    val commit =
      fileStoreTable.newCommit(CoreOptions.createCommitUser(new Options(fileStoreTable.options())))
    try {
      commit.truncatePartitions(toPartitionPredicate(values))
    } finally {
      commit.close()
    }
  }

  override def dropPartitions(rows: Array[InternalRow]): Boolean = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val partitionModification = fileStoreTable.catalogEnvironment().partitionModification()
        if (partitionModification != null) {
          // The catalog drops partitions by name.
          try {
            val partitions = toRemovedPaimonPartitions(rows).toSeq.asJava
            logInfo("Try to drop partitions: " + partitions.asScala.mkString(","))
            partitionModification.dropPartitions(partitions)
          } finally {
            partitionModification.close()
          }
        } else {
          truncatePartitionsByValue(fileStoreTable, rows)
        }
        true

      case _ =>
        throw new UnsupportedOperationException("Only FileStoreTable supports drop partitions.")
    }
  }

  override def truncatePartitions(idents: Array[InternalRow]): Boolean = {
    table match {
      case fileStoreTable: FileStoreTable =>
        truncatePartitionsByValue(fileStoreTable, idents)
      case _ =>
        val partitions = toPaimonPartitions(idents).toSeq.asJava
        val commit = table.newBatchWriteBuilder().newCommit()
        try {
          commit.truncatePartitions(partitions)
        } finally {
          commit.close()
        }
    }
    true
  }

  override def truncatePartition(ident: InternalRow): Boolean = {
    truncatePartitions(Array(ident))
  }

  override def replacePartitionMetadata(
      ident: InternalRow,
      properties: JMap[String, String]): Unit = {
    throw new UnsupportedOperationException("Replace partition is not supported")
  }

  override def loadPartitionMetadata(ident: InternalRow): JMap[String, String] = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val partitionEntry = fileStoreTable
          .newSnapshotReader()
          .withMode(ScanMode.ALL)
          .withPartitionFilter(toPartitionPredicate(ident))
          .partitionEntries()
          .asScala
          .headOption

        partitionEntry match {
          case Some(entry) =>
            Map(
              PartitionStatistics.FIELD_RECORD_COUNT -> entry.recordCount().toString,
              PartitionStatistics.FIELD_FILE_SIZE_IN_BYTES -> entry.fileSizeInBytes().toString,
              PartitionStatistics.FIELD_FILE_COUNT -> entry.fileCount().toString,
              PartitionStatistics.FIELD_LAST_FILE_CREATION_TIME -> entry
                .lastFileCreationTime()
                .toString
            ).asJava
          case None =>
            Map.empty[String, String].asJava
        }
      case formatTable: FormatTable =>
        val partitionManager = formatTable.partitionManager()
        if (partitionManager == null) {
          Map.empty[String, String].asJava
        } else {
          val partitionSpec =
            toPaimonPartition(ident, formatTable.partitionKeys().asScala.toSeq)
          val partitions = partitionManager.listPartitionsByNames(Seq(partitionSpec).asJava)
          if (!partitions.isEmpty) {
            val partition = partitions.get(0)
            val metadata = new java.util.HashMap[String, String]()
            Option(partition.options()).foreach(metadata.putAll)
            metadata
              .keySet()
              .asScala
              .filter(TableCatalog.PROP_LOCATION.equalsIgnoreCase)
              .toSeq
              .foreach(metadata.remove)
            val pathOption = CoreOptions.PATH.key()
            if (metadata.containsKey(pathOption)) {
              val path = metadata.remove(pathOption)
              if (path == null) {
                throw new IllegalStateException(
                  s"Catalog returned a null $pathOption option for partition ${partition.spec()} " +
                    s"of Format Table ${formatTable.fullName()}.")
              }
              metadata.put(TableCatalog.PROP_LOCATION, path)
            }
            metadata.put(PartitionStatistics.FIELD_RECORD_COUNT, partition.recordCount().toString)
            metadata.put(
              PartitionStatistics.FIELD_FILE_SIZE_IN_BYTES,
              partition.fileSizeInBytes().toString)
            metadata.put(PartitionStatistics.FIELD_FILE_COUNT, partition.fileCount().toString)
            metadata.put(
              PartitionStatistics.FIELD_LAST_FILE_CREATION_TIME,
              partition.lastFileCreationTime().toString)
            metadata
          } else {
            Map.empty[String, String].asJava
          }
        }
      case _ =>
        Map.empty[String, String].asJava
    }
  }

  override def listPartitionIdentifiers(
      partitionCols: Array[String],
      internalRow: InternalRow): Array[InternalRow] = {
    assert(
      partitionCols.length == internalRow.numFields,
      s"Number of partition names (${partitionCols.length}) must be equal to " +
        s"the number of partition values (${internalRow.numFields})."
    )
    assert(
      partitionCols.forall(fieldName => partitionSchema.fieldNames.contains(fieldName)),
      s"Some partition names ${partitionCols.mkString("[", ", ", "]")} don't belong to " +
        s"the partition schema '${partitionSchema.sql}'."
    )
    table.newReadBuilder.newScan.listPartitions.asScala
      .map(binaryRow => DataConverter.fromPaimon(binaryRow, partitionRowType))
      .filter(
        sparkInternalRow => {
          partitionCols.zipWithIndex
            .map {
              case (partitionName, index) =>
                val internalRowIndex = partitionSchema.fieldIndex(partitionName)
                val structField = partitionSchema.fields(internalRowIndex)
                // `deepEquals`: a BINARY partition value is a `byte[]`.
                Objects.deepEquals(
                  sparkInternalRow.get(internalRowIndex, structField.dataType),
                  internalRow.get(index, structField.dataType))
            }
            .forall(identity)
        })
      .toArray
  }

  override def createPartitions(
      rows: Array[InternalRow],
      maps: Array[JMap[String, String]]): Unit = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val partitions = toPaimonPartitions(rows)
        val partitionModification = fileStoreTable.catalogEnvironment().partitionModification()
        if (partitionModification != null) {
          try {
            if (fileStoreTable.coreOptions().partitionedTableInMetastore()) {
              partitionModification.createPartitions(partitions.toSeq.asJava)
            }
          } finally {
            partitionModification.close()
          }
        }
      case _ =>
        throw new UnsupportedOperationException("Only FileStoreTable supports create partitions.")
    }
  }
}
