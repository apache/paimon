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

package org.apache.spark.sql.connector.catalog

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition, CatalogUtils, SessionCatalog}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object SparkV1PartitionManagement {

  def wrap(table: Table, delegate: CatalogPlugin): Table = table match {
    case v1Table: V1Table
        if v1Table.catalogTable.partitionColumnNames.nonEmpty &&
          v1Table.catalogTable.tracksPartitionsInCatalog =>
      sessionCatalog(delegate)
        .map(new SparkV1PartitionManagement(v1Table.catalogTable, _))
        .getOrElse(table)

    case _ => table
  }

  private def sessionCatalog(delegate: CatalogPlugin): Option[SessionCatalog] = delegate match {
    case v2SessionCatalog: V2SessionCatalog =>
      // V2SessionCatalog does not expose its SessionCatalog, but fallback V1 partition DDL must
      // delegate to SessionCatalog to update Hive metastore partition metadata. The private field
      // name is stable in Spark 3.2, 3.3, 3.4, 3.5 and 4.0. Do not cache Field across class
      // loaders; if reflective access fails, fall back to the original V1Table.
      try {
        val field = v2SessionCatalog.getClass.getDeclaredField("catalog")
        field.setAccessible(true)
        Some(field.get(v2SessionCatalog).asInstanceOf[SessionCatalog])
      } catch {
        case NonFatal(_) => None
      }

    case _ => None
  }
}

class SparkV1PartitionManagement(catalogTable: CatalogTable, catalog: SessionCatalog)
  extends V1Table(catalogTable)
  with SupportsAtomicPartitionManagement {

  override lazy val partitionSchema: StructType = catalogTable.partitionSchema

  override def createPartitions(
      idents: Array[InternalRow],
      properties: Array[JMap[String, String]]): Unit = {
    val partitions = idents.zip(properties).map {
      case (ident, partitionProperties) =>
        val scalaProperties = partitionProperties.asScala.toMap
        val location = scalaProperties.get("location")
        CatalogTablePartition(
          toPartitionSpec(ident),
          catalogTable.storage.copy(locationUri = location.map(CatalogUtils.stringToURI)),
          parameters = scalaProperties - "location")
    }
    catalog.createPartitions(catalogTable.identifier, partitions, ignoreIfExists = false)
  }

  override def dropPartitions(idents: Array[InternalRow]): Boolean = {
    catalog.dropPartitions(
      catalogTable.identifier,
      idents.map(toPartitionSpec),
      ignoreIfNotExists = false,
      purge = false,
      retainData = false)
    true
  }

  override def purgePartitions(idents: Array[InternalRow]): Boolean = {
    catalog.dropPartitions(
      catalogTable.identifier,
      idents.map(toPartitionSpec),
      ignoreIfNotExists = false,
      purge = true,
      retainData = false)
    true
  }

  override def renamePartition(from: InternalRow, to: InternalRow): Boolean = {
    catalog.renamePartitions(
      catalogTable.identifier,
      Seq(toPartitionSpec(from)),
      Seq(toPartitionSpec(to)))
    true
  }

  override def replacePartitionMetadata(
      ident: InternalRow,
      properties: JMap[String, String]): Unit = {
    val partition = catalog.getPartition(catalogTable.identifier, toPartitionSpec(ident))
    val scalaProperties = properties.asScala.toMap
    val storage = scalaProperties.get("location") match {
      case Some(location) =>
        partition.storage.copy(locationUri = Some(CatalogUtils.stringToURI(location)))
      case None => partition.storage
    }
    catalog.alterPartitions(
      catalogTable.identifier,
      Seq(partition.copy(storage = storage, parameters = scalaProperties - "location")))
  }

  override def loadPartitionMetadata(ident: InternalRow): JMap[String, String] = {
    val partition = catalog.getPartition(catalogTable.identifier, toPartitionSpec(ident))
    (partition.parameters ++
      partition.storage.locationUri.map(uri => "location" -> uri.toString)).asJava
  }

  override def listPartitionIdentifiers(
      partitionCols: Array[String],
      ident: InternalRow): Array[InternalRow] = {
    val partialSpec =
      if (partitionCols.isEmpty) {
        None
      } else {
        Some(toPartitionSpec(partitionCols, ident))
      }

    catalog
      .listPartitions(catalogTable.identifier, partialSpec)
      .map(_.toRow(partitionSchema, SQLConf.get.sessionLocalTimeZone))
      .toArray
  }

  override def partitionExists(ident: InternalRow): Boolean = {
    try {
      catalog.getPartition(catalogTable.identifier, toPartitionSpec(ident))
      true
    } catch {
      case _: NoSuchPartitionException => false
    }
  }

  override def truncatePartitions(idents: Array[InternalRow]): Boolean = {
    throw new UnsupportedOperationException("Truncate partition is not supported for V1 tables.")
  }

  private def toPartitionSpec(ident: InternalRow): Map[String, String] = {
    toPartitionSpec(partitionSchema.fieldNames, ident)
  }

  private def toPartitionSpec(
      partitionCols: Array[String],
      ident: InternalRow): Map[String, String] = {
    val schema = StructType(partitionCols.map(partitionSchema.apply))
    val rowConverter = CatalystTypeConverters.createToScalaConverter(
      CharVarcharUtils.replaceCharVarcharWithString(schema))
    val row = rowConverter(ident).asInstanceOf[Row]
    partitionCols.zipWithIndex.map {
      case (name, index) =>
        val value = row.get(index)
        name -> (if (value == null) null else value.toString)
    }.toMap
  }
}
