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
import org.apache.paimon.catalog.{CatalogContext, CatalogUtils}
import org.apache.paimon.options.Options
import org.apache.paimon.spark.SparkSource._
import org.apache.paimon.spark.commands.WriteIntoPaimonTable
import org.apache.paimon.spark.sources.PaimonSink
import org.apache.paimon.spark.util.OptionUtils.copyWithSQLConf
import org.apache.paimon.table.{DataTable, FileStoreTable, FileStoreTableFactory}
import org.apache.paimon.table.FormatTable.Format
import org.apache.paimon.table.system.AuditLogTable

import org.apache.spark.sql.{DataFrame, PaimonSparkSession, SaveMode => SparkSaveMode, SQLContext}
import org.apache.spark.sql.connector.catalog.{Identifier => SparkIdentifier, SessionConfigSupport, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

class SparkSource
  extends DataSourceRegister
  with SessionConfigSupport
  with CreatableRelationProvider
  with StreamSinkProvider {

  override def shortName(): String = SparkSource.NAME

  override def keyPrefix(): String = SparkSource.NAME

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // ignore schema.
    // getTable will get schema by itself.
    null
  }

  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    // ignore partition.
    // getTable will get partition by itself.
    null
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: JMap[String, String]): Table = {
    SparkTable(loadTable(properties))
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SparkSaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val table = loadTable(parameters.asJava).asInstanceOf[FileStoreTable]
    WriteIntoPaimonTable(table, SaveMode.transform(mode), data, Options.fromMap(parameters.asJava))
      .run(sqlContext.sparkSession)
    SparkSource.toBaseRelation(table, sqlContext)
  }

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    if (outputMode != OutputMode.Append && outputMode != OutputMode.Complete) {
      throw new RuntimeException("Paimon supports only Complete and Append output mode.")
    }
    val table = loadTable(parameters.asJava).asInstanceOf[FileStoreTable]
    val options = Options.fromMap(parameters.asJava)
    new PaimonSink(sqlContext, table, partitionColumns, outputMode, options)
  }

  private def loadTable(options: JMap[String, String]): DataTable = {
    val path = CoreOptions.path(options)
    val sessionState = PaimonSparkSession.active.sessionState

    // Only if user specifies the catalog name, then use catalog to get table, otherwise
    // use FileStoreTableFactory to get table by path
    val table = if (options.containsKey(CATALOG)) {
      val catalogName = options.get(CATALOG)
      val dataBaseName = Option(options.get(DATABASE)).getOrElse(CatalogUtils.database(path))
      val tableName = Option(options.get(TABLE)).getOrElse(CatalogUtils.table(path))
      val sparkCatalog = sessionState.catalogManager.catalog(catalogName).asInstanceOf[TableCatalog]
      sparkCatalog
        .loadTable(SparkIdentifier.of(Array(dataBaseName), tableName))
        .asInstanceOf[SparkTable]
        .getTable
        .asInstanceOf[FileStoreTable]
        .copy(options)
    } else {
      val catalogContext =
        CatalogContext.create(Options.fromMap(options), sessionState.newHadoopConf())
      copyWithSQLConf(FileStoreTableFactory.create(catalogContext), extraOptions = options)
    }

    if (Options.fromMap(options).get(SparkConnectorOptions.READ_CHANGELOG)) {
      new AuditLogTable(table)
    } else {
      table
    }
  }
}

object SparkSource {

  val NAME = "paimon"

  val FORMAT_NAMES: Seq[String] = Format.values.map(_.toString.toLowerCase).toSeq

  // Spark dataframe read options
  private val CATALOG = "catalog"

  private val DATABASE = "database"

  private val TABLE = "table"

  private def toBaseRelation(table: FileStoreTable, _sqlContext: SQLContext): BaseRelation = {
    new BaseRelation {
      override def sqlContext: SQLContext = _sqlContext
      override def schema: StructType = SparkTypeUtils.fromPaimonRowType(table.rowType())
    }
  }
}
