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

package org.apache.spark.sql.execution

import org.apache.paimon.CoreOptions
import org.apache.paimon.iceberg.IcebergOptions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.plans.logical.TableSpec
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.sql.paimon.shims.SparkShimLoader

import scala.collection.JavaConverters._

trait PaimonStrategyHelper {

  def spark: SparkSession

  protected def makeQualifiedDBObjectPath(location: String): String = {
    CatalogUtils.makeQualifiedDBObjectPath(
      spark.sharedState.conf.get(WAREHOUSE_PATH),
      location,
      spark.sharedState.hadoopConf)
  }

  protected def qualifyTableSpec(
      tableSpec: TableSpec,
      tableOptions: Map[String, String]): TableSpec = {
    SparkShimLoader.shim.copyTableSpec(
      tableSpec,
      tableOptions,
      tableSpec.location.map(makeQualifiedDBObjectPath))
  }
}

object PaimonStrategyHelper {
  private val tableOptionKeys: Set[String] =
    (CoreOptions.getOptions.asScala.map(_.key()) ++ IcebergOptions.getOptions.asScala.map(
      _.key())).toSet

  def splitTableAndWriteOptions(
      options: Map[String, String]): (Map[String, String], Map[String, String]) = {
    options.partition { case (key, _) => tableOptionKeys.contains(key) }
  }
}
