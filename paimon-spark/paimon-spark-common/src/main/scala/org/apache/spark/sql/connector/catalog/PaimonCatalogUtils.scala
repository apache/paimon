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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog
import org.apache.spark.sql.connector.catalog.CatalogV2Util
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.paimon.ReflectUtils

object PaimonCatalogUtils {

  def buildExternalCatalog(conf: SparkConf, hadoopConf: Configuration): ExternalCatalog = {
    val externalCatalogClassName =
      if (SparkSession.active.conf.get(CATALOG_IMPLEMENTATION.key).equals("hive")) {
        "org.apache.spark.sql.hive.HiveExternalCatalog"
      } else {
        "org.apache.spark.sql.catalyst.catalog.InMemoryCatalog"
      }
    ReflectUtils.reflect[ExternalCatalog, SparkConf, Configuration](
      externalCatalogClassName,
      conf,
      hadoopConf)
  }

  val TABLE_RESERVED_PROPERTIES: Seq[String] = CatalogV2Util.TABLE_RESERVED_PROPERTIES

}
