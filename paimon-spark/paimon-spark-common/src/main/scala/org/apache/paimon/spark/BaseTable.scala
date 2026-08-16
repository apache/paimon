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

import org.apache.paimon.table.Table
import org.apache.paimon.utils.StringUtils

import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.types.StructType

import java.util.{Collections => JCollections, Map => JMap, Set => JSet}

import scala.collection.JavaConverters._

abstract class BaseTable
  extends org.apache.spark.sql.connector.catalog.Table
  with PaimonPartitionManagement {

  val table: Table

  override def capabilities(): JSet[TableCapability] = JCollections.emptySet[TableCapability]()

  override def name: String = table.fullName

  override lazy val schema: StructType = SparkTypeUtils.fromPaimonRowType(table.rowType)

  override def partitioning: Array[Transform] = {
    table.partitionKeys().asScala.map(p => Expressions.identity(StringUtils.quote(p))).toArray
  }

  override def properties: JMap[String, String] = table.options()

  override def toString: String = {
    s"${table.getClass.getSimpleName}[${table.fullName()}]"
  }
}
