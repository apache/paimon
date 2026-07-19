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

package org.apache.paimon.spark.format

import java.util.{List => JList, Map => JMap}

/** Spark-side gateway to catalog-managed Format Table partition metadata. */
trait FormatTablePartitionCatalog {

  def createPartitions(partitions: JList[JMap[String, String]], ignoreIfExists: Boolean): Unit

  /**
   * Unregister partitions from the catalog (metadata only, ignores missing partitions). Data
   * deletion is the caller's responsibility.
   */
  def dropPartitions(partitions: JList[JMap[String, String]]): Unit

  def listPartitionsByNames(partitions: JList[JMap[String, String]]): JList[JMap[String, String]]

  def listPartitions(
      prefix: JMap[String, String],
      pageToken: String,
      pageSize: Int): FormatTablePartitionPage
}

case class FormatTablePartitionPage(partitions: JList[JMap[String, String]], nextPageToken: String)
