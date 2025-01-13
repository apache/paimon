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

package org.apache.spark.sql.paimon.shims

import java.util.ServiceLoader

import scala.collection.JavaConverters._

/** Load a [[SparkShim]]'s implementation. */
object SparkShimLoader {

  private lazy val sparkShim: SparkShim = loadSparkShim()

  def getSparkShim: SparkShim = {
    sparkShim
  }

  private def loadSparkShim(): SparkShim = {
    val shims = ServiceLoader.load(classOf[SparkShim]).asScala
    if (shims.isEmpty) {
      throw new IllegalStateException("No available spark shim here.")
    } else if (shims.size > 1) {
      throw new IllegalStateException("Found more than one spark shim here.")
    }
    shims.head
  }
}
