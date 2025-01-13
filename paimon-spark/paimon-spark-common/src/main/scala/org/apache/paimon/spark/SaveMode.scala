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

import org.apache.spark.sql.{SaveMode => SparkSaveMode}
import org.apache.spark.sql.sources.{AlwaysTrue, Filter}

sealed private[spark] trait SaveMode extends Serializable

case object InsertInto extends SaveMode

case class Overwrite(filters: Option[Filter]) extends SaveMode

case object DynamicOverWrite extends SaveMode

case object ErrorIfExists extends SaveMode

case object Ignore extends SaveMode

object SaveMode {
  def transform(saveMode: SparkSaveMode): SaveMode = {
    saveMode match {
      case SparkSaveMode.Overwrite => Overwrite(Some(AlwaysTrue))
      case SparkSaveMode.Ignore => Ignore
      case SparkSaveMode.Append => InsertInto
      case SparkSaveMode.ErrorIfExists => ErrorIfExists
    }
  }
}
