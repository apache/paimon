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

package org.apache.paimon.spark.sql

import org.apache.spark.SPARK_VERSION

trait SparkVersionSupport {

  lazy val gteqSpark3_3: Boolean = SparkVersionSupport.gteqSpark3_3

  lazy val gteqSpark3_4: Boolean = SparkVersionSupport.gteqSpark3_4

  lazy val gteqSpark3_5: Boolean = SparkVersionSupport.gteqSpark3_5

  lazy val gteqSpark4_0: Boolean = SparkVersionSupport.gteqSpark4_0
}

object SparkVersionSupport {

  val sparkVersion: String = SPARK_VERSION

  val gteqSpark3_3: Boolean = sparkVersion >= "3.3"

  val gteqSpark3_4: Boolean = sparkVersion >= "3.4"

  val gteqSpark3_5: Boolean = sparkVersion >= "3.5"

  val gteqSpark4_0: Boolean = sparkVersion >= "4.0"

}
