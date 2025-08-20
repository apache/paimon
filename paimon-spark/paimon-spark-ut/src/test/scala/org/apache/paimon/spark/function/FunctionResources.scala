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

package org.apache.paimon.spark.function

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}

object FunctionResources {

  val testUDFJarPath: String =
    getClass.getClassLoader.getResource("function/hive-test-udfs.jar").getPath

  val UDFExampleAdd2Class: String = "org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd2"

  val MyIntSumClass: String = "org.apache.paimon.spark.function.MyIntSum"
}

class MyIntSum extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = new StructType().add("input", IntegerType)

  override def bufferSchema: StructType = new StructType().add("buffer", IntegerType)

  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getInt(0) + input.getInt(0))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0))
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getInt(0)
  }
}
