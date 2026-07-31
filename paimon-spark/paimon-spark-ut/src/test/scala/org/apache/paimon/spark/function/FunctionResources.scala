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

import org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd2
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}

import java.io.{File, FileOutputStream}
import java.util.jar.{JarEntry, JarOutputStream}

object FunctionResources {

  val UDFExampleAdd2Class: String = "org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd2"

  val testUDFJarPath: String = {
    val classResource = UDFExampleAdd2Class.replace('.', '/') + ".class"
    val input = classOf[UDFExampleAdd2].getClassLoader.getResourceAsStream(classResource)
    val jarFile = File.createTempFile("hive-test-udfs", ".jar")
    jarFile.deleteOnExit()

    val output = new JarOutputStream(new FileOutputStream(jarFile))
    try {
      output.putNextEntry(new JarEntry(classResource))
      val buffer = new Array[Byte](8192)
      var bytesRead = input.read(buffer)
      while (bytesRead != -1) {
        output.write(buffer, 0, bytesRead)
        bytesRead = input.read(buffer)
      }
      output.closeEntry()
    } finally {
      input.close()
      output.close()
    }

    jarFile.getAbsolutePath
  }

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
