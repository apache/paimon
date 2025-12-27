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

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SQLContext}
import org.apache.spark.sql.execution.streaming.Offset

import scala.util.Try

/**
 * A wrapper for MemoryStream to handle Spark version compatibility. In Spark 4.1+, MemoryStream was
 * moved from `org.apache.spark.sql.execution.streaming` to
 * `org.apache.spark.sql.execution.streaming.runtime`.
 */
class MemoryStreamWrapper[A] private (stream: AnyRef) {

  private val streamClass = stream.getClass

  def toDS(): Dataset[A] = {
    streamClass.getMethod("toDS").invoke(stream).asInstanceOf[Dataset[A]]
  }

  def toDF(): DataFrame = {
    streamClass.getMethod("toDF").invoke(stream).asInstanceOf[DataFrame]
  }

  def addData(data: A*): Offset = {
    val method = streamClass.getMethod("addData", classOf[TraversableOnce[_]])
    method.invoke(stream, data).asInstanceOf[Offset]
  }
}

object MemoryStreamWrapper {

  /** Creates a MemoryStream wrapper that works across different Spark versions. */
  def apply[A](implicit encoder: Encoder[A], sqlContext: SQLContext): MemoryStreamWrapper[A] = {
    val stream = createMemoryStream[A]
    new MemoryStreamWrapper[A](stream)
  }

  private def createMemoryStream[A](implicit
      encoder: Encoder[A],
      sqlContext: SQLContext): AnyRef = {
    // Try Spark 4.1+ path first (runtime package)
    val spark41Class = Try(
      Class.forName("org.apache.spark.sql.execution.streaming.runtime.MemoryStream$"))
    if (spark41Class.isSuccess) {
      val companion = spark41Class.get.getField("MODULE$").get(null)
      // Spark 4.1+ uses implicit SparkSession instead of SQLContext
      val applyMethod = companion.getClass.getMethod(
        "apply",
        classOf[Encoder[_]],
        classOf[org.apache.spark.sql.SparkSession]
      )
      return applyMethod.invoke(companion, encoder, sqlContext.sparkSession).asInstanceOf[AnyRef]
    }

    // Fallback to Spark 3.x / 4.0 path
    val oldClass =
      Class.forName("org.apache.spark.sql.execution.streaming.MemoryStream$")
    val companion = oldClass.getField("MODULE$").get(null)
    val applyMethod = companion.getClass.getMethod(
      "apply",
      classOf[Encoder[_]],
      classOf[SQLContext]
    )
    applyMethod.invoke(companion, encoder, sqlContext).asInstanceOf[AnyRef]
  }
}
