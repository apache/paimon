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

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SQLContext}

/**
 * Reflection-based wrapper around Spark's `MemoryStream`. Spark 4.1 moved the class from
 * `org.apache.spark.sql.execution.streaming.MemoryStream` to
 * `org.apache.spark.sql.execution.streaming.runtime.MemoryStream`, while Spark 4.0 still only has
 * the legacy location. `paimon-spark4-common` is shared between `paimon-spark-4.0` and
 * `paimon-spark-4.1`, so hard-coding either path into this class's bytecode would break
 * class-loading under the other patch version at test-discovery time.
 *
 * Using reflection keeps the shim's own bytecode version-agnostic while still exposing the same
 * `org.apache.spark.sql.paimon.shims.memstream.MemoryStream` import target that the shared Paimon
 * Spark unit tests use.
 */
object memstream {

  /**
   * Minimal surface of `org.apache.spark.sql.execution.streaming[.runtime].MemoryStream[A]` that
   * Paimon test code relies on. Additional methods can be added here and forwarded reflectively if
   * new tests need them.
   */
  trait MemoryStream[A] {
    def addData(data: A*): Any
    def toDS(): Dataset[A]
    def toDF(): DataFrame
  }

  object MemoryStream {
    def apply[A: Encoder](implicit sqlContext: SQLContext): MemoryStream[A] = {
      val companion = loadCompanion()
      // Spark 4.1 added a second 2-arg `apply(Encoder, SparkSession)` overload alongside the
      // existing `apply(Encoder, SQLContext)`, so filtering only on `parameterCount == 2` is
      // ambiguous — `Class#getMethods` ordering is JVM-dependent, and picking the wrong overload
      // produces `IllegalArgumentException: argument type mismatch` when we pass a `SQLContext`.
      // Pin the selection to the `(Encoder, SQLContext)` variant explicitly.
      val applyMethod = companion.getClass.getMethods
        .find {
          m =>
            m.getName == "apply" &&
            m.getParameterCount == 2 &&
            m.getParameterTypes()(1) == classOf[SQLContext]
        }
        .getOrElse(throw new NoSuchMethodError(
          "No apply(Encoder, SQLContext) found on " + companion.getClass))
      val encoder = implicitly[Encoder[A]].asInstanceOf[AnyRef]
      val underlying = applyMethod.invoke(companion, encoder, sqlContext).asInstanceOf[AnyRef]
      new ReflectiveMemoryStream[A](underlying)
    }

    private def loadCompanion(): AnyRef = {
      val runtimeName = "org.apache.spark.sql.execution.streaming.runtime.MemoryStream$"
      val legacyName = "org.apache.spark.sql.execution.streaming.MemoryStream$"
      val klass =
        try Class.forName(runtimeName)
        catch {
          case _: ClassNotFoundException => Class.forName(legacyName)
        }
      klass.getField("MODULE$").get(null)
    }
  }

  final private class ReflectiveMemoryStream[A](underlying: AnyRef) extends MemoryStream[A] {
    override def addData(data: A*): Any = {
      val method = underlying.getClass.getMethods
        .find(m => m.getName == "addData" && m.getParameterCount == 1)
        .getOrElse(throw new NoSuchMethodError(
          "No 1-arg MemoryStream#addData found on " + underlying.getClass))
      method.invoke(underlying, data.toSeq.asInstanceOf[AnyRef])
    }

    override def toDS(): Dataset[A] = {
      val method = underlying.getClass.getMethod("toDS")
      method.invoke(underlying).asInstanceOf[Dataset[A]]
    }

    override def toDF(): DataFrame = {
      val method = underlying.getClass.getMethod("toDF")
      method.invoke(underlying).asInstanceOf[DataFrame]
    }
  }
}
