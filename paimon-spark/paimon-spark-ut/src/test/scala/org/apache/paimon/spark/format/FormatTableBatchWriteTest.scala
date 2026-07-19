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

import org.apache.paimon.spark.write.FormatTableWriteTaskResult
import org.apache.paimon.table.FormatTable
import org.apache.paimon.table.sink.{BatchTableCommit, BatchWriteBuilder}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.types.StructType

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.concurrent.atomic.AtomicInteger

class FormatTableBatchWriteTest extends SparkFunSuite {

  test("abort after commit starts does not clean up potentially committed files") {
    val commitCalls = new AtomicInteger
    val abortCalls = new AtomicInteger
    val batchWrite =
      new FormatTableBatchWrite(formatTable(commitCalls, abortCalls), None, None, StructType(Nil))
    val messages: Array[WriterCommitMessage] = Array(FormatTableWriteTaskResult(Seq.empty))

    val error = intercept[RuntimeException] {
      batchWrite.commit(messages)
    }
    assert(error.getMessage == "partition registration response lost")

    // Spark invokes abort after commit throws. The commit outcome may be ambiguous, so cleanup
    // could delete files which were already committed and registered remotely.
    batchWrite.abort(messages)

    assert(commitCalls.get == 1)
    assert(abortCalls.get == 0)
  }

  test("abort before commit delegates cleanup") {
    val commitCalls = new AtomicInteger
    val abortCalls = new AtomicInteger
    val batchWrite =
      new FormatTableBatchWrite(formatTable(commitCalls, abortCalls), None, None, StructType(Nil))

    batchWrite.abort(Array(FormatTableWriteTaskResult(Seq.empty)))

    assert(commitCalls.get == 0)
    assert(abortCalls.get == 1)
  }

  private def formatTable(commitCalls: AtomicInteger, abortCalls: AtomicInteger): FormatTable = {
    val tableCommit = proxy(classOf[BatchTableCommit]) {
      case "commit" =>
        commitCalls.incrementAndGet()
        throw new RuntimeException("partition registration response lost")
      case "abort" =>
        abortCalls.incrementAndGet()
        null
    }
    val writeBuilder = proxy(classOf[BatchWriteBuilder]) { case "newCommit" => tableCommit }
    proxy(classOf[FormatTable]) {
      case "newBatchWriteBuilder" => writeBuilder
      case "name" => "test_db.format_table"
    }
  }

  private def proxy[T](clazz: Class[T])(responses: PartialFunction[String, AnyRef]): T = {
    Proxy
      .newProxyInstance(
        clazz.getClassLoader,
        Array(clazz),
        new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
            responses.applyOrElse(
              method.getName,
              (name: String) =>
                throw new UnsupportedOperationException(s"Unexpected $name invocation"))
          }
        }
      )
      .asInstanceOf[T]
  }
}
