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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
 * A helper class for facilitating the comparison of Spark Row objects in Java unit tests, which
 * leverages QueryTest.checkAnswer for the comparison.
 */
class RowTestHelper extends QueryTest {
  override protected def spark: SparkSession = {
    throw new UnsupportedOperationException("Not supported")
  }
}

object RowTestHelper {
  def checkRowEquals(df: DataFrame, expectedRows: java.util.List[Row]): Unit = {
    checkAnswer(df, expectedRows)
  }

  def checkRowEquals(df: DataFrame, expectedRow: Row): Unit = {
    checkAnswer(df, Seq(expectedRow))
  }

  def row(values: Array[Any]): Row = {
    Row.fromSeq(values)
  }

  def seq(values: Array[Any]): Seq[Any] = values.toSeq
}
