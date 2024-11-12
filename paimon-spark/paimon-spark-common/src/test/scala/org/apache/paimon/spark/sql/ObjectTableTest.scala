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

import org.apache.paimon.fs.Path
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

class ObjectTableTest extends PaimonSparkTestBase {

  test(s"Paimon object table") {
    val objectLocation = new Path(tempDBDir + "/object-location")
    val fileIO = LocalFileIO.create

    spark.sql(s"""
                 |CREATE TABLE T TBLPROPERTIES (
                 |    'type' = 'object-table',
                 |    'object-location' = '$objectLocation'
                 |)
                 |""".stripMargin)

    // add new file
    fileIO.overwriteFileUtf8(new Path(objectLocation, "f0"), "1,2,3")
    spark.sql("CALL sys.refresh_object_table('test.T')")
    checkAnswer(
      spark.sql("SELECT name, length FROM T"),
      Row("f0", 5L) :: Nil
    )

    // add new file
    fileIO.overwriteFileUtf8(new Path(objectLocation, "f1"), "4,5,6")
    spark.sql("CALL sys.refresh_object_table('test.T')")
    checkAnswer(
      spark.sql("SELECT name, length FROM T"),
      Row("f0", 5L) :: Row("f1", 5L) :: Nil
    )

    // time travel
    checkAnswer(
      spark.sql("SELECT name, length FROM T VERSION AS OF 1"),
      Row("f0", 5L) :: Nil
    )
  }
}
