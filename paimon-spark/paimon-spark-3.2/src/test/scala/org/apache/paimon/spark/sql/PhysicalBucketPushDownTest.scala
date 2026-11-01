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

import org.apache.paimon.spark.{PhysicalBucketFilter, SparkFilterConverter}
import org.apache.paimon.spark.schema.PaimonMetadataColumn.BUCKET_COLUMN

import org.apache.spark.sql.sources.{And, EqualNullSafe, EqualTo, In, Not, Or}

class PhysicalBucketPushDownTest extends PhysicalBucketPushDownTestBase {

  test("preserve NULL semantics in V1 compound bucket filters") {
    // SQL may fold these predicates before pushdown, so exercise the V1 converter directly.
    val converter = new SparkFilterConverter(PhysicalBucketFilter.rowType)
    val in = In(BUCKET_COLUMN, Array[Any](1, null))
    val equalNull = EqualTo(BUCKET_COLUMN, null)
    val nullSafe = EqualNullSafe(BUCKET_COLUMN, null)
    val equalThree = EqualTo(BUCKET_COLUMN, 3)
    Seq(
      in -> Set(1),
      Not(in) -> Set.empty[Int],
      equalNull -> Set.empty[Int],
      Not(equalNull) -> Set.empty[Int],
      nullSafe -> Set.empty[Int],
      Not(nullSafe) -> Set(-1, 0, 1, 2, 3),
      Or(in, equalThree) -> Set(1, 3),
      Not(Or(in, equalThree)) -> Set.empty[Int],
      Not(And(in, equalThree)) -> Set(-1, 0, 1, 2)
    ).foreach {
      case (source, expected) =>
        val filter = PhysicalBucketFilter(converter.convert(source))
        assert((-1 to 3).filter(bucket => filter.test(bucket)).toSet == expected, source.toString)
    }
  }
}
