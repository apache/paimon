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

import org.apache.paimon.spark.{PaimonAppendBucketedTableTest, PaimonAppendNonBucketTableTest, PaimonPrimaryKeyBucketedTableTest, PaimonPrimaryKeyNonBucketTableTest}

import org.apache.spark.SparkConf

class MergeIntoPrimaryKeyBucketedTableTest
  extends MergeIntoTableTestBase
  with MergeIntoPrimaryKeyTableTest
  with MergeIntoNotMatchedBySourceTest
  with PaimonPrimaryKeyBucketedTableTest {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "false")
  }
}

class MergeIntoPrimaryKeyNonBucketTableTest
  extends MergeIntoTableTestBase
  with MergeIntoPrimaryKeyTableTest
  with MergeIntoNotMatchedBySourceTest
  with PaimonPrimaryKeyNonBucketTableTest {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "false")
  }
}

class MergeIntoAppendBucketedTableTest
  extends MergeIntoTableTestBase
  with MergeIntoAppendTableTest
  with MergeIntoNotMatchedBySourceTest
  with PaimonAppendBucketedTableTest {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "false")
  }
}

class MergeIntoAppendNonBucketedTableTest
  extends MergeIntoTableTestBase
  with MergeIntoAppendTableTest
  with MergeIntoNotMatchedBySourceTest
  with PaimonAppendNonBucketTableTest {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "false")
  }
}

class V2MergeIntoPrimaryKeyBucketedTableTest
  extends MergeIntoTableTestBase
  with MergeIntoPrimaryKeyTableTest
  with MergeIntoNotMatchedBySourceTest
  with PaimonPrimaryKeyBucketedTableTest {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "true")
  }
}

class V2MergeIntoPrimaryKeyNonBucketTableTest
  extends MergeIntoTableTestBase
  with MergeIntoPrimaryKeyTableTest
  with MergeIntoNotMatchedBySourceTest
  with PaimonPrimaryKeyNonBucketTableTest {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "true")
  }
}

class V2MergeIntoAppendBucketedTableTest
  extends MergeIntoTableTestBase
  with MergeIntoAppendTableTest
  with MergeIntoNotMatchedBySourceTest
  with PaimonAppendBucketedTableTest {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "true")
  }
}

class V2MergeIntoAppendNonBucketedTableTest
  extends MergeIntoTableTestBase
  with MergeIntoAppendTableTest
  with MergeIntoNotMatchedBySourceTest
  with PaimonAppendNonBucketTableTest {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "true")
  }
}
