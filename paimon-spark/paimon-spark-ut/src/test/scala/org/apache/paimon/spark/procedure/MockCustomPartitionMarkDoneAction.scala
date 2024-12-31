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

package org.apache.paimon.spark.procedure

import org.apache.paimon.partition.actions.PartitionMarkDoneAction

import java.util

/** The case class is only applicable for {@link MarkPartitionDoneProcedureTest}. */
case class MockCustomPartitionMarkDoneAction() extends PartitionMarkDoneAction {

  override def markDone(partition: String): Unit = {
    MockCustomPartitionMarkDoneAction.add(partition)
  }

  override def close(): Unit = {}
}

object MockCustomPartitionMarkDoneAction {
  val markedDonePartitions = new util.HashSet[String]

  def add(partition: String): Unit = {
    markedDonePartitions.add(partition)
  }

  def getMarkedDonePartitions: util.Set[String] = {
    markedDonePartitions
  }
}
