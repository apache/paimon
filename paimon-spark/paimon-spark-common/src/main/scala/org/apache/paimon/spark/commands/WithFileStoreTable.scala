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

package org.apache.paimon.spark.commands

import org.apache.paimon.{CoreOptions, FileStore}
import org.apache.paimon.fs.Path
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.RowType

private[spark] trait WithFileStoreTable {

  def table: FileStoreTable

  def root: Path = table.tableDataPath()

  def withPrimaryKeys: Boolean = !table.primaryKeys().isEmpty

  def rowType: RowType = table.rowType()

  def coreOptions: CoreOptions = table.coreOptions()

  def fileStore: FileStore[_] = table.store()

  def deletionVectorsEnabled: Boolean = coreOptions.deletionVectorsEnabled()
}
