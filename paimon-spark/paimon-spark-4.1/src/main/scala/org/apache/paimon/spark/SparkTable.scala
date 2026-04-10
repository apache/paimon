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

import org.apache.paimon.table.Table

/**
 * Spark 4.1 shim for SparkTable.
 *
 * In Spark 4.1, RewriteMergeIntoTable / RewriteDeleteFromTable / RewriteUpdateTable were moved
 * into the Resolution batch, running BEFORE Paimon's post-hoc resolution rules. If SparkTable
 * implements SupportsRowLevelOperations, Spark's built-in rewrite rules match and rewrite
 * MergeIntoTable / DeleteFromTable / UpdateTable using the V2 write path, which Paimon's PK/DV
 * tables do not support.
 *
 * This shim removes SupportsRowLevelOperations so that MergeIntoTable.rewritable returns false,
 * preventing Spark's rewrite rules from matching. Paimon's post-hoc rules (PaimonMergeInto,
 * PaimonDeleteTable, PaimonUpdateTable) then handle these commands correctly.
 */
case class SparkTable(override val table: Table) extends PaimonSparkTableBase(table)
