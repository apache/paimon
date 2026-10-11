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

package org.apache.paimon.table.source;

/** Table scan for data table. */
public interface DataTableScan extends InnerTableScan {

    /** Specify the shard to be read, and allocate sharded files to read records. */
    DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks);

    /**
     * Stops the partition part of an authorization rule from pruning here, for a scan whose
     * physical partitions are not the ones the rule speaks about — a chain table maps a branch
     * partition onto a different logical one.
     *
     * <p>Only for a scan whose splits are then read, which applies the whole rule anyway. {@link
     * #listPartitionEntries} answers the caller directly and must keep the pruning.
     */
    default DataTableScan withoutAuthPartitionPushdown() {
        return this;
    }
}
