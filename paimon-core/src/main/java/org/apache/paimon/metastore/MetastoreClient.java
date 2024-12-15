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

package org.apache.paimon.metastore;

import org.apache.paimon.data.BinaryRow;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A metastore client related to a table. All methods of this interface operate on the same specific
 * table.
 */
public interface MetastoreClient extends AutoCloseable {

    void addPartition(BinaryRow partition) throws Exception;

    default void addPartitions(List<BinaryRow> partitions) throws Exception {
        for (BinaryRow partition : partitions) {
            addPartition(partition);
        }
    }

    void addPartition(LinkedHashMap<String, String> partitionSpec) throws Exception;

    default void addPartitionsSpec(List<LinkedHashMap<String, String>> partitionSpecsList)
            throws Exception {
        for (LinkedHashMap<String, String> partitionSpecs : partitionSpecsList) {
            addPartition(partitionSpecs);
        }
    }

    void deletePartition(LinkedHashMap<String, String> partitionSpec) throws Exception;

    void markDone(LinkedHashMap<String, String> partitionSpec) throws Exception;

    default void alterPartition(
            LinkedHashMap<String, String> partitionSpec,
            Map<String, String> parameters,
            long modifyTime,
            boolean ignoreIfNotExist)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    /** Factory to create {@link MetastoreClient}. */
    interface Factory extends Serializable {

        MetastoreClient create();
    }
}
