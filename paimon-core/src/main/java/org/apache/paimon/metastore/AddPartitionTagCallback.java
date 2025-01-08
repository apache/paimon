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

import org.apache.paimon.table.PartitionHandler;
import org.apache.paimon.table.sink.TagCallback;

import java.util.Collections;
import java.util.LinkedHashMap;

/** A {@link TagCallback} to add newly created partitions to metastore. */
public class AddPartitionTagCallback implements TagCallback {

    private final PartitionHandler partitionHandler;
    private final String partitionField;

    public AddPartitionTagCallback(PartitionHandler partitionHandler, String partitionField) {
        this.partitionHandler = partitionHandler;
        this.partitionField = partitionField;
    }

    @Override
    public void notifyCreation(String tagName) {
        LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
        partitionSpec.put(partitionField, tagName);
        try {
            partitionHandler.createPartitions(Collections.singletonList(partitionSpec));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void notifyDeletion(String tagName) {
        LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
        partitionSpec.put(partitionField, tagName);
        try {
            partitionHandler.dropPartitions(Collections.singletonList(partitionSpec));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        partitionHandler.close();
    }
}
