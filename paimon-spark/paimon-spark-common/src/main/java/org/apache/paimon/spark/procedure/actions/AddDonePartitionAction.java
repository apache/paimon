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

package org.apache.paimon.spark.procedure.actions;

import org.apache.paimon.fs.Path;
import org.apache.paimon.metastore.MetastoreClient;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.paimon.utils.PartitionPathUtils.extractPartitionSpecFromPath;

/** A {@link PartitionMarkDoneAction} which add ".done" partition. */
public class AddDonePartitionAction implements PartitionMarkDoneAction {
    private final MetastoreClient metastoreClient;

    public AddDonePartitionAction(MetastoreClient metastoreClient) {
        this.metastoreClient = metastoreClient;
    }

    @Override
    public void markDone(String partition) throws Exception {
        LinkedHashMap<String, String> doneSpec = extractPartitionSpecFromPath(new Path(partition));
        Map.Entry<String, String> lastField = tailEntry(doneSpec);
        doneSpec.put(lastField.getKey(), lastField.getValue() + ".done");
        metastoreClient.addPartition(doneSpec);
    }

    private Map.Entry<String, String> tailEntry(LinkedHashMap<String, String> partitionSpec) {
        return Iterators.getLast(partitionSpec.entrySet().iterator());
    }

    @Override
    public void close() throws IOException {
        try {
            metastoreClient.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
