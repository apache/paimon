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

package org.apache.paimon.partition.actions;

import org.apache.paimon.fs.Path;
import org.apache.paimon.table.PartitionMarkDone;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;

import static org.apache.paimon.utils.PartitionPathUtils.extractPartitionSpecFromPath;

/** A {@link PartitionMarkDoneAction} which add done in metastore. */
public class MarkPartitionDoneEventAction implements PartitionMarkDoneAction {

    private final PartitionMarkDone partitionMarkDone;

    public MarkPartitionDoneEventAction(PartitionMarkDone partitionMarkDone) {
        this.partitionMarkDone = partitionMarkDone;
    }

    @Override
    public void markDone(String partition) throws Exception {
        LinkedHashMap<String, String> partitionSpec =
                extractPartitionSpecFromPath(new Path(partition));
        partitionMarkDone.markDonePartitions(Collections.singletonList(partitionSpec));
    }

    @Override
    public void close() throws IOException {
        try {
            partitionMarkDone.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
