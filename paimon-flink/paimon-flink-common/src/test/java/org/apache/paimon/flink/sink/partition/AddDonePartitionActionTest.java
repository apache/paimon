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

package org.apache.paimon.flink.sink.partition;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metastore.MetastoreClient;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.paimon.utils.PartitionPathUtils.generatePartitionPath;
import static org.assertj.core.api.Assertions.assertThat;

class AddDonePartitionActionTest {

    @Test
    public void test() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        Set<String> donePartitions = new HashSet<>();
        MetastoreClient metastoreClient =
                new MetastoreClient() {
                    @Override
                    public void addPartition(BinaryRow partition) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void addPartition(LinkedHashMap<String, String> partitionSpec) {
                        donePartitions.add(generatePartitionPath(partitionSpec));
                    }

                    @Override
                    public void deletePartition(LinkedHashMap<String, String> partitionSpec) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void close() throws Exception {
                        closed.set(true);
                    }
                };

        AddDonePartitionAction action = new AddDonePartitionAction(metastoreClient);

        // test normal
        action.markDone("dt=20201202");
        assertThat(donePartitions).containsExactlyInAnyOrder("dt=20201202.done/");

        // test multiple partition fields
        action.markDone("dt=20201202/hour=02");
        assertThat(donePartitions)
                .containsExactlyInAnyOrder("dt=20201202.done/", "dt=20201202/hour=02.done/");

        action.close();
        assertThat(closed).isTrue();
    }
}
