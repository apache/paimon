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

package org.apache.paimon.hive;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.RowDataPartitionComputer;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Add all partitions extracted from the list of {@link ManifestCommittable} to Hive metastore and
 * ignore existing partitions.
 */
public class AddPartitionCommitCallback implements CommitCallback {

    private final Identifier identifier;
    private final RowDataPartitionComputer partitionComputer;

    private final IMetaStoreClient client;
    private final StorageDescriptor sd;

    public AddPartitionCommitCallback(
            Identifier identifier, FileStoreTable table, IMetaStoreClient client) {
        this.identifier = identifier;
        this.partitionComputer =
                new RowDataPartitionComputer(
                        table.coreOptions().partitionDefaultName(),
                        table.schema().logicalPartitionType(),
                        table.schema().partitionKeys().toArray(new String[0]));

        this.client = client;
        try {
            this.sd =
                    client.getTable(identifier.getDatabaseName(), identifier.getObjectName())
                            .getSd();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void call(List<ManifestCommittable> committables) {
        committables.stream()
                .flatMap(c -> c.fileCommittables().stream())
                .map(CommitMessage::partition)
                .distinct()
                .forEach(this::createPartitionIfNeeded);
    }

    private void createPartitionIfNeeded(BinaryRow partition) {
        LinkedHashMap<String, String> partitionMap =
                partitionComputer.generatePartValues(partition);
        List<String> partitionValues = new ArrayList<>(partitionMap.values());

        try {
            try {
                client.getPartition(
                        identifier.getDatabaseName(), identifier.getObjectName(), partitionValues);
                // do nothing if the partition already exists
            } catch (NoSuchObjectException e) {
                // partition not found, create new partition
                StorageDescriptor newSd = new StorageDescriptor(sd);
                newSd.setLocation(
                        sd.getLocation()
                                + "/"
                                + PartitionPathUtils.generatePartitionPath(partitionMap));

                Partition hivePartition = new Partition();
                hivePartition.setDbName(identifier.getDatabaseName());
                hivePartition.setTableName(identifier.getObjectName());
                hivePartition.setValues(partitionValues);
                hivePartition.setSd(newSd);
                int currentTime = (int) (System.currentTimeMillis() / 1000);
                hivePartition.setCreateTime(currentTime);
                hivePartition.setLastAccessTime(currentTime);

                client.add_partition(hivePartition);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    /** Factory to create {@link AddPartitionCommitCallback}. */
    public static class Factory implements CommitCallback.Factory {

        private static final long serialVersionUID = 1L;

        private final Identifier identifier;
        private final SerializableHiveConf hiveConf;
        private final String clientClassName;

        public Factory(Identifier identifier, HiveConf hiveConf, String clientClassName) {
            this.identifier = identifier;
            this.hiveConf = new SerializableHiveConf(hiveConf);
            this.clientClassName = clientClassName;
        }

        @Override
        public CommitCallback create(FileStoreTable table) {
            HiveConf conf = hiveConf.conf();
            return new AddPartitionCommitCallback(
                    identifier, table, HiveCatalog.createClient(conf, clientClassName));
        }
    }
}
