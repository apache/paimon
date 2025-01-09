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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.client.ClientPool;
import org.apache.paimon.hive.pool.CachedClientPool;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.PartitionPathUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Catalog.LAST_UPDATE_TIME_PROP;
import static org.apache.paimon.catalog.Catalog.NUM_FILES_PROP;
import static org.apache.paimon.catalog.Catalog.NUM_ROWS_PROP;
import static org.apache.paimon.catalog.Catalog.TOTAL_SIZE_PROP;

/** {@link MetastoreClient} for Hive tables. */
public class HiveMetastoreClient implements MetastoreClient {

    private static final String HIVE_LAST_UPDATE_TIME_PROP = "transient_lastDdlTime";

    private final Identifier identifier;

    private final ClientPool<IMetaStoreClient, TException> clients;
    private final List<String> partitionKeys;
    private final StorageDescriptor sd;
    private final String dataFilePath;

    HiveMetastoreClient(Identifier identifier, ClientPool<IMetaStoreClient, TException> clients)
            throws TException, InterruptedException {
        this.identifier = identifier;
        this.clients = clients;
        Table table =
                this.clients.run(
                        client ->
                                client.getTable(
                                        identifier.getDatabaseName(), identifier.getTableName()));
        this.partitionKeys =
                table.getPartitionKeys().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList());
        this.sd = table.getSd();
        this.dataFilePath =
                table.getParameters().containsKey(CoreOptions.DATA_FILE_PATH_DIRECTORY.key())
                        ? sd.getLocation()
                                + "/"
                                + table.getParameters()
                                        .get(CoreOptions.DATA_FILE_PATH_DIRECTORY.key())
                        : sd.getLocation();
    }

    @Override
    public void addPartition(LinkedHashMap<String, String> partition) throws Exception {
        Partition hivePartition =
                toHivePartition(partition, (int) (System.currentTimeMillis() / 1000));
        clients.execute(
                client -> {
                    try {
                        client.add_partition(hivePartition);
                    } catch (AlreadyExistsException ignore) {
                    }
                });
    }

    @Override
    public void addPartitions(List<LinkedHashMap<String, String>> partitions) throws Exception {
        int currentTime = (int) (System.currentTimeMillis() / 1000);
        List<Partition> hivePartitions =
                partitions.stream()
                        .map(partitionSpec -> toHivePartition(partitionSpec, currentTime))
                        .collect(Collectors.toList());
        clients.execute(client -> client.add_partitions(hivePartitions, true, false));
    }

    @Override
    public void alterPartition(org.apache.paimon.partition.Partition partition) throws Exception {
        Map<String, String> spec = partition.spec();
        List<String> partitionValues =
                partitionKeys.stream().map(spec::get).collect(Collectors.toList());

        Map<String, String> statistic = new HashMap<>();
        statistic.put(NUM_FILES_PROP, String.valueOf(partition.fileCount()));
        statistic.put(TOTAL_SIZE_PROP, String.valueOf(partition.fileSizeInBytes()));
        statistic.put(NUM_ROWS_PROP, String.valueOf(partition.recordCount()));

        String modifyTimeSeconds = String.valueOf(partition.lastFileCreationTime() / 1000);
        statistic.put(LAST_UPDATE_TIME_PROP, modifyTimeSeconds);

        // just for being compatible with hive metastore
        statistic.put(HIVE_LAST_UPDATE_TIME_PROP, modifyTimeSeconds);

        try {
            Partition hivePartition =
                    clients.run(
                            client ->
                                    client.getPartition(
                                            identifier.getDatabaseName(),
                                            identifier.getObjectName(),
                                            partitionValues));
            hivePartition.setValues(partitionValues);
            hivePartition.setLastAccessTime((int) (partition.lastFileCreationTime() / 1000));
            hivePartition.getParameters().putAll(statistic);
            clients.execute(
                    client ->
                            client.alter_partition(
                                    identifier.getDatabaseName(),
                                    identifier.getObjectName(),
                                    hivePartition));
        } catch (NoSuchObjectException e) {
            // do nothing if the partition not exists
        }
    }

    @Override
    public void dropPartition(LinkedHashMap<String, String> partitionSpec) throws Exception {
        List<String> partitionValues = new ArrayList<>(partitionSpec.values());
        try {
            clients.execute(
                    client ->
                            client.dropPartition(
                                    identifier.getDatabaseName(),
                                    identifier.getTableName(),
                                    partitionValues,
                                    false));
        } catch (NoSuchObjectException e) {
            // do nothing if the partition not exists
        }
    }

    @Override
    public void dropPartitions(List<LinkedHashMap<String, String>> partitions) throws Exception {
        for (LinkedHashMap<String, String> partition : partitions) {
            dropPartition(partition);
        }
    }

    @Override
    public void markPartitionDone(LinkedHashMap<String, String> partitionSpec) throws Exception {
        try {
            clients.execute(
                    client ->
                            client.markPartitionForEvent(
                                    identifier.getDatabaseName(),
                                    identifier.getTableName(),
                                    partitionSpec,
                                    PartitionEventType.LOAD_DONE));
        } catch (NoSuchObjectException e) {
            // do nothing if the partition not exists
        }
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }

    public IMetaStoreClient client() throws TException, InterruptedException {
        return clients.run(client -> client);
    }

    private Partition toHivePartition(
            LinkedHashMap<String, String> partitionSpec, int currentTime) {
        Partition hivePartition = new Partition();
        StorageDescriptor newSd = new StorageDescriptor(sd);
        newSd.setLocation(
                dataFilePath + "/" + PartitionPathUtils.generatePartitionPath(partitionSpec));
        hivePartition.setDbName(identifier.getDatabaseName());
        hivePartition.setTableName(identifier.getTableName());
        hivePartition.setValues(new ArrayList<>(partitionSpec.values()));
        hivePartition.setSd(newSd);
        hivePartition.setCreateTime(currentTime);
        hivePartition.setLastAccessTime(currentTime);
        return hivePartition;
    }

    /** Factory to create {@link HiveMetastoreClient}. */
    public static class Factory implements MetastoreClient.Factory {

        private static final long serialVersionUID = 1L;

        private final Identifier identifier;
        private final SerializableHiveConf hiveConf;
        private final String clientClassName;
        private final Options options;

        public Factory(
                Identifier identifier, HiveConf hiveConf, String clientClassName, Options options) {
            this.identifier = identifier;
            this.hiveConf = new SerializableHiveConf(hiveConf);
            this.clientClassName = clientClassName;
            this.options = options;
        }

        @Override
        public MetastoreClient create() {
            HiveConf conf = hiveConf.conf();
            try {
                return new HiveMetastoreClient(
                        identifier, new CachedClientPool(conf, options, clientClassName));
            } catch (TException e) {
                throw new RuntimeException(
                        "Can not get table " + identifier + " info from metastore.", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(
                        "Interrupted in call to new HiveMetastoreClient for table " + identifier,
                        e);
            }
        }
    }
}
