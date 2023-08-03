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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.RowDataPartitionComputer;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/** {@link MetastoreClient} for Hive tables. */
public class HiveMetastoreClient implements MetastoreClient {

    private final Identifier identifier;
    private final RowDataPartitionComputer partitionComputer;

    private final IMetaStoreClient client;

    private HiveMetastoreClient(Identifier identifier, TableSchema schema, IMetaStoreClient client)
            throws Exception {
        this.identifier = identifier;
        this.partitionComputer =
                new RowDataPartitionComputer(
                        new CoreOptions(schema.options()).partitionDefaultName(),
                        schema.logicalPartitionType(),
                        schema.partitionKeys().toArray(new String[0]));

        this.client = client;
    }

    private StorageDescriptor getCurrentSd() throws Exception {
        return client.getTable(identifier.getDatabaseName(), identifier.getObjectName()).getSd();
    }

    @Override
    public void addPartition(BinaryRow partition) throws Exception {
        LinkedHashMap<String, String> partitionMap =
                partitionComputer.generatePartValues(partition);
        List<String> partitionValues = new ArrayList<>(partitionMap.values());

        StorageDescriptor sd = getCurrentSd();

        try {
            Partition currentPartition = client.getPartition(
                    identifier.getDatabaseName(), identifier.getObjectName(), partitionValues);

            // update fields in partition metadata
            if (!CollectionUtils.isEqualCollection(currentPartition.getSd().getCols(), currentPartition.getSd().getCols())) {
                currentPartition.getSd().setCols(sd.getCols());
                client.alter_partition(identifier.getDatabaseName(), identifier.getObjectName(), currentPartition);
            }
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
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    /** Factory to create {@link HiveMetastoreClient}. */
    public static class Factory implements MetastoreClient.Factory {

        private static final long serialVersionUID = 1L;

        private final Identifier identifier;
        private final TableSchema schema;
        private final SerializableHiveConf hiveConf;
        private final String clientClassName;

        public Factory(
                Identifier identifier,
                TableSchema schema,
                HiveConf hiveConf,
                String clientClassName) {
            this.identifier = identifier;
            this.schema = schema;
            this.hiveConf = new SerializableHiveConf(hiveConf);
            this.clientClassName = clientClassName;
        }

        @Override
        public MetastoreClient create() {
            HiveConf conf = hiveConf.conf();
            try {
                return new HiveMetastoreClient(
                        identifier, schema, HiveCatalog.createClient(conf, clientClassName));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
