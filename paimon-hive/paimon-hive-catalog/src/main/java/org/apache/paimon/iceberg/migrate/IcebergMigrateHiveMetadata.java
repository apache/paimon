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

package org.apache.paimon.iceberg.migrate;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.client.ClientPool;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.hive.pool.CachedClientPool;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Get iceberg table latest snapshot metadata in hive. */
public class IcebergMigrateHiveMetadata implements IcebergMigrateMetadata {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergMigrateHiveMetadata.class);

    public static final String TABLE_TYPE_PROP = "table_type";
    public static final String ICEBERG_TABLE_TYPE_VALUE = "iceberg";
    private static final String ICEBERG_METADATA_LOCATION = "metadata_location";

    private final FileIO fileIO;
    private final Identifier icebergIdentifier;

    private final ClientPool<IMetaStoreClient, TException> clients;

    private String metadataLocation = null;

    public IcebergMigrateHiveMetadata(
            Identifier icebergIdentifier, FileIO fileIO, Options icebergOptions) {
        this.fileIO = fileIO;
        this.icebergIdentifier = icebergIdentifier;

        String uri = icebergOptions.get(IcebergOptions.URI);
        String hiveConfDir = icebergOptions.get(IcebergOptions.HIVE_CONF_DIR);
        String hadoopConfDir = icebergOptions.get(IcebergOptions.HADOOP_CONF_DIR);
        Configuration hadoopConf = new Configuration();
        hadoopConf.setClassLoader(IcebergMigrateHiveMetadata.class.getClassLoader());
        HiveConf hiveConf = HiveCatalog.createHiveConf(hiveConfDir, hadoopConfDir, hadoopConf);

        icebergOptions.toMap().forEach(hiveConf::set);
        if (uri != null) {
            hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, uri);
        }

        if (hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname) == null) {
            LOG.error(
                    "Can't find hive metastore uri to connect: "
                            + "either set {} in iceberg options or set hive.metastore.uris "
                            + "in hive-site.xml or hadoop configurations. "
                            + "Will use empty metastore uris, which means we may use a embedded metastore. "
                            + "Please make sure hive metastore uri for iceberg table is correctly set as expected.",
                    IcebergOptions.URI.key());
        }

        this.clients =
                new CachedClientPool(
                        hiveConf,
                        icebergOptions,
                        icebergOptions.getString(IcebergOptions.HIVE_CLIENT_CLASS));
    }

    @Override
    public IcebergMetadata icebergMetadata() {
        try {
            boolean isExist = tableExists(icebergIdentifier);
            if (!isExist) {
                throw new RuntimeException(
                        String.format(
                                "iceberg table %s is not existed in hive metastore",
                                icebergIdentifier));
            }
            Table icebergHiveTable =
                    clients.run(
                            client ->
                                    client.getTable(
                                            icebergIdentifier.getDatabaseName(),
                                            icebergIdentifier.getTableName()));
            // TODO:Is this check necessary?
            // check whether it is an iceberg table
            String tableType = icebergHiveTable.getParameters().get(TABLE_TYPE_PROP);
            Preconditions.checkArgument(
                    tableType != null && tableType.equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE),
                    "not an iceberg table: %s (table-type=%s)",
                    icebergIdentifier.toString(),
                    tableType);

            metadataLocation = icebergHiveTable.getParameters().get(ICEBERG_METADATA_LOCATION);
            LOG.info("iceberg latest metadata location: {}", metadataLocation);

            return IcebergMetadata.fromPath(fileIO, new Path(metadataLocation));
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to read Iceberg metadata from path %s", metadataLocation),
                    e);
        }
    }

    @Override
    public String icebergLatestMetadataLocation() {
        return metadataLocation;
    }

    @Override
    public void deleteOriginTable() {
        LOG.info("Iceberg table in hive to be deleted:{}", icebergIdentifier.toString());
        try {
            clients.run(
                    client -> {
                        client.dropTable(
                                icebergIdentifier.getDatabaseName(),
                                icebergIdentifier.getTableName(),
                                true,
                                true);
                        return null;
                    });
        } catch (Exception e) {
            LOG.warn(
                    "exception occurred when deleting origin table, exception message:{}",
                    e.getMessage());
        }
    }

    private boolean tableExists(Identifier identifier) throws Exception {
        return clients.run(
                client ->
                        client.tableExists(
                                identifier.getDatabaseName(), identifier.getTableName()));
    }
}
