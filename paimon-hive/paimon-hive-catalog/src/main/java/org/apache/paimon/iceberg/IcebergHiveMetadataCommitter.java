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

package org.apache.paimon.iceberg;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.client.ClientPool;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.hive.HiveCatalogFactory;
import org.apache.paimon.hive.HiveTypeUtils;
import org.apache.paimon.hive.pool.CachedClientPool;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * {@link IcebergMetadataCommitter} to commit Iceberg metadata to Hive metastore, so the table can
 * be visited by Iceberg's Hive catalog.
 */
public class IcebergHiveMetadataCommitter implements IcebergMetadataCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergHiveMetadataCommitter.class);

    private final FileStoreTable table;
    private final Identifier identifier;
    private final ClientPool<IMetaStoreClient, TException> clients;

    public IcebergHiveMetadataCommitter(FileStoreTable table) {
        this.table = table;
        this.identifier =
                Preconditions.checkNotNull(
                        table.catalogEnvironment().identifier(),
                        "If you want to sync Paimon Iceberg compatible metadata to Hive, "
                                + "you must use a Paimon table created from a Paimon catalog, "
                                + "instead of a temporary table.");
        Preconditions.checkArgument(
                identifier.getBranchName() == null,
                "Paimon Iceberg compatibility currently does not support branches.");

        Options options = new Options(table.options());
        String uri = options.get(IcebergOptions.URI);
        String hiveConfDir = options.get(IcebergOptions.HIVE_CONF_DIR);
        String hadoopConfDir = options.get(IcebergOptions.HADOOP_CONF_DIR);
        Configuration hadoopConf = new Configuration();
        hadoopConf.setClassLoader(IcebergHiveMetadataCommitter.class.getClassLoader());
        HiveConf hiveConf = HiveCatalog.createHiveConf(hiveConfDir, hadoopConfDir, hadoopConf);

        table.options().forEach(hiveConf::set);
        if (uri != null) {
            hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, uri);
        }

        if (hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname) == null) {
            LOG.error(
                    "Can't find hive metastore uri to connect: "
                            + "either set {} for paimon table or set hive.metastore.uris "
                            + "in hive-site.xml or hadoop configurations. "
                            + "Will use empty metastore uris, which means we may use a embedded metastore. "
                            + "This may cause unpredictable consensus problem.",
                    IcebergOptions.URI.key());
        }

        this.clients =
                new CachedClientPool(
                        hiveConf,
                        options,
                        HiveCatalogFactory.METASTORE_CLIENT_CLASS.defaultValue());
    }

    @Override
    public void commitMetadata(Path newMetadataPath, @Nullable Path baseMetadataPath) {
        try {
            commitMetadataImpl(newMetadataPath, baseMetadataPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void commitMetadataImpl(Path newMetadataPath, @Nullable Path baseMetadataPath)
            throws Exception {
        if (!databaseExists(identifier.getDatabaseName())) {
            createDatabase(identifier.getDatabaseName());
        }

        Table hiveTable;
        if (tableExists(identifier)) {
            hiveTable =
                    clients.run(
                            client ->
                                    client.getTable(
                                            identifier.getDatabaseName(),
                                            identifier.getTableName()));
        } else {
            hiveTable = createTable(newMetadataPath);
        }

        hiveTable.getParameters().put("metadata_location", newMetadataPath.toString());
        if (baseMetadataPath != null) {
            hiveTable
                    .getParameters()
                    .put("previous_metadata_location", baseMetadataPath.toString());
        }

        clients.execute(
                client ->
                        client.alter_table(
                                identifier.getDatabaseName(),
                                identifier.getTableName(),
                                hiveTable,
                                true));
    }

    private boolean databaseExists(String databaseName) throws Exception {
        try {
            clients.run(client -> client.getDatabase(databaseName));
            return true;
        } catch (NoSuchObjectException ignore) {
            return false;
        }
    }

    private void createDatabase(String databaseName) throws Exception {
        Database database = new Database();
        database.setName(databaseName);
        clients.execute(client -> client.createDatabase(database));
    }

    private boolean tableExists(Identifier identifier) throws Exception {
        return clients.run(
                client ->
                        client.tableExists(
                                identifier.getDatabaseName(), identifier.getTableName()));
    }

    private Table createTable(Path metadataPath) throws Exception {
        long currentTimeMillis = System.currentTimeMillis();
        Table hiveTable =
                new Table(
                        identifier.getTableName(),
                        identifier.getDatabaseName(),
                        // current linux user
                        System.getProperty("user.name"),
                        (int) (currentTimeMillis / 1000),
                        (int) (currentTimeMillis / 1000),
                        Integer.MAX_VALUE,
                        new StorageDescriptor(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        null,
                        null,
                        "EXTERNAL_TABLE");

        hiveTable.getParameters().put("DO_NOT_UPDATE_STATS", "true");
        hiveTable.getParameters().put("EXTERNAL", "TRUE");
        hiveTable.getParameters().put("table_type", "ICEBERG");

        StorageDescriptor sd = hiveTable.getSd();
        sd.setLocation(metadataPath.getParent().getParent().toString());
        sd.setCols(
                table.schema().fields().stream()
                        .map(this::convertToFieldSchema)
                        .collect(Collectors.toList()));
        sd.setInputFormat("org.apache.hadoop.mapred.FileInputFormat");
        sd.setOutputFormat("org.apache.hadoop.mapred.FileOutputFormat");

        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        hiveTable.getSd().setSerdeInfo(serDeInfo);

        clients.execute(client -> client.createTable(hiveTable));
        return hiveTable;
    }

    private FieldSchema convertToFieldSchema(DataField dataField) {
        return new FieldSchema(
                dataField.name(),
                HiveTypeUtils.toTypeInfo(dataField.type()).getTypeName(),
                dataField.description());
    }
}
