/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.hive;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.store.file.catalog.Catalog;
import org.apache.flink.table.store.file.schema.DataField;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.thrift.TException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/** A catalog implementation for Hive. */
public class HiveCatalog implements Catalog {

    // we don't include flink-table-store-hive-mr as dependencies because it depends on hive-exec
    private static final String INPUT_FORMAT_CLASS_NAME =
            "org.apache.flink.table.store.mapred.TableStoreInputFormat";
    private static final String OUTPUT_FORMAT_CLASS_NAME =
            "org.apache.flink.table.store.mapred.TableStoreOutputFormat";
    private static final String SERDE_CLASS_NAME =
            "org.apache.flink.table.store.hive.TableStoreSerDe";
    private static final String STORAGE_HANDLER_CLASS_NAME =
            "org.apache.flink.table.store.hive.TableStoreHiveStorageHandler";

    private final HiveConf hiveConf;
    private final IMetaStoreClient client;

    public HiveCatalog(String thriftUri, String warehousePath) {
        Configuration conf = new Configuration();
        conf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUri);
        conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehousePath);
        this.hiveConf = new HiveConf(conf, HiveConf.class);
        try {
            IMetaStoreClient client =
                    RetryingMetaStoreClient.getProxy(
                            hiveConf, tbl -> null, HiveMetaStoreClient.class.getName());
            this.client =
                    StringUtils.isNullOrWhitespaceOnly(thriftUri)
                            ? client
                            : HiveMetaStoreClient.newSynchronizedClient(client);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> listDatabases() {
        try {
            return client.getAllDatabases();
        } catch (TException e) {
            throw new RuntimeException("Failed to list all databases", e);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) {
        try {
            client.getDatabase(databaseName);
            return true;
        } catch (NoSuchObjectException e) {
            return false;
        } catch (TException e) {
            throw new RuntimeException(
                    "Failed to determine if database " + databaseName + " exists", e);
        }
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        try {
            client.createDatabase(convertToDatabase(name));
        } catch (AlreadyExistsException e) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(name, e);
            }
        } catch (TException e) {
            throw new RuntimeException("Failed to create database " + name, e);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        try {
            if (!cascade && client.getAllTables(name).size() > 0) {
                throw new DatabaseNotEmptyException(name);
            }
            client.dropDatabase(name, true, false, true);
        } catch (NoSuchObjectException | UnknownDBException e) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(name, e);
            }
        } catch (TException e) {
            throw new RuntimeException("Failed to drop database " + name, e);
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        try {
            return client.getAllTables(databaseName);
        } catch (UnknownDBException e) {
            throw new DatabaseNotExistException(databaseName, e);
        } catch (TException e) {
            throw new RuntimeException("Failed to list all tables in database " + databaseName, e);
        }
    }

    @Override
    public Path getTableLocation(ObjectPath tablePath) {
        return new Path(
                getDatabaseLocation(tablePath.getDatabaseName()), tablePath.getObjectName());
    }

    @Override
    public TableSchema getTable(ObjectPath tablePath) throws TableNotExistException {
        if (isTableStoreTableNotExisted(tablePath)) {
            throw new TableNotExistException(tablePath);
        }
        Path tableLocation = getTableLocation(tablePath);
        return new SchemaManager(tableLocation)
                .latest()
                .orElseThrow(
                        () -> new RuntimeException("There is no table stored in " + tableLocation));
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) {
        try {
            client.getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
            return true;
        } catch (NoSuchObjectException e) {
            return false;
        } catch (TException e) {
            throw new RuntimeException(
                    "Failed to determine if table " + tablePath.getFullName() + " exists", e);
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException {
        if (isTableStoreTableNotExisted(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new TableNotExistException(tablePath);
            }
        }

        try {
            client.dropTable(
                    tablePath.getDatabaseName(), tablePath.getObjectName(), true, false, true);
        } catch (TException e) {
            throw new RuntimeException("Failed to drop table " + tablePath.getFullName(), e);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, UpdateSchema updateSchema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        String databaseName = tablePath.getDatabaseName();
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(databaseName);
        }
        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return;
            } else {
                throw new TableAlreadyExistException(tablePath);
            }
        }

        // first commit changes to underlying files
        // if changes on Hive fails there is no harm to perform the same changes to files again
        TableSchema schema = commitToUnderlyingFiles(tablePath, updateSchema);
        Table table = newHmsTable(tablePath);
        updateHmsTable(table, tablePath, schema);
        try {
            client.createTable(table);
        } catch (TException e) {
            throw new RuntimeException("Failed to create table " + tablePath.getFullName(), e);
        }
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, UpdateSchema updateSchema, boolean ignoreIfNotExists)
            throws TableNotExistException {
        if (isTableStoreTableNotExisted(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new TableNotExistException(tablePath);
            }
        }

        // first commit changes to underlying files
        // if changes on Hive fails there is no harm to perform the same changes to files again
        TableSchema schema = commitToUnderlyingFiles(tablePath, updateSchema);
        try {
            Table table = client.getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
            updateHmsTable(table, tablePath, schema);
            client.alter_table(tablePath.getDatabaseName(), tablePath.getObjectName(), table);
        } catch (TException e) {
            throw new RuntimeException("Failed to alter table " + tablePath.getFullName(), e);
        }
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    private String getWarehouseRoot() {
        return hiveConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
    }

    private Path getDatabaseLocation(String name) {
        return new Path(getWarehouseRoot(), name + ".db");
    }

    private Database convertToDatabase(String name) {
        Database database = new Database();
        database.setName(name);
        database.setLocationUri(getDatabaseLocation(name).toString());
        return database;
    }

    private Table newHmsTable(ObjectPath tablePath) {
        long currentTimeMillis = System.currentTimeMillis();
        Table table =
                new Table(
                        tablePath.getObjectName(),
                        tablePath.getDatabaseName(),
                        // current linux user
                        System.getProperty("user.name"),
                        (int) (currentTimeMillis / 1000),
                        (int) (currentTimeMillis / 1000),
                        Integer.MAX_VALUE,
                        null,
                        Collections.emptyList(),
                        new HashMap<>(),
                        null,
                        null,
                        TableType.MANAGED_TABLE.toString());
        table.getParameters()
                .put(hive_metastoreConstants.META_TABLE_STORAGE, STORAGE_HANDLER_CLASS_NAME);
        return table;
    }

    private void updateHmsTable(Table table, ObjectPath tablePath, TableSchema schema) {
        StorageDescriptor sd = convertToStorageDescriptor(tablePath, schema);
        table.setSd(sd);
    }

    private StorageDescriptor convertToStorageDescriptor(ObjectPath tablePath, TableSchema schema) {
        StorageDescriptor sd = new StorageDescriptor();

        sd.setCols(
                schema.fields().stream()
                        .map(this::convertToFieldSchema)
                        .collect(Collectors.toList()));
        sd.setLocation(getTableLocation(tablePath).toString());

        sd.setInputFormat(INPUT_FORMAT_CLASS_NAME);
        sd.setOutputFormat(OUTPUT_FORMAT_CLASS_NAME);

        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setParameters(new HashMap<>());
        serDeInfo.setSerializationLib(SERDE_CLASS_NAME);
        sd.setSerdeInfo(serDeInfo);

        return sd;
    }

    private FieldSchema convertToFieldSchema(DataField dataField) {
        return new FieldSchema(
                dataField.name(),
                HiveTypeUtils.logicalTypeToTypeInfo(dataField.type().logicalType()).getTypeName(),
                dataField.description());
    }

    private boolean isTableStoreTableNotExisted(ObjectPath tablePath) {
        Table table;
        try {
            table = client.getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
        } catch (NoSuchObjectException e) {
            return true;
        } catch (TException e) {
            throw new RuntimeException(
                    "Cannot determine if table "
                            + tablePath.getFullName()
                            + " is a table store table.",
                    e);
        }

        if (!INPUT_FORMAT_CLASS_NAME.equals(table.getSd().getInputFormat())
                || !OUTPUT_FORMAT_CLASS_NAME.equals(table.getSd().getOutputFormat())) {
            throw new IllegalArgumentException(
                    "Table "
                            + tablePath.getFullName()
                            + " is not a table store table. It's input format is "
                            + table.getSd().getInputFormat()
                            + " and its output format is "
                            + table.getSd().getOutputFormat());
        }
        return false;
    }

    private TableSchema commitToUnderlyingFiles(ObjectPath tablePath, UpdateSchema schema) {
        Path path = getTableLocation(tablePath);
        try {
            return new SchemaManager(path).commitNewVersion(schema);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to commit changes of table "
                            + tablePath.getFullName()
                            + " to underlying files",
                    e);
        }
    }
}
