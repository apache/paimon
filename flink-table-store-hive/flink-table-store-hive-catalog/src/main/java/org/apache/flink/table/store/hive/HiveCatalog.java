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
import org.apache.flink.table.store.file.catalog.AbstractCatalog;
import org.apache.flink.table.store.file.catalog.CatalogLock;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.file.schema.DataField;
import org.apache.flink.table.store.file.schema.SchemaChange;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.table.TableType;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
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
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.CatalogOptions.LOCK_ENABLED;
import static org.apache.flink.table.store.CatalogOptions.TABLE_TYPE;
import static org.apache.flink.table.store.hive.HiveCatalogLock.acquireTimeout;
import static org.apache.flink.table.store.hive.HiveCatalogLock.checkMaxSleep;

/** A catalog implementation for Hive. */
public class HiveCatalog extends AbstractCatalog {

    // we don't include flink-table-store-hive-connector as dependencies because it depends on
    // hive-exec
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

    public HiveCatalog(Configuration hadoopConfig) {
        this.hiveConf = new HiveConf(hadoopConfig, HiveConf.class);
        this.client = createClient(hiveConf);
    }

    @Override
    public Optional<CatalogLock.Factory> lockFactory() {
        return lockEnabled()
                ? Optional.of(HiveCatalogLock.createFactory(hiveConf))
                : Optional.empty();
    }

    private boolean lockEnabled() {
        return Boolean.parseBoolean(
                hiveConf.get(LOCK_ENABLED.key(), LOCK_ENABLED.defaultValue().toString()));
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
            return client.getAllTables(databaseName).stream()
                    .parallel()
                    .filter(
                            tableName ->
                                    tableStoreTableExists(
                                            new ObjectPath(databaseName, tableName), false))
                    .collect(Collectors.toList());
        } catch (UnknownDBException e) {
            throw new DatabaseNotExistException(databaseName, e);
        } catch (TException e) {
            throw new RuntimeException("Failed to list all tables in database " + databaseName, e);
        }
    }

    @Override
    public TableSchema getTableSchema(ObjectPath tablePath) throws TableNotExistException {
        if (!tableStoreTableExists(tablePath)) {
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
        return tableStoreTableExists(tablePath);
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException {
        if (!tableStoreTableExists(tablePath)) {
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
        TableSchema schema;
        try {
            schema = schemaManager(tablePath).commitNewVersion(updateSchema);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to commit changes of table "
                            + tablePath.getFullName()
                            + " to underlying files",
                    e);
        }
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
            ObjectPath tablePath, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException {
        if (!tableStoreTableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new TableNotExistException(tablePath);
            }
        }

        try {
            // first commit changes to underlying files
            TableSchema schema = schemaManager(tablePath).commitChanges(changes);

            // sync to hive hms
            Table table = client.getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
            updateHmsTable(table, tablePath, schema);
            client.alter_table(tablePath.getDatabaseName(), tablePath.getObjectName(), table);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    protected String warehouse() {
        return hiveConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
    }

    private Database convertToDatabase(String name) {
        Database database = new Database();
        database.setName(name);
        database.setLocationUri(databasePath(name).toString());
        return database;
    }

    private Table newHmsTable(ObjectPath tablePath) {
        long currentTimeMillis = System.currentTimeMillis();
        final TableType tableType = hiveConf.getEnum(TABLE_TYPE.key(), TableType.MANAGED);
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
                        tableType.toString());
        table.getParameters()
                .put(hive_metastoreConstants.META_TABLE_STORAGE, STORAGE_HANDLER_CLASS_NAME);
        if (TableType.EXTERNAL.equals(tableType)) {
            table.getParameters().put("EXTERNAL", "TRUE");
        }
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

    private boolean tableStoreTableExists(ObjectPath tablePath) {
        return tableStoreTableExists(tablePath, true);
    }

    private boolean tableStoreTableExists(ObjectPath tablePath, boolean throwException) {
        Table table;
        try {
            table = client.getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
        } catch (NoSuchObjectException e) {
            return false;
        } catch (TException e) {
            throw new RuntimeException(
                    "Cannot determine if table "
                            + tablePath.getFullName()
                            + " is a table store table.",
                    e);
        }

        if (!INPUT_FORMAT_CLASS_NAME.equals(table.getSd().getInputFormat())
                || !OUTPUT_FORMAT_CLASS_NAME.equals(table.getSd().getOutputFormat())) {
            if (throwException) {
                throw new IllegalArgumentException(
                        "Table "
                                + tablePath.getFullName()
                                + " is not a table store table. It's input format is "
                                + table.getSd().getInputFormat()
                                + " and its output format is "
                                + table.getSd().getOutputFormat());
            } else {
                return false;
            }
        }
        return true;
    }

    private SchemaManager schemaManager(ObjectPath tablePath) {
        return new SchemaManager(getTableLocation(tablePath)).withLock(lock(tablePath));
    }

    private Lock lock(ObjectPath tablePath) {
        if (!lockEnabled()) {
            return null;
        }

        HiveCatalogLock lock =
                new HiveCatalogLock(client, checkMaxSleep(hiveConf), acquireTimeout(hiveConf));
        return Lock.fromCatalog(lock, tablePath);
    }

    static IMetaStoreClient createClient(HiveConf hiveConf) {
        IMetaStoreClient client;
        try {
            client =
                    RetryingMetaStoreClient.getProxy(
                            hiveConf, tbl -> null, HiveMetaStoreClient.class.getName());
        } catch (MetaException e) {
            throw new RuntimeException(e);
        }
        return StringUtils.isNullOrWhitespaceOnly(
                        hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname))
                ? client
                : HiveMetaStoreClient.newSynchronizedClient(client);
    }
}
