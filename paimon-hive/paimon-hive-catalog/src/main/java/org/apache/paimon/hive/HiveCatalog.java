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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.client.ClientPool;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.pool.CachedClientPool;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.OptionsUtils;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.TableType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.table.hive.LegacyHiveClasses;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREWAREHOUSE;
import static org.apache.paimon.hive.HiveCatalogLock.acquireTimeout;
import static org.apache.paimon.hive.HiveCatalogLock.checkMaxSleep;
import static org.apache.paimon.hive.HiveCatalogOptions.HADOOP_CONF_DIR;
import static org.apache.paimon.hive.HiveCatalogOptions.HIVE_CONF_DIR;
import static org.apache.paimon.hive.HiveCatalogOptions.IDENTIFIER;
import static org.apache.paimon.hive.HiveCatalogOptions.LOCATION_IN_PROPERTIES;
import static org.apache.paimon.options.CatalogOptions.ALLOW_UPPER_CASE;
import static org.apache.paimon.options.CatalogOptions.SYNC_ALL_PROPERTIES;
import static org.apache.paimon.options.CatalogOptions.TABLE_TYPE;
import static org.apache.paimon.options.OptionsUtils.convertToPropertiesPrefixKey;
import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.StringUtils.isNullOrWhitespaceOnly;

/** A catalog implementation for Hive. */
public class HiveCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

    // Reserved properties
    public static final String TABLE_TYPE_PROP = "table_type";
    public static final String PAIMON_TABLE_TYPE_VALUE = "paimon";

    // we don't include paimon-hive-connector as dependencies because it depends on
    // hive-exec
    private static final String INPUT_FORMAT_CLASS_NAME =
            "org.apache.paimon.hive.mapred.PaimonInputFormat";
    private static final String OUTPUT_FORMAT_CLASS_NAME =
            "org.apache.paimon.hive.mapred.PaimonOutputFormat";
    private static final String SERDE_CLASS_NAME = "org.apache.paimon.hive.PaimonSerDe";
    private static final String STORAGE_HANDLER_CLASS_NAME =
            "org.apache.paimon.hive.PaimonStorageHandler";
    private static final String HIVE_PREFIX = "hive.";
    public static final String HIVE_SITE_FILE = "hive-site.xml";

    private final HiveConf hiveConf;
    private final String clientClassName;
    private final Options options;
    private final ClientPool<IMetaStoreClient, TException> clients;
    private final String warehouse;

    private final LocationHelper locationHelper;

    public HiveCatalog(FileIO fileIO, HiveConf hiveConf, String clientClassName, String warehouse) {
        this(fileIO, hiveConf, clientClassName, new Options(), warehouse);
    }

    public HiveCatalog(
            FileIO fileIO,
            HiveConf hiveConf,
            String clientClassName,
            Options options,
            String warehouse) {
        super(fileIO, options);
        this.hiveConf = hiveConf;
        this.clientClassName = clientClassName;
        this.options = options;
        this.warehouse = warehouse;

        boolean needLocationInProperties =
                hiveConf.getBoolean(
                        LOCATION_IN_PROPERTIES.key(), LOCATION_IN_PROPERTIES.defaultValue());
        if (needLocationInProperties) {
            locationHelper = new TBPropertiesLocationHelper();
        } else {
            // set the warehouse location to the hiveConf
            hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouse);
            locationHelper = new StorageLocationHelper();
        }

        this.clients = new CachedClientPool(hiveConf, options, clientClassName);
    }

    @Override
    public Optional<CatalogLockFactory> defaultLockFactory() {
        return Optional.of(new HiveCatalogLockFactory());
    }

    @Override
    public Optional<CatalogLockContext> lockContext() {
        return Optional.of(
                new HiveCatalogLockContext(
                        new SerializableHiveConf(hiveConf), clientClassName, catalogOptions));
    }

    @Override
    public Optional<MetastoreClient.Factory> metastoreClientFactory(Identifier identifier) {
        Identifier tableIdentifier =
                new Identifier(identifier.getDatabaseName(), identifier.getTableName());
        try {
            return Optional.of(
                    new HiveMetastoreClient.Factory(
                            tableIdentifier,
                            getDataTableSchema(tableIdentifier),
                            hiveConf,
                            clientClassName,
                            options));
        } catch (TableNotExistException e) {
            throw new RuntimeException(
                    "Table " + identifier + " does not exist. This is unexpected.", e);
        }
    }

    @Override
    public Path getTableLocation(Identifier identifier) {
        try {
            String databaseName = identifier.getDatabaseName();
            String tableName = identifier.getTableName();
            Optional<Path> tablePath =
                    clients.run(
                            client -> {
                                if (client.tableExists(databaseName, tableName)) {
                                    String location =
                                            locationHelper.getTableLocation(
                                                    client.getTable(databaseName, tableName));
                                    if (location != null) {
                                        return Optional.of(new Path(location));
                                    }
                                } else {
                                    // If the table does not exist,
                                    // we should use the database path to generate the table path.
                                    String dbLocation =
                                            locationHelper.getDatabaseLocation(
                                                    client.getDatabase(databaseName));
                                    if (dbLocation != null) {
                                        return Optional.of(new Path(dbLocation, tableName));
                                    }
                                }
                                return Optional.empty();
                            });
            return tablePath.orElse(super.getTableLocation(identifier));
        } catch (TException e) {
            throw new RuntimeException("Can not get table " + identifier + " from metastore.", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted in call to getDataTableLocation " + identifier, e);
        }
    }

    @Override
    public List<String> listDatabases() {
        try {
            return clients.run(IMetaStoreClient::getAllDatabases);
        } catch (TException e) {
            throw new RuntimeException("Failed to list all databases", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to listDatabases", e);
        }
    }

    @Override
    protected void createDatabaseImpl(String name, Map<String, String> properties) {
        try {
            Database database = convertToHiveDatabase(name, properties);
            Path databasePath =
                    database.getLocationUri() == null
                            ? newDatabasePath(name)
                            : new Path(database.getLocationUri());
            locationHelper.createPathIfRequired(databasePath, fileIO);
            locationHelper.specifyDatabaseLocation(databasePath, database);
            clients.execute(client -> client.createDatabase(database));
        } catch (TException | IOException e) {
            throw new RuntimeException("Failed to create database " + name, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to createDatabase " + name, e);
        }
    }

    private Database convertToHiveDatabase(String name, Map<String, String> properties) {
        Database database = new Database();
        database.setName(name);
        Map<String, String> parameter = new HashMap<>();
        properties.forEach(
                (key, value) -> {
                    if (key.equals(COMMENT_PROP)) {
                        database.setDescription(value);
                    } else if (key.equals(DB_LOCATION_PROP)) {
                        database.setLocationUri(value);
                    } else if (value != null) {
                        parameter.put(key, value);
                    }
                });
        database.setParameters(parameter);
        return database;
    }

    @Override
    public Map<String, String> loadDatabasePropertiesImpl(String name)
            throws DatabaseNotExistException {
        try {
            return convertToProperties(clients.run(client -> client.getDatabase(name)));
        } catch (NoSuchObjectException e) {
            throw new DatabaseNotExistException(name);
        } catch (TException e) {
            throw new RuntimeException(
                    String.format("Failed to get database %s properties", name), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to loadDatabaseProperties " + name, e);
        }
    }

    private Map<String, String> convertToProperties(Database database) {
        Map<String, String> properties = new HashMap<>(database.getParameters());
        if (database.getLocationUri() != null) {
            properties.put(DB_LOCATION_PROP, database.getLocationUri());
        }
        if (database.getDescription() != null) {
            properties.put(COMMENT_PROP, database.getDescription());
        }
        return properties;
    }

    @Override
    public void dropPartition(Identifier identifier, Map<String, String> partitionSpec)
            throws TableNotExistException {
        TableSchema tableSchema = getDataTableSchema(identifier);
        if (!tableSchema.partitionKeys().isEmpty()
                && new CoreOptions(tableSchema.options()).partitionedTableInMetastore()
                && !partitionExistsInOtherBranches(identifier, partitionSpec)) {
            try {
                // Do not close client, it is for HiveCatalog
                @SuppressWarnings("resource")
                HiveMetastoreClient metastoreClient =
                        new HiveMetastoreClient(
                                new Identifier(
                                        identifier.getDatabaseName(), identifier.getTableName()),
                                tableSchema,
                                clients);
                metastoreClient.deletePartition(new LinkedHashMap<>(partitionSpec));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        super.dropPartition(identifier, partitionSpec);
    }

    private boolean partitionExistsInOtherBranches(
            Identifier identifier, Map<String, String> partitionSpec)
            throws TableNotExistException {
        FileStoreTable mainTable =
                (FileStoreTable)
                        getTable(
                                new Identifier(
                                        identifier.getDatabaseName(), identifier.getTableName()));
        List<String> branchNames = new ArrayList<>(mainTable.branchManager().branches());
        branchNames.add(DEFAULT_MAIN_BRANCH);

        for (String branchName : branchNames) {
            if (branchName.equals(identifier.getBranchNameOrDefault())) {
                continue;
            }

            Optional<TableSchema> branchSchema =
                    tableSchemaInFileSystem(mainTable.location(), branchName);
            if (!branchSchema.isPresent()) {
                continue;
            }

            FileStoreTable table =
                    FileStoreTableFactory.create(
                            mainTable.fileIO(), mainTable.location(), branchSchema.get());
            if (!table.newScan().withPartitionFilter(partitionSpec).listPartitions().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void dropDatabaseImpl(String name) {
        try {
            Database database = clients.run(client -> client.getDatabase(name));
            String location = locationHelper.getDatabaseLocation(database);
            locationHelper.dropPathIfRequired(new Path(location), fileIO);
            clients.execute(client -> client.dropDatabase(name, true, false, true));
        } catch (TException | IOException e) {
            throw new RuntimeException("Failed to drop database " + name, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to dropDatabase " + name, e);
        }
    }

    @Override
    protected List<String> listTablesImpl(String databaseName) {
        try {
            return clients.run(
                    client ->
                            client.getAllTables(databaseName).stream()
                                    .filter(
                                            tableName -> {
                                                Identifier identifier =
                                                        new Identifier(databaseName, tableName);
                                                return tableExists(identifier);
                                            })
                                    .collect(Collectors.toList()));
        } catch (TException e) {
            throw new RuntimeException("Failed to list all tables in database " + databaseName, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to listTables " + databaseName, e);
        }
    }

    @Override
    public boolean tableExists(Identifier identifier) {
        if (isSystemTable(identifier)) {
            return super.tableExists(identifier);
        }

        Table table;
        try {
            table =
                    clients.run(
                            client ->
                                    client.getTable(
                                            identifier.getDatabaseName(),
                                            identifier.getTableName()));
        } catch (NoSuchObjectException e) {
            return false;
        } catch (TException e) {
            throw new RuntimeException(
                    "Cannot determine if table " + identifier.getFullName() + " is a paimon table.",
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted in call to tableExists " + identifier.getFullName(), e);
        }

        return isPaimonTable(table)
                && tableSchemaInFileSystem(
                                getTableLocation(identifier), identifier.getBranchNameOrDefault())
                        .isPresent();
    }

    private static boolean isPaimonTable(Table table) {
        boolean isPaimonTable =
                INPUT_FORMAT_CLASS_NAME.equals(table.getSd().getInputFormat())
                        && OUTPUT_FORMAT_CLASS_NAME.equals(table.getSd().getOutputFormat());
        return isPaimonTable || LegacyHiveClasses.isPaimonTable(table);
    }

    @Override
    public TableSchema getDataTableSchema(Identifier identifier) throws TableNotExistException {
        if (!tableExists(identifier)) {
            throw new TableNotExistException(identifier);
        }

        return tableSchemaInFileSystem(
                        getTableLocation(identifier), identifier.getBranchNameOrDefault())
                .orElseThrow(() -> new TableNotExistException(identifier));
    }

    private boolean usingExternalTable() {
        TableType tableType =
                OptionsUtils.convertToEnum(
                        hiveConf.get(TABLE_TYPE.key(), TableType.MANAGED.toString()),
                        TableType.class);
        return TableType.EXTERNAL.equals(tableType);
    }

    @Override
    protected void dropTableImpl(Identifier identifier) {
        try {
            clients.execute(
                    client ->
                            client.dropTable(
                                    identifier.getDatabaseName(),
                                    identifier.getTableName(),
                                    true,
                                    false,
                                    true));

            // When drop a Hive external table, only the hive metadata is deleted and the data files
            // are not deleted.
            if (usingExternalTable()) {
                return;
            }

            // Deletes table directory to avoid schema in filesystem exists after dropping hive
            // table successfully to keep the table consistency between which in filesystem and
            // which in Hive metastore.
            Path path = getTableLocation(identifier);
            try {
                if (fileIO.exists(path)) {
                    fileIO.deleteDirectoryQuietly(path);
                }
            } catch (Exception ee) {
                LOG.error("Delete directory[{}] fail for table {}", path, identifier, ee);
            }
        } catch (TException e) {
            throw new RuntimeException("Failed to drop table " + identifier.getFullName(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted in call to dropTable " + identifier.getFullName(), e);
        }
    }

    @Override
    protected void createTableImpl(Identifier identifier, Schema schema) {
        // first commit changes to underlying files
        // if changes on Hive fails there is no harm to perform the same changes to files again
        TableSchema tableSchema;
        try {
            tableSchema = schemaManager(identifier).createTable(schema, usingExternalTable());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to commit changes of table "
                            + identifier.getFullName()
                            + " to underlying files.",
                    e);
        }

        try {
            clients.execute(client -> client.createTable(createHiveTable(identifier, tableSchema)));
        } catch (Exception e) {
            Path path = getTableLocation(identifier);
            try {
                fileIO.deleteDirectoryQuietly(path);
            } catch (Exception ee) {
                LOG.error("Delete directory[{}] fail for table {}", path, identifier, ee);
            }
            throw new RuntimeException("Failed to create table " + identifier.getFullName(), e);
        }
    }

    private Table createHiveTable(Identifier identifier, TableSchema tableSchema) {
        Map<String, String> tblProperties;
        if (syncAllProperties()) {
            tblProperties = new HashMap<>(tableSchema.options());
        } else {
            tblProperties = convertToPropertiesPrefixKey(tableSchema.options(), HIVE_PREFIX);
        }

        Table table = newHmsTable(identifier, tblProperties);
        updateHmsTable(table, identifier, tableSchema);
        return table;
    }

    @Override
    protected void renameTableImpl(Identifier fromTable, Identifier toTable) {
        try {
            String fromDB = fromTable.getDatabaseName();
            String fromTableName = fromTable.getTableName();
            Table table = clients.run(client -> client.getTable(fromDB, fromTableName));
            table.setDbName(toTable.getDatabaseName());
            table.setTableName(toTable.getTableName());
            clients.execute(client -> client.alter_table(fromDB, fromTableName, table));

            Path fromPath = getTableLocation(fromTable);
            if (!new SchemaManager(fileIO, fromPath).listAllIds().isEmpty()) {
                // Rename the file system's table directory. Maintain consistency between tables in
                // the file system and tables in the Hive Metastore.
                Path toPath = getTableLocation(toTable);
                try {
                    fileIO.rename(fromPath, toPath);
                } catch (IOException e) {
                    throw new RuntimeException(
                            "Failed to rename changes of table "
                                    + toTable.getFullName()
                                    + " to underlying files.",
                            e);
                }

                // update location
                locationHelper.specifyTableLocation(table, toPath.toString());
                clients.execute(
                        client ->
                                client.alter_table(
                                        toTable.getDatabaseName(), toTable.getTableName(), table));
            }
        } catch (TException e) {
            throw new RuntimeException("Failed to rename table " + fromTable.getFullName(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to renameTable", e);
        }
    }

    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        final SchemaManager schemaManager = schemaManager(identifier);
        // first commit changes to underlying files
        TableSchema schema = schemaManager.commitChanges(changes);

        // currently only changes to main branch affects metastore
        if (!DEFAULT_MAIN_BRANCH.equals(identifier.getBranchNameOrDefault())) {
            return;
        }
        try {
            Table table =
                    clients.run(
                            client ->
                                    client.getTable(
                                            identifier.getDatabaseName(),
                                            identifier.getTableName()));
            alterTableToHms(table, identifier, schema);
        } catch (Exception te) {
            schemaManager.deleteSchema(schema.id());
            throw new RuntimeException(te);
        }
    }

    private void alterTableToHms(Table table, Identifier identifier, TableSchema newSchema)
            throws TException, InterruptedException {
        updateHmsTablePars(table, newSchema);
        updateHmsTable(table, identifier, newSchema);
        clients.execute(
                client ->
                        client.alter_table(
                                identifier.getDatabaseName(),
                                identifier.getTableName(),
                                table,
                                true));
    }

    @Override
    public boolean allowUpperCase() {
        return catalogOptions.getOptional(ALLOW_UPPER_CASE).orElse(false);
    }

    public boolean syncAllProperties() {
        return catalogOptions.get(SYNC_ALL_PROPERTIES);
    }

    @Override
    public void repairCatalog() {
        List<String> databases = null;
        try {
            databases = listDatabasesInFileSystem(new Path(warehouse));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (String database : databases) {
            repairDatabase(database);
        }
    }

    @Override
    public void repairDatabase(String databaseName) {
        checkNotSystemDatabase(databaseName);

        // create database if needed
        if (!databaseExists(databaseName)) {
            createDatabaseImpl(databaseName, Collections.emptyMap());
        }

        // tables from file system
        List<String> tables;
        try {
            Database database = clients.run(client -> client.getDatabase(databaseName));
            tables = listTablesInFileSystem(new Path(locationHelper.getDatabaseLocation(database)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // repair tables
        for (String table : tables) {
            try {
                repairTable(Identifier.create(databaseName, table));
            } catch (TableNotExistException ignore) {
            }
        }
    }

    @Override
    public void repairTable(Identifier identifier) throws TableNotExistException {
        checkNotBranch(identifier, "repairTable");
        checkNotSystemTable(identifier, "repairTable");
        validateIdentifierNameCaseInsensitive(identifier);

        TableSchema tableSchema =
                tableSchemaInFileSystem(
                                getTableLocation(identifier), identifier.getBranchNameOrDefault())
                        .orElseThrow(() -> new TableNotExistException(identifier));
        Table newTable = createHiveTable(identifier, tableSchema);
        try {
            try {
                Table table =
                        clients.run(
                                client ->
                                        client.getTable(
                                                identifier.getDatabaseName(),
                                                identifier.getTableName()));
                checkArgument(
                        isPaimonTable(table),
                        "Table %s is not a paimon table in hive metastore.",
                        identifier.getFullName());
                if (!newTable.getSd().getCols().equals(table.getSd().getCols())) {
                    alterTableToHms(table, identifier, tableSchema);
                }
            } catch (NoSuchObjectException e) {
                // hive table does not exist.
                clients.execute(client -> client.createTable(newTable));
            }

            // repair partitions
            if (!tableSchema.partitionKeys().isEmpty() && !newTable.getPartitionKeys().isEmpty()) {
                // Do not close client, it is for HiveCatalog
                @SuppressWarnings("resource")
                HiveMetastoreClient metastoreClient =
                        new HiveMetastoreClient(identifier, tableSchema, clients);
                List<BinaryRow> partitions =
                        getTable(identifier).newReadBuilder().newScan().listPartitions();
                for (BinaryRow partition : partitions) {
                    metastoreClient.addPartition(partition);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }

    @Override
    public String warehouse() {
        return warehouse;
    }

    private Table newHmsTable(Identifier identifier, Map<String, String> tableParameters) {
        long currentTimeMillis = System.currentTimeMillis();
        TableType tableType =
                OptionsUtils.convertToEnum(
                        hiveConf.get(TABLE_TYPE.key(), TableType.MANAGED.toString()),
                        TableType.class);
        Table table =
                new Table(
                        identifier.getTableName(),
                        identifier.getDatabaseName(),
                        // current linux user
                        System.getProperty("user.name"),
                        (int) (currentTimeMillis / 1000),
                        (int) (currentTimeMillis / 1000),
                        Integer.MAX_VALUE,
                        null,
                        Collections.emptyList(),
                        tableParameters,
                        null,
                        null,
                        tableType.toString().toUpperCase(Locale.ROOT) + "_TABLE");
        table.getParameters().put(TABLE_TYPE_PROP, PAIMON_TABLE_TYPE_VALUE.toUpperCase());
        table.getParameters()
                .put(hive_metastoreConstants.META_TABLE_STORAGE, STORAGE_HANDLER_CLASS_NAME);
        if (TableType.EXTERNAL.equals(tableType)) {
            table.getParameters().put("EXTERNAL", "TRUE");
        }
        return table;
    }

    private void updateHmsTable(Table table, Identifier identifier, TableSchema schema) {
        StorageDescriptor sd = table.getSd() != null ? table.getSd() : new StorageDescriptor();

        sd.setInputFormat(INPUT_FORMAT_CLASS_NAME);
        sd.setOutputFormat(OUTPUT_FORMAT_CLASS_NAME);

        SerDeInfo serDeInfo = sd.getSerdeInfo() != null ? sd.getSerdeInfo() : new SerDeInfo();
        serDeInfo.setParameters(new HashMap<>());
        serDeInfo.setSerializationLib(SERDE_CLASS_NAME);
        sd.setSerdeInfo(serDeInfo);

        CoreOptions options = new CoreOptions(schema.options());
        if (options.partitionedTableInMetastore() && !schema.partitionKeys().isEmpty()) {
            Map<String, DataField> fieldMap =
                    schema.fields().stream()
                            .collect(Collectors.toMap(DataField::name, Function.identity()));
            List<FieldSchema> partitionFields = new ArrayList<>();
            for (String partitionKey : schema.partitionKeys()) {
                partitionFields.add(convertToFieldSchema(fieldMap.get(partitionKey)));
            }
            table.setPartitionKeys(partitionFields);

            Set<String> partitionKeys = new HashSet<>(schema.partitionKeys());
            List<FieldSchema> normalFields = new ArrayList<>();
            for (DataField field : schema.fields()) {
                if (!partitionKeys.contains(field.name())) {
                    normalFields.add(convertToFieldSchema(field));
                }
            }
            sd.setCols(normalFields);
        } else {
            if (options.tagToPartitionField() != null) {
                // map a non-partitioned table to a partitioned table
                // partition field is tag field which is offered by user
                checkArgument(
                        schema.partitionKeys().isEmpty(),
                        "Partition table can not use timeTravelToPartitionField.");
                table.setPartitionKeys(
                        Collections.singletonList(
                                convertToFieldSchema(
                                        new DataField(
                                                0,
                                                options.tagToPartitionField(),
                                                DataTypes.STRING()))));
            }

            sd.setCols(
                    schema.fields().stream()
                            .map(this::convertToFieldSchema)
                            .collect(Collectors.toList()));
        }
        table.setSd(sd);
        if (schema.comment() != null) {
            table.getParameters().put(COMMENT_PROP, schema.comment());
        }

        // update location
        locationHelper.specifyTableLocation(table, getTableLocation(identifier).toString());
    }

    private void updateHmsTablePars(Table table, TableSchema schema) {
        table.getParameters().putAll(convertToPropertiesPrefixKey(schema.options(), HIVE_PREFIX));
    }

    @VisibleForTesting
    public IMetaStoreClient getHmsClient() {
        try {
            return clients.run(client -> client);
        } catch (Exception e) {
            throw new RuntimeException("Failed to close hms client:", e);
        }
    }

    private FieldSchema convertToFieldSchema(DataField dataField) {
        return new FieldSchema(
                dataField.name(),
                HiveTypeUtils.toTypeInfo(dataField.type()).getTypeName(),
                dataField.description());
    }

    private SchemaManager schemaManager(Identifier identifier) {
        return new SchemaManager(
                        fileIO, getTableLocation(identifier), identifier.getBranchNameOrDefault())
                .withLock(lock(identifier));
    }

    private Lock lock(Identifier identifier) {
        if (!lockEnabled()) {
            return new Lock.EmptyLock();
        }

        HiveCatalogLock lock =
                new HiveCatalogLock(clients, checkMaxSleep(hiveConf), acquireTimeout(hiveConf));
        return Lock.fromCatalog(lock, identifier);
    }

    public static HiveConf createHiveConf(
            @Nullable String hiveConfDir,
            @Nullable String hadoopConfDir,
            Configuration defaultHadoopConf) {
        // try to load from system env.
        if (isNullOrWhitespaceOnly(hiveConfDir)) {
            hiveConfDir = possibleHiveConfPath();
        }

        // create HiveConf from hadoop configuration with hadoop conf directory configured.
        Configuration hadoopConf = defaultHadoopConf;
        if (!isNullOrWhitespaceOnly(hadoopConfDir)) {
            hadoopConf = getHadoopConfiguration(hadoopConfDir);
            if (hadoopConf == null) {
                String possiableUsedConfFiles =
                        "core-site.xml | hdfs-site.xml | yarn-site.xml | mapred-site.xml";
                throw new RuntimeException(
                        "Failed to load the hadoop conf from specified path:" + hadoopConfDir,
                        new FileNotFoundException(
                                "Please check the path none of the conf files ("
                                        + possiableUsedConfFiles
                                        + ") exist in the folder."));
            }
        }

        LOG.info("Setting hive conf dir as {}", hiveConfDir);
        if (hiveConfDir != null) {
            // ignore all the static conf file URLs that HiveConf may have set
            HiveConf.setHiveSiteLocation(null);
            HiveConf.setLoadMetastoreConfig(false);
            HiveConf.setLoadHiveServer2Config(false);
            HiveConf hiveConf = new HiveConf(hadoopConf, HiveConf.class);
            org.apache.hadoop.fs.Path hiveSite =
                    new org.apache.hadoop.fs.Path(hiveConfDir, HIVE_SITE_FILE);
            if (!hiveSite.toUri().isAbsolute()) {
                hiveSite = new org.apache.hadoop.fs.Path(new File(hiveSite.toString()).toURI());
            }
            try (InputStream inputStream = hiveSite.getFileSystem(hadoopConf).open(hiveSite)) {
                hiveConf.addResource(inputStream, hiveSite.toString());
                // trigger a read from the conf to avoid input stream is closed
                hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to load hive-site.xml from specified path:" + hiveSite, e);
            }
            hiveConf.addResource(hiveSite);

            return hiveConf;
        } else {
            HiveConf hiveConf = new HiveConf(hadoopConf, HiveConf.class);
            // user doesn't provide hive conf dir, we try to find it in classpath
            URL hiveSite =
                    Thread.currentThread().getContextClassLoader().getResource(HIVE_SITE_FILE);
            if (hiveSite != null) {
                LOG.info("Found {} in classpath: {}", HIVE_SITE_FILE, hiveSite);
                hiveConf.addResource(hiveSite);
            }
            return hiveConf;
        }
    }

    public static Catalog createHiveCatalog(CatalogContext context) {
        HiveConf hiveConf = createHiveConf(context);
        Options options = context.options();
        String warehouseStr = options.get(CatalogOptions.WAREHOUSE);
        if (warehouseStr == null) {
            warehouseStr =
                    hiveConf.get(METASTOREWAREHOUSE.varname, METASTOREWAREHOUSE.defaultStrVal);
        }
        Path warehouse = new Path(warehouseStr);
        Path uri =
                warehouse.toUri().getScheme() == null
                        ? new Path(FileSystem.getDefaultUri(hiveConf))
                        : warehouse;
        FileIO fileIO;
        try {
            fileIO = FileIO.get(uri, context);
            fileIO.checkOrMkdirs(warehouse);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return new HiveCatalog(
                fileIO,
                hiveConf,
                options.get(HiveCatalogFactory.METASTORE_CLIENT_CLASS),
                options,
                warehouse.toUri().toString());
    }

    public static HiveConf createHiveConf(CatalogContext context) {
        String uri = context.options().get(CatalogOptions.URI);
        String hiveConfDir = context.options().get(HIVE_CONF_DIR);
        String hadoopConfDir = context.options().get(HADOOP_CONF_DIR);
        HiveConf hiveConf =
                HiveCatalog.createHiveConf(hiveConfDir, hadoopConfDir, context.hadoopConf());

        // always using user-set parameters overwrite hive-site.xml parameters
        context.options().toMap().forEach(hiveConf::set);
        if (uri != null) {
            hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, uri);
        }

        if (hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname) == null) {
            LOG.error(
                    "Can't find hive metastore uri to connect: "
                            + " either set "
                            + CatalogOptions.URI.key()
                            + " for paimon "
                            + IDENTIFIER
                            + " catalog or set hive.metastore.uris in hive-site.xml or hadoop configurations."
                            + " Will use empty metastore uris, which means we may use a embedded metastore. The may cause unpredictable consensus problem.");
        }

        return hiveConf;
    }

    /**
     * Returns a new Hadoop Configuration object using the path to the hadoop conf configured.
     *
     * @param hadoopConfDir Hadoop conf directory path.
     * @return A Hadoop configuration instance.
     */
    public static Configuration getHadoopConfiguration(String hadoopConfDir) {
        if (new File(hadoopConfDir).exists()) {
            List<File> possiableConfFiles = new ArrayList<File>();
            File coreSite = new File(hadoopConfDir, "core-site.xml");
            if (coreSite.exists()) {
                possiableConfFiles.add(coreSite);
            }
            File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");
            if (hdfsSite.exists()) {
                possiableConfFiles.add(hdfsSite);
            }
            File yarnSite = new File(hadoopConfDir, "yarn-site.xml");
            if (yarnSite.exists()) {
                possiableConfFiles.add(yarnSite);
            }
            // Add mapred-site.xml. We need to read configurations like compression codec.
            File mapredSite = new File(hadoopConfDir, "mapred-site.xml");
            if (mapredSite.exists()) {
                possiableConfFiles.add(mapredSite);
            }
            if (possiableConfFiles.isEmpty()) {
                return null;
            } else {
                Configuration hadoopConfiguration = new Configuration();
                for (File confFile : possiableConfFiles) {
                    hadoopConfiguration.addResource(
                            new org.apache.hadoop.fs.Path(confFile.getAbsolutePath()));
                }
                return hadoopConfiguration;
            }
        }
        return null;
    }

    public static String possibleHiveConfPath() {
        return System.getenv("HIVE_CONF_DIR");
    }
}
