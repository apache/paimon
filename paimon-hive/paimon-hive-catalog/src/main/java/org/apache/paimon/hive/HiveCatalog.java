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

package org.apache.paimon.hive;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.OptionsUtils;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.TableType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.flink.table.hive.LegacyHiveClasses;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.hive.HiveCatalogLock.acquireTimeout;
import static org.apache.paimon.hive.HiveCatalogLock.checkMaxSleep;
import static org.apache.paimon.hive.HiveCatalogOptions.LOCATION_IN_PROPERTIES;
import static org.apache.paimon.options.CatalogOptions.LOCK_ENABLED;
import static org.apache.paimon.options.CatalogOptions.TABLE_TYPE;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;
import static org.apache.paimon.utils.StringUtils.isNullOrWhitespaceOnly;

/** A catalog implementation for Hive. */
public class HiveCatalog extends AbstractCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

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
    private static final int HIVE_PREFIX_LENGTH = HIVE_PREFIX.length();
    public static final String HIVE_SITE_FILE = "hive-site.xml";

    private final HiveConf hiveConf;
    private final String clientClassName;
    private final IMetaStoreClient client;
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

        this.client = createClient(hiveConf, clientClassName);
    }

    @Override
    public Optional<CatalogLock.Factory> lockFactory() {
        return lockEnabled()
                ? Optional.of(HiveCatalogLock.createFactory(hiveConf, clientClassName))
                : Optional.empty();
    }

    private boolean lockEnabled() {
        return Boolean.parseBoolean(
                hiveConf.get(LOCK_ENABLED.key(), LOCK_ENABLED.defaultValue().toString()));
    }

    @Override
    public Optional<MetastoreClient.Factory> metastoreClientFactory(Identifier identifier) {
        try {
            return Optional.of(
                    new HiveMetastoreClient.Factory(
                            identifier, getDataTableSchema(identifier), hiveConf, clientClassName));
        } catch (TableNotExistException e) {
            throw new RuntimeException(
                    "Table " + identifier + " does not exist. This is unexpected.", e);
        }
    }

    @Override
    public Path getDataTableLocation(Identifier identifier) {
        try {
            String databaseName = identifier.getDatabaseName();
            String tableName = identifier.getObjectName();
            if (client.tableExists(databaseName, tableName)) {
                String location =
                        locationHelper.getTableLocation(client.getTable(databaseName, tableName));
                if (location != null) {
                    return new Path(location);
                }
            } else {
                // If the table does not exist,
                // we should use the database path to generate the table path.
                String dbLocation =
                        locationHelper.getDatabaseLocation(client.getDatabase(databaseName));
                if (dbLocation != null) {
                    return new Path(dbLocation, tableName);
                }
            }

            return super.getDataTableLocation(identifier);
        } catch (TException e) {
            throw new RuntimeException("Can not get table " + identifier + " from metastore.", e);
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
    protected boolean databaseExistsImpl(String databaseName) {
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
    protected void createDatabaseImpl(String name) {
        try {
            client.createDatabase(convertToDatabase(name));
            locationHelper.createPathIfRequired(databasePath(name), fileIO);
        } catch (TException | IOException e) {
            throw new RuntimeException("Failed to create database " + name, e);
        }
    }

    @Override
    protected void dropDatabaseImpl(String name) {
        try {
            locationHelper.dropPathIfRequired(databasePath(name), fileIO);
            client.dropDatabase(name, true, false, true);
        } catch (TException | IOException e) {
            throw new RuntimeException("Failed to drop database " + name, e);
        }
    }

    @Override
    protected List<String> listTablesImpl(String databaseName) {
        try {
            return client.getAllTables(databaseName).stream()
                    .filter(
                            tableName -> {
                                Identifier identifier = new Identifier(databaseName, tableName);
                                // the environment here may not be able to access non-paimon
                                // tables, so we just check the schema file first
                                return schemaFileExists(identifier) && tableExists(identifier);
                            })
                    .collect(Collectors.toList());
        } catch (TException e) {
            throw new RuntimeException("Failed to list all tables in database " + databaseName, e);
        }
    }

    @Override
    public boolean tableExists(Identifier identifier) {
        if (isSystemTable(identifier)) {
            return super.tableExists(identifier);
        }

        Table table;
        try {
            table = client.getTable(identifier.getDatabaseName(), identifier.getObjectName());
        } catch (NoSuchObjectException e) {
            return false;
        } catch (TException e) {
            throw new RuntimeException(
                    "Cannot determine if table " + identifier.getFullName() + " is a paimon table.",
                    e);
        }

        return isPaimonTable(table) || LegacyHiveClasses.isPaimonTable(table);
    }

    private static boolean isPaimonTable(Table table) {
        return INPUT_FORMAT_CLASS_NAME.equals(table.getSd().getInputFormat())
                && OUTPUT_FORMAT_CLASS_NAME.equals(table.getSd().getOutputFormat());
    }

    @Override
    public TableSchema getDataTableSchema(Identifier identifier) throws TableNotExistException {
        if (!tableExists(identifier)) {
            throw new TableNotExistException(identifier);
        }
        Path tableLocation = getDataTableLocation(identifier);
        return new SchemaManager(fileIO, tableLocation)
                .latest()
                .orElseThrow(
                        () -> new RuntimeException("There is no paimon table in " + tableLocation));
    }

    @Override
    protected void dropTableImpl(Identifier identifier) {
        try {
            client.dropTable(
                    identifier.getDatabaseName(), identifier.getObjectName(), true, false, true);
            // Deletes table directory to avoid schema in filesystem exists after dropping hive
            // table successfully to keep the table consistency between which in filesystem and
            // which in Hive metastore.
            Path path = getDataTableLocation(identifier);
            try {
                if (fileIO.exists(path)) {
                    fileIO.deleteDirectoryQuietly(path);
                }
            } catch (Exception ee) {
                LOG.error("Delete directory[{}] fail for table {}", path, identifier, ee);
            }
        } catch (TException e) {
            throw new RuntimeException("Failed to drop table " + identifier.getFullName(), e);
        }
    }

    @Override
    protected void createTableImpl(Identifier identifier, Schema schema) {
        // first commit changes to underlying files
        // if changes on Hive fails there is no harm to perform the same changes to files again
        TableSchema tableSchema;
        try {
            tableSchema = schemaManager(identifier).createTable(schema);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to commit changes of table "
                            + identifier.getFullName()
                            + " to underlying files.",
                    e);
        }

        Table table =
                newHmsTable(
                        identifier,
                        tableSchema.options().entrySet().stream()
                                .filter(entry -> entry.getKey().startsWith(HIVE_PREFIX))
                                .collect(
                                        Collectors.toMap(
                                                entry ->
                                                        entry.getKey()
                                                                .substring(HIVE_PREFIX_LENGTH),
                                                Map.Entry::getValue)));
        try {
            updateHmsTable(table, identifier, tableSchema);
            client.createTable(table);
        } catch (Exception e) {
            Path path = getDataTableLocation(identifier);
            try {
                fileIO.deleteDirectoryQuietly(path);
            } catch (Exception ee) {
                LOG.error("Delete directory[{}] fail for table {}", path, identifier, ee);
            }
            throw new RuntimeException("Failed to create table " + identifier.getFullName(), e);
        }
    }

    @Override
    protected void renameTableImpl(Identifier fromTable, Identifier toTable) {
        try {
            String fromDB = fromTable.getDatabaseName();
            String fromTableName = fromTable.getObjectName();
            Table table = client.getTable(fromDB, fromTableName);
            table.setDbName(toTable.getDatabaseName());
            table.setTableName(toTable.getObjectName());
            client.alter_table(fromDB, fromTableName, table);

            Path fromPath = getDataTableLocation(fromTable);
            if (new SchemaManager(fileIO, fromPath).listAllIds().size() > 0) {
                // Rename the file system's table directory. Maintain consistency between tables in
                // the file system and tables in the Hive Metastore.
                Path toPath = getDataTableLocation(toTable);
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
                client.alter_table(toTable.getDatabaseName(), toTable.getObjectName(), table);
            }
        } catch (TException e) {
            throw new RuntimeException("Failed to rename table " + fromTable.getFullName(), e);
        }
    }

    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {

        final SchemaManager schemaManager = schemaManager(identifier);
        // first commit changes to underlying files
        TableSchema schema = schemaManager.commitChanges(changes);

        try {
            // sync to hive hms
            Table table = client.getTable(identifier.getDatabaseName(), identifier.getObjectName());
            updateHmsTable(table, identifier, schema);
            client.alter_table(identifier.getDatabaseName(), identifier.getObjectName(), table);
        } catch (Exception te) {
            schemaManager.deleteSchema(schema.id());
            throw new RuntimeException(te);
        }
    }

    @Override
    public boolean caseSensitive() {
        return false;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    public String warehouse() {
        return warehouse;
    }

    private void checkIdentifierUpperCase(Identifier identifier) {
        checkState(
                identifier.getDatabaseName().equals(identifier.getDatabaseName().toLowerCase()),
                String.format(
                        "Database name[%s] cannot contain upper case in hive catalog",
                        identifier.getDatabaseName()));
        checkState(
                identifier.getObjectName().equals(identifier.getObjectName().toLowerCase()),
                String.format(
                        "Table name[%s] cannot contain upper case in hive catalog",
                        identifier.getObjectName()));
    }

    private Database convertToDatabase(String name) {
        Database database = new Database();
        database.setName(name);
        locationHelper.specifyDatabaseLocation(databasePath(name), database);
        return database;
    }

    private Table newHmsTable(Identifier identifier, Map<String, String> tableParameters) {
        long currentTimeMillis = System.currentTimeMillis();
        TableType tableType =
                OptionsUtils.convertToEnum(
                        hiveConf.get(TABLE_TYPE.key(), TableType.MANAGED.toString()),
                        TableType.class);
        Table table =
                new Table(
                        identifier.getObjectName(),
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
        table.getParameters()
                .put(hive_metastoreConstants.META_TABLE_STORAGE, STORAGE_HANDLER_CLASS_NAME);
        if (TableType.EXTERNAL.equals(tableType)) {
            table.getParameters().put("EXTERNAL", "TRUE");
        }
        return table;
    }

    private void updateHmsTable(Table table, Identifier identifier, TableSchema schema) {
        StorageDescriptor sd = new StorageDescriptor();

        sd.setInputFormat(INPUT_FORMAT_CLASS_NAME);
        sd.setOutputFormat(OUTPUT_FORMAT_CLASS_NAME);

        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setParameters(new HashMap<>());
        serDeInfo.setSerializationLib(SERDE_CLASS_NAME);
        sd.setSerdeInfo(serDeInfo);

        CoreOptions options = new CoreOptions(schema.options());
        if (options.partitionedTableInMetastore() && schema.partitionKeys().size() > 0) {
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

        // update location
        locationHelper.specifyTableLocation(table, getDataTableLocation(identifier).toString());
    }

    @VisibleForTesting
    IMetaStoreClient getHmsClient() {
        return client;
    }

    private FieldSchema convertToFieldSchema(DataField dataField) {
        return new FieldSchema(
                dataField.name(),
                HiveTypeUtils.toTypeInfo(dataField.type()).getTypeName(),
                dataField.description());
    }

    private boolean schemaFileExists(Identifier identifier) {
        return new SchemaManager(fileIO, getDataTableLocation(identifier)).latest().isPresent();
    }

    private SchemaManager schemaManager(Identifier identifier) {
        return new SchemaManager(fileIO, getDataTableLocation(identifier))
                .withLock(lock(identifier));
    }

    private Lock lock(Identifier identifier) {
        if (!lockEnabled()) {
            return new Lock.EmptyLock();
        }

        HiveCatalogLock lock =
                new HiveCatalogLock(client, checkMaxSleep(hiveConf), acquireTimeout(hiveConf));
        return Lock.fromCatalog(lock, identifier);
    }

    private static final Map<Class<?>[], HiveMetastoreProxySupplier> PROXY_SUPPLIERS =
            ImmutableMap.<Class<?>[], HiveMetastoreProxySupplier>builder()
                    // for hive 1.x
                    .put(
                            new Class<?>[] {
                                HiveConf.class,
                                HiveMetaHookLoader.class,
                                ConcurrentHashMap.class,
                                String.class
                            },
                            (getProxyMethod, hiveConf, clientClassName) ->
                                    (IMetaStoreClient)
                                            getProxyMethod.invoke(
                                                    null,
                                                    hiveConf,
                                                    (HiveMetaHookLoader) (tbl -> null),
                                                    new ConcurrentHashMap<>(),
                                                    clientClassName))
                    // for hive 2.x
                    .put(
                            new Class<?>[] {
                                HiveConf.class,
                                HiveMetaHookLoader.class,
                                ConcurrentHashMap.class,
                                String.class,
                                Boolean.TYPE
                            },
                            (getProxyMethod, hiveConf, clientClassName) ->
                                    (IMetaStoreClient)
                                            getProxyMethod.invoke(
                                                    null,
                                                    hiveConf,
                                                    (HiveMetaHookLoader) (tbl -> null),
                                                    new ConcurrentHashMap<>(),
                                                    clientClassName,
                                                    true))
                    // for hive 3.x
                    .put(
                            new Class<?>[] {
                                Configuration.class,
                                HiveMetaHookLoader.class,
                                ConcurrentHashMap.class,
                                String.class,
                                Boolean.TYPE
                            },
                            (getProxyMethod, hiveConf, clientClassName) ->
                                    (IMetaStoreClient)
                                            getProxyMethod.invoke(
                                                    null,
                                                    hiveConf,
                                                    (HiveMetaHookLoader) (tbl -> null),
                                                    new ConcurrentHashMap<>(),
                                                    clientClassName,
                                                    true))
                    .build();
    // If clientClassName is HiveMetaStoreClient,
    // we can revert to the simplest creation method,
    // which allows us to use shaded Hive packages to avoid dependency conflicts,
    // such as using apache-hive2.jar in Presto and Trino.
    private static final Map<Class<?>[], HiveMetastoreProxySupplier> PROXY_SUPPLIERS_SHADED =
            ImmutableMap.<Class<?>[], HiveMetastoreProxySupplier>builder()
                    .put(
                            new Class<?>[] {HiveConf.class},
                            (getProxyMethod, hiveConf, clientClassName) ->
                                    (IMetaStoreClient) getProxyMethod.invoke(null, hiveConf))
                    .put(
                            new Class<?>[] {HiveConf.class, Boolean.TYPE},
                            (getProxyMethod, hiveConf, clientClassName) ->
                                    (IMetaStoreClient) getProxyMethod.invoke(null, hiveConf, true))
                    .put(
                            new Class<?>[] {Configuration.class, Boolean.TYPE},
                            (getProxyMethod, hiveConf, clientClassName) ->
                                    (IMetaStoreClient) getProxyMethod.invoke(null, hiveConf, true))
                    .build();

    static IMetaStoreClient createClient(HiveConf hiveConf, String clientClassName) {
        Method getProxy = null;
        HiveMetastoreProxySupplier supplier = null;
        RuntimeException methodNotFound =
                new RuntimeException(
                        "Failed to find desired getProxy method from RetryingMetaStoreClient");
        Map<Class<?>[], HiveMetastoreProxySupplier> suppliers =
                new LinkedHashMap<>(PROXY_SUPPLIERS);
        if (HiveMetaStoreClient.class.getName().equals(clientClassName)) {
            suppliers.putAll(PROXY_SUPPLIERS_SHADED);
        }
        for (Entry<Class<?>[], HiveMetastoreProxySupplier> entry : suppliers.entrySet()) {
            Class<?>[] classes = entry.getKey();
            try {
                getProxy = RetryingMetaStoreClient.class.getMethod("getProxy", classes);
                supplier = entry.getValue();
            } catch (NoSuchMethodException e) {
                methodNotFound.addSuppressed(e);
            }
        }
        if (getProxy == null) {
            throw methodNotFound;
        }

        IMetaStoreClient client;
        try {
            client = supplier.get(getProxy, hiveConf, clientClassName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return isNullOrWhitespaceOnly(hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname))
                ? client
                : HiveMetaStoreClient.newSynchronizedClient(client);
    }

    public static HiveConf createHiveConf(
            @Nullable String hiveConfDir, @Nullable String hadoopConfDir) {
        // try to load from system env.
        if (isNullOrWhitespaceOnly(hiveConfDir)) {
            hiveConfDir = possibleHiveConfPath();
        }
        if (isNullOrWhitespaceOnly(hadoopConfDir)) {
            hadoopConfDir = possibleHadoopConfPath();
        }

        // create HiveConf from hadoop configuration with hadoop conf directory configured.
        Configuration hadoopConf = null;
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
        if (hadoopConf == null) {
            hadoopConf = new Configuration();
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
                isEmbeddedMetastore(hiveConf);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to load hive-site.xml from specified path:" + hiveSite, e);
            }
            hiveConf.addResource(hiveSite);

            return hiveConf;
        } else {
            return new HiveConf(hadoopConf, HiveConf.class);
        }
    }

    public static boolean isEmbeddedMetastore(HiveConf hiveConf) {
        return isNullOrWhitespaceOnly(hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
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

    public static String possibleHadoopConfPath() {
        String possiblePath = null;
        if (System.getenv("HADOOP_CONF_DIR") != null) {
            possiblePath = System.getenv("HADOOP_CONF_DIR");
        } else if (System.getenv("HADOOP_HOME") != null) {
            String possiblePath1 = System.getenv("HADOOP_HOME") + "/conf";
            String possiblePath2 = System.getenv("HADOOP_HOME") + "/etc/hadoop";
            if (new File(possiblePath1).exists()) {
                possiblePath = possiblePath1;
            } else if (new File(possiblePath2).exists()) {
                possiblePath = possiblePath2;
            }
        }
        return possiblePath;
    }

    public static String possibleHiveConfPath() {
        return System.getenv("HIVE_CONF_DIR");
    }

    /** Function interface for creating hive metastore proxy. */
    public interface HiveMetastoreProxySupplier {
        IMetaStoreClient get(Method getProxyMethod, Configuration conf, String clientClassName)
                throws IllegalAccessException, IllegalArgumentException, InvocationTargetException;
    }
}
