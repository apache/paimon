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
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.mapred.PaimonInputFormat;
import org.apache.paimon.hive.mapred.PaimonOutputFormat;
import org.apache.paimon.hive.utils.HiveUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.Warehouse.getDnsPath;
import static org.apache.paimon.CoreOptions.METASTORE_PARTITIONED_TABLE;
import static org.apache.paimon.catalog.Catalog.COMMENT_PROP;
import static org.apache.paimon.hive.HiveTypeUtils.toPaimonType;

/**
 * {@link HiveMetaHook} for paimon. Currently this class is only used to set input and output
 * formats.
 */
public class PaimonMetaHook implements HiveMetaHook {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonMetaHook.class);

    private final Configuration conf;

    // paimon table existed before create hive table
    private final Set<Identifier> existingPaimonTable = new HashSet<>();

    public PaimonMetaHook(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public void preCreateTable(Table table) throws MetaException {

        // hive ql parse cannot recognize input near '$' in table name, no need to add paimon system
        // table verification.

        table.getSd().setInputFormat(PaimonInputFormat.class.getCanonicalName());
        table.getSd().setOutputFormat(PaimonOutputFormat.class.getCanonicalName());
        table.setDbName(table.getDbName().toLowerCase());
        table.setTableName(table.getTableName().toLowerCase());
        String location = LocationKeyExtractor.getPaimonLocation(conf, table);
        Identifier identifier = Identifier.create(table.getDbName(), table.getTableName());
        if (location == null) {
            String warehouse = conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
            org.apache.hadoop.fs.Path hadoopPath =
                    getDnsPath(new org.apache.hadoop.fs.Path(warehouse), conf);
            warehouse = hadoopPath.toUri().toString();
            location = AbstractCatalog.newTableLocation(warehouse, identifier).toUri().toString();
            table.getSd().setLocation(location);
        }

        Path path = new Path(location);
        CatalogContext context = catalogContext(table, location);
        FileIO fileIO;
        try {
            fileIO = FileIO.get(path, context);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        SchemaManager schemaManager = new SchemaManager(fileIO, path);
        Optional<TableSchema> tableSchema = schemaManager.latest();
        if (tableSchema.isPresent()) {
            existingPaimonTable.add(identifier);
            // paimon table already exists
            return;
        }
        // create paimon table
        List<FieldSchema> cols = table.getSd().getCols();
        Schema.Builder schemaBuilder =
                Schema.newBuilder().comment(table.getParameters().get(COMMENT_PROP));
        cols.iterator()
                .forEachRemaining(
                        fieldSchema ->
                                schemaBuilder.column(
                                        fieldSchema.getName().toLowerCase(),
                                        toPaimonType(fieldSchema.getType()),
                                        fieldSchema.getComment()));
        // partition columns
        if (table.getPartitionKeysSize() > 0) {
            // set metastore.partitioned-table = true
            context.options().set(METASTORE_PARTITIONED_TABLE, true);

            table.getPartitionKeys()
                    .iterator()
                    .forEachRemaining(
                            fieldSchema ->
                                    schemaBuilder.column(
                                            fieldSchema.getName().toLowerCase(),
                                            toPaimonType(fieldSchema.getType()),
                                            fieldSchema.getComment()));

            List<String> partitionKeys =
                    table.getPartitionKeys().stream()
                            .map(FieldSchema::getName)
                            .map(String::toLowerCase)
                            .collect(Collectors.toList());
            schemaBuilder.partitionKeys(partitionKeys);
        }
        schemaBuilder.options(context.options().toMap());

        try {
            schemaManager.createTable(schemaBuilder.build());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rollbackCreateTable(Table table) throws MetaException {
        Identifier identifier = Identifier.create(table.getDbName(), table.getTableName());
        if (existingPaimonTable.contains(identifier)) {
            return;
        }

        // we have created a paimon table, so we delete it to roll back;
        String location = LocationKeyExtractor.getPaimonLocation(conf, table);

        Path path = new Path(location);
        CatalogContext context = catalogContext(table, location);
        try {
            FileIO fileIO = FileIO.get(path, context);
            if (fileIO.exists(path)) {
                fileIO.deleteDirectoryQuietly(path);
            }
        } catch (IOException e) {
            LOG.error("Delete directory [{}] fail for the paimon table.", path, e);
        }
    }

    @Override
    public void commitCreateTable(Table table) throws MetaException {}

    @Override
    public void preDropTable(Table table) throws MetaException {}

    @Override
    public void rollbackDropTable(Table table) throws MetaException {}

    @Override
    public void commitDropTable(Table table, boolean b) throws MetaException {}

    private CatalogContext catalogContext(Table table, String location) {
        Options options = HiveUtils.extractCatalogConfig(conf);
        options.set(CoreOptions.TABLE_SCHEMA_PATH, location);
        table.getParameters().forEach(options::set);
        return CatalogContext.create(options, conf);
    }
}
