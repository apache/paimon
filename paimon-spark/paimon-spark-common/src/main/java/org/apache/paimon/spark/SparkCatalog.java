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

package org.apache.paimon.spark;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.spark.catalog.SparkBaseCatalog;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.internal.SessionState;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.options.CatalogOptions.ALLOW_UPPER_CASE;
import static org.apache.paimon.spark.SparkCatalogOptions.DEFAULT_DATABASE;
import static org.apache.paimon.spark.SparkTypeUtils.toPaimonType;
import static org.apache.paimon.spark.util.OptionUtils.copyWithSQLConf;
import static org.apache.paimon.spark.util.OptionUtils.mergeSQLConf;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Spark {@link TableCatalog} for paimon. */
public class SparkCatalog extends SparkBaseCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(SparkCatalog.class);

    private static final String PRIMARY_KEY_IDENTIFIER = "primary-key";

    protected Catalog catalog = null;

    private String defaultDatabase;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.catalogName = name;
        Map<String, String> newOptions = new HashMap<>(options.asCaseSensitiveMap());
        SessionState sessionState = SparkSession.active().sessionState();

        CatalogContext catalogContext =
                CatalogContext.create(Options.fromMap(options), sessionState.newHadoopConf());

        // if spark is case-insensitive, set allow upper case to catalog
        if (!sessionState.conf().caseSensitiveAnalysis()) {
            newOptions.put(ALLOW_UPPER_CASE.key(), "true");
        }
        options = new CaseInsensitiveStringMap(newOptions);

        this.catalog = CatalogFactory.createCatalog(catalogContext);
        this.defaultDatabase =
                options.getOrDefault(DEFAULT_DATABASE.key(), DEFAULT_DATABASE.defaultValue());
        if (!catalog.databaseExists(defaultNamespace()[0])) {
            try {
                createNamespace(defaultNamespace(), new HashMap<>());
            } catch (NamespaceAlreadyExistsException ignored) {
            }
        }
    }

    @Override
    public Catalog paimonCatalog() {
        return catalog;
    }

    @Override
    public String[] defaultNamespace() {
        return new String[] {defaultDatabase};
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws NamespaceAlreadyExistsException {
        checkArgument(
                isValidateNamespace(namespace),
                "Namespace %s is not valid",
                Arrays.toString(namespace));
        try {
            catalog.createDatabase(namespace[0], false, metadata);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            throw new NamespaceAlreadyExistsException(namespace);
        }
    }

    @Override
    public String[][] listNamespaces() {
        List<String> databases = catalog.listDatabases();
        String[][] namespaces = new String[databases.size()][];
        for (int i = 0; i < databases.size(); i++) {
            namespaces[i] = new String[] {databases.get(i)};
        }
        return namespaces;
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length == 0) {
            return listNamespaces();
        }
        if (!isValidateNamespace(namespace)) {
            throw new NoSuchNamespaceException(namespace);
        }
        if (catalog.databaseExists(namespace[0])) {
            return new String[0][];
        }
        throw new NoSuchNamespaceException(namespace);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
            throws NoSuchNamespaceException {
        checkArgument(
                isValidateNamespace(namespace),
                "Namespace %s is not valid",
                Arrays.toString(namespace));
        String dataBaseName = namespace[0];
        try {
            return catalog.loadDatabaseProperties(dataBaseName);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new NoSuchNamespaceException(namespace);
        }
    }

    /**
     * Drop a namespace from the catalog, recursively dropping all objects within the namespace.
     * This interface implementation only supports the Spark 3.0, 3.1 and 3.2.
     *
     * <p>If the catalog implementation does not support this operation, it may throw {@link
     * UnsupportedOperationException}.
     *
     * @param namespace a multi-part namespace
     * @return true if the namespace was dropped
     * @throws UnsupportedOperationException If drop is not a supported operation
     */
    public boolean dropNamespace(String[] namespace) throws NoSuchNamespaceException {
        return dropNamespace(namespace, false);
    }

    /**
     * Drop a namespace from the catalog with cascade mode, recursively dropping all objects within
     * the namespace if cascade is true. This interface implementation supports the Spark 3.3+.
     *
     * <p>If the catalog implementation does not support this operation, it may throw {@link
     * UnsupportedOperationException}.
     *
     * @param namespace a multi-part namespace
     * @param cascade When true, deletes all objects under the namespace
     * @return true if the namespace was dropped
     * @throws UnsupportedOperationException If drop is not a supported operation
     */
    public boolean dropNamespace(String[] namespace, boolean cascade)
            throws NoSuchNamespaceException {
        checkArgument(
                isValidateNamespace(namespace),
                "Namespace %s is not valid",
                Arrays.toString(namespace));
        try {
            catalog.dropDatabase(namespace[0], false, cascade);
            return true;
        } catch (Catalog.DatabaseNotExistException e) {
            throw new NoSuchNamespaceException(namespace);
        } catch (Catalog.DatabaseNotEmptyException e) {
            throw new UnsupportedOperationException(
                    String.format("Namespace %s is not empty", Arrays.toString(namespace)));
        }
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        checkArgument(
                isValidateNamespace(namespace),
                "Missing database in namespace: %s",
                Arrays.toString(namespace));
        try {
            return catalog.listTables(namespace[0]).stream()
                    .map(table -> Identifier.of(namespace, table))
                    .toArray(Identifier[]::new);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new NoSuchNamespaceException(namespace);
        }
    }

    @Override
    public SparkTable loadTable(Identifier ident) throws NoSuchTableException {
        return loadSparkTable(ident, Collections.emptyMap());
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Spark 3.2-.
     */
    public SparkTable loadTable(Identifier ident, String version) throws NoSuchTableException {
        LOG.info("Time travel to version '{}'.", version);
        return loadSparkTable(
                ident, Collections.singletonMap(CoreOptions.SCAN_VERSION.key(), version));
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Spark 3.2-.
     *
     * <p>NOTE: Time unit of timestamp here is microsecond (see {@link
     * TableCatalog#loadTable(Identifier, long)}). But in SQL you should use seconds.
     */
    public SparkTable loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
        // Paimon's timestamp use millisecond
        timestamp = timestamp / 1000;
        LOG.info("Time travel target timestamp is {} milliseconds.", timestamp);
        return loadSparkTable(
                ident,
                Collections.singletonMap(
                        CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), String.valueOf(timestamp)));
    }

    @Override
    public boolean tableExists(Identifier ident) {
        try {
            return catalog.tableExists(toIdentifier(ident));
        } catch (NoSuchTableException e) {
            return false;
        }
    }

    @Override
    public org.apache.spark.sql.connector.catalog.Table alterTable(
            Identifier ident, TableChange... changes) throws NoSuchTableException {
        List<SchemaChange> schemaChanges =
                Arrays.stream(changes).map(this::toSchemaChange).collect(Collectors.toList());
        try {
            catalog.alterTable(toIdentifier(ident), schemaChanges, false);
            return loadTable(ident);
        } catch (Catalog.TableNotExistException e) {
            throw new NoSuchTableException(ident);
        } catch (Catalog.ColumnAlreadyExistException | Catalog.ColumnNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SparkTable createTable(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        try {
            String provider = properties.get(TableCatalog.PROP_PROVIDER);
            checkArgument(
                    usePaimon(provider),
                    "SparkCatalog can only create paimon table, but current provider is %s",
                    provider);
            catalog.createTable(
                    toIdentifier(ident), toInitialSchema(schema, partitions, properties), false);
            return loadTable(ident);
        } catch (Catalog.TableAlreadyExistException e) {
            throw new TableAlreadyExistsException(ident);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new NoSuchNamespaceException(ident.namespace());
        } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean dropTable(Identifier ident) {
        try {
            catalog.dropTable(toIdentifier(ident), false);
            return true;
        } catch (Catalog.TableNotExistException | NoSuchTableException e) {
            return false;
        }
    }

    private SchemaChange toSchemaChange(TableChange change) {
        if (change instanceof TableChange.SetProperty) {
            TableChange.SetProperty set = (TableChange.SetProperty) change;
            validateAlterProperty(set.property());
            if (set.property().equals(TableCatalog.PROP_COMMENT)) {
                return SchemaChange.updateComment(set.value());
            } else {
                return SchemaChange.setOption(set.property(), set.value());
            }
        } else if (change instanceof TableChange.RemoveProperty) {
            TableChange.RemoveProperty remove = (TableChange.RemoveProperty) change;
            validateAlterProperty(remove.property());
            if (remove.property().equals(TableCatalog.PROP_COMMENT)) {
                return SchemaChange.updateComment(null);
            } else {
                return SchemaChange.removeOption(remove.property());
            }
        } else if (change instanceof TableChange.AddColumn) {
            TableChange.AddColumn add = (TableChange.AddColumn) change;
            validateAlterNestedField(add.fieldNames());
            SchemaChange.Move move = getMove(add.position(), add.fieldNames());
            return SchemaChange.addColumn(
                    add.fieldNames()[0],
                    toPaimonType(add.dataType()).copy(add.isNullable()),
                    add.comment(),
                    move);
        } else if (change instanceof TableChange.RenameColumn) {
            TableChange.RenameColumn rename = (TableChange.RenameColumn) change;
            validateAlterNestedField(rename.fieldNames());
            return SchemaChange.renameColumn(rename.fieldNames()[0], rename.newName());
        } else if (change instanceof TableChange.DeleteColumn) {
            TableChange.DeleteColumn delete = (TableChange.DeleteColumn) change;
            validateAlterNestedField(delete.fieldNames());
            return SchemaChange.dropColumn(delete.fieldNames()[0]);
        } else if (change instanceof TableChange.UpdateColumnType) {
            TableChange.UpdateColumnType update = (TableChange.UpdateColumnType) change;
            validateAlterNestedField(update.fieldNames());
            return SchemaChange.updateColumnType(
                    update.fieldNames()[0], toPaimonType(update.newDataType()));
        } else if (change instanceof TableChange.UpdateColumnNullability) {
            TableChange.UpdateColumnNullability update =
                    (TableChange.UpdateColumnNullability) change;
            return SchemaChange.updateColumnNullability(update.fieldNames(), update.nullable());
        } else if (change instanceof TableChange.UpdateColumnComment) {
            TableChange.UpdateColumnComment update = (TableChange.UpdateColumnComment) change;
            return SchemaChange.updateColumnComment(update.fieldNames(), update.newComment());
        } else if (change instanceof TableChange.UpdateColumnPosition) {
            TableChange.UpdateColumnPosition update = (TableChange.UpdateColumnPosition) change;
            SchemaChange.Move move = getMove(update.position(), update.fieldNames());
            return SchemaChange.updateColumnPosition(move);
        } else {
            throw new UnsupportedOperationException(
                    "Change is not supported: " + change.getClass());
        }
    }

    private static SchemaChange.Move getMove(
            TableChange.ColumnPosition columnPosition, String[] fieldNames) {
        SchemaChange.Move move = null;
        if (columnPosition instanceof TableChange.First) {
            move = SchemaChange.Move.first(fieldNames[0]);
        } else if (columnPosition instanceof TableChange.After) {
            move =
                    SchemaChange.Move.after(
                            fieldNames[0], ((TableChange.After) columnPosition).column());
        }
        return move;
    }

    private Schema toInitialSchema(
            StructType schema, Transform[] partitions, Map<String, String> properties) {
        checkArgument(
                Arrays.stream(partitions)
                        .allMatch(
                                partition -> {
                                    NamedReference[] references = partition.references();
                                    return references.length == 1
                                            && references[0] instanceof FieldReference;
                                }));
        Map<String, String> normalizedProperties = mergeSQLConf(properties);
        normalizedProperties.remove(PRIMARY_KEY_IDENTIFIER);
        normalizedProperties.remove(TableCatalog.PROP_COMMENT);
        String pkAsString = properties.get(PRIMARY_KEY_IDENTIFIER);
        List<String> primaryKeys =
                pkAsString == null
                        ? Collections.emptyList()
                        : Arrays.stream(pkAsString.split(","))
                                .map(String::trim)
                                .collect(Collectors.toList());
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .options(normalizedProperties)
                        .primaryKey(primaryKeys)
                        .partitionKeys(
                                Arrays.stream(partitions)
                                        .map(partition -> partition.references()[0].describe())
                                        .collect(Collectors.toList()))
                        .comment(properties.getOrDefault(TableCatalog.PROP_COMMENT, null));

        for (StructField field : schema.fields()) {
            schemaBuilder.column(
                    field.name(),
                    toPaimonType(field.dataType()).copy(field.nullable()),
                    field.getComment().getOrElse(() -> null));
        }
        return schemaBuilder.build();
    }

    private void validateAlterNestedField(String[] fieldNames) {
        if (fieldNames.length > 1) {
            throw new UnsupportedOperationException(
                    "Alter nested column is not supported: " + Arrays.toString(fieldNames));
        }
    }

    private void validateAlterProperty(String alterKey) {
        if (PRIMARY_KEY_IDENTIFIER.equals(alterKey)) {
            throw new UnsupportedOperationException("Alter primary key is not supported");
        }
    }

    private boolean isValidateNamespace(String[] namespace) {
        return namespace.length == 1;
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException {
        try {
            catalog.renameTable(toIdentifier(oldIdent), toIdentifier(newIdent), false);
        } catch (Catalog.TableNotExistException e) {
            throw new NoSuchTableException(oldIdent);
        } catch (Catalog.TableAlreadyExistException e) {
            throw new TableAlreadyExistsException(newIdent);
        }
    }

    // --------------------- tools ------------------------------------------

    protected org.apache.paimon.catalog.Identifier toIdentifier(Identifier ident)
            throws NoSuchTableException {
        if (!isValidateNamespace(ident.namespace())) {
            throw new NoSuchTableException(ident);
        }

        return new org.apache.paimon.catalog.Identifier(ident.namespace()[0], ident.name());
    }

    protected SparkTable loadSparkTable(Identifier ident, Map<String, String> extraOptions)
            throws NoSuchTableException {
        try {
            return new SparkTable(
                    copyWithSQLConf(catalog.getTable(toIdentifier(ident)), extraOptions));
        } catch (Catalog.TableNotExistException e) {
            throw new NoSuchTableException(ident);
        }
    }

    // --------------------- unsupported methods ----------------------------

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes) {
        throw new UnsupportedOperationException("Alter namespace in Spark is not supported yet.");
    }
}
