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
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionDefinition;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.spark.catalog.FormatTableCatalog;
import org.apache.paimon.spark.catalog.SparkBaseCatalog;
import org.apache.paimon.spark.catalog.SupportView;
import org.apache.paimon.spark.catalog.functions.PaimonFunctions;
import org.apache.paimon.spark.utils.CatalogUtils;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.FormatTableOptions;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.TypeUtils;

import org.apache.spark.sql.PaimonSparkSession$;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalogCapability;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.PartitionedCSVTable;
import org.apache.spark.sql.execution.PartitionedJsonTable;
import org.apache.spark.sql.execution.PartitionedOrcTable;
import org.apache.spark.sql.execution.PartitionedParquetTable;
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat;
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat;
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.execution.datasources.v2.FileTable;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.TableType.FORMAT_TABLE;
import static org.apache.paimon.spark.SparkCatalogOptions.DEFAULT_DATABASE;
import static org.apache.paimon.spark.SparkTypeUtils.CURRENT_DEFAULT_COLUMN_METADATA_KEY;
import static org.apache.paimon.spark.SparkTypeUtils.toPaimonType;
import static org.apache.paimon.spark.util.OptionUtils.checkRequiredConfigurations;
import static org.apache.paimon.spark.util.OptionUtils.copyWithSQLConf;
import static org.apache.paimon.spark.utils.CatalogUtils.checkNamespace;
import static org.apache.paimon.spark.utils.CatalogUtils.checkNoDefaultValue;
import static org.apache.paimon.spark.utils.CatalogUtils.isUpdateColumnDefaultValue;
import static org.apache.paimon.spark.utils.CatalogUtils.removeCatalogName;
import static org.apache.paimon.spark.utils.CatalogUtils.toIdentifier;
import static org.apache.paimon.spark.utils.CatalogUtils.toUpdateColumnDefaultValue;
import static org.apache.spark.sql.connector.catalog.TableCatalogCapability.SUPPORT_COLUMN_DEFAULT_VALUE;

/** Spark {@link TableCatalog} for paimon. */
public class SparkCatalog extends SparkBaseCatalog
        implements SupportView, FunctionCatalog, SupportsNamespaces, FormatTableCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(SparkCatalog.class);

    public static final String FUNCTION_DEFINITION_NAME = "spark";
    private static final String PRIMARY_KEY_IDENTIFIER = "primary-key";

    protected Catalog catalog = null;

    private String defaultDatabase;

    public Set<TableCatalogCapability> capabilities() {
        return Collections.singleton(SUPPORT_COLUMN_DEFAULT_VALUE);
    }

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        checkRequiredConfigurations();

        this.catalogName = name;
        CatalogContext catalogContext =
                CatalogContext.create(
                        Options.fromMap(options),
                        PaimonSparkSession$.MODULE$.active().sessionState().newHadoopConf());
        this.catalog = CatalogFactory.createCatalog(catalogContext);
        this.defaultDatabase =
                options.getOrDefault(DEFAULT_DATABASE.key(), DEFAULT_DATABASE.defaultValue());
        try {
            catalog.getDatabase(defaultNamespace()[0]);
        } catch (Catalog.DatabaseNotExistException e) {
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
        checkNamespace(namespace);
        try {
            String databaseName = getDatabaseNameFromNamespace(namespace);
            catalog.createDatabase(databaseName, false, metadata);
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
        checkNamespace(namespace);
        try {
            String databaseName = getDatabaseNameFromNamespace(namespace);
            catalog.getDatabase(databaseName);
            return new String[0][];
        } catch (Catalog.DatabaseNotExistException e) {
            throw new NoSuchNamespaceException(namespace);
        }
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
            throws NoSuchNamespaceException {
        checkNamespace(namespace);
        try {
            String databaseName = getDatabaseNameFromNamespace(namespace);
            return catalog.getDatabase(databaseName).options();
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
        checkNamespace(namespace);
        try {
            String databaseName = getDatabaseNameFromNamespace(namespace);
            catalog.dropDatabase(databaseName, false, cascade);
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
        checkNamespace(namespace);
        try {
            String databaseName = getDatabaseNameFromNamespace(namespace);
            return catalog.listTables(databaseName).stream()
                    .map(table -> Identifier.of(namespace, table))
                    .toArray(Identifier[]::new);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new NoSuchNamespaceException(namespace);
        }
    }

    @Override
    public void invalidateTable(Identifier ident) {
        catalog.invalidateTable(toIdentifier(ident));
    }

    @Override
    public org.apache.spark.sql.connector.catalog.Table loadTable(Identifier ident)
            throws NoSuchTableException {
        return loadSparkTable(ident, Collections.emptyMap());
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Spark 3.2-.
     */
    public SparkTable loadTable(Identifier ident, String version) throws NoSuchTableException {
        LOG.info("Time travel to version '{}'.", version);
        org.apache.spark.sql.connector.catalog.Table table =
                loadSparkTable(
                        ident, Collections.singletonMap(CoreOptions.SCAN_VERSION.key(), version));
        if (table instanceof SparkTable) {
            return (SparkTable) table;
        } else {
            throw new NoSuchTableException(ident);
        }
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
        org.apache.spark.sql.connector.catalog.Table table =
                loadSparkTable(
                        ident,
                        Collections.singletonMap(
                                CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
                                String.valueOf(timestamp)));
        if (table instanceof SparkTable) {
            return (SparkTable) table;
        } else {
            throw new NoSuchTableException(ident);
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
    public org.apache.spark.sql.connector.catalog.Table createTable(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        try {
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
        } catch (Catalog.TableNotExistException e) {
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
            SchemaChange.Move move = getMove(add.position(), add.fieldNames());
            checkNoDefaultValue(add);
            return SchemaChange.addColumn(
                    add.fieldNames(),
                    toPaimonType(add.dataType()).copy(add.isNullable()),
                    add.comment(),
                    move);
        } else if (change instanceof TableChange.RenameColumn) {
            TableChange.RenameColumn rename = (TableChange.RenameColumn) change;
            return SchemaChange.renameColumn(rename.fieldNames(), rename.newName());
        } else if (change instanceof TableChange.DeleteColumn) {
            TableChange.DeleteColumn delete = (TableChange.DeleteColumn) change;
            return SchemaChange.dropColumn(delete.fieldNames());
        } else if (change instanceof TableChange.UpdateColumnType) {
            TableChange.UpdateColumnType update = (TableChange.UpdateColumnType) change;
            return SchemaChange.updateColumnType(
                    update.fieldNames(), toPaimonType(update.newDataType()), true);
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
        } else if (isUpdateColumnDefaultValue(change)) {
            return toUpdateColumnDefaultValue(change);
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
        Map<String, String> normalizedProperties = new HashMap<>(properties);
        String provider = properties.get(TableCatalog.PROP_PROVIDER);
        if (!usePaimon(provider) && isFormatTable(provider)) {
            normalizedProperties.put(TYPE.key(), FORMAT_TABLE.toString());
            normalizedProperties.put(FILE_FORMAT.key(), provider.toLowerCase());
        }
        normalizedProperties.remove(TableCatalog.PROP_PROVIDER);
        normalizedProperties.remove(PRIMARY_KEY_IDENTIFIER);
        normalizedProperties.remove(TableCatalog.PROP_COMMENT);
        if (normalizedProperties.containsKey(TableCatalog.PROP_LOCATION)) {
            String path = normalizedProperties.remove(TableCatalog.PROP_LOCATION);
            normalizedProperties.put(CoreOptions.PATH.key(), path);
        }
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
                        .partitionKeys(convertPartitionTransforms(partitions))
                        .comment(properties.getOrDefault(TableCatalog.PROP_COMMENT, null));

        for (StructField field : schema.fields()) {
            String name = field.name();
            DataType type = toPaimonType(field.dataType()).copy(field.nullable());
            String comment = field.getComment().getOrElse(() -> null);
            if (field.metadata().contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY)) {
                String defaultValue =
                        field.metadata().getString(CURRENT_DEFAULT_COLUMN_METADATA_KEY);
                schemaBuilder.column(name, type, comment, defaultValue);
            } else {
                schemaBuilder.column(name, type, comment);
            }
        }
        return schemaBuilder.build();
    }

    private void validateAlterProperty(String alterKey) {
        if (PRIMARY_KEY_IDENTIFIER.equals(alterKey)) {
            throw new UnsupportedOperationException("Alter primary key is not supported");
        }
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException {
        try {
            catalog.renameTable(
                    toIdentifier(oldIdent),
                    toIdentifier(removeCatalogName(newIdent, catalogName)),
                    false);
        } catch (Catalog.TableNotExistException e) {
            throw new NoSuchTableException(oldIdent);
        } catch (Catalog.TableAlreadyExistException e) {
            throw new TableAlreadyExistsException(newIdent);
        }
    }

    // --------------------- tools ------------------------------------------

    protected org.apache.spark.sql.connector.catalog.Table loadSparkTable(
            Identifier ident, Map<String, String> extraOptions) throws NoSuchTableException {
        try {
            org.apache.paimon.table.Table paimonTable = catalog.getTable(toIdentifier(ident));
            if (paimonTable instanceof FormatTable) {
                return convertToFileTable(ident, (FormatTable) paimonTable);
            } else {
                return new SparkTable(
                        copyWithSQLConf(
                                paimonTable, catalogName, toIdentifier(ident), extraOptions));
            }
        } catch (Catalog.TableNotExistException e) {
            throw new NoSuchTableException(ident);
        }
    }

    private static FileTable convertToFileTable(Identifier ident, FormatTable formatTable) {
        SparkSession spark = PaimonSparkSession$.MODULE$.active();
        StructType schema = SparkTypeUtils.fromPaimonRowType(formatTable.rowType());
        StructType partitionSchema =
                SparkTypeUtils.fromPaimonRowType(
                        TypeUtils.project(formatTable.rowType(), formatTable.partitionKeys()));
        List<String> pathList = new ArrayList<>();
        pathList.add(formatTable.location());
        Options options = Options.fromMap(formatTable.options());
        CaseInsensitiveStringMap dsOptions = new CaseInsensitiveStringMap(options.toMap());
        if (formatTable.format() == FormatTable.Format.CSV) {
            options.set("sep", options.get(FormatTableOptions.FIELD_DELIMITER));
            dsOptions = new CaseInsensitiveStringMap(options.toMap());
            return new PartitionedCSVTable(
                    ident.name(),
                    spark,
                    dsOptions,
                    scala.collection.JavaConverters.asScalaBuffer(pathList).toSeq(),
                    scala.Option.apply(schema),
                    CSVFileFormat.class,
                    partitionSchema);
        } else if (formatTable.format() == FormatTable.Format.ORC) {
            return new PartitionedOrcTable(
                    ident.name(),
                    spark,
                    dsOptions,
                    scala.collection.JavaConverters.asScalaBuffer(pathList).toSeq(),
                    scala.Option.apply(schema),
                    OrcFileFormat.class,
                    partitionSchema);
        } else if (formatTable.format() == FormatTable.Format.PARQUET) {
            return new PartitionedParquetTable(
                    ident.name(),
                    spark,
                    dsOptions,
                    scala.collection.JavaConverters.asScalaBuffer(pathList).toSeq(),
                    scala.Option.apply(schema),
                    ParquetFileFormat.class,
                    partitionSchema);
        } else if (formatTable.format() == FormatTable.Format.JSON) {
            return new PartitionedJsonTable(
                    ident.name(),
                    spark,
                    dsOptions,
                    scala.collection.JavaConverters.asScalaBuffer(pathList).toSeq(),
                    scala.Option.apply(schema),
                    JsonFileFormat.class,
                    partitionSchema);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported format table "
                            + ident.name()
                            + " format "
                            + formatTable.format().name());
        }
    }

    protected List<String> convertPartitionTransforms(Transform[] transforms) {
        List<String> partitionColNames = new ArrayList<>(transforms.length);
        for (Transform transform : transforms) {
            if (!(transform instanceof IdentityTransform)) {
                throw new UnsupportedOperationException(
                        "Unsupported partition transform: " + transform);
            }
            NamedReference ref = ((IdentityTransform) transform).ref();
            if (!(ref instanceof FieldReference || ref.fieldNames().length != 1)) {
                throw new UnsupportedOperationException(
                        "Unsupported partition transform: " + transform);
            }
            partitionColNames.add(ref.fieldNames()[0]);
        }
        return partitionColNames;
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes)
            throws NoSuchNamespaceException {
        checkNamespace(namespace);
        try {
            String databaseName = getDatabaseNameFromNamespace(namespace);
            List<PropertyChange> propertyChanges =
                    Arrays.stream(changes).map(this::toPropertyChange).collect(Collectors.toList());
            catalog.alterDatabase(databaseName, propertyChanges, false);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new NoSuchNamespaceException(namespace);
        }
    }

    @Override
    public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
        if (isFunctionNamespace(namespace)) {
            List<Identifier> functionIdentifiers = new ArrayList<>();
            PaimonFunctions.names()
                    .forEach(name -> functionIdentifiers.add(Identifier.of(namespace, name)));
            if (namespace.length > 0) {
                String databaseName = getDatabaseNameFromNamespace(namespace);
                try {
                    catalog.listFunctions(databaseName)
                            .forEach(
                                    name ->
                                            functionIdentifiers.add(
                                                    Identifier.of(namespace, name)));
                } catch (Catalog.DatabaseNotExistException e) {
                    throw new NoSuchNamespaceException(namespace);
                }
            }
            return functionIdentifiers.toArray(new Identifier[0]);
        }
        throw new NoSuchNamespaceException(namespace);
    }

    @Override
    public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
        if (isFunctionNamespace(ident.namespace())) {
            UnboundFunction func = PaimonFunctions.load(ident.name());
            if (func != null) {
                return func;
            }
            try {
                Function paimonFunction = catalog.getFunction(toIdentifier(ident));
                FunctionDefinition functionDefinition =
                        paimonFunction.definition(FUNCTION_DEFINITION_NAME);
                if (functionDefinition != null
                        && functionDefinition
                                instanceof FunctionDefinition.LambdaFunctionDefinition) {
                    FunctionDefinition.LambdaFunctionDefinition lambdaFunctionDefinition =
                            (FunctionDefinition.LambdaFunctionDefinition) functionDefinition;
                    if (paimonFunction.returnParams().isPresent()) {
                        List<DataField> dataFields = paimonFunction.returnParams().get();
                        if (dataFields.size() == 1) {
                            DataField dataField = dataFields.get(0);
                            return new LambdaScalarFunction(
                                    ident.name(),
                                    CatalogUtils.paimonType2SparkType(dataField.type()),
                                    CatalogUtils.paimonType2JavaType(dataField.type()),
                                    lambdaFunctionDefinition.definition());
                        } else {
                            throw new UnsupportedOperationException(
                                    "outParams size > 1 is not supported");
                        }
                    }
                }
            } catch (Catalog.FunctionNotExistException e) {
                throw new NoSuchFunctionException(ident);
            }
        }

        throw new NoSuchFunctionException(ident);
    }

    private boolean isFunctionNamespace(String[] namespace) {
        // Allow for empty namespace, as Spark's bucket join will use `bucket` function with empty
        // namespace to generate transforms for partitioning.
        // Otherwise, check if it is paimon namespace.
        return namespace.length == 0 || (namespace.length == 1 && namespaceExists(namespace));
    }

    private PropertyChange toPropertyChange(NamespaceChange change) {
        if (change instanceof NamespaceChange.SetProperty) {
            NamespaceChange.SetProperty set = (NamespaceChange.SetProperty) change;
            return PropertyChange.setProperty(set.property(), set.value());
        } else if (change instanceof NamespaceChange.RemoveProperty) {
            NamespaceChange.RemoveProperty remove = (NamespaceChange.RemoveProperty) change;
            return PropertyChange.removeProperty(remove.property());

        } else {
            throw new UnsupportedOperationException(
                    "Change is not supported: " + change.getClass());
        }
    }

    private String getDatabaseNameFromNamespace(String[] namespace) {
        return namespace[0];
    }
}
