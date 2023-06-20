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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.TableChange.AddColumn;
import org.apache.spark.sql.connector.catalog.TableChange.DeleteColumn;
import org.apache.spark.sql.connector.catalog.TableChange.RemoveProperty;
import org.apache.spark.sql.connector.catalog.TableChange.RenameColumn;
import org.apache.spark.sql.connector.catalog.TableChange.SetProperty;
import org.apache.spark.sql.connector.catalog.TableChange.UpdateColumnComment;
import org.apache.spark.sql.connector.catalog.TableChange.UpdateColumnNullability;
import org.apache.spark.sql.connector.catalog.TableChange.UpdateColumnPosition;
import org.apache.spark.sql.connector.catalog.TableChange.UpdateColumnType;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.spark.SparkTypeUtils.toPaimonType;

/** Base implementation of Spark {@link TableCatalog} for paimon. */
public abstract class SparkCatalogBase implements TableCatalog, SupportsNamespaces {

    private static final String PRIMARY_KEY_IDENTIFIER = "primary-key";

    private String name = null;
    protected Catalog catalog = null;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.name = name;
        CatalogContext catalogContext =
                CatalogContext.create(
                        Options.fromMap(options),
                        SparkSession.active().sessionState().newHadoopConf());
        this.catalog = CatalogFactory.createCatalog(catalogContext);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String[] defaultNamespace() {
        return new String[] {Catalog.DEFAULT_DATABASE};
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws NamespaceAlreadyExistsException {
        Preconditions.checkArgument(
                isValidateNamespace(namespace),
                "Namespace %s is not valid",
                Arrays.toString(namespace));
        try {
            catalog.createDatabase(namespace[0], false);
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
        Preconditions.checkArgument(
                isValidateNamespace(namespace),
                "Namespace %s is not valid",
                Arrays.toString(namespace));
        if (catalog.databaseExists(namespace[0])) {
            return Collections.emptyMap();
        }
        throw new NoSuchNamespaceException(namespace);
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
        Preconditions.checkArgument(
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
        Preconditions.checkArgument(
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
        try {
            return new SparkTable(load(ident));
        } catch (Catalog.TableNotExistException e) {
            throw new NoSuchTableException(ident);
        }
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
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        List<SchemaChange> schemaChanges =
                Arrays.stream(changes).map(this::toSchemaChange).collect(Collectors.toList());
        try {
            catalog.alterTable(toIdentifier(ident), schemaChanges, false);
            return loadTable(ident);
        } catch (Catalog.TableNotExistException e) {
            throw new NoSuchTableException(ident);
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
            catalog.createTable(
                    toIdentifier(ident), toUpdateSchema(schema, partitions, properties), false);
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
        if (change instanceof SetProperty) {
            SetProperty set = (SetProperty) change;
            validateAlterProperty(set.property());
            return SchemaChange.setOption(set.property(), set.value());
        } else if (change instanceof RemoveProperty) {
            RemoveProperty remove = (RemoveProperty) change;
            validateAlterProperty(remove.property());
            return SchemaChange.removeOption(remove.property());
        } else if (change instanceof AddColumn) {
            AddColumn add = (AddColumn) change;
            validateAlterNestedField(add.fieldNames());
            SchemaChange.Move move = getMove(add.position(), add.fieldNames());
            return SchemaChange.addColumn(
                    add.fieldNames()[0],
                    toPaimonType(add.dataType()).copy(add.isNullable()),
                    add.comment(),
                    move);
        } else if (change instanceof RenameColumn) {
            RenameColumn rename = (RenameColumn) change;
            validateAlterNestedField(rename.fieldNames());
            return SchemaChange.renameColumn(rename.fieldNames()[0], rename.newName());
        } else if (change instanceof DeleteColumn) {
            DeleteColumn delete = (DeleteColumn) change;
            validateAlterNestedField(delete.fieldNames());
            return SchemaChange.dropColumn(delete.fieldNames()[0]);
        } else if (change instanceof UpdateColumnType) {
            UpdateColumnType update = (UpdateColumnType) change;
            validateAlterNestedField(update.fieldNames());
            return SchemaChange.updateColumnType(
                    update.fieldNames()[0], toPaimonType(update.newDataType()));
        } else if (change instanceof UpdateColumnNullability) {
            UpdateColumnNullability update = (UpdateColumnNullability) change;
            return SchemaChange.updateColumnNullability(update.fieldNames(), update.nullable());
        } else if (change instanceof UpdateColumnComment) {
            UpdateColumnComment update = (UpdateColumnComment) change;
            return SchemaChange.updateColumnComment(update.fieldNames(), update.newComment());
        } else if (change instanceof UpdateColumnPosition) {
            UpdateColumnPosition update = (UpdateColumnPosition) change;
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

    private Schema toUpdateSchema(
            StructType schema, Transform[] partitions, Map<String, String> properties) {
        Preconditions.checkArgument(
                Arrays.stream(partitions)
                        .allMatch(
                                partition -> {
                                    NamedReference[] references = partition.references();
                                    return references.length == 1
                                            && references[0] instanceof FieldReference;
                                }));
        Map<String, String> normalizedProperties = new HashMap<>(properties);
        normalizedProperties.remove(PRIMARY_KEY_IDENTIFIER);
        String pkAsString = properties.get(PRIMARY_KEY_IDENTIFIER);
        List<String> primaryKeys =
                pkAsString == null
                        ? Collections.emptyList()
                        : Arrays.stream(pkAsString.split(","))
                                .map(String::trim)
                                .collect(Collectors.toList());
        return new Schema(
                ((RowType) toPaimonType(schema)).getFields(),
                Arrays.stream(partitions)
                        .map(partition -> partition.references()[0].describe())
                        .collect(Collectors.toList()),
                primaryKeys,
                normalizedProperties,
                properties.getOrDefault(TableCatalog.PROP_COMMENT, ""));
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

    /** Load a Table Store table. */
    protected org.apache.paimon.table.Table load(Identifier ident)
            throws Catalog.TableNotExistException, NoSuchTableException {
        return catalog.getTable(toIdentifier(ident));
    }

    // --------------------- unsupported methods ----------------------------

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes) {
        throw new UnsupportedOperationException("Alter namespace in Spark is not supported yet.");
    }
}
