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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.SparkUtils;
import org.apache.paimon.spark.catalog.SparkBaseCatalog;
import org.apache.paimon.spark.copy.CopyDataFilesOperator;
import org.apache.paimon.spark.copy.CopyFileInfo;
import org.apache.paimon.spark.copy.CopyFilesCommitOperator;
import org.apache.paimon.spark.copy.CopySchemaOperator;
import org.apache.paimon.spark.copy.ListDataFilesOperator;
import org.apache.paimon.spark.copy.ListIndexFilesOperator;
import org.apache.paimon.spark.utils.CatalogUtils;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Copy files procedure for latest snapshot.
 *
 * <pre><code>
 *  CALL sys.copy(source_table => 'clg.db.tbl', target_table => 'clg.db.tbl_copy')
 * </code></pre>
 */
public class CopyFilesProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(CopyFilesProcedure.class);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("source_table", StringType),
                ProcedureParameter.required("target_table", StringType),
                ProcedureParameter.optional("where", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected CopyFilesProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        // 1. get source catalog and identifier
        SparkUtils.CatalogAndIdentifier sourceCatalogAndIdentifier =
                toCatalogAndIdentifier(args.getString(0), PARAMETERS[0].name(), tableCatalog());
        Preconditions.checkState(
                sourceCatalogAndIdentifier.catalog() instanceof SparkBaseCatalog,
                String.format(
                        "%s is not a Paimon catalog", sourceCatalogAndIdentifier.catalog().name()));
        SparkBaseCatalog sourceCatalog = (SparkBaseCatalog) sourceCatalogAndIdentifier.catalog();
        Catalog sourcePaimonCatalog = sourceCatalog.paimonCatalog();
        Identifier sourceTableIdentifier =
                CatalogUtils.toIdentifier(
                        sourceCatalogAndIdentifier.identifier(), sourceCatalog.paimonCatalogName());

        // 2. get target catalog and identifier
        SparkUtils.CatalogAndIdentifier targetCatalogIdentifier =
                toCatalogAndIdentifier(args.getString(1), PARAMETERS[1].name(), tableCatalog());
        Preconditions.checkState(
                targetCatalogIdentifier.catalog() instanceof SparkBaseCatalog,
                String.format(
                        "%s is not a Paimon catalog", targetCatalogIdentifier.catalog().name()));
        SparkBaseCatalog targetCatalog = (SparkBaseCatalog) targetCatalogIdentifier.catalog();
        Catalog targetPaimonCatalog = targetCatalog.paimonCatalog();
        Identifier targetTableIdentifier =
                CatalogUtils.toIdentifier(
                        targetCatalogIdentifier.identifier(), targetCatalog.paimonCatalogName());

        // 3. get partition predicate
        String where = args.isNullAt(2) ? null : args.getString(2);
        PartitionPredicate partitionPredicate = null;
        try {
            partitionPredicate =
                    getPartitionPredicate(
                            where,
                            sourceCatalogAndIdentifier,
                            sourcePaimonCatalog,
                            sourceTableIdentifier);
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException("Failed to get partition predicate", e);
        }

        // 4. do copy
        try {
            doCopy(
                    sourcePaimonCatalog,
                    targetPaimonCatalog,
                    sourceTableIdentifier,
                    targetTableIdentifier,
                    partitionPredicate);
            return new InternalRow[] {newInternalRow(true)};
        } catch (Exception e) {
            throw new RuntimeException("Failed to copy files", e);
        }
    }

    private void doCopy(
            Catalog sourcePaimonCatalog,
            Catalog targetPaimonCatalog,
            Identifier sourceTableIdentifier,
            Identifier targetTableIdentifier,
            @Nullable PartitionPredicate partitionPredicate)
            throws Exception {
        CopySchemaOperator copySchemaOperator =
                new CopySchemaOperator(spark(), sourcePaimonCatalog, targetPaimonCatalog);
        ListDataFilesOperator listDataFilesOperator =
                new ListDataFilesOperator(spark(), sourcePaimonCatalog, targetPaimonCatalog);
        CopyDataFilesOperator copyDataFilesOperator =
                new CopyDataFilesOperator(spark(), sourcePaimonCatalog, targetPaimonCatalog);
        CopyFilesCommitOperator copyFilesCommitOperator =
                new CopyFilesCommitOperator(spark(), sourcePaimonCatalog, targetPaimonCatalog);
        ListIndexFilesOperator listIndexFilesOperator =
                new ListIndexFilesOperator(spark(), sourcePaimonCatalog, targetPaimonCatalog);

        // 1. create target table and get latest snapshot
        Snapshot snapshot =
                copySchemaOperator.execute(sourceTableIdentifier, targetTableIdentifier);

        // 2. list data and index files
        List<CopyFileInfo> dataFilesRdd =
                listDataFilesOperator.execute(
                        sourceTableIdentifier, targetTableIdentifier, snapshot, partitionPredicate);
        List<CopyFileInfo> indexFilesRdd =
                listIndexFilesOperator.execute(
                        sourceTableIdentifier, targetTableIdentifier, snapshot, partitionPredicate);

        // 3. copy data and index files
        JavaRDD<CopyFileInfo> dataCopyFileInfoRdd =
                copyDataFilesOperator.execute(
                        sourceTableIdentifier, targetTableIdentifier, dataFilesRdd);
        JavaRDD<CopyFileInfo> indexCopeFileInfoRdd =
                copyDataFilesOperator.execute(
                        sourceTableIdentifier, targetTableIdentifier, indexFilesRdd);
        // 4. commit table
        copyFilesCommitOperator.execute(
                targetTableIdentifier, dataCopyFileInfoRdd, indexCopeFileInfoRdd);
    }

    private PartitionPredicate getPartitionPredicate(
            String where,
            SparkUtils.CatalogAndIdentifier catalogAndIdentifier,
            Catalog sourceCatalog,
            Identifier sourceIdentifier)
            throws Catalog.TableNotExistException {
        DataSourceV2Relation relation = createRelation(catalogAndIdentifier.identifier());
        Table originalSourceTable = sourceCatalog.getTable(sourceIdentifier);
        Preconditions.checkState(
                originalSourceTable instanceof FileStoreTable,
                String.format(
                        "Only support copy FileStoreTable, but this table %s is %s.",
                        sourceIdentifier, sourceIdentifier.getClass()));
        FileStoreTable sourceTable = (FileStoreTable) originalSourceTable;

        PartitionPredicate partitionPredicate =
                SparkProcedureUtils.convertToPartitionPredicate(
                        where, sourceTable.schema().logicalPartitionType(), spark(), relation);
        return partitionPredicate;
    }

    public static ProcedureBuilder builder() {
        return new Builder<CopyFilesProcedure>() {
            @Override
            public CopyFilesProcedure doBuild() {
                return new CopyFilesProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "CopyFilesProcedure";
    }
}
