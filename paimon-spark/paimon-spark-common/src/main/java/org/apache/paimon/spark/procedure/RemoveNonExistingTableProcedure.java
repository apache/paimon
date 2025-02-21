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

import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.utils.Preconditions;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Procedure to remove table in metastore but does not exist in file system. */
public class RemoveNonExistingTableProcedure extends BaseProcedure {

    private static final Logger LOG =
            LoggerFactory.getLogger(RemoveNonExistingTableProcedure.class);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {ProcedureParameter.required("table", DataTypes.StringType)};

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    private RemoveNonExistingTableProcedure(TableCatalog tableCatalog) {
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
        String tableName = args.getString(0);
        Preconditions.checkArgument(
                tableName != null && !tableName.isEmpty(),
                "Cannot drop table in metastore with empty tableName for argument %s",
                tableName);
        org.apache.paimon.catalog.Identifier identifier =
                org.apache.paimon.catalog.Identifier.fromString(
                        toIdentifier(args.getString(0), PARAMETERS[0].name()).toString());
        LOG.info("identifier in RemoveNonExistingTableProcedure is {}.", identifier);

        Catalog paimonCatalog = ((WithPaimonCatalog) tableCatalog()).paimonCatalog();
        Catalog wrappedCatalog;
        if (paimonCatalog instanceof CachingCatalog) {
            wrappedCatalog = ((CachingCatalog) paimonCatalog).wrapped();
        } else {
            wrappedCatalog = paimonCatalog;
        }
        if (!(wrappedCatalog instanceof HiveCatalog)) {
            throw new IllegalArgumentException(
                    "Only support Hive Catalog in RemoveNonExistingTableProcedure");
        }

        try {
            wrappedCatalog.getTable(identifier);
            LOG.info(
                    "table {}.{} exists in file system too, so will not drop it in metastore",
                    identifier.getDatabaseName(),
                    identifier.getTableName());
            return new InternalRow[] {newInternalRow(false)};
        } catch (Catalog.TableNotExistException e) {
            // drop table in metastore
            try {
                ((HiveCatalog) wrappedCatalog)
                        .getHmsClient()
                        .dropTable(
                                identifier.getDatabaseName(),
                                identifier.getTableName(),
                                false,
                                false);

                if (paimonCatalog instanceof CachingCatalog) {
                    LOG.info(
                            "dropped table {}.{} in metastore, so invalidateTable in CachingCatalog",
                            identifier.getDatabaseName(),
                            identifier.getTableName());
                    paimonCatalog.invalidateTable(identifier);
                }
            } catch (NoSuchObjectException ex) {
                LOG.error(
                        "table {}.{} does not exist in metastore, so will not drop it",
                        identifier.getDatabaseName(),
                        identifier.getTableName());
                throw new RuntimeException(
                        String.format(
                                "table %s.%s not found in metastore",
                                identifier.getDatabaseName(), identifier.getTableName()),
                        ex);
            } catch (TException ex) {
                throw new RuntimeException(
                        String.format(
                                "drop table %s.%s in metastore failed: ",
                                identifier.getDatabaseName(), identifier.getTableName()),
                        e);
            }
            return new InternalRow[] {newInternalRow(true)};
        } catch (Exception e) {
            LOG.info(
                    "exception happen when try to get table {}.{}, so will not drop it in metastore",
                    identifier.getDatabaseName(),
                    identifier.getTableName(),
                    e);
            return new InternalRow[] {newInternalRow(false)};
        }
    }

    public static ProcedureBuilder builder() {
        return new Builder<RemoveNonExistingTableProcedure>() {
            @Override
            public RemoveNonExistingTableProcedure doBuild() {
                return new RemoveNonExistingTableProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "RemoveNonExistingTableProcedure";
    }
}
