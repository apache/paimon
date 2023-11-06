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

package org.apache.paimon.lineage;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.EncodingUtils;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.paimon.lineage.LineageMetaUtils.SINK_TABLE_LINEAGE;
import static org.apache.paimon.lineage.LineageMetaUtils.SOURCE_TABLE_LINEAGE;
import static org.apache.paimon.lineage.LineageMetaUtils.tableLineagePrimaryKeys;
import static org.apache.paimon.lineage.LineageMetaUtils.tableLineageRowType;
import static org.apache.paimon.options.CatalogOptions.JDBC_AUTO_DDL;
import static org.apache.paimon.options.CatalogOptions.JDBC_PASSWORD;
import static org.apache.paimon.options.CatalogOptions.JDBC_URL;
import static org.apache.paimon.options.CatalogOptions.JDBC_USER;

/** Use jdbc to Paimon meta inforation such as table and data lineage. */
public class JdbcLineageMeta implements LineageMeta {
    private final Connection connection;
    private final Statement statement;

    public JdbcLineageMeta(Options options) throws Exception {
        String url = options.get(JDBC_URL);
        String user = options.getOptional(JDBC_USER).orElse(null);
        String password = options.getOptional(JDBC_PASSWORD).orElse(null);

        this.connection =
                user != null && password != null
                        ? DriverManager.getConnection(url, user, password)
                        : DriverManager.getConnection(url);
        this.statement = connection.createStatement();

        if (options.get(JDBC_AUTO_DDL)) {
            initializeTables();
        }
    }

    private void initializeTables() throws Exception {
        String sourceTableLineageSql =
                buildDDL(SOURCE_TABLE_LINEAGE, tableLineageRowType(), tableLineagePrimaryKeys());
        statement.execute(sourceTableLineageSql);

        String sinkTableLineageSql =
                buildDDL(SINK_TABLE_LINEAGE, tableLineageRowType(), tableLineagePrimaryKeys());
        statement.execute(sinkTableLineageSql);
    }

    private String buildDDL(String tableName, RowType rowType, List<String> primaryKeys) {
        List<String> fieldSqlList = new ArrayList<>();
        for (DataField dataField : rowType.getFields()) {
            fieldSqlList.add(
                    dataField
                            .asSQLString()
                            .replace(
                                    EncodingUtils.escapeIdentifier(dataField.name()),
                                    dataField.name())
                            .replaceAll("TIMESTAMP\\([0-9]+\\)", "TIMESTAMP"));
        }
        if (!primaryKeys.isEmpty()) {
            fieldSqlList.add(
                    String.format(
                            "PRIMARY KEY(%s)", StringUtils.join(primaryKeys.iterator(), ",")));
        }
        return String.format(
                "CREATE TABLE %s ( %s )",
                tableName, StringUtils.join(fieldSqlList.iterator(), ",\n"));
    }

    @Override
    public void saveSourceTableLineage(TableLineageEntity entity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteSourceTableLineage(String job) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<TableLineageEntity> sourceTableLineages(@Nullable Predicate predicate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void saveSinkTableLineage(TableLineageEntity entity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<TableLineageEntity> sinkTableLineages(@Nullable Predicate predicate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteSinkTableLineage(String job) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void saveSourceDataLineage(DataLineageEntity entity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<DataLineageEntity> sourceDataLineages(@Nullable Predicate predicate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void saveSinkDataLineage(DataLineageEntity entity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<DataLineageEntity> sinkDataLineages(@Nullable Predicate predicate) {
        throw new UnsupportedOperationException();
    }

    @VisibleForTesting
    Statement statement() {
        return statement;
    }

    @Override
    public void close() throws Exception {
        this.statement.close();
        this.connection.close();
    }
}
