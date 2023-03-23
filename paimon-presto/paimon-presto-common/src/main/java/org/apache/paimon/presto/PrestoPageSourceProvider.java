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

package org.apache.paimon.presto;

import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowType;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.presto.ClassLoaderUtils.runWithContextClassLoader;

/** Presto {@link ConnectorPageSourceProvider}. */
public class PrestoPageSourceProvider implements ConnectorPageSourceProvider {

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableLayoutHandle layout,
            List<ColumnHandle> columns,
            SplitContext splitContext) {
        return runWithContextClassLoader(
                () ->
                        createPageSource(
                                ((PrestoTableLayoutHandle) layout).getTableHandle(),
                                (PrestoSplit) split,
                                columns),
                PrestoPageSourceProvider.class.getClassLoader());
    }

    private ConnectorPageSource createPageSource(
            PrestoTableHandle tableHandle, PrestoSplit split, List<ColumnHandle> columns) {
        Table table = tableHandle.table();
        ReadBuilder read = table.newReadBuilder();
        RowType rowType = table.rowType();
        List<String> fieldNames = FieldNameUtils.fieldNames(rowType);
        List<String> projectedFields =
                columns.stream()
                        .map(PrestoColumnHandle.class::cast)
                        .map(PrestoColumnHandle::getColumnName)
                        .collect(Collectors.toList());
        if (!fieldNames.equals(projectedFields)) {
            int[] projected = projectedFields.stream().mapToInt(fieldNames::indexOf).toArray();
            read.withProjection(projected);
        }

        new PrestoFilterConverter(rowType)
                .convert(tableHandle.getFilter())
                .ifPresent(read::withFilter);

        try {
            return new PrestoPageSource(read.newRead().createReader(split.decodeSplit()), columns);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
