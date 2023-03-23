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
import org.apache.paimon.table.source.Split;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.List;
import java.util.stream.Collectors;

/** Presto {@link ConnectorSplitManager}. */
public class PrestoSplitManager implements ConnectorSplitManager {

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext) {

        PrestoTableHandle tableHandle = ((PrestoTableLayoutHandle) layout).getTableHandle();
        Table table = tableHandle.table();
        ReadBuilder readBuilder = table.newReadBuilder();
        new PrestoFilterConverter(table.rowType())
                .convert(tableHandle.getFilter())
                .ifPresent(readBuilder::withFilter);
        List<Split> splits = readBuilder.newScan().plan().splits();
        return new PrestoSplitSource(
                splits.stream().map(PrestoSplit::fromSplit).collect(Collectors.toList()));
    }
}
