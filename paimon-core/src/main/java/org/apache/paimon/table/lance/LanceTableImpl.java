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

package org.apache.paimon.table.lance;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** An implementation for {@link LanceTable}. */
public class LanceTableImpl implements ReadonlyTable, LanceTable {

    private final Identifier identifier;
    private final FileIO fileIO;
    private final RowType rowType;
    private final String location;
    private final Map<String, String> options;
    @Nullable private final String comment;

    public LanceTableImpl(
            Identifier identifier,
            FileIO fileIO,
            RowType rowType,
            String location,
            Map<String, String> options,
            @Nullable String comment) {
        this.identifier = identifier;
        this.fileIO = fileIO;
        this.rowType = rowType;
        this.location = location;
        this.options = options;
        this.comment = comment;
    }

    @Override
    public String name() {
        return identifier.getTableName();
    }

    @Override
    public String fullName() {
        return identifier.getFullName();
    }

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public List<String> partitionKeys() {
        return Collections.emptyList();
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, String> options() {
        return options;
    }

    @Override
    public Optional<String> comment() {
        return Optional.ofNullable(comment);
    }

    @Override
    public Optional<Statistics> statistics() {
        return ReadonlyTable.super.statistics();
    }

    @Override
    public FileIO fileIO() {
        return fileIO;
    }

    @Override
    public String location() {
        return location;
    }

    @Override
    public LanceTable copy(Map<String, String> dynamicOptions) {
        Map<String, String> newOptions = new HashMap<>(options);
        newOptions.putAll(dynamicOptions);
        return new LanceTableImpl(identifier, fileIO, rowType, location, newOptions, comment);
    }

    @Override
    public InnerTableScan newScan() {
        throw new UnsupportedOperationException(
                "LanceTable does not support InnerTableScan. Use newRead() instead.");
    }

    @Override
    public InnerTableRead newRead() {
        throw new UnsupportedOperationException(
                "LanceTable does not support InnerTableRead. Use newScan() instead.");
    }
}
