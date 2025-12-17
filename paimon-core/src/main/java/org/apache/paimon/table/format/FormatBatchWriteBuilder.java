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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.WriteSelector;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** A builder to build {@link FormatBatchWriteBuilder}. */
public class FormatBatchWriteBuilder implements BatchWriteBuilder {

    private static final long serialVersionUID = 1L;

    private final FormatTable table;
    protected final CoreOptions options;
    private Map<String, String> staticPartition;
    private boolean overwrite = false;

    public FormatBatchWriteBuilder(FormatTable table) {
        this.table = table;
        this.options = new CoreOptions(table.options());
    }

    @Override
    public String tableName() {
        return table.name();
    }

    @Override
    public RowType rowType() {
        return table.rowType();
    }

    @Override
    public Optional<WriteSelector> newWriteSelector() {
        return table.newBatchWriteBuilder().newWriteSelector();
    }

    @Override
    public BatchTableWrite newWrite() {
        return new FormatTableWrite(
                table.fileIO(),
                rowType(),
                this.options,
                table.partitionType(),
                table.partitionKeys());
    }

    @Override
    public BatchTableCommit newCommit() {
        CoreOptions options = new CoreOptions(table.options());
        boolean formatTablePartitionOnlyValueInPath = options.formatTablePartitionOnlyValueInPath();
        String syncHiveUri = options.formatTableCommitSyncPartitionHiveUri();
        return new FormatTableCommit(
                table.location(),
                table.partitionKeys(),
                table.fileIO(),
                formatTablePartitionOnlyValueInPath,
                overwrite,
                Identifier.fromString(table.fullName()),
                staticPartition,
                syncHiveUri,
                table.catalogContext());
    }

    @Override
    public BatchWriteBuilder withOverwrite(@Nullable Map<String, String> staticPartition) {
        this.overwrite = true;
        validateStaticPartition(staticPartition, table.partitionKeys());
        this.staticPartition = staticPartition;
        return this;
    }

    protected static void validateStaticPartition(
            Map<String, String> staticPartition, List<String> partitionKeys) {
        if (staticPartition != null && !staticPartition.isEmpty()) {
            if (partitionKeys == null || partitionKeys.isEmpty()) {
                throw new IllegalArgumentException(
                        "Format table is not partitioned, static partition values are not allowed.");
            }

            boolean missingLeadingKey = false;
            for (String partitionKey : partitionKeys) {
                boolean contains = staticPartition.containsKey(partitionKey);
                if (missingLeadingKey && contains) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Static partition column '%s' cannot be specified without its leading partition.",
                                    partitionKey));
                }
                if (!contains) {
                    missingLeadingKey = true;
                }
            }

            for (String key : staticPartition.keySet()) {
                if (!partitionKeys.contains(key)) {
                    throw new IllegalArgumentException(
                            String.format("Unknown static partition column '%s'.", key));
                }
            }
        }
    }
}
