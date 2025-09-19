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
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.table.sink.WriteSelector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

/** A builder to build {@link FormatBatchWriteBuilder}. */
public class FormatBatchWriteBuilder implements BatchWriteBuilder {

    private static final long serialVersionUID = 1L;

    private final FormatTable table;
    protected final CoreOptions options;

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
        FormatTableFileWrite writer =
                new FormatTableFileWrite(
                        table.fileIO(), table.rowType().notNull(), pathFactory(), options);
        return new FormatTableWrite(
                rowType(),
                writer,
                new RowPartitionKeyExtractor(table.rowType(), table.partitionKeys()),
                CoreOptions.fromMap(table.options()).ignoreDelete());
    }

    public FileStorePathFactory pathFactory() {
        return pathFactory(options, options.fileFormatString());
    }

    protected FileStorePathFactory pathFactory(CoreOptions options, String format) {

        return new FileStorePathFactory(
                options.path(),
                table.partitionType(),
                options.partitionDefaultName(),
                format,
                options.dataFilePrefix(),
                options.changelogFilePrefix(),
                options.legacyPartitionName(),
                options.fileSuffixIncludeCompression(),
                options.fileCompression(),
                options.dataFilePathDirectory(),
                null,
                false);
    }

    @Override
    public BatchTableCommit newCommit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BatchWriteBuilder withOverwrite(@Nullable Map<String, String> staticPartition) {
        throw new UnsupportedOperationException();
    }
}
