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
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.AppendFileStoreWrite;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.NoNeedBucketRowKeyExtractor;
import org.apache.paimon.table.sink.RowKindGenerator;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.sink.WriteSelector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.createCommitUser;

/** A builder to build {@link FormatBatchWriteBuilder}. */
public class FormatBatchWriteBuilder implements BatchWriteBuilder {

    private static final long serialVersionUID = 1L;

    private final FormatTable table;
    private final String commitUser;
    protected final FileIO fileIO;
    protected final SchemaManager schemaManager;
    protected final TableSchema schema;
    protected final String tableName;
    protected final CoreOptions options;
    protected final RowType partitionType;
    protected final CatalogEnvironment catalogEnvironment;

    public FormatBatchWriteBuilder(FormatTable table) {
        this.table = table;
        this.commitUser = createCommitUser(new Options(table.options()));
        this.fileIO = table.fileIO();
        this.schemaManager = new SchemaManager(fileIO, new Path(table.location()));
        this.schema = table.schema();
        this.tableName = table.name();
        this.options = new CoreOptions(table.options());
        this.partitionType = table.partitionType();
        this.catalogEnvironment = null;
    }

    @Override
    public BatchWriteBuilder withOverwrite(@Nullable Map<String, String> staticPartition) {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public BatchTableWrite newWrite() {
        TableSchema tableSchema = table.schema();
        RawFileSplitRead readForCompact = newRead();
        BaseAppendFileStoreWrite writer =
                new AppendFileStoreWrite(
                        fileIO,
                        readForCompact,
                        tableSchema.id(),
                        tableSchema.logicalRowType().notNull(),
                        tableSchema.logicalPartitionType(),
                        pathFactory(),
                        null,
                        null,
                        options,
                        null,
                        tableName);
        return new TableWriteImpl<>(
                rowType(),
                writer,
                new NoNeedBucketRowKeyExtractor(tableSchema),
                (record, rowKind) -> {
                    Preconditions.checkState(
                            rowKind.isAdd(),
                            "Append only writer can not accept row with RowKind %s",
                            rowKind);
                    return record.row();
                },
                rowKindGenerator(),
                CoreOptions.fromMap(tableSchema.options()).ignoreDelete());
    }

    protected RowKindGenerator rowKindGenerator() {
        return RowKindGenerator.create(schema, options);
    }

    public RawFileSplitRead newRead() {
        return new RawFileSplitRead(
                fileIO,
                schemaManager,
                schema,
                table.schema().logicalRowType().notNull(),
                FileFormatDiscover.of(options),
                pathFactory(options, table.format().name()),
                options.fileIndexReadEnabled(),
                options.rowTrackingEnabled());
    }

    public FileStorePathFactory pathFactory() {
        return pathFactory(options, options.fileFormatString());
    }

    protected FileStorePathFactory pathFactory(CoreOptions options, String format) {
        return new FileStorePathFactory(
                options.path(),
                partitionType,
                options.partitionDefaultName(),
                format,
                options.dataFilePrefix(),
                options.changelogFilePrefix(),
                options.legacyPartitionName(),
                options.fileSuffixIncludeCompression(),
                options.fileCompression(),
                options.dataFilePathDirectory(),
                null);
    }

    @Override
    public BatchTableCommit newCommit() {
        throw new UnsupportedOperationException();
    }
}
